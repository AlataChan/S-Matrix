"""
Excel 上传处理器
"""
import pandas as pd
import requests
import asyncio
from typing import Dict, Any, List
from io import BytesIO
from config import (
    DORIS_STREAM_LOAD,
    DORIS_CONFIG,
    DORIS_MAX_COLUMNS,
    STREAM_LOAD_BATCH_ROWS,
    STREAM_LOAD_MAX_BYTES,
    STREAM_LOAD_TIMEOUT,
)
from db import doris_client


class ExcelUploadHandler:
    """Excel 上传和导入处理器"""
    
    def __init__(self):
        self.db = doris_client
        self.stream_load_config = DORIS_STREAM_LOAD
    
    def preview_excel(self, file_content: bytes, rows: int = 10) -> Dict[str, Any]:
        """
        预览 Excel 文件

        Args:
            file_content: 文件内容
            rows: 预览行数

        Returns:
            预览数据和列信息
        """
        import json
        import numpy as np

        df = pd.read_excel(BytesIO(file_content), nrows=rows)
        if len(df.columns) > DORIS_MAX_COLUMNS:
            raise ValueError(f"列数过多 ({len(df.columns)})，超过 Doris 最大列数 {DORIS_MAX_COLUMNS}")

        # 替换 NaN 和 Infinity 为 None (JSON null)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.replace({np.nan: None})

        # 推断列类型
        column_types = {}
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                column_types[col] = 'INT'
            elif pd.api.types.is_float_dtype(dtype):
                column_types[col] = 'DECIMAL(18,2)'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                column_types[col] = 'DATETIME'
            else:
                column_types[col] = 'VARCHAR(500)'

        # 转换为字典并确保 JSON 兼容
        data = json.loads(df.to_json(orient='records'))

        return {
            'columns': [str(col) for col in df.columns],
            'data': data,
            'row_count': len(df),
            'inferred_types': column_types
        }
    
    async def preview_excel_async(self, file_content: bytes, rows: int = 10) -> Dict[str, Any]:
        """异步预览 Excel 文件"""
        return await asyncio.to_thread(self.preview_excel, file_content, rows)
    
    def create_table(self, table_name: str, columns: Dict[str, str], 
                     key_columns: List[str] = None) -> str:
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义 {列名: 类型}
            key_columns: 主键列 (可选,默认使用第一列)
        
        Returns:
            CREATE TABLE SQL
        """
        if len(columns) > DORIS_MAX_COLUMNS:
            raise ValueError(f"列数过多 ({len(columns)})，超过 Doris 最大列数 {DORIS_MAX_COLUMNS}")
        if not key_columns:
            key_columns = [list(columns.keys())[0]]
        
        # 校验表名
        safe_table_name = self.db.validate_identifier(table_name)

        # 构造列定义
        column_defs = []
        for col_name, col_type in columns.items():
            # 处理列名中的特殊字符，校验列名
            safe_col_name_raw = col_name.replace(' ', '_').replace('-', '_')
            safe_col_name = self.db.validate_identifier(safe_col_name_raw)
            # validate_identifier 返回带反引号的字符串，如 `id`
            # 但这里我们可能需要裸字符串拼接 SQL 还是怎样？
            # validate_identifier 返回 "`id`"
            # 我们的 SQL 模板是 `{column_defs_str}`
            # column_defs.append(f"{safe_col_name} {col_type}") 即可
            column_defs.append(f"{safe_col_name} {col_type}")
        
        column_defs_str = ',\n    '.join(column_defs)
        
        # 处理 key_columns
        safe_keys = []
        for k in key_columns:
             safe_k_raw = k.replace(' ', '_').replace('-', '_')
             safe_keys.append(self.db.validate_identifier(safe_k_raw))
             
        key_columns_str = ', '.join(safe_keys)
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {safe_table_name} (
            {column_defs_str}
        )
        DUPLICATE KEY({key_columns_str})
        DISTRIBUTED BY HASH({key_columns_str}) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        
        self.db.execute_update(sql)
        return sql

    async def create_table_async(self, table_name: str, columns: Dict[str, str], 
                     key_columns: List[str] = None) -> str:
        """异步创建表"""
        return await asyncio.to_thread(self.create_table, table_name, columns, key_columns)
    
    def import_excel(self, file_content: bytes, table_name: str, 
                     column_mapping: Dict[str, str] = None,
                     create_table_if_not_exists: bool = True,
                     column_types: Dict[str, str] = None) -> Dict[str, Any]:
        """
        导入 Excel 到 Doris
        
        Args:
            file_content: 文件内容
            table_name: 目标表名
            column_mapping: 列映射 {Excel列名: Doris列名}
            create_table_if_not_exists: 如果表不存在是否创建
            column_types: 列类型定义 {列名: 类型}
        
        Returns:
            导入结果
        """
        import numpy as np

        # 读取 Excel
        df = pd.read_excel(BytesIO(file_content))
        if len(df.columns) > DORIS_MAX_COLUMNS:
            raise ValueError(f"列数过多 ({len(df.columns)})，超过 Doris 最大列数 {DORIS_MAX_COLUMNS}")

        # 替换 NaN 和 Infinity 为空字符串 (Doris 可以处理)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna('')

        # 应用列映射
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # 清理列名
        df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

        # 清理数据:移除字段中的换行符和制表符,避免 CSV 解析错误
        for col in df.columns:
            if df[col].dtype == 'object':  # 只处理字符串列
                df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
                df[col] = df[col].str.replace('\r', ' ', regex=False)
                df[col] = df[col].str.replace('\t', ' ', regex=False)

        # 检查表是否存在
        table_exists = self.db.table_exists(table_name)

        if not table_exists:
            if not create_table_if_not_exists:
                raise ValueError(f"Table '{table_name}' does not exist")

            # 自动推断列类型
            if not column_types:
                column_types = {}
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        column_types[col] = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        column_types[col] = 'DECIMAL(18,2)'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        column_types[col] = 'DATETIME'
                    else:
                        column_types[col] = 'VARCHAR(500)'

            # 创建表
            create_sql = self.create_table(table_name, column_types)
        else:
            # 表已存在,检查列数是否匹配
            existing_schema = self.db.get_table_schema(table_name)

            if len(existing_schema) != len(df.columns):
                # 列数不匹配,删除旧表并重新创建
                self.db.execute_update(f"DROP TABLE `{table_name}`")

                # 自动推断列类型
                column_types = {}
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        column_types[col] = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        column_types[col] = 'DECIMAL(18,2)'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        column_types[col] = 'DATETIME'
                    else:
                        column_types[col] = 'VARCHAR(500)'

                # 重新创建表
                create_sql = self.create_table(table_name, column_types)
                table_exists = False
        
        # 使用 Stream Load 导入数据
        result = self.stream_load(df, table_name)
        
        return {
            'success': True,
            'table': table_name,
            'rows_imported': len(df),
            'table_created': not table_exists,
            'stream_load_result': result
        }
    
    def _dataframe_to_csv_bytes(self, df: pd.DataFrame) -> bytes:
        from io import BytesIO

        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8', sep='\t', na_rep='')
        return csv_buffer.getvalue()

    def _send_stream_load(self, csv_bytes: bytes, table_name: str) -> Dict[str, Any]:
        url = f"http://{self.stream_load_config['host']}:{self.stream_load_config['port']}/api/{DORIS_CONFIG['database']}/{table_name}/_stream_load"

        headers = {
            'Expect': '100-continue',
            'Content-Type': 'text/plain; charset=utf-8',
            'format': 'csv',
            'strict_mode': 'false',
            'max_filter_ratio': '0.2',
        }

        # ????????????(????????????????????????)
        response = requests.put(
            url,
            data=csv_bytes,
            headers=headers,
            auth=(self.stream_load_config['user'], self.stream_load_config['password']),
            timeout=STREAM_LOAD_TIMEOUT
        )

        if response.status_code != 200:
            raise Exception(f"Stream Load failed: {response.text}")

        result = response.json()

        if result.get('Status') != 'Success':
            raise Exception(f"Stream Load failed: {result}")

        return result

    def _merge_stream_load_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not results:
            return {'Status': 'Success', 'NumberLoadedRows': 0, 'NumberTotalRows': 0}

        def _to_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        summary: Dict[str, Any] = {'Status': 'Success', 'ChunkResults': results}
        for key in ['NumberLoadedRows', 'NumberTotalRows', 'NumberFilteredRows', 'NumberUnselectedRows', 'LoadBytes']:
            total = sum(_to_int(r.get(key, 0)) for r in results)
            if total:
                summary[key] = total
        return summary

    def _sanitize_for_stream_load(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        sanitized = df.copy()

        for col in sanitized.columns:
            series = sanitized[col]
            if not (pd.api.types.is_object_dtype(series.dtype) or pd.api.types.is_string_dtype(series.dtype)):
                continue

            def _normalize(value: Any) -> str:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return ''
                if isinstance(value, bytes):
                    try:
                        text = value.decode('utf-8', errors='replace')
                    except Exception:
                        text = value.decode(errors='replace')
                else:
                    text = value if isinstance(value, str) else str(value)

                if '\t' in text or '\n' in text or '\r' in text:
                    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                return text

            sanitized[col] = series.map(_normalize)

        return sanitized

    def _stream_load_with_max_bytes(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        if df.empty:
            return {'Status': 'Success', 'NumberLoadedRows': 0, 'NumberTotalRows': 0}

        csv_bytes = self._dataframe_to_csv_bytes(df)
        if STREAM_LOAD_MAX_BYTES and len(csv_bytes) > STREAM_LOAD_MAX_BYTES and len(df) > 1:
            mid = len(df) // 2
            left = self._stream_load_with_max_bytes(df.iloc[:mid], table_name)
            right = self._stream_load_with_max_bytes(df.iloc[mid:], table_name)
            return self._merge_stream_load_results([left, right])

        return self._send_stream_load(csv_bytes, table_name)

    def stream_load(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        ?????? Stream Load API ????????????

        Args:
            df: Pandas DataFrame
            table_name: ????????????

        Returns:
            Stream Load ??????
        """
        if df is None or df.empty:
            return {'Status': 'Success', 'NumberLoadedRows': 0, 'NumberTotalRows': 0}

        df = self._sanitize_for_stream_load(df)

        batch_rows = STREAM_LOAD_BATCH_ROWS if STREAM_LOAD_BATCH_ROWS and STREAM_LOAD_BATCH_ROWS > 0 else len(df)
        if len(df) > batch_rows:
            results: List[Dict[str, Any]] = []
            for start in range(0, len(df), batch_rows):
                chunk = df.iloc[start:start + batch_rows]
                results.append(self._stream_load_with_max_bytes(chunk, table_name))
            return self._merge_stream_load_results(results)

        return self._stream_load_with_max_bytes(df, table_name)

    async def stream_load_async(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """异步执行 Stream Load"""
        return await asyncio.to_thread(self.stream_load, df, table_name)
    
    async def import_excel_async(self, file_content: bytes, table_name: str, 
                     column_mapping: Dict[str, str] = None,
                     create_table_if_not_exists: bool = True,
                     column_types: Dict[str, str] = None) -> Dict[str, Any]:
        """异步导入 Excel"""
        return await asyncio.to_thread(
            self.import_excel, file_content, table_name, column_mapping, 
            create_table_if_not_exists, column_types
        )

# 全局实例
excel_handler = ExcelUploadHandler()
