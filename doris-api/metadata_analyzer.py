"""
表格元数据分析器
使用 LLM 分析表格结构和用途
"""
import os
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
from db import doris_client


class MetadataAnalyzer:
    """表格元数据分析器"""
    
    def __init__(self):
        self.db = doris_client
        # DeepSeek API 配置
        self.api_key = os.getenv('DEEPSEEK_API_KEY') or os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('DEEPSEEK_MODEL', 'deepseek-chat')
        self.base_url = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
    
    async def analyze_table_async(self, table_name: str, source_type: str = 'excel') -> Dict[str, Any]:
        """异步分析表格元数据"""
        return await asyncio.to_thread(self.analyze_table, table_name, source_type)

    def analyze_table(self, table_name: str, source_type: str = 'excel') -> Dict[str, Any]:
        """
        分析表格元数据
        
        Args:
            table_name: 表名
            source_type: 来源类型 (excel/database_sync)
        
        Returns:
            分析结果
        """
        if not self.api_key:
            return {
                'success': False,
                'error': 'API key not configured. Set DEEPSEEK_API_KEY environment variable.'
            }
        
        try:
            # 校验表名
            safe_table_name = self.db.validate_identifier(table_name)
            
            # 1. 获取表结构
            schema = self.db.get_table_schema(table_name)
            columns = [col['Field'] for col in schema]
            
            # 2. 获取样本数据 (前10行)
            # 使用 safe_table_name，注意它已经包含了反引号，但我们的 validate_identifier 返回的是 `table`
            # 这里的 sample_sql 是 f"SELECT * FROM `{table_name}` ..."
            # 如果 validate_identifier 返回 "`table`"，那么 SQL 变成 "SELECT * FROM `table`" 是对的
            # 但如果 validate_identifier 返回的是不带反引号的 safe string？
            # 看 db.py: return f"`{identifier}`"
            # 所以这里不需要再加反引号
            sample_sql = f"SELECT * FROM {safe_table_name} LIMIT 10"
            sample_data = self.db.execute_query(sample_sql)
            
            # 3. 构造 prompt
            prompt = self._build_analysis_prompt(table_name, columns, sample_data)
            
            # 4. 调用 LLM
            analysis = self._call_llm(prompt)
            
            # 5. 保存元数据
            self._save_metadata(table_name, analysis, source_type)
            
            return {
                'success': True,
                'table_name': table_name,
                'analysis': analysis
            }
            
        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }
    
    def _build_analysis_prompt(self, table_name: str, columns: list, 
                               sample_data: list) -> str:
        """构造分析 prompt"""
        # 格式化样本数据
        sample_str = ""
        for i, row in enumerate(sample_data[:5], 1):  # 只取前5行避免太长
            row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
            sample_str += f"  行{i}: {row_str}\n"
        
        prompt = f"""请分析以下数据表，提供结构化的元数据信息。

表名: {table_name}
列名: {', '.join(columns)}

样本数据:
{sample_str}

请以 JSON 格式返回以下信息：
{{
    "description": "一句话描述这张表的用途和内容",
    "columns": {{
        "列名1": "该列的含义和数据类型说明",
        "列名2": "该列的含义和数据类型说明"
    }},
    "suggested_queries": [
        "可以基于这张表回答的问题示例1",
        "可以基于这张表回答的问题示例2",
        "可以基于这张表回答的问题示例3"
    ],
    "data_domain": "数据领域（如：销售、用户、财务、环保等）",
    "key_dimensions": ["主要分析维度1", "主要分析维度2"]
}}

只返回 JSON，不要其他解释。"""
        
        return prompt
    
    def _call_llm(self, prompt: str) -> Dict[str, Any]:
        """调用 LLM API"""
        from openai import OpenAI
        
        client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个数据分析专家，擅长分析数据表结构和用途。请用中文回答。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content
        
        # 尝试解析 JSON
        try:
            # 清理可能的 markdown 代码块
            if '```json' in content:
                content = content.split('```json')[1].split('```')[0]
            elif '```' in content:
                content = content.split('```')[1].split('```')[0]
            
            return json.loads(content.strip())
        except json.JSONDecodeError:
            return {
                "description": content,
                "columns": {},
                "suggested_queries": [],
                "raw_response": content
            }
    
    def _save_metadata(self, table_name: str, analysis: Dict[str, Any], 
                       source_type: str):
        """保存元数据到系统表"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 先删除旧记录
        delete_sql = "DELETE FROM `_sys_table_metadata` WHERE table_name = %s"
        self.db.execute_update(delete_sql, (table_name,))
        
        # 插入新记录
        sql = """
        INSERT INTO `_sys_table_metadata` 
        (`table_name`, `description`, `columns_info`, `sample_queries`, `analyzed_at`, `source_type`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        self.db.execute_update(sql, (
            table_name,
            analysis.get('description', ''),
            json.dumps(analysis.get('columns', {}), ensure_ascii=False),
            json.dumps(analysis.get('suggested_queries', []), ensure_ascii=False),
            now,
            source_type
        ))

    def get_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表格元数据"""
        sql = "SELECT * FROM `_sys_table_metadata` WHERE table_name = %s"
        results = self.db.execute_query(sql, (table_name,))

        if results:
            meta = results[0]
            # 解析 JSON 字段
            try:
                meta['columns_info'] = json.loads(meta.get('columns_info', '{}'))
            except:
                meta['columns_info'] = {}
            try:
                meta['sample_queries'] = json.loads(meta.get('sample_queries', '[]'))
            except:
                meta['sample_queries'] = []
            return meta
        return None

    def list_all_metadata(self) -> list:
        """获取所有表格元数据"""
        sql = "SELECT * FROM `_sys_table_metadata` ORDER BY analyzed_at DESC"
        results = self.db.execute_query(sql)

        for meta in results:
            try:
                meta['columns_info'] = json.loads(meta.get('columns_info', '{}'))
            except:
                meta['columns_info'] = {}
            try:
                meta['sample_queries'] = json.loads(meta.get('sample_queries', '[]'))
            except:
                meta['sample_queries'] = []

        return results

    def _list_table_registry(self) -> list:
        """List table registry entries for display name/description."""
        try:
            sql = "SELECT table_name, display_name, description FROM `_sys_table_registry`"
            return self.db.execute_query(sql)
        except Exception:
            return []

    def get_all_tables_context(self) -> str:
        """
        ??????????????????????????????????????????????????? AI ??????
        """
        registry_list = self._list_table_registry()
        if not registry_list:
            return ""

        metadata_map = {meta.get('table_name'): meta for meta in self.list_all_metadata()}
        context_parts = ["?????????????????????????????????????????????\n"]

        for reg in registry_list:
            table_name = reg.get('table_name')
            if not table_name:
                continue
            meta = metadata_map.get(table_name, {})
            display_name = reg.get('display_name') or table_name
            description = reg.get('description') or meta.get('description') or '?????????'

            context_parts.append(f"- ??????: {table_name}")
            if display_name != table_name:
                context_parts.append(f"  ?????????: {display_name}")
            context_parts.append(f"  ??????: {description}")
            if meta.get('columns_info'):
                cols = ", ".join(meta['columns_info'].keys())
                context_parts.append(f"  ????????????: {cols}")
            context_parts.append("")

        return "\n".join(context_parts)


# 全局实例
metadata_analyzer = MetadataAnalyzer()

