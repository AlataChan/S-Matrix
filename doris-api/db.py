"""
Doris 数据库连接和查询工具
"""
import pymysql
import asyncio
import re
from typing import List, Dict, Any, Union
from config import DORIS_CONFIG


class DorisClient:
    """Doris 数据库客户端"""
    
    def __init__(self):
        self.config = DORIS_CONFIG
    
    def get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.config)
    
    def validate_identifier(self, identifier: str) -> str:
        """
        验证并转义 SQL 标识符 (表名、列名)
        防止 SQL 注入
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        
        # 只允许字母、数字、下划线、中划线
        if not re.match(r'^[a-zA-Z0-9_\-]+$', identifier):
            # 如果包含其他字符，尝试用反引号包裹并转义反引号
            # 但为了安全起见，我们暂时只允许常规字符
            # 如果是中文表名，需要放宽正则
             if not re.match(r'^[\w\-\u4e00-\u9fa5]+$', identifier):
                raise ValueError(f"Invalid identifier: {identifier}")
        
        return f"`{identifier}`"

    def execute_query(self, sql: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回结果
        
        Args:
            sql: SQL 语句
            params: 参数 (可选)
        
        Returns:
            查询结果列表
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            result = cursor.fetchall()
            return result
        finally:
            conn.close()

    async def execute_query_async(self, sql: str, params: tuple = None) -> List[Dict[str, Any]]:
        """异步执行查询"""
        return await asyncio.to_thread(self.execute_query, sql, params)
    
    def execute_update(self, sql: str, params: tuple = None) -> int:
        """
        执行更新操作 (INSERT/UPDATE/DELETE/CREATE)
        
        Args:
            sql: SQL 语句
            params: 参数 (可选)
        
        Returns:
            影响的行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            affected_rows = cursor.execute(sql, params)
            conn.commit()
            return affected_rows
        finally:
            conn.close()

    async def execute_update_async(self, sql: str, params: tuple = None) -> int:
        """异步执行更新"""
        return await asyncio.to_thread(self.execute_update, sql, params)
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        sql = "SHOW TABLES"
        result = self.execute_query(sql)
        # 结果格式: [{'Tables_in_default': 'table1'}, ...]
        key = f"Tables_in_{self.config['database']}"
        return [row[key] for row in result]
    
    async def get_tables_async(self) -> List[str]:
        """异步获取所有表名"""
        return await asyncio.to_thread(self.get_tables)

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """获取表结构"""
        safe_table_name = self.validate_identifier(table_name)
        sql = f"DESCRIBE {safe_table_name}"
        return self.execute_query(sql)
    
    async def get_table_schema_async(self, table_name: str) -> List[Dict[str, str]]:
        """异步获取表结构"""
        return await asyncio.to_thread(self.get_table_schema, table_name)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.get_tables()
    
    async def table_exists_async(self, table_name: str) -> bool:
        """异步检查表是否存在"""
        tables = await self.get_tables_async()
        return table_name in tables

    def _escape_string(self, text: str) -> str:
        """
        转义字符串用于 SQL 查询

        Args:
            text: 原始文本

        Returns:
            转义后的文本 (带引号)
        """
        # 使用 pymysql 的转义功能
        conn = self.get_connection()
        try:
            escaped = conn.escape_string(text)
            return f"'{escaped}'"
        finally:
            conn.close()


# 全局单例
doris_client = DorisClient()

