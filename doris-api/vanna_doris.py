"""
Vanna.AI integration for Apache Doris
"""
import os
import json
from typing import List, Dict, Any, Optional
from vanna.base import VannaBase
from db import DorisClient
from config import DORIS_CONFIG


class VannaDoris(VannaBase):
    """
    Vanna.AI adapter for Apache Doris database
    """
    
    def __init__(self, doris_client: DorisClient, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Vanna with Doris client
        
        Args:
            doris_client: DorisClient instance
            config: Optional configuration dict
        """
        VannaBase.__init__(self, config=config)
        self.doris_client = doris_client
        
    def connect_to_doris(self, host: str = None, port: int = None, 
                         user: str = None, password: str = None, 
                         database: str = None) -> None:
        """
        Connect to Doris database (already connected via doris_client)
        """
        # Already connected via doris_client, this is just for compatibility
        pass
    
    async def run_sql_async(self, sql: str) -> Any:
        """
        Asynchronously execute SQL query on Doris
        """
        try:
            # Remove any trailing semicolons
            sql = sql.strip().rstrip(';')
            
            # Execute query using DorisClient
            results = await self.doris_client.execute_query_async(sql)
            
            return results
        except Exception as e:
            raise Exception(f"Error executing SQL: {str(e)}")

    def run_sql(self, sql: str) -> Any:
        """
        Execute SQL query on Doris
        
        Args:
            sql: SQL query string
            
        Returns:
            Query results as list of dicts
        """
        try:
            # Remove any trailing semicolons
            sql = sql.strip().rstrip(';')
            
            # Execute query using DorisClient
            results = self.doris_client.execute_query(sql)
            
            return results
        except Exception as e:
            raise Exception(f"Error executing SQL: {str(e)}")
    
    def get_table_names(self) -> List[str]:
        """
        Get list of all table names in the database
        
        Returns:
            List of table names
        """
        try:
            tables = self.doris_client.get_tables()
            return tables
        except Exception as e:
            raise Exception(f"Error getting table names: {str(e)}")
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dicts
        """
        try:
            schema = self.doris_client.get_table_schema(table_name)
            return schema
        except Exception as e:
            raise Exception(f"Error getting schema for table {table_name}: {str(e)}")
    
    def get_related_ddl(self, question: str, **kwargs) -> List[str]:
        """
        Get DDL statements for tables related to the question.
        Optimized to use information_schema for batch retrieval.
        """
        try:
            # 1. 尝试从元数据表中获取相关的表（如果有简单的关键词匹配最好，这里暂时全量但限制数量）
            # 为了避免 Prompt 过大，我们限制最多返回 20 张表
            limit = 20
            
            # 使用 information_schema 批量获取 Schema，避免 N+1 查询
            # 获取当前数据库的所有 Base Table
            sql = f"""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{self.doris_client.config['database']}'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
            
            # 这里虽然是同步调用，但在 vanna 流程中是在线程池里跑的（我们在 main.py 做了 to_thread）
            # 但为了保险，还是应该尽量快
            results = self.doris_client.execute_query(sql)
            
            tables_schema = {}
            for row in results:
                table = row['TABLE_NAME']
                if table not in tables_schema:
                    tables_schema[table] = []
                tables_schema[table].append(row)
            
            # 限制表数量，优先取有元数据的表（如果能关联的话），这里简单截断
            # 更好的做法是根据 question 进行关键词过滤
            relevant_tables = list(tables_schema.keys())
            
            # 简单的关键词匹配过滤
            keywords = [w.lower() for w in question.split() if len(w) > 1]
            if keywords:
                scored_tables = []
                for table in relevant_tables:
                    score = 0
                    table_lower = table.lower()
                    for kw in keywords:
                        if kw in table_lower:
                            score += 5 # 表名匹配权重高
                    
                    # 检查列名匹配
                    for col in tables_schema[table]:
                        col_name = col['COLUMN_NAME'].lower()
                        col_comment = (col.get('COLUMN_COMMENT') or '').lower()
                        for kw in keywords:
                            if kw in col_name:
                                score += 1
                            if kw in col_comment:
                                score += 1
                    
                    scored_tables.append((score, table))
                
                # 按分数排序
                scored_tables.sort(key=lambda x: x[0], reverse=True)
                # 取前 limit 个，或者分数 > 0 的
                relevant_tables = [t for s, t in scored_tables[:limit] if s > 0]
                
                # 如果没有匹配的，回退到取前几个
                if not relevant_tables:
                    relevant_tables = list(tables_schema.keys())[:5]
            else:
                relevant_tables = relevant_tables[:limit]

            ddl_statements = []
            for table in relevant_tables:
                columns = []
                for col in tables_schema[table]:
                    col_def = f"`{col['COLUMN_NAME']}` {col['DATA_TYPE']}"
                    if col['IS_NULLABLE'] == 'NO':
                        col_def += " NOT NULL"
                    if col['COLUMN_DEFAULT']:
                        col_def += f" DEFAULT {col['COLUMN_DEFAULT']}"
                    if col.get('COLUMN_COMMENT'):
                        col_def += f" COMMENT '{col['COLUMN_COMMENT']}'"
                    columns.append(col_def)
                
                ddl = f"CREATE TABLE `{table}` (\n  " + ",\n  ".join(columns) + "\n);"
                ddl_statements.append(ddl)
            
            return ddl_statements
        except Exception as e:
            # Fallback to empty list or basic implementation if information_schema fails
            print(f"Error optimizing DDL retrieval: {e}")
            return []
    
    def get_related_documentation(self, question: str, **kwargs) -> List[str]:
        """
        Get documentation related to the question

        Args:
            question: Natural language question

        Returns:
            List of documentation strings
        """
        try:
            sql = """
            SELECT r.table_name, r.display_name, r.description,
                   m.description AS auto_description, m.columns_info
            FROM `_sys_table_registry` r
            LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
            ORDER BY r.updated_at DESC
            LIMIT 50
            """
            rows = self.doris_client.execute_query(sql)
        except Exception:
            return []

        docs: List[str] = []
        for row in rows:
            table_name = row.get('table_name')
            if not table_name:
                continue
            display_name = row.get('display_name') or table_name
            description = row.get('description') or row.get('auto_description') or ''

            columns_info = {}
            try:
                columns_info = json.loads(row.get('columns_info') or '{}')
            except Exception:
                columns_info = {}

            if not description and display_name == table_name and not columns_info:
                continue

            parts = [f"Table: {table_name}"]
            if display_name != table_name:
                parts.append(f"Display Name: {display_name}")
            if description:
                parts.append(f"Description: {description}")
            if columns_info:
                cols = ", ".join(list(columns_info.keys())[:20])
                if cols:
                    parts.append(f"Key Columns: {cols}")
            docs.append("\n".join(parts))

        return docs

    # Training data methods (required by VannaBase but not used in our implementation)
    def add_ddl(self, ddl: str, **kwargs) -> str:
        """Add DDL to training data (not implemented)"""
        return "DDL storage not implemented"

    def add_documentation(self, documentation: str, **kwargs) -> str:
        """Add documentation to training data (not implemented)"""
        return "Documentation storage not implemented"

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        """Add question-SQL pair to training data (not implemented)"""
        return "Question-SQL storage not implemented"

    def get_training_data(self, **kwargs) -> Any:
        """Get training data (not implemented)"""
        return []

    def remove_training_data(self, id: str, **kwargs) -> bool:
        """Remove training data (not implemented)"""
        return True

    def generate_embedding(self, data: str, **kwargs) -> List[float]:
        """Generate embedding for data (not implemented)"""
        return []

    def get_similar_question_sql(self, question: str, **kwargs) -> List[Dict[str, str]]:
        """Get similar question-SQL pairs (not implemented)"""
        return []
    
    def get_column_sample_values(self, table_name: str, column_name: str, limit: int = 20) -> List[str]:
        """
        Get sample distinct values from a column to help with fuzzy matching

        Args:
            table_name: Name of the table
            column_name: Name of the column
            limit: Maximum number of distinct values to return

        Returns:
            List of distinct values
        """
        try:
            sql = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL LIMIT {limit}"
            results = self.run_sql(sql)
            return [str(row[column_name]) for row in results if row.get(column_name)]
        except:
            return []

    def get_sql_prompt(self, question: str, question_sql_list: List[Dict[str, str]],
                       ddl_list: List[str], doc_list: List[str], **kwargs) -> str:
        """
        Generate the prompt for SQL generation with intelligent data context

        Args:
            question: Natural language question
            question_sql_list: List of example question-SQL pairs
            ddl_list: List of DDL statements
            doc_list: List of documentation strings

        Returns:
            Formatted prompt string with data context
        """
        # Build the prompt
        prompt = "You are an expert SQL query generator for Apache Doris database.\n\n"

        prompt += "=" * 80 + "\n"
        prompt += "IMPORTANT: Your goal is to generate SQL that ACTUALLY RETURNS DATA.\n"
        prompt += "=" * 80 + "\n\n"

        # Add DDL information
        if ddl_list:
            prompt += "# 1. Database Schema\n\n"
            for ddl in ddl_list:
                prompt += f"{ddl}\n\n"

        # Add sample data with actual values from the database
        prompt += "# 2. Sample Data (ACTUAL VALUES from database)\n\n"
        prompt += "**CRITICAL**: Use these actual values to understand the data format!\n\n"

        try:
            tables = self.get_table_names()
            for table in tables[:5]:  # Limit to first 5 tables
                try:
                    # Get sample rows
                    sample_data = self.run_sql(f"SELECT * FROM `{table}` LIMIT 3")
                    if sample_data:
                        prompt += f"Table: `{table}`\n"
                        prompt += f"Sample rows:\n"
                        for i, row in enumerate(sample_data, 1):
                            prompt += f"  Row {i}: {row}\n"

                        # Get distinct values for text columns (likely to contain city/province names)
                        schema = self.get_table_schema(table)
                        for col in schema:
                            col_name = col.get('Field', '')
                            col_type = col.get('Type', '').upper()

                            # Focus on VARCHAR/TEXT columns that might contain location data
                            if 'VARCHAR' in col_type or 'TEXT' in col_type or 'CHAR' in col_type:
                                # Check if column name suggests it's a location field
                                if any(keyword in col_name.lower() for keyword in ['city', 'province', 'region', 'location', '城市', '省', '地区', '区域']):
                                    distinct_values = self.get_column_sample_values(table, col_name, limit=30)
                                    if distinct_values:
                                        prompt += f"\n  Column `{col_name}` contains these ACTUAL values:\n"
                                        prompt += f"  {distinct_values}\n"

                        prompt += "\n"
                except Exception as e:
                    # Skip tables that fail
                    pass
        except:
            pass

        # Add example queries if available
        if question_sql_list:
            prompt += "# 3. Example Queries\n\n"
            for example in question_sql_list:
                prompt += f"Question: {example.get('question', '')}\n"
                prompt += f"SQL: {example.get('sql', '')}\n\n"

        # Add documentation if available
        if doc_list:
            prompt += "# 4. Additional Documentation\n\n"
            for doc in doc_list:
                prompt += f"{doc}\n\n"

        # Add the actual question
        prompt += "# 5. Your Task\n\n"
        prompt += f"Question: {question}\n\n"

        prompt += "# 6. Critical Instructions\n\n"
        prompt += "**READ THE SAMPLE DATA ABOVE CAREFULLY!**\n\n"
        prompt += "Rules:\n"
        prompt += "1. Generate ONLY the SQL query, no explanations or comments\n"
        prompt += "2. Use backticks for Chinese column/table names: `城市`, `省份`\n"
        prompt += "3. The SQL MUST be executable in Apache Doris\n"
        prompt += "4. Do NOT use markdown code blocks (no ```sql)\n"
        prompt += "5. Return ONLY the raw SQL statement\n\n"

        prompt += "**FUZZY MATCHING RULES** (MOST IMPORTANT):\n"
        prompt += "- If the question mentions a location (city/province), CHECK the sample data above\n"
        prompt += "- If sample data shows '广州市' but question asks '广州', use: WHERE column LIKE '%广州%'\n"
        prompt += "- If sample data shows '北京市' but question asks '北京', use: WHERE column LIKE '%北京%'\n"
        prompt += "- ALWAYS use LIKE '%keyword%' for location searches unless you see an EXACT match in sample data\n"
        prompt += "- For numeric comparisons (year, count, etc.), use exact match (=, >, <)\n"
        prompt += "- For text searches (names, locations), prefer LIKE '%keyword%' for better recall\n\n"

        prompt += "**EXAMPLES**:\n"
        prompt += "- Question: '来自广州的机构' + Sample data has '广州市' → WHERE `城市` LIKE '%广州%'\n"
        prompt += "- Question: '2022年的数据' → WHERE `年份` = 2022\n"
        prompt += "- Question: '包含科技的公司' → WHERE `公司名` LIKE '%科技%'\n\n"

        prompt += "Now generate the SQL query:\n\n"
        prompt += "SQL:"

        return prompt
    
    def submit_prompt(self, prompt: str, **kwargs) -> str:
        """
        Submit prompt to LLM and get response
        This method should be overridden by the LLM-specific class
        
        Args:
            prompt: The prompt to submit
            
        Returns:
            LLM response
        """
        raise NotImplementedError("This method should be implemented by LLM-specific class")
    
    def generate_sql(self, question: str, allow_llm_to_see_data: bool = False) -> str:
        """
        Generate SQL from natural language question
        
        Args:
            question: Natural language question
            allow_llm_to_see_data: Whether to allow LLM to see sample data
            
        Returns:
            Generated SQL query
        """
        # Get related DDL
        ddl_list = self.get_related_ddl(question)
        
        # Get related documentation
        doc_list = self.get_related_documentation(question)
        
        # Get example queries (empty for now, can be extended)
        question_sql_list = []
        
        # Generate prompt
        prompt = self.get_sql_prompt(
            question=question,
            question_sql_list=question_sql_list,
            ddl_list=ddl_list,
            doc_list=doc_list
        )
        
        # Submit to LLM
        sql = self.submit_prompt(prompt)
        
        # Clean up the response
        sql = sql.strip()

        # Remove markdown code blocks if present
        if sql.startswith('```sql'):
            sql = sql[6:]
        elif sql.startswith('```'):
            sql = sql[3:]

        if sql.endswith('```'):
            sql = sql[:-3]

        sql = sql.strip()

        # 🔥 POST-PROCESS: Auto-convert exact matches to fuzzy matches for location fields
        sql = self.auto_fuzzy_match_locations(sql)

        return sql

    def auto_fuzzy_match_locations(self, sql: str) -> str:
        """
        Automatically convert exact matches to fuzzy matches for location-related fields.

        Examples:
        - WHERE 所在省 = '福建省' → WHERE 所在省 LIKE '%福建%'
        - WHERE 所在市 = '广州市' → WHERE 所在市 LIKE '%广州%'
        - WHERE `城市` = '北京' → WHERE `城市` LIKE '%北京%'

        Args:
            sql: Original SQL query

        Returns:
            Modified SQL with fuzzy matching for locations
        """
        import re

        # Define location-related column patterns (Chinese)
        location_keywords = [
            '省', '市', '区', '县', '城市', '地区', '省份', '所在省', '所在市',
            '所在地', '地域', '区域', '省市', '城镇', '乡镇'
        ]

        # Pattern to match: column_name = 'value' or `column_name` = 'value'
        # We'll convert these to LIKE '%value_core%'
        for keyword in location_keywords:
            # Pattern 1: With backticks `column` = 'value'
            pattern1 = rf"`([^`]*{keyword}[^`]*)` = '([^']+)'"
            matches1 = re.finditer(pattern1, sql)
            for match in matches1:
                original = match.group(0)
                column = match.group(1)
                value = match.group(2)

                # Extract core location name (remove 省/市 suffix if present)
                value_core = value
                if value.endswith('省'):
                    value_core = value[:-1]
                elif value.endswith('市'):
                    value_core = value[:-1]

                # Replace with LIKE pattern
                new_clause = f"`{column}` LIKE '%{value_core}%'"
                sql = sql.replace(original, new_clause)

            # Pattern 2: Without backticks column = 'value'
            # Only match if column name contains the keyword
            pattern2 = rf"(\b\w*{keyword}\w*) = '([^']+)'"
            matches2 = re.finditer(pattern2, sql)
            for match in matches2:
                original = match.group(0)
                column = match.group(1)
                value = match.group(2)

                # Skip if already processed (has backticks)
                if f"`{column}`" in sql:
                    continue

                # Extract core location name
                value_core = value
                if value.endswith('省'):
                    value_core = value[:-1]
                elif value.endswith('市'):
                    value_core = value[:-1]

                # Replace with LIKE pattern
                new_clause = f"{column} LIKE '%{value_core}%'"
                sql = sql.replace(original, new_clause)

        return sql


class VannaDorisOpenAI(VannaDoris):
    """
    Vanna.AI with OpenAI (or compatible API like DeepSeek) for Doris
    """

    def __init__(self, doris_client: DorisClient,
                 api_key: str = None,
                 model: str = None,
                 base_url: str = None,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize Vanna with OpenAI-compatible API

        Args:
            doris_client: DorisClient instance
            api_key: API key for OpenAI-compatible service
            model: Model name (e.g., 'gpt-4', 'deepseek-chat')
            base_url: Base URL for API (e.g., 'https://api.deepseek.com')
            config: Optional configuration dict
        """
        # Initialize Vanna Doris
        VannaDoris.__init__(self, doris_client=doris_client, config=config)

        # Set API configuration
        self.api_key = api_key
        self.model = model or 'deepseek-chat'
        self.base_url = base_url or 'https://api.deepseek.com'

    def system_message(self, message: str) -> Dict[str, str]:
        """Create a system message"""
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> Dict[str, str]:
        """Create a user message"""
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> Dict[str, str]:
        """Create an assistant message"""
        return {"role": "assistant", "content": message}

    def submit_prompt(self, prompt: str, **kwargs) -> str:
        """
        Submit prompt to OpenAI-compatible API

        Args:
            prompt: The prompt to submit

        Returns:
            LLM response
        """
        import requests

        # Prepare API request
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                self.system_message("You are an expert SQL query generator for Apache Doris database."),
                self.user_message(prompt)
            ],
            "temperature": self.config.get('temperature', 0.1) if self.config else 0.1,
            "max_tokens": self.config.get('max_tokens', 2000) if self.config else 2000
        }

        # Submit to API
        response = requests.post(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result['choices'][0]['message']['content']
