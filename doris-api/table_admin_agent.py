"""
Table admin agent that specializes SQL generation for one table/subtask.
"""

import os
from typing import Dict, Any, Optional

from db import doris_client
from vanna_doris import VannaDorisOpenAI


class TableAdminAgent:
    def __init__(self, doris_client_override=None):
        self.doris_client = doris_client_override or doris_client

    def generate_sql_for_subtask(
        self,
        subtask: Dict[str, Any],
        question: str,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        api_config = api_config or {}
        vanna = VannaDorisOpenAI(
            doris_client=self.doris_client,
            api_key=api_config.get("api_key") or os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY"),
            model=api_config.get("model") or os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            base_url=api_config.get("base_url") or os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            config={"temperature": 0.1},
        )
        task_question = subtask.get("question") or question
        return vanna.generate_sql(task_question)
