"""
Auto-repair agent for failed SQL execution.
"""
import json
from typing import Any, Dict, List, Optional

import requests

from db import DorisClient


class RepairAgent:
    """Use an LLM to repair failed SQL with schema context."""

    def __init__(
        self,
        doris_client: DorisClient,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.doris_client = doris_client
        self.api_key = api_key
        self.model = model or "deepseek-chat"
        self.base_url = base_url or "https://api.deepseek.com"

    def repair_sql(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        ddl_list: Optional[List[str]] = None,
        api_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        api_key = (api_config or {}).get("api_key") or self.api_key
        model = (api_config or {}).get("model") or self.model
        base_url = (api_config or {}).get("base_url") or self.base_url
        if not api_key:
            raise ValueError("RepairAgent requires an API key")

        prompt = self._build_prompt(question, failed_sql, error_message, ddl_list or [])
        response = requests.post(
            f"{base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You repair Apache Doris SQL. Return only the corrected SQL.",
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.0,
                "max_tokens": 2000,
            },
            timeout=60,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return self._clean_sql(content)

    def _build_prompt(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        ddl_list: List[str],
    ) -> str:
        ddl_block = "\n\n".join(ddl_list[:10])
        return (
            "修复下面这条 Apache Doris SQL，并只返回最终 SQL。\n\n"
            f"用户问题:\n{question}\n\n"
            f"失败 SQL:\n{failed_sql}\n\n"
            f"错误信息:\n{error_message}\n\n"
            f"相关 DDL:\n{ddl_block}\n"
        )

    def _clean_sql(self, sql: str) -> str:
        cleaned = (sql or "").strip()
        if cleaned.startswith("```sql"):
            cleaned = cleaned[6:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()
