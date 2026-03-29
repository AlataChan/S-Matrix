"""
Planner agent for multi-table natural language queries.
"""

import json
from typing import Dict, Any, List, Optional


class PlannerAgent:
    def __init__(self, tables_context: Optional[List[Dict[str, Any]]] = None):
        self.tables_context = tables_context or []

    def plan(self, question: str) -> Dict[str, Any]:
        normalized_question = self._normalize_text(question.lower())
        question_tokens = self._tokens(normalized_question)
        scored_tables = []

        for index, table in enumerate(self.tables_context):
            table_name = table.get("table_name", "")
            if not table_name:
                continue

            haystack = self._build_haystack(table)
            score = 0
            if table_name.lower() in normalized_question:
                score += 4

            for token in question_tokens:
                if token and token in haystack:
                    score += 1

            if score > 0:
                scored_tables.append((score, index, table_name))

        scored_tables.sort(key=lambda item: (-item[0], item[1]))
        tables = [table_name for _, _, table_name in scored_tables[:3]]
        if not tables and self.tables_context:
            fallback_table = self.tables_context[0].get("table_name")
            tables = [fallback_table] if fallback_table else []

        intent = self._detect_intent(normalized_question)
        resolved_tables = [table for table in tables if table]
        subtasks = [{"table": table, "question": question} for table in resolved_tables]
        return {
            "intent": intent,
            "tables": resolved_tables,
            "subtasks": subtasks,
            "needs_join": len(resolved_tables) > 1,
        }

    def _build_haystack(self, table: Dict[str, Any]) -> str:
        parts = [
            table.get("table_name", ""),
            table.get("display_name", ""),
            table.get("description", ""),
            table.get("auto_description", ""),
            self._stringify(table.get("columns_info")),
            self._stringify(table.get("agent_config")),
        ]
        return self._normalize_text(" ".join(part for part in parts if part).lower())

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except Exception:
                return value
            return self._stringify(decoded)
        if isinstance(value, dict):
            return " ".join(
                filter(
                    None,
                    [self._stringify(key) + " " + self._stringify(item) for key, item in value.items()],
                )
            )
        if isinstance(value, list):
            return " ".join(self._stringify(item) for item in value)
        return str(value)

    def _detect_intent(self, question: str) -> str:
        if any(token in question for token in ["多少", "数量", "count", "总数"]):
            return "count"
        if any(token in question for token in ["占比", "比例", "平均", "sum", "总和"]):
            return "aggregate"
        return "list"

    def _normalize_text(self, text: str) -> str:
        return (
            text.replace("？", "")
            .replace("?", "")
            .replace("，", "")
            .replace(",", "")
            .replace("。", "")
            .replace(".", "")
            .replace("、", "")
            .replace(" ", "")
        )

    def _tokens(self, question: str) -> List[str]:
        if len(question) <= 4:
            return [question] if question else []

        tokens = {question[i : i + 2] for i in range(len(question) - 1)}
        tokens.update(question[i : i + 3] for i in range(len(question) - 2))
        return sorted(token for token in tokens if token)
