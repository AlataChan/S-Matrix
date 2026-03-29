"""
Coordinator agent for combining per-table SQL into a final executable query.
"""

from typing import Dict, Any, Optional, List


class CoordinatorAgent:
    def coordinate(
        self,
        plan: Dict[str, Any],
        sql_map: Dict[str, str],
        relationships: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        if not plan.get("needs_join") or len(sql_map) <= 1:
            return next(iter(sql_map.values()))

        relationships = relationships or []
        preferred = self._select_relationship(sql_map, relationships)
        if preferred:
            return self._build_join_sql(sql_map, preferred)

        return next(iter(sql_map.values()))

    def _select_relationship(
        self,
        sql_map: Dict[str, str],
        relationships: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        manual_matches = [
            rel
            for rel in relationships
            if rel.get("is_manual")
            and rel.get("table_a") in sql_map
            and rel.get("table_b") in sql_map
        ]
        if manual_matches:
            return manual_matches[0]

        ranked_matches = [
            rel
            for rel in relationships
            if rel.get("table_a") in sql_map and rel.get("table_b") in sql_map
        ]
        ranked_matches.sort(key=lambda rel: float(rel.get("confidence", 0.0)), reverse=True)
        return ranked_matches[0] if ranked_matches else None

    def _build_join_sql(self, sql_map: Dict[str, str], relationship: Dict[str, Any]) -> str:
        left_table = relationship["table_a"]
        right_table = relationship["table_b"]
        left_alias = self._subquery_alias(left_table)
        right_alias = self._subquery_alias(right_table)
        left_sql = self._normalize_sql(sql_map[left_table])
        right_sql = self._normalize_sql(sql_map[right_table])

        return (
            "SELECT * "
            f"FROM (\n{left_sql}\n) AS `{left_alias}` "
            f"JOIN (\n{right_sql}\n) AS `{right_alias}` "
            f"ON `{left_alias}`.`{relationship['column_a']}` = "
            f"`{right_alias}`.`{relationship['column_b']}`"
        )

    def _normalize_sql(self, sql: str) -> str:
        return (sql or "").strip().rstrip(";")

    def _subquery_alias(self, table_name: str) -> str:
        return f"{table_name}_sub".replace("-", "_")
