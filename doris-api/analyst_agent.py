"""
Data analysis agent for on-demand and replay-based reporting.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from repair_agent import RepairAgent


logger = logging.getLogger(__name__)

_NUMERIC_MARKERS = ("int", "decimal", "float", "double", "numeric", "real")
_TEMPORAL_MARKERS = ("date", "time", "year")
_DEPTH_MAX_STEPS = {"quick": 3, "standard": 6, "deep": 10}


class AnalystAgent:
    """
    Data analysis expert agent built on top of the existing Doris stack.
    """

    def __init__(self, doris_client, build_api_config_fn):
        self.db = doris_client
        self.build_api_config = build_api_config_fn

    def init_tables(self) -> bool:
        """Create system tables required by the analyst workflow."""
        sql = """
        CREATE TABLE IF NOT EXISTS `_sys_analysis_reports` (
            `id` VARCHAR(64),
            `table_names` TEXT,
            `trigger_type` VARCHAR(50),
            `depth` VARCHAR(20),
            `schedule_id` VARCHAR(64),
            `history_id` VARCHAR(64),
            `summary` TEXT,
            `report_json` TEXT,
            `insight_count` INT DEFAULT "0",
            `anomaly_count` INT DEFAULT "0",
            `failed_step_count` INT DEFAULT "0",
            `status` VARCHAR(20) DEFAULT "completed",
            `error_message` TEXT,
            `duration_ms` INT DEFAULT "0",
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        self.db.execute_update(sql)
        return True

    def analyze_table(
        self,
        table_name: str,
        depth: str = "standard",
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        started_at = time.time()
        raw_table_name = (table_name or "").strip()
        safe_table_name = self.db.validate_identifier(raw_table_name)
        api_config = self.build_api_config(resource_name=resource_name)
        if not api_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        profile = self._profile_table(raw_table_name, safe_table_name=safe_table_name)
        metadata = self._get_table_metadata(raw_table_name)
        plan_steps = self._plan_analysis(profile, metadata, depth, api_config)
        step_results = self._run_plan_steps(plan_steps, api_config)

        report = self._build_report(
            table_names=self._merge_table_names([raw_table_name], step_results),
            profile=profile,
            insights=self._generate_insights(step_results, profile, api_config),
            depth=depth,
            trigger_type="table_analysis",
            step_results=step_results,
            started_at=started_at,
        )
        self._save_report(report)
        return report

    def replay_from_history(
        self,
        history_id: str,
        resource_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        api_config = self.build_api_config(resource_name=resource_name)
        if not api_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        history_rows = self.db.execute_query(
            """
            SELECT `id`, `question`, `sql`, `table_names`
            FROM `_sys_query_history`
            WHERE `id` = %s
            LIMIT 1
            """,
            (history_id,),
        )
        if not history_rows:
            return {"success": False, "error": f"History record '{history_id}' not found."}

        history_row = history_rows[0]
        table_names = self._decode_table_names(history_row.get("table_names"))
        primary_table = table_names[0] if table_names else None
        started_at = time.time()
        profile = (
            self._profile_table(primary_table)
            if primary_table
            else {"row_count": 0, "sampled": False, "sample_size": 0, "columns": {}}
        )
        data, final_sql, success, error_message = self._execute_with_repair(
            history_row.get("sql") or "",
            history_row.get("question") or "Replay saved query",
            api_config,
        )
        step_results = [
            {
                "title": "Replay saved query",
                "question": history_row.get("question") or "Replay saved query",
                "sql": final_sql,
                "success": success,
                "error_message": error_message,
                "row_count": len(data or []),
                "data": data if success else None,
            }
        ]

        report = self._build_report(
            table_names=self._merge_table_names(table_names, step_results),
            profile=profile,
            insights=self._generate_insights(step_results, profile, api_config),
            depth="quick",
            trigger_type="history_replay",
            step_results=step_results,
            started_at=started_at,
            history_id=history_id,
            note="Replayed against current data.",
        )
        self._save_report(report)
        return report

    def list_reports(
        self,
        table_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        sql = """
        SELECT `id`, `table_names`, `trigger_type`, `depth`, `schedule_id`, `history_id`,
               `summary`, `insight_count`, `anomaly_count`, `failed_step_count`, `status`,
               `error_message`, `duration_ms`, `created_at`
        FROM `_sys_analysis_reports`
        """
        params: List[Any] = []
        if table_name:
            sql += " WHERE FIND_IN_SET(%s, `table_names`)"
            params.append(table_name)
        sql += " ORDER BY `created_at` DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        rows = self.db.execute_query(sql, tuple(params))
        return {
            "success": True,
            "reports": rows,
            "count": len(rows),
            "limit": limit,
            "offset": offset,
        }

    def get_report(self, report_id: str) -> Dict[str, Any]:
        rows = self.db.execute_query(
            "SELECT `report_json` FROM `_sys_analysis_reports` WHERE `id` = %s LIMIT 1",
            (report_id,),
        )
        if not rows:
            return {"success": False, "error": f"Report '{report_id}' not found."}
        payload = rows[0].get("report_json") or "{}"
        return json.loads(payload) if isinstance(payload, str) else payload

    def delete_report(self, report_id: str) -> Dict[str, Any]:
        self.db.execute_update("DELETE FROM `_sys_analysis_reports` WHERE `id` = %s", (report_id,))
        return {"success": True, "deleted": True, "id": report_id}

    def get_latest_report(self, table_name: str) -> Dict[str, Any]:
        rows = self.db.execute_query(
            """
            SELECT `report_json`
            FROM `_sys_analysis_reports`
            WHERE FIND_IN_SET(%s, `table_names`)
            ORDER BY `created_at` DESC
            LIMIT 1
            """,
            (table_name,),
        )
        if not rows:
            return {"success": False, "error": f"No report found for '{table_name}'."}
        payload = rows[0].get("report_json") or "{}"
        return json.loads(payload) if isinstance(payload, str) else payload

    def _profile_table(
        self,
        table_name: str,
        safe_table_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        raw_name = (table_name or "").strip()
        safe_name = safe_table_name or self.db.validate_identifier(raw_name)
        row_count_rows = self.db.execute_query(f"SELECT COUNT(*) AS row_count FROM {safe_name}")
        row_count = int((row_count_rows[0] or {}).get("row_count", 0)) if row_count_rows else 0
        sampled = row_count > 100000
        sample_size = max(int(row_count * 0.1), 1) if sampled and row_count else row_count
        source_sql = f"{safe_name} TABLESAMPLE(10 PERCENT)" if sampled else safe_name
        fallback_source_sql = (
            f"(SELECT * FROM {safe_name} ORDER BY RAND() LIMIT 50000) AS sampled_source"
            if sampled
            else None
        )
        schema = self.db.get_table_schema(raw_name)

        columns: Dict[str, Dict[str, Any]] = {}
        for column in schema:
            field_name = column.get("Field")
            if not field_name:
                continue
            field_type = column.get("Type") or ""
            safe_field = self.db.validate_identifier(field_name)
            lowered_type = field_type.lower()

            if self._is_numeric_type(lowered_type):
                stat_rows, source_sql = self._execute_profile_query(
                    f"""
                    SELECT MIN({safe_field}) AS min_value,
                           MAX({safe_field}) AS max_value,
                           AVG({safe_field}) AS avg_value,
                           STDDEV({safe_field}) AS stddev_value,
                           SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM {source_sql}
                    """,
                    source_sql,
                    fallback_source_sql,
                )
                stat_row = stat_rows[0] if stat_rows else {}
                columns[field_name] = {
                    "type": field_type,
                    "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                    "min": stat_row.get("min_value"),
                    "max": stat_row.get("max_value"),
                    "avg": stat_row.get("avg_value"),
                    "stddev": stat_row.get("stddev_value"),
                }
                continue

            if self._is_temporal_type(lowered_type):
                stat_rows, source_sql = self._execute_profile_query(
                    f"""
                    SELECT MIN({safe_field}) AS min_value,
                           MAX({safe_field}) AS max_value,
                           SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM {source_sql}
                    """,
                    source_sql,
                    fallback_source_sql,
                )
                stat_row = stat_rows[0] if stat_rows else {}
                columns[field_name] = {
                    "type": field_type,
                    "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                    "min": stat_row.get("min_value"),
                    "max": stat_row.get("max_value"),
                }
                continue

            stat_rows, source_sql = self._execute_profile_query(
                f"""
                SELECT COUNT(DISTINCT {safe_field}) AS unique_count,
                       SUM(CASE WHEN {safe_field} IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM {source_sql}
                """,
                source_sql,
                fallback_source_sql,
            )
            stat_row = stat_rows[0] if stat_rows else {}
            top_rows, source_sql = self._execute_profile_query(
                f"""
                SELECT {safe_field} AS value, COUNT(*) AS count
                FROM {source_sql}
                WHERE {safe_field} IS NOT NULL
                GROUP BY {safe_field}
                ORDER BY count DESC
                LIMIT 10
                """,
                source_sql,
                fallback_source_sql,
            )
            columns[field_name] = {
                "type": field_type,
                "null_rate": self._null_rate(stat_row.get("null_count"), sample_size or row_count),
                "unique_count": stat_row.get("unique_count"),
                "top_values": [
                    {"value": row.get("value"), "count": row.get("count")}
                    for row in top_rows
                ],
            }

        return {
            "row_count": row_count,
            "sampled": sampled,
            "sample_size": sample_size,
            "columns": columns,
        }

    def _get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        rows = self.db.execute_query(
            """
            SELECT r.table_name, r.display_name, r.description, m.description AS auto_description, m.columns_info
            FROM `_sys_table_registry` r
            LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
            WHERE r.table_name = %s
            LIMIT 1
            """,
            (table_name,),
        )
        if not rows:
            return {"table_name": table_name}
        row = rows[0]
        description = row.get("description") or row.get("auto_description")
        columns_info = row.get("columns_info")
        try:
            parsed_columns = json.loads(columns_info) if isinstance(columns_info, str) and columns_info else columns_info
        except Exception:
            parsed_columns = columns_info
        return {
            "table_name": table_name,
            "display_name": row.get("display_name"),
            "description": description,
            "columns_info": parsed_columns or {},
        }

    def _plan_analysis(
        self,
        profile: Dict[str, Any],
        metadata: Dict[str, Any],
        depth: str,
        api_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        prompt = (
            "Design a compact Apache Doris analysis plan.\n"
            f"Depth: {depth}\n"
            f"Metadata: {json.dumps(metadata, ensure_ascii=False, default=str)}\n"
            f"Profile: {json.dumps(profile, ensure_ascii=False, default=str)}\n\n"
            "Return JSON with a top-level `steps` array. Each step must include "
            "`title`, `question`, and `sql`. Return only JSON."
        )
        try:
            payload = self._call_json_completion(
                system_prompt="You are a business analyst who plans Apache Doris SQL analysis steps.",
                user_prompt=prompt,
                api_config=api_config,
            )
        except Exception as exc:
            logger.warning("analysis planner failed, using fallback plan: %s", exc)
            payload = {}
        steps = payload.get("steps") if isinstance(payload, dict) else payload
        if isinstance(steps, list) and steps:
            return steps[: self._get_max_steps(depth)]

        return [
            {
                "title": "Table overview",
                "question": "How many rows exist in this table?",
                "sql": f"SELECT COUNT(*) AS total_rows FROM {self.db.validate_identifier(metadata.get('table_name') or '')}",
            }
        ]

    def _execute_with_repair(
        self,
        sql: str,
        question_context: str,
        api_config: Dict[str, Any],
    ) -> Tuple[Optional[List[Dict[str, Any]]], str, bool, Optional[str]]:
        repair_agent = RepairAgent(
            doris_client=self.db,
            api_key=api_config.get("api_key"),
            model=api_config.get("model"),
            base_url=api_config.get("base_url"),
        )

        final_sql = (sql or "").strip().rstrip(";")
        last_error: Optional[Exception] = None
        try:
            result = self.db.execute_query(final_sql)
            return result, final_sql, True, None
        except Exception as exec_error:
            last_error = exec_error

        for _ in range(2):
            repaired_sql = repair_agent.repair_sql(
                question_context,
                final_sql,
                str(last_error),
                [],
                api_config=api_config,
            )
            final_sql = (repaired_sql or "").strip().rstrip(";")
            try:
                result = self.db.execute_query(final_sql)
                return result, final_sql, True, None
            except Exception as retry_error:
                last_error = retry_error

        return None, final_sql, False, str(last_error) if last_error else "unknown execution error"

    def _generate_insights(
        self,
        step_results: Sequence[Dict[str, Any]],
        profile: Dict[str, Any],
        api_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        prompt = (
            "Interpret the analysis results and return structured JSON.\n"
            f"Profile: {json.dumps(profile, ensure_ascii=False, default=str)}\n"
            f"Steps: {json.dumps(list(step_results), ensure_ascii=False, default=str)}\n\n"
            "Return JSON with `summary`, `insights`, `anomalies`, and `recommendations`. "
            "Each insight or anomaly item should include `title` and `detail`."
        )
        try:
            payload = self._call_json_completion(
                system_prompt="You explain business insights from Apache Doris query results.",
                user_prompt=prompt,
                api_config=api_config,
            )
        except Exception as exc:
            logger.warning("insight generation failed, using fallback summary: %s", exc)
            return {
                "summary": "Analysis completed with limited automated insights.",
                "insights": [],
                "anomalies": [],
                "recommendations": [],
            }
        if isinstance(payload, dict):
            return {
                "summary": payload.get("summary", ""),
                "insights": payload.get("insights") or [],
                "anomalies": payload.get("anomalies") or [],
                "recommendations": payload.get("recommendations") or [],
            }

        return {
            "summary": "Analysis completed.",
            "insights": [],
            "anomalies": [],
            "recommendations": [],
        }

    def _execute_profile_query(
        self,
        query_sql: str,
        source_sql: str,
        fallback_source_sql: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], str]:
        try:
            return self.db.execute_query(query_sql), source_sql
        except Exception as exc:
            if fallback_source_sql and "TABLESAMPLE" in source_sql:
                logger.warning("TABLESAMPLE failed, falling back to RAND() sample: %s", exc)
                fallback_query = query_sql.replace(source_sql, fallback_source_sql, 1)
                return self.db.execute_query(fallback_query), fallback_source_sql
            raise

    def _build_report(
        self,
        *,
        table_names: List[str],
        profile: Dict[str, Any],
        insights: Dict[str, Any],
        depth: str,
        trigger_type: str,
        step_results: Sequence[Dict[str, Any]],
        started_at: float,
        history_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        failed_step_count = sum(1 for step in step_results if not step.get("success"))
        error_messages = [step.get("error_message") for step in step_results if step.get("error_message")]
        if failed_step_count == len(step_results) and step_results:
            status = "failed"
        elif failed_step_count:
            status = "partial"
        else:
            status = "completed"

        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        report = {
            "success": True,
            "id": str(uuid.uuid4()),
            "table_names": ",".join(table_names),
            "trigger_type": trigger_type,
            "depth": depth,
            "schedule_id": schedule_id,
            "history_id": history_id,
            "summary": insights.get("summary", ""),
            "profile": profile,
            "insights": insights.get("insights") or [],
            "anomalies": insights.get("anomalies") or [],
            "recommendations": insights.get("recommendations") or [],
            "steps": list(step_results),
            "insight_count": len(insights.get("insights") or []),
            "anomaly_count": len(insights.get("anomalies") or []),
            "failed_step_count": failed_step_count,
            "status": status,
            "error_message": "; ".join(error_messages) if error_messages else None,
            "duration_ms": int((time.time() - started_at) * 1000),
            "created_at": created_at,
        }
        if note:
            report["note"] = note
        return report

    def _save_report(self, report: Dict[str, Any]) -> None:
        self.db.execute_update(
            """
            INSERT INTO `_sys_analysis_reports`
            (`id`, `table_names`, `trigger_type`, `depth`, `schedule_id`, `history_id`,
             `summary`, `report_json`, `insight_count`, `anomaly_count`,
             `failed_step_count`, `status`, `error_message`, `duration_ms`, `created_at`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                report.get("id"),
                report.get("table_names"),
                report.get("trigger_type"),
                report.get("depth"),
                report.get("schedule_id"),
                report.get("history_id"),
                report.get("summary"),
                json.dumps(report, ensure_ascii=False, default=str),
                report.get("insight_count", 0),
                report.get("anomaly_count", 0),
                report.get("failed_step_count", 0),
                report.get("status"),
                report.get("error_message"),
                report.get("duration_ms", 0),
                report.get("created_at"),
            ),
        )

    def _run_plan_steps(
        self,
        plan_steps: Sequence[Dict[str, Any]],
        api_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        step_results: List[Dict[str, Any]] = []
        for step in plan_steps:
            sql = step.get("sql") or ""
            question_context = step.get("question") or step.get("title") or "Analysis step"
            data, final_sql, success, error_message = self._execute_with_repair(
                sql,
                question_context,
                api_config,
            )
            step_results.append(
                {
                    "title": step.get("title") or "Analysis step",
                    "question": question_context,
                    "sql": final_sql,
                    "success": success,
                    "error_message": error_message,
                    "row_count": len(data or []),
                    "data": data if success else None,
                }
            )
        return step_results

    def _call_json_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        api_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        content = self._call_chat_completion(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_config=api_config,
        )
        cleaned = self._strip_markdown_fences(content)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning("analyst agent received non-JSON response: %s", cleaned)
            return {}

    def _call_chat_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        api_config: Dict[str, Any],
    ) -> str:
        response = requests.post(
            f"{api_config['base_url'].rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_config['api_key']}",
                "Content-Type": "application/json",
            },
            json={
                "model": api_config["model"],
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 3000,
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def _merge_table_names(
        self,
        explicit_tables: Sequence[str],
        step_results: Sequence[Dict[str, Any]],
    ) -> List[str]:
        merged = {table for table in explicit_tables if table}
        for step in step_results:
            merged.update(self._extract_table_names(step.get("sql") or ""))
        return sorted(merged)

    def _extract_table_names(self, sql: str) -> List[str]:
        return sorted(
            {
                match.group(1)
                for match in re.finditer(
                    r"(?:FROM|JOIN)\s+`?([A-Za-z0-9_\-\u4e00-\u9fa5]+)`?",
                    sql or "",
                    flags=re.IGNORECASE,
                )
                if match.group(1)
            }
        )

    def _decode_table_names(self, value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [name.strip() for name in str(value).split(",") if name.strip()]

    def _get_max_steps(self, depth: str) -> int:
        return _DEPTH_MAX_STEPS.get((depth or "").lower(), _DEPTH_MAX_STEPS["standard"])

    def _is_numeric_type(self, column_type: str) -> bool:
        lowered = (column_type or "").lower()
        return any(marker in lowered for marker in _NUMERIC_MARKERS)

    def _is_temporal_type(self, column_type: str) -> bool:
        lowered = (column_type or "").lower()
        return any(marker in lowered for marker in _TEMPORAL_MARKERS)

    def _null_rate(self, null_count: Any, total_count: int) -> float:
        if not total_count:
            return 0.0
        try:
            return float(null_count or 0) / float(total_count)
        except Exception:
            return 0.0

    def _strip_markdown_fences(self, value: str) -> str:
        cleaned = (value or "").strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()
