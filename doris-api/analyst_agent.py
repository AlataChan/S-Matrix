"""
Data analysis agent for on-demand and replay-based reporting.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from config import (
    ANALYST_MAX_REASONING_CHARS,
    ANALYST_MAX_ROUNDS,
)
from repair_agent import RepairAgent


logger = logging.getLogger(__name__)

_NUMERIC_MARKERS = ("int", "decimal", "float", "double", "numeric", "real")
_TEMPORAL_MARKERS = ("date", "time", "year")
_DEPTH_MAX_STEPS = {"quick": 3, "standard": 6, "deep": 10}
_EXPERT_RESULT_ROW_LIMIT = 100
_EXPERT_CONTEXT_ROW_LIMIT = 20
_EXPERT_CONTEXT_COLUMN_LIMIT = 5
_EXPERT_FALLBACK_DEPTH = "deep"
_EXPERT_MAIN_SECTION_LIMIT = 3


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
        *,
        trigger_type: str = "table_analysis",
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        if (depth or "").strip().lower() == "expert":
            return self.analyze_table_expert(
                table_name,
                resource_name=resource_name,
                trigger_type=trigger_type,
                schedule_id=schedule_id,
                note=note,
            )

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
            trigger_type=trigger_type,
            step_results=step_results,
            started_at=started_at,
            schedule_id=schedule_id,
            note=note,
        )
        self._save_report(report)
        return report

    def analyze_table_expert(
        self,
        table_name: str,
        resource_name: Optional[str] = None,
        *,
        trigger_type: str = "table_analysis",
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        started_at = time.time()
        raw_table_name = (table_name or "").strip()
        safe_table_name = self.db.validate_identifier(raw_table_name)
        executor_config = self._build_executor_config(self.build_api_config(resource_name=resource_name))
        if not executor_config.get("api_key"):
            return {
                "success": False,
                "error": "No API key configured. Set DEEPSEEK_API_KEY or OPENAI_API_KEY.",
            }

        strategist_config = self._build_strategist_config()
        if not strategist_config.get("api_key"):
            return self._fallback_from_expert(
                table_name=raw_table_name,
                resource_name=resource_name,
                trigger_type=trigger_type,
                schedule_id=schedule_id,
                note=note,
                error=RuntimeError("No strategist API key configured."),
            )
        profile = self._profile_table(raw_table_name, safe_table_name=safe_table_name)
        metadata = self._get_table_metadata(raw_table_name)
        stats = self._compute_statistical_facts(safe_table_name, profile)

        compressed_history: List[Dict[str, Any]] = []
        all_step_results: List[Dict[str, Any]] = []
        reasoning_traces: List[Dict[str, Any]] = []

        max_rounds = int(os.getenv("ANALYST_MAX_ROUNDS", str(ANALYST_MAX_ROUNDS)))
        for round_num in range(1, max_rounds + 1):
            prompt = self._build_round_prompt(round_num, profile, stats, metadata, list(compressed_history))
            try:
                strategist_output = self._call_strategist(prompt, strategist_config)
            except Exception as exc:
                return self._fallback_from_expert(
                    table_name=raw_table_name,
                    resource_name=resource_name,
                    trigger_type=trigger_type,
                    schedule_id=schedule_id,
                    note=note,
                    error=exc,
                )
            reasoning = strategist_output.get("reasoning")
            if reasoning:
                reasoning_traces.append(
                    {
                        "round": round_num,
                        "trace": self._truncate_reasoning(self._sanitize_reasoning(reasoning)),
                    }
                )

            queries = self._extract_queries_from_strategist(strategist_output, round_num)
            round_step_results: List[Dict[str, Any]] = []
            for query in queries:
                sql = self._executor_translate_to_sql(query, metadata, executor_config)
                data, final_sql, success, error = self._execute_with_repair(
                    sql,
                    query.get("query_description") or query.get("title") or f"Expert analysis round {round_num}",
                    executor_config,
                )
                capped_data = list(data or [])[:_EXPERT_RESULT_ROW_LIMIT] if success else None
                step = {
                    "round": round_num,
                    "title": query.get("title") or f"Round {round_num} analysis",
                    "question": query.get("query_description") or query.get("title") or "",
                    "sql": final_sql,
                    "success": success,
                    "error_message": error,
                    "row_count": len(capped_data or []),
                    "data": capped_data,
                }
                round_step_results.append(step)
                all_step_results.append(step)

            round_data = {
                "round": round_num,
                "strategist": strategist_output,
                "results": round_step_results,
            }
            compressed_history.append(self._compress_round_for_context(round_data))

            if not strategist_output.get("response", {}).get("continue", False):
                break

        report = self._build_expert_report(
            table_names=[raw_table_name],
            profile=profile,
            compressed_history=compressed_history,
            reasoning_traces=reasoning_traces,
            all_step_results=all_step_results,
            trigger_type=trigger_type,
            started_at=started_at,
            schedule_id=schedule_id,
            note=note,
        )
        self._save_report(report)
        return report

    def _fallback_from_expert(
        self,
        *,
        table_name: str,
        resource_name: Optional[str],
        trigger_type: str,
        schedule_id: Optional[str],
        note: Optional[str],
        error: Exception,
    ) -> Dict[str, Any]:
        logger.warning(
            "expert strategist unavailable for %s, falling back to %s analysis: %s",
            table_name,
            _EXPERT_FALLBACK_DEPTH,
            error,
        )
        fallback_note = "Expert mode unavailable; fell back to deep analysis."
        if note:
            fallback_note = f"{note} {fallback_note}"
        return self.analyze_table(
            table_name,
            depth=_EXPERT_FALLBACK_DEPTH,
            resource_name=resource_name,
            trigger_type=trigger_type,
            schedule_id=schedule_id,
            note=fallback_note,
        )

    def replay_from_history(
        self,
        history_id: str,
        resource_name: Optional[str] = None,
        *,
        trigger_type: str = "history_replay",
        schedule_id: Optional[str] = None,
        note: str = "Replayed against current data.",
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
            trigger_type=trigger_type,
            step_results=step_results,
            started_at=started_at,
            history_id=history_id,
            schedule_id=schedule_id,
            note=note,
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

    def get_report(self, report_id: str, include_reasoning: bool = False) -> Dict[str, Any]:
        rows = self.db.execute_query(
            "SELECT `report_json` FROM `_sys_analysis_reports` WHERE `id` = %s LIMIT 1",
            (report_id,),
        )
        if not rows:
            return {"success": False, "error": f"Report '{report_id}' not found."}
        payload = rows[0].get("report_json") or "{}"
        report = json.loads(payload) if isinstance(payload, str) else payload
        return self._filter_report_reasoning(report, include_reasoning=include_reasoning)

    def delete_report(self, report_id: str) -> Dict[str, Any]:
        self.db.execute_update("DELETE FROM `_sys_analysis_reports` WHERE `id` = %s", (report_id,))
        return {"success": True, "deleted": True, "id": report_id}

    def get_latest_report(self, table_name: str, include_reasoning: bool = False) -> Dict[str, Any]:
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
        report = json.loads(payload) if isinstance(payload, str) else payload
        return self._filter_report_reasoning(report, include_reasoning=include_reasoning)

    def _filter_report_reasoning(
        self,
        report: Dict[str, Any],
        *,
        include_reasoning: bool = False,
    ) -> Dict[str, Any]:
        payload = self._hydrate_expert_sections(dict(report or {}))
        if not include_reasoning:
            payload.pop("reasoning_traces", None)
        return payload

    def _hydrate_expert_sections(self, report: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(report or {})
        if (payload.get("depth") or "").strip().lower() != "expert":
            return payload

        raw_findings = [
            self._coerce_expert_item(item)
            for item in (
                payload.get("findings")
                or payload.get("insights")
                or payload.get("top_insights")
                or []
            )
        ]
        insights = self._normalize_expert_findings(raw_findings)
        recommendations = self._normalize_expert_text_list(payload.get("recommendations") or [])
        action_items = self._normalize_expert_action_items(
            payload.get("action_items") or payload.get("recommendations") or [],
            raw_findings,
        )
        if not recommendations and action_items:
            recommendations = [self._action_item_as_text(item) for item in action_items]

        executive_summary = self._normalize_expert_summary(
            payload.get("executive_summary") or payload.get("summary"),
            insights[:_EXPERT_MAIN_SECTION_LIMIT],
            recommendations[:_EXPERT_MAIN_SECTION_LIMIT],
        )

        payload["summary"] = executive_summary
        payload["executive_summary"] = executive_summary
        payload["insights"] = insights
        payload["top_insights"] = insights[:_EXPERT_MAIN_SECTION_LIMIT]
        payload["recommendations"] = recommendations
        payload["action_items"] = action_items[:_EXPERT_MAIN_SECTION_LIMIT]
        return payload

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

    def _build_strategist_config(self) -> Dict[str, Any]:
        env_model = os.getenv("ANALYST_STRATEGIST_MODEL")
        env_base_url = os.getenv("ANALYST_STRATEGIST_BASE_URL")
        env_api_key = os.getenv("ANALYST_STRATEGIST_API_KEY")

        custom_vars = {
            "model": env_model,
            "base_url": env_base_url,
            "api_key": env_api_key,
        }
        custom_count = sum(1 for value in custom_vars.values() if value)
        if custom_count == 3:
            return custom_vars
        if custom_count > 0:
            logger.warning(
                "Partial ANALYST_STRATEGIST_* config (%d/3 set). Set all three or none. Falling back to default.",
                custom_count,
            )

        return {
            "model": "deepseek-reasoner",
            "base_url": os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            "api_key": os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY"),
        }

    def _build_executor_config(self, base_api_config: Dict[str, Any]) -> Dict[str, Any]:
        return dict(base_api_config or {})

    def _call_strategist(self, prompt: str, strategist_config: Dict[str, Any]) -> Dict[str, Any]:
        import httpx as _httpx

        model_name = (strategist_config.get("model") or "").lower()
        is_reasoner = "reasoner" in model_name or re.search(r"(^|[^a-z0-9])r1([^a-z0-9]|$)", model_name) is not None
        if is_reasoner:
            payload = {
                "model": strategist_config["model"],
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 8000,
                "stream": True,
            }
            timeout = 300
        else:
            payload = {
                "model": strategist_config["model"],
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 4000,
                "stream": True,
            }
            timeout = 120

        url = f"{strategist_config['base_url'].rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {strategist_config['api_key']}",
            "Content-Type": "application/json",
        }

        # Use SSE streaming to avoid Docker TCP keepalive drops on long R1 responses
        reasoning_chunks: List[str] = []
        content_chunks: List[str] = []

        with _httpx.Client(timeout=_httpx.Timeout(float(timeout), connect=30.0)) as client:
            with client.stream("POST", url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                        delta = chunk["choices"][0].get("delta", {})
                        if delta.get("reasoning_content"):
                            reasoning_chunks.append(delta["reasoning_content"])
                        if delta.get("content"):
                            content_chunks.append(delta["content"])
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass

        content = "".join(content_chunks)
        reasoning = "".join(reasoning_chunks)
        return {
            "reasoning": reasoning,
            "response": self._parse_json_from_text(content),
        }

    def _compute_statistical_facts(
        self,
        safe_name: str,
        profile: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        facts: List[Dict[str, Any]] = [
            {
                "type": "table_overview",
                "metric": "row_count",
                "value": profile.get("row_count", 0),
                "sampled": profile.get("sampled", False),
                "sample_size": profile.get("sample_size", 0),
            }
        ]
        columns = profile.get("columns") or {}
        time_cols = [
            name
            for name, info in columns.items()
            if self._is_temporal_type(str(info.get("type", "")).lower())
        ]
        numeric_cols = [
            name
            for name, info in columns.items()
            if self._is_numeric_type(str(info.get("type", "")).lower())
        ]

        for column_name, column_profile in columns.items():
            base_fact = {
                "type": "column_profile",
                "column": column_name,
                "null_rate": column_profile.get("null_rate"),
            }
            if any(key in column_profile for key in ("avg", "stddev", "min", "max")):
                facts.append(
                    {
                        **base_fact,
                        "min": column_profile.get("min"),
                        "max": column_profile.get("max"),
                        "avg": column_profile.get("avg"),
                        "stddev": column_profile.get("stddev"),
                    }
                )
            elif any(key in column_profile for key in ("unique_count", "top_values")):
                facts.append(
                    {
                        **base_fact,
                        "unique_count": column_profile.get("unique_count"),
                        "top_values": (column_profile.get("top_values") or [])[:5],
                    }
                )

        if time_cols and numeric_cols:
            time_column = time_cols[0]
            safe_time_column = self.db.validate_identifier(time_column)
            for numeric_column in numeric_cols[:3]:
                safe_numeric_column = self.db.validate_identifier(numeric_column)
                try:
                    growth_rows = self.db.execute_query(
                        f"""
                        SELECT DATE_FORMAT({safe_time_column}, '%Y-%m') AS period,
                               SUM({safe_numeric_column}) AS metric_value
                        FROM {safe_name}
                        WHERE {safe_time_column} IS NOT NULL
                          AND {safe_numeric_column} IS NOT NULL
                        GROUP BY DATE_FORMAT({safe_time_column}, '%Y-%m')
                        ORDER BY period DESC
                        LIMIT 3
                        """
                    )
                except Exception as exc:
                    logger.debug("expert growth precompute failed for %s: %s", numeric_column, exc)
                    growth_rows = []

                if len(growth_rows) >= 2:
                    latest_value = growth_rows[0].get("metric_value")
                    previous_value = growth_rows[1].get("metric_value")
                    try:
                        if previous_value not in (None, 0):
                            growth_pct = ((float(latest_value) - float(previous_value)) / abs(float(previous_value))) * 100
                            facts.append(
                                {
                                    "type": "growth",
                                    "column": numeric_column,
                                    "time_column": time_column,
                                    "latest_period": growth_rows[0].get("period"),
                                    "previous_period": growth_rows[1].get("period"),
                                    "latest_value": latest_value,
                                    "previous_value": previous_value,
                                    "growth_pct": round(growth_pct, 2),
                                }
                            )
                    except (TypeError, ValueError, ZeroDivisionError):
                        pass

        for numeric_column in numeric_cols[:5]:
            safe_numeric_column = self.db.validate_identifier(numeric_column)
            try:
                concentration_rows = self.db.execute_query(
                    f"""
                    SELECT ROUND(
                        100.0 * SUM(CASE WHEN decile = 1 THEN metric_value ELSE 0 END) / NULLIF(SUM(metric_value), 0),
                        2
                    ) AS top_share_pct
                    FROM (
                        SELECT {safe_numeric_column} AS metric_value,
                               NTILE(10) OVER (ORDER BY {safe_numeric_column} DESC) AS decile
                        FROM {safe_name}
                        WHERE {safe_numeric_column} IS NOT NULL
                    ) ranked_values
                    """
                )
                top_share_pct = (concentration_rows[0] or {}).get("top_share_pct") if concentration_rows else None
                if top_share_pct is not None:
                    facts.append(
                        {
                            "type": "concentration",
                            "column": numeric_column,
                            "top_percent": 10,
                            "top_share_pct": top_share_pct,
                        }
                    )
            except Exception as exc:
                logger.debug("expert concentration precompute failed for %s: %s", numeric_column, exc)

            column_profile = columns.get(numeric_column) or {}
            avg_value = column_profile.get("avg")
            stddev_value = column_profile.get("stddev")
            try:
                avg_float = float(avg_value)
                stddev_float = float(stddev_value)
            except (TypeError, ValueError):
                avg_float = None
                stddev_float = None

            if avg_float is not None and stddev_float not in (None, 0.0):
                lower_bound = avg_float - (2 * stddev_float)
                upper_bound = avg_float + (2 * stddev_float)
                try:
                    outlier_rows = self.db.execute_query(
                        f"""
                        SELECT COUNT(*) AS outlier_count
                        FROM {safe_name}
                        WHERE {safe_numeric_column} IS NOT NULL
                          AND ({safe_numeric_column} < %s OR {safe_numeric_column} > %s)
                        """,
                        (lower_bound, upper_bound),
                    )
                    outlier_count = (outlier_rows[0] or {}).get("outlier_count") if outlier_rows else 0
                except Exception as exc:
                    logger.debug("expert outlier precompute failed for %s: %s", numeric_column, exc)
                    outlier_count = None

                facts.append(
                    {
                        "type": "outlier",
                        "column": numeric_column,
                        "lower_bound": round(lower_bound, 4),
                        "upper_bound": round(upper_bound, 4),
                        "outlier_count": outlier_count,
                    }
                )

        if len(numeric_cols) >= 2:
            pair_candidates: List[Tuple[str, str]] = []
            limited_numeric = numeric_cols[:3]
            for left_index, left_column in enumerate(limited_numeric):
                for right_column in limited_numeric[left_index + 1 :]:
                    pair_candidates.append((left_column, right_column))

            for left_column, right_column in pair_candidates:
                safe_left = self.db.validate_identifier(left_column)
                safe_right = self.db.validate_identifier(right_column)
                try:
                    correlation_rows = self.db.execute_query(
                        f"""
                        SELECT CORR({safe_left}, {safe_right}) AS correlation_value
                        FROM {safe_name}
                        WHERE {safe_left} IS NOT NULL
                          AND {safe_right} IS NOT NULL
                        """
                    )
                    correlation_value = (correlation_rows[0] or {}).get("correlation_value") if correlation_rows else None
                    if correlation_value is not None:
                        facts.append(
                            {
                                "type": "correlation",
                                "columns": [left_column, right_column],
                                "value": correlation_value,
                            }
                        )
                except Exception as exc:
                    logger.debug("expert correlation precompute failed for %s/%s: %s", left_column, right_column, exc)

        return facts

    def _build_round_prompt(
        self,
        round_num: int,
        profile: Dict[str, Any],
        stats: Sequence[Dict[str, Any]],
        metadata: Dict[str, Any],
        compressed_history: Sequence[Dict[str, Any]],
    ) -> str:
        preamble = (
            "You are a Senior Data Scientist conducting a rigorous analysis.\n"
            "Methodology: descriptive -> diagnostic -> predictive.\n"
            "Quantify every claim, compare against baselines, flag outliers beyond 2 stddev, "
            "distinguish correlation from causation, and end with actionable recommendations.\n"
            "Write in Chinese for business context and English for technical terms.\n"
            "Return only JSON."
        )
        context = json.dumps(list(compressed_history), ensure_ascii=False, default=str)
        profile_json = json.dumps(profile, ensure_ascii=False, default=str)
        stats_json = json.dumps(list(stats), ensure_ascii=False, default=str)
        metadata_json = json.dumps(metadata, ensure_ascii=False, default=str)
        base_context = (
            f"Metadata: {metadata_json}\n"
            f"Profile: {profile_json}\n"
            f"Statistics: {stats_json}"
        )

        if round_num == 1:
            return (
                f"{preamble}\n\n"
                f"Data context:\n{base_context}\n\n"
                "Generate 3-5 analytical hypotheses ordered by business value.\n"
                "Each item must include `id`, `title`, `methodology`, `query_description`, "
                "`confirm_condition`, and `refute_condition`.\n"
                "Return JSON with `hypotheses` and `continue`."
            )
        if round_num == 2:
            return (
                f"{preamble}\n\n"
                f"Original data context:\n{base_context}\n\n"
                f"Prior context: {context}\n\n"
                "Critically evaluate the prior findings.\n"
                "Return JSON with `assessments`, `follow_ups`, and `continue`."
            )
        return (
            f"{preamble}\n\n"
            f"Original data context:\n{base_context}\n\n"
            f"Full compressed analysis history: {context}\n\n"
            "Synthesize the full analysis into a final verdict.\n"
            "Return JSON with:\n"
            "- `summary`: a concise Chinese executive summary for business readers, 2-3 sentences, "
            "no methodology narration, no raw JSON\n"
            "- `findings`: an array of objects with `title`, `detail`, optional `severity`, "
            "`quantification`, and optional `recommendation`, sorted by business impact\n"
            "- `recommendations`: at most 3 action items, each as string or object with `title`, `detail`, optional `urgency`\n"
            "- `anomalies`, `root_causes`, `limitations`, `confidence_overall`, and `continue`."
        )

    def _extract_queries_from_strategist(
        self,
        strategist_output: Dict[str, Any],
        round_num: int,
    ) -> List[Dict[str, Any]]:
        response = strategist_output.get("response") or {}
        if response.get("hypotheses"):
            return [
                {
                    "id": item.get("id"),
                    "title": item.get("title") or f"Hypothesis {index + 1}",
                    "query_description": item.get("query_description") or item.get("title") or "",
                }
                for index, item in enumerate(response.get("hypotheses") or [])
                if item.get("query_description") or item.get("title")
            ]
        if response.get("follow_ups"):
            return [
                {
                    "id": item.get("id"),
                    "title": item.get("reason") or f"Follow-up {index + 1}",
                    "query_description": item.get("query_description") or item.get("reason") or "",
                }
                for index, item in enumerate(response.get("follow_ups") or [])
                if item.get("query_description") or item.get("reason")
            ]
        return []

    def _executor_translate_to_sql(
        self,
        query: Dict[str, Any],
        metadata: Dict[str, Any],
        api_config: Dict[str, Any],
    ) -> str:
        table_name = metadata.get("table_name") or ""
        safe_table_name = self.db.validate_identifier(table_name) if table_name else ""
        prompt = (
            "You are a SQL engineer for Apache Doris. Translate analytical queries to SQL.\n"
            f"Table: {table_name}\n"
            f"Metadata: {json.dumps(metadata, ensure_ascii=False, default=str)}\n"
            f"Query request: {query.get('query_description') or query.get('title') or ''}\n\n"
            "Rules:\n"
            "- Use backtick quoting for all identifiers\n"
            "- Doris syntax (not MySQL-specific features)\n"
            "- Return at most 100 rows (expert mode focuses on patterns, not raw data)\n"
            "- Always alias computed columns for clarity\n"
            "Return only the SQL query."
        )
        try:
            sql = self._call_chat_completion(
                system_prompt="You translate natural-language analytical requests into Apache Doris SQL.",
                user_prompt=prompt,
                api_config=api_config,
            )
            cleaned = self._strip_markdown_fences(sql).strip().rstrip(";")
        except Exception as exc:
            logger.warning("expert executor translation failed, using fallback SQL: %s", exc)
            cleaned = ""

        if not cleaned.lower().startswith("select"):
            return f"SELECT COUNT(*) AS total_rows FROM {safe_table_name}"
        if " limit " not in cleaned.lower():
            cleaned = f"{cleaned} LIMIT {_EXPERT_RESULT_ROW_LIMIT}"
        return cleaned

    def _compress_round_for_context(self, round_data: Dict[str, Any]) -> Dict[str, Any]:
        compressed = {
            "round": round_data["round"],
            "strategist_output": round_data["strategist"]["response"],
        }
        compressed_results: List[Dict[str, Any]] = []
        for step in round_data.get("results", []):
            truncated_step = {
                "title": step.get("title"),
                "success": step.get("success"),
                "row_count": step.get("row_count"),
                "error_message": step.get("error_message"),
            }
            data = list(step.get("data") or [])[:_EXPERT_CONTEXT_ROW_LIMIT]
            if data:
                if len(data[0]) > _EXPERT_CONTEXT_COLUMN_LIMIT:
                    key_columns = list(data[0].keys())[:_EXPERT_CONTEXT_COLUMN_LIMIT]
                    data = [{key: row.get(key) for key in key_columns} for row in data]
                truncated_step["data_sample"] = data
                if len(step.get("data") or []) > _EXPERT_CONTEXT_ROW_LIMIT:
                    truncated_step["data_note"] = (
                        f"Showing {_EXPERT_CONTEXT_ROW_LIMIT} of {len(step.get('data') or [])} rows"
                    )
            compressed_results.append(truncated_step)
        compressed["results"] = compressed_results
        return compressed

    def _sanitize_reasoning(self, text: str) -> str:
        lines = []
        for line in (text or "").splitlines():
            lowered = line.lower()
            if "api_key" in lowered or "authorization:" in lowered or "system prompt" in lowered:
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    def _truncate_reasoning(self, text: str) -> str:
        max_chars = int(os.getenv("ANALYST_MAX_REASONING_CHARS", str(ANALYST_MAX_REASONING_CHARS)))
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + f"\n\n[truncated, {len(text) - max_chars} chars omitted]"

    def _build_expert_report(
        self,
        *,
        table_names: Sequence[str],
        profile: Dict[str, Any],
        compressed_history: Sequence[Dict[str, Any]],
        reasoning_traces: Sequence[Dict[str, Any]],
        all_step_results: Sequence[Dict[str, Any]],
        trigger_type: str,
        started_at: float,
        history_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        final_output = (compressed_history[-1] or {}).get("strategist_output") if compressed_history else {}
        raw_findings = [self._coerce_expert_item(item) for item in (final_output.get("findings") or [])]
        findings = self._normalize_expert_findings(raw_findings)
        anomalies = self._normalize_expert_findings(final_output.get("anomalies") or [], default_prefix="异常")
        recommendations = self._normalize_expert_text_list(final_output.get("recommendations") or [])
        action_items = self._normalize_expert_action_items(final_output.get("recommendations") or [], raw_findings)
        if not recommendations and action_items:
            recommendations = [self._action_item_as_text(item) for item in action_items]
        limitations = self._normalize_expert_text_list(final_output.get("limitations") or [])
        root_causes = self._normalize_expert_text_list(final_output.get("root_causes") or [])
        failed_step_count = sum(1 for step in all_step_results if not step.get("success"))
        if failed_step_count == len(all_step_results) and all_step_results:
            status = "failed"
        elif failed_step_count:
            status = "partial"
        else:
            status = "completed"

        report = {
            "success": True,
            "id": str(uuid.uuid4()),
            "table_names": ",".join([name for name in table_names if name]),
            "trigger_type": trigger_type,
            "depth": "expert",
            "schedule_id": schedule_id,
            "history_id": history_id,
            "summary": self._normalize_expert_summary(
                final_output.get("summary"),
                findings[:_EXPERT_MAIN_SECTION_LIMIT],
                recommendations[:_EXPERT_MAIN_SECTION_LIMIT],
            ),
            "profile": profile,
            "insights": findings,
            "top_insights": findings[:_EXPERT_MAIN_SECTION_LIMIT],
            "anomalies": anomalies,
            "recommendations": recommendations,
            "action_items": action_items[:_EXPERT_MAIN_SECTION_LIMIT],
            "limitations": limitations,
            "root_causes": root_causes,
            "conversation_chain": list(compressed_history),
            "reasoning_traces": list(reasoning_traces),
            "evidence_chains": self._build_evidence_chains(raw_findings, compressed_history),
            "confidence_ratings": {"overall": final_output.get("confidence_overall")},
            "steps": list(all_step_results),
            "insight_count": len(findings),
            "anomaly_count": len(anomalies),
            "failed_step_count": failed_step_count,
            "status": status,
            "error_message": None,
            "duration_ms": int((time.time() - started_at) * 1000),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        report["executive_summary"] = report["summary"]
        if note:
            report["note"] = note
        return report

    def _build_evidence_chains(
        self,
        findings: Sequence[Dict[str, Any]],
        compressed_history: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not findings:
            return []
        hypotheses = []
        assessments = []
        follow_ups = []
        for item in compressed_history:
            output = item.get("strategist_output") or {}
            hypotheses.extend(output.get("hypotheses") or [])
            assessments.extend(output.get("assessments") or [])
            follow_ups.extend(output.get("follow_ups") or [])

        evidence_chains: List[Dict[str, Any]] = []
        for finding in findings:
            finding_payload = finding if isinstance(finding, dict) else {"title": str(finding)}
            match_text = self._matching_text(finding_payload)
            evidence_ids = self._extract_evidence_ids(finding_payload)

            matched_hypotheses = [
                item
                for item in hypotheses
                if self._matches_evidence(item, evidence_ids, match_text, id_keys=("id",), text_keys=("title", "query_description"))
            ]
            matched_hypothesis_ids = {
                item.get("id")
                for item in matched_hypotheses
                if item.get("id")
            }
            matched_assessments = [
                item
                for item in assessments
                if item.get("hypothesis_id") in matched_hypothesis_ids
                or self._matches_evidence(item, evidence_ids, match_text, id_keys=("hypothesis_id",), text_keys=("verdict",))
            ]
            matched_follow_ups = [
                item
                for item in follow_ups
                if item.get("hypothesis_id") in matched_hypothesis_ids
                or self._matches_evidence(
                    item,
                    evidence_ids,
                    match_text,
                    id_keys=("hypothesis_id", "id"),
                    text_keys=("reason", "query_description"),
                )
            ]

            evidence_chains.append(
                {
                    "finding": (
                        finding_payload.get("title")
                        or finding_payload.get("category")
                        or finding_payload.get("headline")
                        or finding_payload.get("description")
                        or finding_payload
                    ),
                    "detail": self._compose_expert_detail(finding_payload),
                    "hypotheses": matched_hypotheses,
                    "assessments": matched_assessments,
                    "follow_ups": matched_follow_ups,
                }
            )
        return evidence_chains

    def _coerce_expert_item(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("{") and cleaned.endswith("}"):
                parsed = self._parse_json_from_text(cleaned)
                if parsed:
                    return parsed
            return {"detail": value}
        if value is None:
            return {}
        return {"detail": str(value)}

    def _normalize_expert_findings(
        self,
        items: Sequence[Any],
        *,
        default_prefix: str = "洞察",
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(items):
            payload = self._coerce_expert_item(item)
            title = (
                payload.get("title")
                or payload.get("category")
                or payload.get("headline")
                or payload.get("theme")
                or f"{default_prefix} {index + 1}"
            )
            detail = self._compose_expert_detail(payload, include_recommendation=False) or str(title)
            normalized_item = {
                "title": str(title),
                "detail": detail,
            }
            if payload.get("severity"):
                normalized_item["severity"] = payload.get("severity")
            normalized.append(normalized_item)
        return normalized

    def _normalize_expert_text_list(self, items: Sequence[Any]) -> List[str]:
        normalized: List[str] = []
        for item in items:
            payload = self._coerce_expert_item(item)
            title = payload.get("title") or payload.get("category") or payload.get("headline")
            detail = self._compose_expert_detail(payload)
            if title and detail:
                normalized.append(f"{title}：{detail}")
            elif title:
                normalized.append(str(title))
            elif detail:
                normalized.append(detail)
        return normalized

    def _normalize_expert_action_items(
        self,
        items: Sequence[Any],
        findings: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen: set[Tuple[str, str]] = set()

        def append_item(title: Optional[str], detail: Optional[str], urgency: Optional[Any] = None) -> None:
            clean_title = str(title or f"动作建议 {len(normalized) + 1}").strip()
            clean_detail = str(detail or "").strip() or clean_title
            marker = (clean_title, clean_detail)
            if marker in seen:
                return
            seen.add(marker)
            item = {"title": clean_title, "detail": clean_detail}
            if urgency:
                item["urgency"] = urgency
            normalized.append(item)

        for item in items:
            payload = self._coerce_expert_item(item)
            title = payload.get("title") or payload.get("category") or payload.get("headline") or payload.get("action")
            detail = self._compose_expert_action_detail(payload)
            append_item(title, detail, payload.get("urgency"))
            if len(normalized) >= _EXPERT_MAIN_SECTION_LIMIT:
                return normalized

        for finding in findings:
            recommendation = finding.get("recommendation")
            if not recommendation:
                continue
            title = finding.get("title") or finding.get("category") or finding.get("headline")
            detail = str(recommendation).strip()
            implication = finding.get("implication") or finding.get("business_impact")
            if implication:
                detail = f"{detail}；预期影响：{str(implication).strip()}"
            append_item(title, detail)
            if len(normalized) >= _EXPERT_MAIN_SECTION_LIMIT:
                return normalized

        return normalized

    def _action_item_as_text(self, item: Dict[str, Any]) -> str:
        title = str(item.get("title") or "").strip()
        detail = str(item.get("detail") or "").strip()
        if title and detail and title != detail:
            return f"{title}：{detail}"
        return detail or title

    def _compose_expert_detail(self, payload: Dict[str, Any], *, include_recommendation: bool = True) -> str:
        parts: List[str] = []
        keys = [
            "detail",
            "description",
            "quantification",
            "business_impact",
            "evidence",
            "implication",
        ]
        if include_recommendation:
            keys.append("recommendation")
        for key in keys:
            value = payload.get(key)
            if value:
                text = str(value).strip()
                if text and text not in parts:
                    parts.append(text)
        return "；".join(parts)

    def _compose_expert_action_detail(self, payload: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in (
            "detail",
            "action",
            "description",
            "recommendation",
            "expected_outcome",
            "business_impact",
            "owner_hint",
            "urgency",
        ):
            value = payload.get(key)
            if value:
                text = str(value).strip()
                if text and text not in parts:
                    parts.append(text)
        return "；".join(parts)

    def _normalize_expert_summary(
        self,
        summary: Any,
        insights: Sequence[Dict[str, Any]],
        recommendations: Sequence[str],
    ) -> str:
        text = str(summary or "").strip()
        if text:
            cleaned_sentences = []
            for sentence in re.split(r"[。！？!?]", text):
                normalized = sentence.strip()
                lowered = normalized.lower()
                if not normalized:
                    continue
                if "descriptive -> diagnostic -> predictive" in lowered:
                    continue
                if "methodology" in lowered or "方法论" in normalized:
                    continue
                cleaned_sentences.append(normalized)
            if cleaned_sentences:
                candidate = "；".join(cleaned_sentences[:2]).strip("； ")
                if candidate:
                    return self._shorten_expert_text(candidate + "。", 120)

        if insights:
            parts = []
            for insight in insights[:2]:
                title = insight.get("title") or "关键洞察"
                detail = self._shorten_expert_text(insight.get("detail") or "", 36)
                parts.append(f"{title}：{detail}" if detail else str(title))
            return "；".join(parts) + "。"

        if recommendations:
            return self._shorten_expert_text(f"本次 expert 分析已完成，建议优先执行：{recommendations[0]}", 120)

        return "本次 expert 分析已完成，请优先查看下方关键洞察与建议。"

    def _shorten_expert_text(self, text: str, max_chars: int) -> str:
        normalized = re.sub(r"\s+", " ", (text or "").strip())
        if len(normalized) <= max_chars:
            return normalized
        return normalized[: max_chars - 1].rstrip("；，, ") + "…"

    def _extract_evidence_ids(self, finding: Dict[str, Any]) -> set[str]:
        values: set[str] = set()
        for key in ("hypothesis_id", "assessment_id", "follow_up_id"):
            value = finding.get(key)
            if value:
                values.add(str(value).lower())
        for key in ("hypothesis_ids", "evidence_ids", "supporting_ids"):
            for value in finding.get(key) or []:
                values.add(str(value).lower())
        return values

    def _matching_text(self, payload: Any) -> str:
        return json.dumps(payload or {}, ensure_ascii=False, default=str).lower()

    def _matches_evidence(
        self,
        item: Dict[str, Any],
        evidence_ids: set[str],
        match_text: str,
        *,
        id_keys: Sequence[str],
        text_keys: Sequence[str],
    ) -> bool:
        for key in id_keys:
            value = item.get(key)
            if value and str(value).lower() in evidence_ids:
                return True
        for key in text_keys:
            value = item.get(key)
            if value and str(value).lower() in match_text:
                return True
        return False

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
        return self._parse_json_from_text(content)

    def _call_chat_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        api_config: Dict[str, Any],
    ) -> str:
        url = f"{api_config['base_url'].rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_config['api_key']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": api_config["model"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 3000,
        }
        import httpx as _httpx

        payload["stream"] = True
        content_chunks: List[str] = []

        with _httpx.Client(timeout=_httpx.Timeout(120.0, connect=30.0)) as client:
            with client.stream("POST", url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                        delta = chunk["choices"][0].get("delta", {})
                        if delta.get("content"):
                            content_chunks.append(delta["content"])
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass

        return "".join(content_chunks)

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

    def _parse_json_from_text(self, value: str) -> Dict[str, Any]:
        cleaned = self._strip_markdown_fences(value)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(cleaned[start : end + 1])
                except json.JSONDecodeError:
                    pass
            logger.warning("analyst agent received non-JSON response: %s", cleaned)
            return {}
