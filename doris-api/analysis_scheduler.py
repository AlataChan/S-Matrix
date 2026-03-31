"""
Scheduled multi-table analysis orchestration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet


logger = logging.getLogger(__name__)

_SENSITIVE_DELIVERY_FIELDS = {"webhook_url", "webhook_token"}
_REDACTED_VALUE = "***configured***"
_SUPPORTED_SCHEDULE_TYPES = {"hourly", "daily", "weekly", "monthly"}
_SCHEDULE_TIMING_KEYS = {
    "schedule_type",
    "schedule_hour",
    "schedule_minute",
    "schedule_day_of_week",
    "schedule_day_of_month",
    "timezone",
}


class AnalysisScheduler:
    def __init__(self, analyst_agent, doris_client, dispatcher=None):
        self.agent = analyst_agent
        self.db = doris_client
        self.dispatcher = dispatcher
        self.event_loop = None
        self._run_lock = threading.Lock()

        key = os.getenv("ENCRYPTION_KEY")
        if key:
            self.cipher = Fernet(key.encode() if isinstance(key, str) else key)
        else:
            logger.warning(
                "ENCRYPTION_KEY not set: analysis delivery secrets use an ephemeral key for this process"
            )
            self.cipher = Fernet(Fernet.generate_key())

    def set_event_loop(self, loop) -> None:
        self.event_loop = loop

    def init_tables(self) -> bool:
        self.db.execute_update(
            """
            CREATE TABLE IF NOT EXISTS `_sys_analysis_schedules` (
                `id` VARCHAR(64),
                `name` VARCHAR(200),
                `tables_json` TEXT,
                `depth` VARCHAR(20) DEFAULT "standard",
                `resource_name` VARCHAR(200),
                `schedule_type` VARCHAR(20),
                `schedule_hour` INT DEFAULT "8",
                `schedule_minute` INT DEFAULT "0",
                `schedule_day_of_week` INT DEFAULT "1",
                `schedule_day_of_month` INT DEFAULT "1",
                `timezone` VARCHAR(50) DEFAULT "UTC",
                `delivery_json` TEXT,
                `enabled` TINYINT DEFAULT "1",
                `last_run_at` DATETIME,
                `next_run_at` DATETIME,
                `created_at` DATETIME,
                `updated_at` DATETIME
            )
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            """
        )
        return True

    def register(self, shared_scheduler) -> None:
        shared_scheduler.register_interval(
            self._check_and_execute,
            minutes=1,
            job_id="analysis_schedule_checker",
        )

    def create_schedule(self, config: Dict[str, Any]) -> Dict[str, Any]:
        schedule = self._normalize_schedule_config(config)
        self.db.execute_update(
            """
            INSERT INTO `_sys_analysis_schedules`
            (`id`, `name`, `tables_json`, `depth`, `resource_name`, `schedule_type`,
             `schedule_hour`, `schedule_minute`, `schedule_day_of_week`, `schedule_day_of_month`,
             `timezone`, `delivery_json`, `enabled`, `last_run_at`, `next_run_at`,
             `created_at`, `updated_at`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            self._schedule_to_params(schedule),
        )
        return {"success": True, "schedule": self._serialize_schedule(schedule)}

    def update_schedule(self, schedule_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        existing_row = self._get_schedule_row(schedule_id)
        if existing_row is None:
            return {"success": False, "error": f"Schedule '{schedule_id}' not found."}

        existing = self._deserialize_row(existing_row, redact=False)
        merged = {**existing, **config}
        merged["id"] = schedule_id
        merged["created_at"] = existing.get("created_at")
        merged["last_run_at"] = existing.get("last_run_at")
        if any(key in config for key in _SCHEDULE_TIMING_KEYS) or (
            config.get("enabled") is True and not existing.get("enabled", True)
        ):
            merged["next_run_at"] = None
        schedule = self._normalize_schedule_config(merged, preserve_id=True)

        self.db.execute_update(
            """
            UPDATE `_sys_analysis_schedules`
            SET `name` = %s,
                `tables_json` = %s,
                `depth` = %s,
                `resource_name` = %s,
                `schedule_type` = %s,
                `schedule_hour` = %s,
                `schedule_minute` = %s,
                `schedule_day_of_week` = %s,
                `schedule_day_of_month` = %s,
                `timezone` = %s,
                `delivery_json` = %s,
                `enabled` = %s,
                `last_run_at` = %s,
                `next_run_at` = %s,
                `updated_at` = %s
            WHERE `id` = %s
            """,
            (
                schedule["name"],
                schedule["tables_json"],
                schedule["depth"],
                schedule.get("resource_name"),
                schedule["schedule_type"],
                schedule["schedule_hour"],
                schedule["schedule_minute"],
                schedule["schedule_day_of_week"],
                schedule["schedule_day_of_month"],
                schedule["timezone"],
                schedule.get("delivery_json"),
                1 if schedule.get("enabled", True) else 0,
                schedule.get("last_run_at"),
                schedule.get("next_run_at"),
                schedule["updated_at"],
                schedule_id,
            ),
        )
        return {"success": True, "schedule": self._serialize_schedule(schedule)}

    def delete_schedule(self, schedule_id: str) -> Dict[str, Any]:
        self.db.execute_update("DELETE FROM `_sys_analysis_schedules` WHERE `id` = %s", (schedule_id,))
        return {"success": True, "deleted": True, "id": schedule_id}

    def list_schedules(self) -> Dict[str, Any]:
        rows = self.db.execute_query(
            """
            SELECT `id`, `name`, `tables_json`, `depth`, `resource_name`, `schedule_type`,
                   `schedule_hour`, `schedule_minute`, `schedule_day_of_week`, `schedule_day_of_month`,
                   `timezone`, `delivery_json`, `enabled`, `last_run_at`, `next_run_at`,
                   `created_at`, `updated_at`
            FROM `_sys_analysis_schedules`
            ORDER BY `created_at` DESC
            """
        )
        schedules = [self._deserialize_row(row, redact=True) for row in rows]
        return {"success": True, "count": len(schedules), "schedules": schedules}

    def toggle_schedule(self, schedule_id: str) -> Dict[str, Any]:
        row = self._get_schedule_row(schedule_id)
        if row is None:
            return {"success": False, "error": f"Schedule '{schedule_id}' not found."}

        schedule = self._deserialize_row(row, redact=False)
        enabled = not schedule.get("enabled", True)
        updated_at = self._format_utc(datetime.now(timezone.utc))
        self.db.execute_update(
            """
            UPDATE `_sys_analysis_schedules`
            SET `enabled` = %s,
                `updated_at` = %s
            WHERE `id` = %s
            """,
            (1 if enabled else 0, updated_at, schedule_id),
        )
        row["enabled"] = 1 if enabled else 0
        row["updated_at"] = updated_at
        return {"success": True, "schedule": self._deserialize_row(row, redact=True)}

    def run_now(self, schedule_id: str) -> Dict[str, Any]:
        row = self._get_schedule_row(schedule_id)
        if row is None:
            return {"success": False, "error": f"Schedule '{schedule_id}' not found."}
        return self._execute_schedule_row(row)

    def _check_and_execute(self) -> None:
        if not self._run_lock.acquire(blocking=False):
            logger.info("analysis scheduler check skipped because another run is still active")
            return

        try:
            now_utc = datetime.now(timezone.utc)
            rows = self.db.execute_query(
                """
                SELECT `id`, `name`, `tables_json`, `depth`, `resource_name`, `schedule_type`,
                       `schedule_hour`, `schedule_minute`, `schedule_day_of_week`, `schedule_day_of_month`,
                       `timezone`, `delivery_json`, `enabled`, `last_run_at`, `next_run_at`,
                       `created_at`, `updated_at`
                FROM `_sys_analysis_schedules`
                WHERE `enabled` = 1
                  AND `next_run_at` <= %s
                ORDER BY `next_run_at` ASC
                """,
                (self._format_utc(now_utc),),
            )
            for row in rows:
                next_run_at = self._parse_utc(row.get("next_run_at"))
                lateness = (now_utc - next_run_at).total_seconds() if next_run_at else 0
                if lateness > 3600:
                    logger.warning("Skipping stale analysis schedule %s delayed by %.0f seconds", row.get("id"), lateness)
                    self._update_next_run_only(row["id"], self._compute_next_run(row, now_utc=now_utc))
                    continue
                self._execute_schedule_row(row, now_utc=now_utc)
        finally:
            self._run_lock.release()

    def _execute_schedule_row(self, row: Dict[str, Any], now_utc: Optional[datetime] = None) -> Dict[str, Any]:
        schedule = self._deserialize_row(row, redact=False)
        now_utc = now_utc or datetime.now(timezone.utc)
        reports = []

        for table_name in schedule["tables"]:
            report = self.agent.analyze_table(
                table_name,
                schedule["depth"],
                schedule.get("resource_name"),
                trigger_type="scheduled_analysis",
                schedule_id=schedule["id"],
            )
            reports.append(report)
            if schedule.get("delivery"):
                self._dispatch_report(report, schedule["delivery"])

        last_run_at = self._format_utc(now_utc)
        next_run_at = self._compute_next_run(schedule, now_utc=now_utc)
        self.db.execute_update(
            """
            UPDATE `_sys_analysis_schedules`
            SET `last_run_at` = %s,
                `next_run_at` = %s,
                `updated_at` = %s
            WHERE `id` = %s
            """,
            (last_run_at, next_run_at, self._format_utc(datetime.now(timezone.utc)), schedule["id"]),
        )
        row["last_run_at"] = last_run_at
        row["next_run_at"] = next_run_at
        row["updated_at"] = self._format_utc(datetime.now(timezone.utc))

        return {
            "success": True,
            "schedule": self._deserialize_row(row, redact=True),
            "count": len(reports),
            "reports": reports,
        }

    def _dispatch_report(self, report: Dict[str, Any], delivery_config: Dict[str, Any] | None) -> None:
        channels = (delivery_config or {}).get("channels") or []
        if not self.dispatcher or not channels:
            return

        coroutine = self.dispatcher.dispatch(report, delivery_config)
        if self.event_loop is not None and getattr(self.event_loop, "is_running", lambda: False)():
            future = asyncio.run_coroutine_threadsafe(coroutine, self.event_loop)

            def _consume_result(done_future):
                try:
                    done_future.result()
                except Exception as exc:
                    logger.warning("analysis dispatch failed: %s", exc)

            future.add_done_callback(_consume_result)
            return

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coroutine)
        except RuntimeError:
            asyncio.run(coroutine)

    def _compute_next_run(self, schedule_row: Dict[str, Any], now_utc: Optional[datetime] = None) -> str:
        now_utc = now_utc or datetime.now(timezone.utc)
        timezone_name = schedule_row.get("timezone") or "UTC"
        tzinfo = ZoneInfo(timezone_name)
        local_now = now_utc.astimezone(tzinfo)
        schedule_type = schedule_row.get("schedule_type") or "daily"
        minute = int(schedule_row.get("schedule_minute") or 0)
        hour = int(schedule_row.get("schedule_hour") or 0)
        day_of_week = int(schedule_row.get("schedule_day_of_week") or 1)
        day_of_month = int(schedule_row.get("schedule_day_of_month") or 1)

        if schedule_type == "hourly":
            candidate = local_now.replace(minute=minute, second=0, microsecond=0)
            if candidate <= local_now:
                candidate = candidate + timedelta(hours=1)
        elif schedule_type == "daily":
            candidate = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= local_now:
                candidate = candidate + timedelta(days=1)
        elif schedule_type == "weekly":
            target_weekday = max(1, min(day_of_week, 7)) - 1
            days_ahead = (target_weekday - local_now.weekday()) % 7
            candidate = (local_now + timedelta(days=days_ahead)).replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )
            if candidate <= local_now:
                candidate = candidate + timedelta(days=7)
        elif schedule_type == "monthly":
            candidate = self._build_monthly_candidate(local_now, day_of_month, hour, minute)
            if candidate <= local_now:
                next_month_anchor = (local_now.replace(day=28) + timedelta(days=4)).replace(day=1)
                candidate = self._build_monthly_candidate(next_month_anchor, day_of_month, hour, minute)
        else:
            raise ValueError(f"Unsupported schedule_type: {schedule_type}")

        return self._format_utc(candidate.astimezone(timezone.utc))

    def _build_monthly_candidate(self, anchor: datetime, day_of_month: int, hour: int, minute: int) -> datetime:
        if anchor.month == 12:
            next_month = anchor.replace(year=anchor.year + 1, month=1, day=1)
        else:
            next_month = anchor.replace(month=anchor.month + 1, day=1)
        last_day = (next_month - timedelta(days=1)).day
        safe_day = max(1, min(day_of_month, last_day))
        return anchor.replace(day=safe_day, hour=hour, minute=minute, second=0, microsecond=0)

    def _normalize_schedule_config(self, config: Dict[str, Any], preserve_id: bool = False) -> Dict[str, Any]:
        tables = [
            str(table_name).strip()
            for table_name in (config.get("tables") or [])
            if str(table_name).strip()
        ]
        if not tables:
            raise ValueError("tables must include at least one table name")
        for table_name in tables:
            self.db.validate_identifier(table_name)

        depth = (config.get("depth") or "standard").strip().lower()
        if depth not in {"quick", "standard", "deep"}:
            raise ValueError("depth must be one of quick, standard, deep")

        schedule_type = (config.get("schedule_type") or "").strip().lower()
        if schedule_type not in _SUPPORTED_SCHEDULE_TYPES:
            raise ValueError("schedule_type must be one of hourly, daily, weekly, monthly")

        timezone_name = config.get("timezone") or "UTC"
        ZoneInfo(timezone_name)

        now_utc = datetime.now(timezone.utc)
        schedule = {
            "id": config.get("id") if preserve_id else str(uuid.uuid4()),
            "name": (config.get("name") or "").strip() or "Untitled analysis schedule",
            "tables": tables,
            "tables_json": json.dumps(tables, ensure_ascii=False),
            "depth": depth,
            "resource_name": config.get("resource_name"),
            "schedule_type": schedule_type,
            "schedule_hour": int(config.get("schedule_hour", 8) or 0),
            "schedule_minute": int(config.get("schedule_minute", 0) or 0),
            "schedule_day_of_week": int(config.get("schedule_day_of_week", 1) or 1),
            "schedule_day_of_month": int(config.get("schedule_day_of_month", 1) or 1),
            "timezone": timezone_name,
            "delivery": deepcopy(config.get("delivery")) if config.get("delivery") else None,
            "delivery_json": self._encrypt_delivery_config(config.get("delivery")) if config.get("delivery") else None,
            "enabled": bool(config.get("enabled", True)),
            "last_run_at": config.get("last_run_at"),
            "next_run_at": config.get("next_run_at"),
            "created_at": config.get("created_at") or self._format_utc(now_utc),
            "updated_at": self._format_utc(now_utc),
        }
        if schedule["enabled"]:
            schedule["next_run_at"] = schedule.get("next_run_at") or self._compute_next_run(schedule, now_utc=now_utc)
        return schedule

    def _encrypt_delivery_config(self, config: Dict[str, Any] | None) -> Optional[str]:
        if not config:
            return None
        payload = deepcopy(config)
        return json.dumps(self._transform_delivery_fields(payload, encrypt=True), ensure_ascii=False)

    def _decrypt_delivery_config(self, config_json: str | None) -> Optional[Dict[str, Any]]:
        if not config_json:
            return None
        payload = json.loads(config_json)
        return self._transform_delivery_fields(payload, decrypt=True)

    def _redact_delivery_config(self, config: Dict[str, Any] | None) -> Optional[Dict[str, Any]]:
        if not config:
            return None
        payload = deepcopy(config)
        return self._transform_delivery_fields(payload, redact=True)

    def _transform_delivery_fields(
        self,
        payload: Any,
        *,
        encrypt: bool = False,
        decrypt: bool = False,
        redact: bool = False,
    ) -> Any:
        if isinstance(payload, list):
            return [self._transform_delivery_fields(item, encrypt=encrypt, decrypt=decrypt, redact=redact) for item in payload]
        if not isinstance(payload, dict):
            return payload

        transformed = {}
        for key, value in payload.items():
            if key in _SENSITIVE_DELIVERY_FIELDS and value:
                if encrypt:
                    if isinstance(value, str) and value.startswith("enc:"):
                        transformed[key] = value
                    else:
                        transformed[key] = f"enc:{self.cipher.encrypt(str(value).encode()).decode()}"
                elif decrypt:
                    if isinstance(value, str) and value.startswith("enc:"):
                        transformed[key] = self.cipher.decrypt(value[4:].encode()).decode()
                    else:
                        transformed[key] = value
                elif redact:
                    transformed[key] = _REDACTED_VALUE
                else:
                    transformed[key] = value
            else:
                transformed[key] = self._transform_delivery_fields(
                    value,
                    encrypt=encrypt,
                    decrypt=decrypt,
                    redact=redact,
                )
        return transformed

    def _schedule_to_params(self, schedule: Dict[str, Any]):
        return (
            schedule["id"],
            schedule["name"],
            schedule["tables_json"],
            schedule["depth"],
            schedule.get("resource_name"),
            schedule["schedule_type"],
            schedule["schedule_hour"],
            schedule["schedule_minute"],
            schedule["schedule_day_of_week"],
            schedule["schedule_day_of_month"],
            schedule["timezone"],
            schedule.get("delivery_json"),
            1 if schedule.get("enabled", True) else 0,
            schedule.get("last_run_at"),
            schedule.get("next_run_at"),
            schedule["created_at"],
            schedule["updated_at"],
        )

    def _serialize_schedule(self, schedule: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._deserialize_row(schedule, redact=True)
        return payload

    def _deserialize_row(self, row: Dict[str, Any], *, redact: bool) -> Dict[str, Any]:
        tables_json = row.get("tables_json")
        if isinstance(tables_json, str):
            tables = json.loads(tables_json)
        else:
            tables = row.get("tables") or []
        delivery = row.get("delivery")
        if delivery is None:
            delivery = self._decrypt_delivery_config(row.get("delivery_json"))
        if redact:
            delivery = self._redact_delivery_config(delivery)
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "tables": tables,
            "depth": row.get("depth") or "standard",
            "resource_name": row.get("resource_name"),
            "schedule_type": row.get("schedule_type"),
            "schedule_hour": int(row.get("schedule_hour") or 0),
            "schedule_minute": int(row.get("schedule_minute") or 0),
            "schedule_day_of_week": int(row.get("schedule_day_of_week") or 1),
            "schedule_day_of_month": int(row.get("schedule_day_of_month") or 1),
            "timezone": row.get("timezone") or "UTC",
            "delivery": delivery,
            "enabled": bool(row.get("enabled")),
            "last_run_at": row.get("last_run_at"),
            "next_run_at": row.get("next_run_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _get_schedule_row(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        rows = self.db.execute_query(
            """
            SELECT `id`, `name`, `tables_json`, `depth`, `resource_name`, `schedule_type`,
                   `schedule_hour`, `schedule_minute`, `schedule_day_of_week`, `schedule_day_of_month`,
                   `timezone`, `delivery_json`, `enabled`, `last_run_at`, `next_run_at`,
                   `created_at`, `updated_at`
            FROM `_sys_analysis_schedules`
            WHERE `id` = %s
            LIMIT 1
            """,
            (schedule_id,),
        )
        return rows[0] if rows else None

    def _update_next_run_only(self, schedule_id: str, next_run_at: str) -> None:
        self.db.execute_update(
            """
            UPDATE `_sys_analysis_schedules`
            SET `next_run_at` = %s,
                `updated_at` = %s
            WHERE `id` = %s
            """,
            (next_run_at, self._format_utc(datetime.now(timezone.utc)), schedule_id),
        )

    def _format_utc(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _parse_utc(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
