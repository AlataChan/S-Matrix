import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from analysis_scheduler import AnalysisScheduler


def _utc_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class RecordingScheduleDB:
    def __init__(self):
        self.updates = []
        self.queries = []
        self.schedules = {}

    def validate_identifier(self, identifier):
        if "!" in identifier:
            raise ValueError(f"Invalid identifier: {identifier}")
        return f"`{identifier}`"

    def execute_update(self, sql, params=None):
        self.updates.append((sql, params))
        compact_sql = " ".join(sql.split())

        if "CREATE TABLE IF NOT EXISTS `_sys_analysis_schedules`" in compact_sql:
            return 1

        if "INSERT INTO `_sys_analysis_schedules`" in compact_sql:
            row = {
                "id": params[0],
                "name": params[1],
                "tables_json": params[2],
                "depth": params[3],
                "resource_name": params[4],
                "schedule_type": params[5],
                "schedule_hour": params[6],
                "schedule_minute": params[7],
                "schedule_day_of_week": params[8],
                "schedule_day_of_month": params[9],
                "timezone": params[10],
                "delivery_json": params[11],
                "enabled": params[12],
                "last_run_at": params[13],
                "next_run_at": params[14],
                "created_at": params[15],
                "updated_at": params[16],
            }
            self.schedules[row["id"]] = row
            return 1

        if "SET `name` = %s" in compact_sql:
            schedule_id = params[-1]
            self.schedules[schedule_id].update(
                {
                    "name": params[0],
                    "tables_json": params[1],
                    "depth": params[2],
                    "resource_name": params[3],
                    "schedule_type": params[4],
                    "schedule_hour": params[5],
                    "schedule_minute": params[6],
                    "schedule_day_of_week": params[7],
                    "schedule_day_of_month": params[8],
                    "timezone": params[9],
                    "delivery_json": params[10],
                    "enabled": params[11],
                    "last_run_at": params[12],
                    "next_run_at": params[13],
                    "updated_at": params[14],
                }
            )
            return 1

        if "SET `enabled` = %s" in compact_sql:
            schedule_id = params[-1]
            self.schedules[schedule_id]["enabled"] = params[0]
            self.schedules[schedule_id]["updated_at"] = params[1]
            return 1

        if "SET `last_run_at` = %s" in compact_sql:
            schedule_id = params[-1]
            self.schedules[schedule_id]["last_run_at"] = params[0]
            self.schedules[schedule_id]["next_run_at"] = params[1]
            self.schedules[schedule_id]["updated_at"] = params[2]
            return 1

        if "SET `next_run_at` = %s" in compact_sql:
            schedule_id = params[-1]
            self.schedules[schedule_id]["next_run_at"] = params[0]
            self.schedules[schedule_id]["updated_at"] = params[1]
            return 1

        if "DELETE FROM `_sys_analysis_schedules`" in compact_sql:
            self.schedules.pop(params[0], None)
            return 1

        raise AssertionError(f"Unexpected SQL update: {sql}")

    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        compact_sql = " ".join(sql.split())

        if "WHERE `id` = %s" in compact_sql:
            schedule = self.schedules.get(params[0])
            return [dict(schedule)] if schedule else []

        if "WHERE `enabled` = 1" in compact_sql:
            due_before = params[0]
            return [
                dict(row)
                for row in sorted(self.schedules.values(), key=lambda item: item["next_run_at"] or "")
                if row.get("enabled") and row.get("next_run_at") and row["next_run_at"] <= due_before
            ]

        if "FROM `_sys_analysis_schedules` ORDER BY `created_at` DESC" in compact_sql:
            return [
                dict(row)
                for row in sorted(self.schedules.values(), key=lambda item: item["created_at"], reverse=True)
            ]

        raise AssertionError(f"Unexpected SQL query: {sql}")


class FakeAnalystAgent:
    def __init__(self):
        self.calls = []

    def analyze_table(self, table_name, depth="standard", resource_name=None, **kwargs):
        self.calls.append((table_name, depth, resource_name, kwargs))
        return {
            "success": True,
            "id": f"report-{table_name}",
            "table_names": table_name,
            "depth": depth,
            "schedule_id": kwargs.get("schedule_id"),
            "trigger_type": kwargs.get("trigger_type", "table_analysis"),
            "summary": f"analysis for {table_name}",
            "insight_count": 1,
            "anomaly_count": 0,
            "failed_step_count": 0,
            "status": "completed",
        }


class FakeDispatcher:
    def __init__(self):
        self.calls = []

    async def dispatch(self, report, delivery_config):
        self.calls.append((report, delivery_config))


def _build_scheduler():
    db = RecordingScheduleDB()
    scheduler = AnalysisScheduler(FakeAnalystAgent(), db, dispatcher=FakeDispatcher())
    return scheduler, db


def test_init_tables_creates_schedules_table():
    scheduler, db = _build_scheduler()

    assert scheduler.init_tables() is True

    ddl = db.updates[0][0]
    assert "_sys_analysis_schedules" in ddl
    assert "timezone" in ddl
    assert 'PROPERTIES ("replication_num" = "1")' in ddl


def test_create_schedule_round_trip():
    scheduler, _db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Morning revenue",
            "tables": ["sales", "orders"],
            "depth": "deep",
            "resource_name": "Deepseek",
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 30,
            "timezone": "UTC",
        }
    )

    payload = scheduler.list_schedules()

    assert created["success"] is True
    assert created["schedule"]["tables"] == ["sales", "orders"]
    assert payload["count"] == 1
    assert payload["schedules"][0]["name"] == "Morning revenue"


def test_run_now_triggers_analysis_for_each_table():
    scheduler, _db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Morning revenue",
            "tables": ["sales", "orders"],
            "depth": "quick",
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
            "delivery": {"channels": [{"type": "websocket"}]},
        }
    )

    result = scheduler.run_now(created["schedule"]["id"])

    assert result["success"] is True
    assert result["count"] == 2
    assert [call[0] for call in scheduler.agent.calls] == ["sales", "orders"]
    assert len(scheduler.dispatcher.calls) == 2
    assert all(call[3]["schedule_id"] == created["schedule"]["id"] for call in scheduler.agent.calls)


def test_toggle_schedule_disables_and_enables():
    scheduler, _db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Toggle me",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
        }
    )

    disabled = scheduler.toggle_schedule(created["schedule"]["id"])
    enabled = scheduler.toggle_schedule(created["schedule"]["id"])

    assert disabled["schedule"]["enabled"] is False
    assert enabled["schedule"]["enabled"] is True


def test_update_schedule_recalculates_next_run_when_timing_changes():
    scheduler, _db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Retime me",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
        }
    )
    original_next_run = created["schedule"]["next_run_at"]

    updated = scheduler.update_schedule(created["schedule"]["id"], {"schedule_hour": 10})

    assert updated["schedule"]["schedule_hour"] == 10
    assert updated["schedule"]["next_run_at"] != original_next_run


def test_delete_schedule_removes_schedule():
    scheduler, _db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Delete me",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
        }
    )

    deleted = scheduler.delete_schedule(created["schedule"]["id"])
    listed = scheduler.list_schedules()

    assert deleted["deleted"] is True
    assert listed["count"] == 0


def test_check_and_execute_fires_due_schedules(monkeypatch):
    scheduler, db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Due now",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
        }
    )
    db.schedules[created["schedule"]["id"]]["next_run_at"] = _utc_string(datetime.now(timezone.utc) - timedelta(minutes=2))
    monkeypatch.setattr(
        scheduler,
        "_compute_next_run",
        lambda row, now_utc=None: _utc_string(datetime.now(timezone.utc) + timedelta(days=1)),
    )

    scheduler._check_and_execute()

    assert scheduler.agent.calls
    assert db.schedules[created["schedule"]["id"]]["last_run_at"] is not None


def test_check_and_execute_skips_old_schedules(monkeypatch, caplog):
    scheduler, db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Too old",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
        }
    )
    db.schedules[created["schedule"]["id"]]["next_run_at"] = _utc_string(datetime.now(timezone.utc) - timedelta(hours=2))
    monkeypatch.setattr(
        scheduler,
        "_compute_next_run",
        lambda row, now_utc=None: _utc_string(datetime.now(timezone.utc) + timedelta(days=1)),
    )

    scheduler._check_and_execute()

    assert not scheduler.agent.calls
    assert "skipping stale analysis schedule" in caplog.text.lower()


def test_delivery_json_encryption_and_redaction():
    scheduler, db = _build_scheduler()
    created = scheduler.create_schedule(
        {
            "name": "Webhook delivery",
            "tables": ["sales"],
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
            "delivery": {
                "channels": [
                    {
                        "type": "webhook",
                        "format": "generic",
                        "webhook_url": "https://hooks.example.com/abc",
                        "webhook_token": "token-123",
                    }
                ]
            },
        }
    )

    raw_delivery = db.schedules[created["schedule"]["id"]]["delivery_json"]
    listed = scheduler.list_schedules()["schedules"][0]["delivery"]["channels"][0]

    assert "hooks.example.com" not in raw_delivery
    assert "token-123" not in raw_delivery
    assert listed["webhook_url"] == "***configured***"
    assert listed["webhook_token"] == "***configured***"


def test_compute_next_run_honors_dst_timezone():
    scheduler, _db = _build_scheduler()
    next_run = scheduler._compute_next_run(
        {
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "schedule_day_of_week": 1,
            "schedule_day_of_month": 1,
            "timezone": "America/New_York",
        },
        now_utc=datetime(2026, 3, 8, 11, 30, tzinfo=timezone.utc),
    )

    assert next_run == "2026-03-08 12:00:00"


def test_compute_next_run_monthly_clamps_invalid_day():
    scheduler, _db = _build_scheduler()
    next_run = scheduler._compute_next_run(
        {
            "schedule_type": "monthly",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "schedule_day_of_week": 1,
            "schedule_day_of_month": 31,
            "timezone": "UTC",
        },
        now_utc=datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
    )

    assert next_run == "2026-04-30 08:00:00"


def test_dispatch_from_background_thread_uses_run_coroutine_threadsafe(monkeypatch):
    scheduler, _db = _build_scheduler()
    captured = {}

    class DummyFuture:
        def add_done_callback(self, callback):
            callback(self)

        def result(self):
            return None

    def fake_run_coroutine_threadsafe(coro, loop):
        captured["loop"] = loop
        asyncio.run(coro)
        return DummyFuture()

    loop = SimpleNamespace(is_running=lambda: True)
    scheduler.set_event_loop(loop)
    monkeypatch.setattr("analysis_scheduler.asyncio.run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    scheduler._dispatch_report({"id": "report-1"}, {"channels": [{"type": "websocket"}]})

    assert captured["loop"] is loop
    assert len(scheduler.dispatcher.calls) == 1


def test_check_and_execute_skips_when_lock_is_held():
    scheduler, _db = _build_scheduler()
    scheduler._run_lock.acquire()
    try:
        scheduler._check_and_execute()
    finally:
        scheduler._run_lock.release()

    assert scheduler.agent.calls == []
