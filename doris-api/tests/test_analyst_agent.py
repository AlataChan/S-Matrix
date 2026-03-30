import pytest

from analyst_agent import AnalystAgent
import analyst_agent as analyst_agent_module


class RecordingDB:
    def __init__(self):
        self.queries = []
        self.updates = []

    def validate_identifier(self, identifier):
        if "!" in identifier:
            raise ValueError(f"Invalid identifier: {identifier}")
        return f"`{identifier}`"

    def get_table_schema(self, table_name):
        return []

    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        return []

    def execute_update(self, sql, params=None):
        self.updates.append((sql, params))
        return 1


def test_init_tables_creates_reports_table():
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {})

    assert agent.init_tables() is True

    ddl = db.updates[0][0]
    assert "_sys_analysis_reports" in ddl
    assert "UNIQUE KEY(`id`)" in ddl
    assert 'PROPERTIES ("replication_num" = "1")' in ddl


def test_profile_large_table_uses_sampling():
    class ProfilingDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [
                {"Field": "amount", "Type": "INT"},
                {"Field": "city", "Type": "VARCHAR(50)"},
            ]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 250000}]
            if "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 99, "avg_value": 40.5, "stddev_value": 12.3, "null_count": 5}]
            if "COUNT(DISTINCT `city`)" in sql:
                return [{"unique_count": 3, "null_count": 0}]
            if "GROUP BY `city`" in sql:
                return [{"value": "Shanghai", "count": 10}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(ProfilingDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is True
    assert profile["columns"]["amount"]["max"] == 99
    assert profile["columns"]["city"]["top_values"][0]["value"] == "Shanghai"
    assert any("TABLESAMPLE" in sql for sql, _ in agent.db.queries)


def test_profile_small_table_skips_sampling():
    class SmallTableDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [{"Field": "amount", "Type": "INT"}]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 12}]
            if "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 12, "avg_value": 6.0, "stddev_value": 3.4, "null_count": 0}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(SmallTableDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is False
    assert profile["sample_size"] == 12
    assert all("TABLESAMPLE" not in sql and "ORDER BY RAND()" not in sql for sql, _ in agent.db.queries)


def test_profile_large_table_falls_back_when_tablesample_is_unsupported():
    class FallbackDB(RecordingDB):
        def get_table_schema(self, table_name):
            return [{"Field": "amount", "Type": "INT"}]

        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "COUNT(*) AS row_count" in sql:
                return [{"row_count": 250000}]
            if "TABLESAMPLE" in sql:
                raise Exception("syntax error near TABLESAMPLE")
            if "ORDER BY RAND()" in sql and "MIN(`amount`)" in sql:
                return [{"min_value": 1, "max_value": 99, "avg_value": 40.5, "stddev_value": 12.3, "null_count": 5}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    agent = AnalystAgent(FallbackDB(), lambda **kwargs: {"api_key": "env-key"})

    profile = agent._profile_table("sales")

    assert profile["sampled"] is True
    assert any("ORDER BY RAND()" in sql for sql, _ in agent.db.queries)


def test_execute_with_repair_fixes_bad_sql(monkeypatch):
    class RepairingDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if len(self.queries) == 1:
                raise Exception("syntax error")
            return [{"total": 3}]

    class FakeRepairAgent:
        def __init__(self, *args, **kwargs):
            pass

        def repair_sql(self, question, failed_sql, error_message, ddl_list, api_config=None):
            assert question == "How many rows?"
            assert failed_sql == "SELECT BROKEN"
            assert "syntax error" in error_message
            return "SELECT 3 AS total"

    monkeypatch.setattr(analyst_agent_module, "RepairAgent", FakeRepairAgent)
    agent = AnalystAgent(RepairingDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    data, final_sql, success, error_message = agent._execute_with_repair(
        "SELECT BROKEN",
        "How many rows?",
        {"api_key": "key", "model": "model", "base_url": "https://example.com"},
    )

    assert success is True
    assert error_message is None
    assert final_sql == "SELECT 3 AS total"
    assert data == [{"total": 3}]


def test_analyze_table_rejects_invalid_identifier():
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    with pytest.raises(ValueError):
        agent.analyze_table("bad!name")


def test_analyze_table_passes_safe_name_to_profile(monkeypatch):
    class CountingDB(RecordingDB):
        def __init__(self):
            super().__init__()
            self.validate_calls = []

        def validate_identifier(self, identifier):
            self.validate_calls.append(identifier)
            return super().validate_identifier(identifier)

    db = CountingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    captured = {}

    def fake_profile(table_name, safe_table_name=None):
        captured["table_name"] = table_name
        captured["safe_table_name"] = safe_table_name
        return {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}}

    monkeypatch.setattr(agent, "_profile_table", fake_profile)
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(
        agent,
        "_plan_analysis",
        lambda profile, metadata, depth, api_config: [{"title": "Volume", "question": "How many rows?", "sql": "SELECT COUNT(*) AS total FROM `sales`"}],
    )
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {"summary": "ok", "insights": [], "anomalies": [], "recommendations": []},
    )

    agent.analyze_table("sales", depth="quick", resource_name="Deepseek")

    assert db.validate_calls == ["sales"]
    assert captured == {"table_name": "sales", "safe_table_name": "`sales`"}


def test_analyze_table_produces_report(monkeypatch):
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})

    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(
        agent,
        "_plan_analysis",
        lambda profile, metadata, depth, api_config: [
            {"title": "Volume", "question": "How many rows?", "sql": "SELECT COUNT(*) AS total FROM `sales`"}
        ],
    )
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {
            "summary": "Sales volume is stable.",
            "insights": [{"title": "Stable volume", "detail": "10 rows returned"}],
            "anomalies": [],
            "recommendations": ["Monitor weekly growth."],
        },
    )

    report = agent.analyze_table("sales", depth="quick", resource_name="Deepseek")

    assert report["success"] is True
    assert report["table_names"] == "sales"
    assert report["insight_count"] == 1
    assert report["failed_step_count"] == 0
    assert any("_sys_analysis_reports" in sql for sql, _ in db.updates)


def test_generate_insights_returns_fallback_on_llm_failure(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    monkeypatch.setattr(
        agent,
        "_call_json_completion",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("LLM unavailable")),
    )

    insights = agent._generate_insights(
        step_results=[{"title": "Volume", "success": True, "data": [{"total": 10}]}],
        profile={"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
        api_config={"api_key": "key", "model": "model", "base_url": "https://example.com"},
    )

    assert insights["summary"] == "Analysis completed with limited automated insights."
    assert insights["insights"] == []
    assert insights["anomalies"] == []


def test_replay_from_history_reuses_saved_query(monkeypatch):
    class ReplayDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            if "FROM `_sys_query_history`" in sql:
                return [{
                    "id": "history-1",
                    "question": "How many sales?",
                    "sql": "SELECT COUNT(*) AS total FROM `sales`",
                    "table_names": "sales",
                }]
            return []

    agent = AnalystAgent(ReplayDB(), lambda **kwargs: {"api_key": "key", "model": "model", "base_url": "https://example.com"})
    monkeypatch.setattr(agent, "_profile_table", lambda table_name: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}})
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 10}], sql, True, None),
    )
    monkeypatch.setattr(
        agent,
        "_generate_insights",
        lambda step_results, profile, api_config: {
            "summary": "Replay succeeded.",
            "insights": [{"title": "Current total", "detail": "10 rows"}],
            "anomalies": [],
            "recommendations": [],
        },
    )

    report = agent.replay_from_history("history-1", resource_name="Deepseek")

    assert report["success"] is True
    assert report["history_id"] == "history-1"
    assert report["trigger_type"] == "history_replay"
    assert "current data" in report["note"].lower()
