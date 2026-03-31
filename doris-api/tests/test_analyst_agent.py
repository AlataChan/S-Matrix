import httpx
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


def test_build_strategist_config_uses_default_provider_without_resource(monkeypatch):
    def build_api_config(**kwargs):
        raise AssertionError("_build_strategist_config must not depend on build_api_config")

    agent = AnalystAgent(RecordingDB(), build_api_config)
    monkeypatch.delenv("ANALYST_STRATEGIST_MODEL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_BASE_URL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "default-key")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://default.example.com")

    strategist = agent._build_strategist_config()

    assert strategist == {
        "api_key": "default-key",
        "model": "deepseek-reasoner",
        "base_url": "https://default.example.com",
    }


def test_build_strategist_config_warns_on_partial_override(monkeypatch, caplog):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    monkeypatch.setenv("ANALYST_STRATEGIST_MODEL", "custom-reasoner")
    monkeypatch.delenv("ANALYST_STRATEGIST_BASE_URL", raising=False)
    monkeypatch.delenv("ANALYST_STRATEGIST_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fallback-key")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://fallback.example.com")

    strategist = agent._build_strategist_config()

    assert "Partial ANALYST_STRATEGIST_* config" in caplog.text
    assert strategist == {
        "api_key": "fallback-key",
        "model": "deepseek-reasoner",
        "base_url": "https://fallback.example.com",
    }


def test_build_round_prompt_keeps_base_context_after_round_one():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    profile = {"row_count": 12, "sampled": False, "columns": {"amount": {"type": "DOUBLE"}}}
    stats = [{"type": "growth", "column": "amount", "growth_pct": 12.5}]
    metadata = {"table_name": "sales", "description": "sales facts"}
    compressed_history = [{"round": 1, "strategist_output": {"hypotheses": [{"id": "H1"}]}, "results": []}]

    round_two_prompt = agent._build_round_prompt(2, profile, stats, metadata, compressed_history)
    round_three_prompt = agent._build_round_prompt(3, profile, stats, metadata, compressed_history)

    for prompt in (round_two_prompt, round_three_prompt):
        assert "Metadata:" in prompt
        assert "Profile:" in prompt
        assert "Statistics:" in prompt
        assert "sales facts" in prompt
        assert '"growth_pct": 12.5' in prompt


def test_call_strategist_reasoner_uses_user_only_message_and_captures_reasoning(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    captured = {}

    class FakeStreamResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"choices":[{"delta":{"reasoning_content":"step by step"}}]}'
            yield 'data: {"choices":[{"delta":{"content":"{\\"summary\\":\\"ok\\",\\"continue\\":false}"}}]}'
            yield "data: [DONE]"

    class FakeClient:
        def __init__(self, timeout=None):
            captured["timeout"] = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def stream(self, method, url, headers=None, json=None):
            captured["method"] = method
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            return FakeStreamResponse()

    monkeypatch.setattr(httpx, "Client", FakeClient)

    payload = agent._call_strategist(
        "Investigate the data",
        {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "secret"},
    )

    assert captured["url"] == "https://api.example.com/chat/completions"
    assert captured["method"] == "POST"
    assert captured["json"]["messages"] == [{"role": "user", "content": "Investigate the data"}]
    assert captured["json"]["max_tokens"] == 8000
    assert captured["json"]["stream"] is True
    assert "temperature" not in captured["json"]
    assert captured["timeout"].read == 300
    assert payload["reasoning"] == "step by step"
    assert payload["response"]["summary"] == "ok"


def test_call_strategist_non_reasoner_avoids_generic_system_prompt(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "unused"})
    captured = {}

    class FakeStreamResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"choices":[{"delta":{"content":"{\\"summary\\":\\"ok\\",\\"continue\\":false}"}}]}'
            yield "data: [DONE]"

    class FakeClient:
        def __init__(self, timeout=None):
            captured["timeout"] = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def stream(self, method, url, headers=None, json=None):
            captured["json"] = json
            return FakeStreamResponse()

    monkeypatch.setattr(httpx, "Client", FakeClient)

    payload = agent._call_strategist(
        "Investigate the data",
        {"model": "deepseek-chat", "base_url": "https://api.example.com", "api_key": "secret"},
    )

    assert captured["json"]["messages"] == [{"role": "user", "content": "Investigate the data"}]
    assert captured["json"]["temperature"] == 0.1
    assert captured["json"]["stream"] is True
    assert captured["timeout"].read == 120
    assert payload["response"]["summary"] == "ok"


def test_detect_temporal_dimensions_returns_candidates_and_limits():
    class StatsDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            compact_sql = " ".join(sql.split())
            if "MIN(`order_date`) AS min_value" in compact_sql:
                return [
                    {
                        "min_value": "2024-01-01 00:00:00",
                        "max_value": "2026-03-31 00:00:00",
                        "non_null_count": 800,
                        "distinct_day_count": 500,
                    }
                ]
            if "MIN(`snapshot_month`) AS min_value" in compact_sql:
                return [
                    {
                        "min_value": "2024-01-01",
                        "max_value": "2026-03-01",
                        "non_null_count": 24,
                        "distinct_day_count": 24,
                    }
                ]
            return []

    agent = AnalystAgent(StatsDB(), lambda **kwargs: {"api_key": "key"})
    dimensions = agent._detect_temporal_dimensions(
        "`sales`",
        {
            "row_count": 1000,
            "sampled": False,
            "sample_size": 1000,
            "columns": {
                "order_date": {"type": "DATETIME", "null_rate": 0.2},
                "snapshot_month": {"type": "DATE", "null_rate": 0.0},
                "revenue": {"type": "DOUBLE"},
            },
        },
    )

    assert dimensions[0]["column"] == "order_date"
    assert dimensions[0]["candidate_grains"] == ["day", "week", "month", "quarter", "year"]
    assert dimensions[0]["recommended_grains"] == ["month", "quarter"]
    assert dimensions[0]["time_window_limits"]["month"] == 28
    assert dimensions[1]["column"] == "snapshot_month"
    assert dimensions[1]["candidate_grains"] == ["month", "quarter", "year"]
    assert dimensions[1]["recommended_grains"] == ["month", "quarter"]


def test_compute_statistical_facts_includes_temporal_dimension_candidates():
    class StatsDB(RecordingDB):
        def execute_query(self, sql, params=None):
            self.queries.append((sql, params))
            compact_sql = " ".join(sql.split())
            if "NTILE(10) OVER (ORDER BY `revenue` DESC)" in compact_sql:
                return [{"top_share_pct": 37.5}]
            if "COUNT(*) AS outlier_count" in compact_sql and "`revenue`" in compact_sql:
                assert params == (30.0, 70.0)
                return [{"outlier_count": 4}]
            if "CORR(`revenue`, `profit`)" in compact_sql:
                return [{"correlation_value": 0.82}]
            return []

    agent = AnalystAgent(StatsDB(), lambda **kwargs: {"api_key": "key"})
    facts = agent._compute_statistical_facts(
        "`sales`",
        {
            "row_count": 200,
            "sampled": False,
            "sample_size": 200,
            "columns": {
                "order_date": {"type": "DATETIME"},
                "revenue": {"type": "DOUBLE", "avg": 50.0, "stddev": 10.0, "min": 10.0, "max": 100.0},
                "profit": {"type": "DOUBLE", "avg": 5.0, "stddev": 2.0, "min": 0.0, "max": 15.0},
            },
        },
        temporal_dimensions=[
            {
                "column": "order_date",
                "candidate_grains": ["week", "month", "quarter"],
                "recommended_grains": ["month", "quarter"],
                "time_window_limits": {"week": 52, "month": 24, "quarter": 8},
                "span_days": 420,
                "density_ratio": 0.71,
                "null_rate": 0.05,
            }
        ],
    )

    assert any(fact.get("type") == "temporal_dimension" and fact.get("column") == "order_date" for fact in facts)
    assert any(fact.get("type") == "concentration" and fact.get("column") == "revenue" for fact in facts)
    assert any(fact.get("type") == "outlier" and fact.get("column") == "revenue" for fact in facts)
    assert any(fact.get("type") == "correlation" and fact.get("columns") == ["revenue", "profit"] for fact in facts)


def test_build_round_prompt_requests_temporal_plans_from_candidates():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    prompt = agent._build_round_prompt(
        1,
        {"row_count": 500},
        [
            {
                "type": "temporal_dimension",
                "column": "order_date",
                "candidate_grains": ["week", "month", "quarter"],
                "recommended_grains": ["month", "quarter"],
                "time_window_limits": {"week": 52, "month": 24, "quarter": 8},
            }
        ],
        {"table_name": "sales"},
        [],
    )

    assert "time_plan" in prompt
    assert "candidate_grains" in prompt
    assert "lookback_periods" in prompt
    assert "choose at most 1-2 temporal analysis plans" in prompt.lower()


def test_extract_queries_from_strategist_preserves_time_plan():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    queries = agent._extract_queries_from_strategist(
        {
            "response": {
                "hypotheses": [
                    {
                        "id": "H1",
                        "title": "收入趋势",
                        "query_description": "查看收入变化趋势",
                        "time_plan": {
                            "time_column": "order_date",
                            "grain": "month",
                            "analysis_type": "trend",
                            "comparison_mode": "mom",
                            "lookback_periods": 18,
                        },
                    }
                ]
            }
        },
        1,
    )

    assert queries[0]["time_plan"]["time_column"] == "order_date"
    assert queries[0]["time_plan"]["grain"] == "month"


def test_executor_translate_to_sql_includes_temporal_constraints(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})
    captured = {}

    def fake_call_chat_completion(system_prompt, user_prompt, api_config):
        captured["prompt"] = user_prompt
        return "SELECT DATE_FORMAT(`order_date`, '%Y-%m') AS period, COUNT(*) AS metric_value FROM `sales` GROUP BY DATE_FORMAT(`order_date`, '%Y-%m')"

    monkeypatch.setattr(agent, "_call_chat_completion", fake_call_chat_completion)

    sql = agent._executor_translate_to_sql(
        {
            "title": "收入趋势",
            "query_description": "分析月度收入趋势",
            "time_plan": {
                "time_column": "order_date",
                "grain": "month",
                "analysis_type": "trend",
                "comparison_mode": "mom",
                "lookback_periods": 40,
            },
        },
        {
            "table_name": "sales",
            "temporal_dimensions": [
                {
                    "column": "order_date",
                    "candidate_grains": ["week", "month", "quarter"],
                    "recommended_grains": ["month", "quarter"],
                    "time_window_limits": {"week": 52, "month": 24, "quarter": 8},
                }
            ],
        },
        {"api_key": "key"},
    )

    assert "Selected time plan" in captured["prompt"]
    assert '"lookback_periods": 24' in captured["prompt"]
    assert "Only use candidate time columns" in captured["prompt"]
    assert sql.endswith("LIMIT 100")


def test_executor_translate_to_sql_falls_back_when_llm_uses_disallowed_time_column(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    monkeypatch.setattr(
        agent,
        "_call_chat_completion",
        lambda system_prompt, user_prompt, api_config: (
            "SELECT DATE_FORMAT(`created_at`, '%Y-%m') AS period, COUNT(*) AS metric_value "
            "FROM `sales` GROUP BY DATE_FORMAT(`created_at`, '%Y-%m')"
        ),
    )

    sql = agent._executor_translate_to_sql(
        {
            "title": "收入趋势",
            "query_description": "分析月度收入趋势",
            "time_plan": {
                "time_column": "order_date",
                "grain": "month",
                "analysis_type": "trend",
                "comparison_mode": "mom",
                "lookback_periods": 40,
            },
        },
        {
            "table_name": "sales",
            "temporal_dimensions": [
                {
                    "column": "order_date",
                    "candidate_grains": ["week", "month", "quarter"],
                    "recommended_grains": ["month", "quarter"],
                    "time_window_limits": {"week": 52, "month": 24, "quarter": 8},
                },
                {
                    "column": "created_at",
                    "candidate_grains": ["day", "week", "month"],
                    "recommended_grains": ["week", "month"],
                    "time_window_limits": {"day": 90, "week": 52, "month": 24},
                },
            ],
        },
        {"api_key": "key"},
    )

    assert "`order_date`" in sql
    assert "`created_at`" not in sql
    assert "INTERVAL 24 MONTH" in sql


def test_analyze_table_expert_falls_back_to_deep_on_strategist_failure(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile, temporal_dimensions=None: [])
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: (_ for _ in ()).throw(RuntimeError("R1 unavailable")))

    captured = {}

    def fake_standard_analyze(self, table_name, depth="standard", resource_name=None, **kwargs):
        captured["table_name"] = table_name
        captured["depth"] = depth
        captured["resource_name"] = resource_name
        captured["kwargs"] = kwargs
        return {"success": True, "depth": depth, "summary": "fallback"}

    monkeypatch.setattr(AnalystAgent, "analyze_table", fake_standard_analyze)

    report = agent.analyze_table_expert("sales", resource_name="OpenAI", trigger_type="scheduled_analysis", schedule_id="schedule-1")

    assert report["depth"] == "deep"
    assert captured["table_name"] == "sales"
    assert captured["depth"] == "deep"
    assert captured["resource_name"] == "OpenAI"
    assert captured["kwargs"]["trigger_type"] == "scheduled_analysis"
    assert captured["kwargs"]["schedule_id"] == "schedule-1"


def test_analyze_table_expert_validates_strategist_api_key_before_call(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "executor-key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile, temporal_dimensions=None: [])
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": None})

    called = {"strategist": False}

    def fake_call_strategist(prompt, strategist_config):
        called["strategist"] = True
        raise AssertionError("strategist should not be called without api_key")

    monkeypatch.setattr(agent, "_call_strategist", fake_call_strategist)

    captured = {}

    def fake_standard_analyze(self, table_name, depth="standard", resource_name=None, **kwargs):
        captured["table_name"] = table_name
        captured["depth"] = depth
        return {"success": True, "depth": depth}

    monkeypatch.setattr(AnalystAgent, "analyze_table", fake_standard_analyze)

    report = agent.analyze_table_expert("sales", resource_name="OpenAI")

    assert called["strategist"] is False
    assert report["depth"] == "deep"
    assert captured["table_name"] == "sales"


def test_analyze_table_expert_runs_multiple_rounds_and_truncates_reasoning(monkeypatch):
    db = RecordingDB()
    agent = AnalystAgent(db, lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setenv("ANALYST_MAX_REASONING_CHARS", "10")
    monkeypatch.setenv("ANALYST_MAX_ROUNDS", "3")

    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name, "description": "sales facts"})
    monkeypatch.setattr(
        agent,
        "_compute_statistical_facts",
        lambda safe_name, profile, temporal_dimensions=None: [{"title": "rows", "value": 10}],
    )
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "strategist-key"})

    prompts = []

    def fake_build_round_prompt(round_num, profile, stats, metadata, compressed_history):
        prompts.append((round_num, compressed_history))
        return f"round {round_num}"

    monkeypatch.setattr(agent, "_build_round_prompt", fake_build_round_prompt)

    strategist_outputs = iter(
        [
            {
                "reasoning": "1234567890ABCDEFGHIJ",
                "response": {
                    "hypotheses": [{"id": "H1", "title": "Volume", "query_description": "Count rows"}],
                    "continue": True,
                },
            },
            {
                "reasoning": "second-round-reasoning",
                "response": {
                    "summary": "done",
                    "findings": [],
                    "recommendations": [],
                    "limitations": [],
                    "continue": False,
                },
            },
        ]
    )
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: next(strategist_outputs))
    monkeypatch.setattr(
        agent,
        "_extract_queries_from_strategist",
        lambda strategist_output, round_num: strategist_output["response"].get("hypotheses", []),
    )
    monkeypatch.setattr(agent, "_executor_translate_to_sql", lambda query, metadata, api_config: "SELECT 1 AS total")
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 1}] * 150, sql, True, None),
    )

    captured = {}

    def fake_build_expert_report(**kwargs):
        captured.update(kwargs)
        return {
            "success": True,
            "id": "expert-1",
            "table_names": "sales",
            "depth": "expert",
            "summary": "done",
            "insight_count": 1,
            "anomaly_count": 0,
            "failed_step_count": 0,
            "status": "completed",
            "reasoning_traces": kwargs["reasoning_traces"],
            "conversation_chain": kwargs["compressed_history"],
            "steps": kwargs["all_step_results"],
        }

    monkeypatch.setattr(agent, "_build_expert_report", fake_build_expert_report)

    report = agent.analyze_table("sales", depth="expert", resource_name="OpenAI")

    assert report["depth"] == "expert"
    assert prompts[0] == (1, [])
    assert len(prompts[1][1]) == 1
    assert captured["all_step_results"][0]["row_count"] == 100
    assert len(captured["all_step_results"][0]["data"]) == 100
    assert captured["reasoning_traces"][0]["trace"].startswith("1234567890")
    assert "truncated" in captured["reasoning_traces"][0]["trace"]


def test_analyze_table_expert_supports_three_round_synthesis_without_followup_queries(monkeypatch):
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key", "model": "chat", "base_url": "https://api.example.com"})
    monkeypatch.setenv("ANALYST_MAX_ROUNDS", "3")
    monkeypatch.setattr(
        agent,
        "_profile_table",
        lambda table_name, safe_table_name=None: {"row_count": 10, "sampled": False, "sample_size": 10, "columns": {}},
    )
    monkeypatch.setattr(agent, "_get_table_metadata", lambda table_name: {"table_name": table_name})
    monkeypatch.setattr(agent, "_compute_statistical_facts", lambda safe_name, profile, temporal_dimensions=None: [])
    monkeypatch.setattr(agent, "_build_strategist_config", lambda: {"model": "deepseek-reasoner", "base_url": "https://api.example.com", "api_key": "strategist-key"})

    strategist_outputs = iter(
        [
            {
                "reasoning": "",
                "response": {
                    "hypotheses": [{"id": "H1", "title": "Volume", "query_description": "Count rows"}],
                    "continue": True,
                },
            },
            {
                "reasoning": "",
                "response": {
                    "assessments": [{"hypothesis_id": "H1", "verdict": "confirmed"}],
                    "follow_ups": [],
                    "continue": True,
                },
            },
            {
                "reasoning": "",
                "response": {
                    "summary": "final synthesis",
                    "findings": [{"title": "Volume stable", "hypothesis_id": "H1"}],
                    "anomalies": [],
                    "recommendations": [],
                    "limitations": [],
                    "continue": False,
                },
            },
        ]
    )
    monkeypatch.setattr(agent, "_call_strategist", lambda prompt, strategist_config: next(strategist_outputs))
    monkeypatch.setattr(
        agent,
        "_extract_queries_from_strategist",
        lambda strategist_output, round_num: strategist_output["response"].get("hypotheses", []),
    )
    monkeypatch.setattr(agent, "_executor_translate_to_sql", lambda query, metadata, api_config: "SELECT 1 AS total")
    monkeypatch.setattr(
        agent,
        "_execute_with_repair",
        lambda sql, question_context, api_config: ([{"total": 1}], sql, True, None),
    )

    report = agent.analyze_table_expert("sales")

    assert report["summary"].startswith("final synthesis")
    assert len(report["conversation_chain"]) == 3
    assert report["steps"][0]["round"] == 1


def test_build_evidence_chains_scopes_history_per_finding():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    chains = agent._build_evidence_chains(
        [
            {"title": "Revenue decline", "hypothesis_id": "H1"},
            {"title": "Profit stable", "hypothesis_id": "H2"},
        ],
        [
            {
                "round": 1,
                "strategist_output": {
                    "hypotheses": [
                        {"id": "H1", "title": "Revenue"},
                        {"id": "H2", "title": "Profit"},
                    ]
                },
                "results": [],
            },
            {
                "round": 2,
                "strategist_output": {
                    "assessments": [
                        {"hypothesis_id": "H1", "verdict": "confirmed"},
                        {"hypothesis_id": "H2", "verdict": "refuted"},
                    ],
                    "follow_ups": [
                        {"id": "F1", "hypothesis_id": "H1", "reason": "Verify revenue"},
                        {"id": "F2", "hypothesis_id": "H2", "reason": "Verify profit"},
                    ],
                },
                "results": [],
            },
        ],
    )

    assert chains[0]["hypotheses"] == [{"id": "H1", "title": "Revenue"}]
    assert chains[0]["assessments"] == [{"hypothesis_id": "H1", "verdict": "confirmed"}]
    assert chains[0]["follow_ups"] == [{"id": "F1", "hypothesis_id": "H1", "reason": "Verify revenue"}]
    assert chains[1]["hypotheses"] == [{"id": "H2", "title": "Profit"}]
    assert chains[1]["assessments"] == [{"hypothesis_id": "H2", "verdict": "refuted"}]
    assert chains[1]["follow_ups"] == [{"id": "F2", "hypothesis_id": "H2", "reason": "Verify profit"}]


def test_build_expert_report_normalizes_findings_into_readable_insights():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    report = agent._build_expert_report(
        table_names=["warehouse_stock_in_items"],
        profile={"row_count": 608, "sampled": False, "sample_size": 608, "columns": {}},
        compressed_history=[
            {
                "round": 3,
                "strategist_output": {
                    "summary": (
                        "分析遵循 descriptive -> diagnostic -> predictive 方法论。"
                        "当前运营存在显著供应链集中风险与数据治理缺口。"
                    ),
                    "findings": [
                        (
                            '{"category":"供应链风险","description":"供应商集中度过高，单一供应商覆盖核心门店。",'
                            '"quantification":"核心供应商覆盖6家门店与4个仓库。","recommendation":"优先推进替代供应商。"}'
                        ),
                        {
                            "category": "库存价值集中",
                            "description": "Top 10% 商品贡献 67.02% 的总入库金额。",
                            "quantification": "高价值商品波动明显。",
                        },
                    ],
                    "recommendations": ["优先推进替代供应商。"],
                    "anomalies": [],
                    "limitations": [],
                    "root_causes": [],
                    "confidence_overall": 0.82,
                    "continue": False,
                },
                "results": [],
            }
        ],
        reasoning_traces=[],
        all_step_results=[],
        trigger_type="table_analysis",
        started_at=0.0,
    )

    assert "descriptive -> diagnostic -> predictive" not in report["summary"]
    assert "供应链集中风险" in report["summary"]
    assert report["executive_summary"] == report["summary"]
    assert len(report["top_insights"]) == 2
    assert report["top_insights"][0]["title"] == "供应链风险"
    assert report["top_insights"][1]["title"] == "库存价值集中"
    assert report["action_items"][0]["title"] == "动作建议 1"
    assert "优先推进替代供应商" in report["action_items"][0]["detail"]
    assert report["insights"][0]["title"] == "供应链风险"
    assert "供应商集中度过高" in report["insights"][0]["detail"]
    assert "核心供应商覆盖6家门店与4个仓库" in report["insights"][0]["detail"]
    assert report["insights"][1]["title"] == "库存价值集中"


def test_build_evidence_chains_uses_readable_label_when_finding_has_no_title():
    agent = AnalystAgent(RecordingDB(), lambda **kwargs: {"api_key": "key"})

    chains = agent._build_evidence_chains(
        [
            {
                "category": "供应链风险",
                "description": "供应商集中度极高",
                "hypothesis_id": "H1",
            }
        ],
        [
            {
                "round": 1,
                "strategist_output": {"hypotheses": [{"id": "H1", "title": "供应商集中"}]},
                "results": [],
            }
        ],
    )

    assert chains[0]["finding"] == "供应链风险"


def test_get_report_and_latest_report_hide_reasoning_by_default():
    class ReportDB(RecordingDB):
        def execute_query(self, sql, params=None):
            payload = {
                "success": True,
                "id": "report-1",
                "table_names": "sales",
                "reasoning_traces": [{"round": 1, "trace": "secret"}],
            }
            return [{"report_json": payload}]

    agent = AnalystAgent(ReportDB(), lambda **kwargs: {"api_key": "key"})

    detail = agent.get_report("report-1")
    with_reasoning = agent.get_report("report-1", include_reasoning=True)
    latest = agent.get_latest_report("sales")

    assert "reasoning_traces" not in detail
    assert with_reasoning["reasoning_traces"][0]["trace"] == "secret"
    assert "reasoning_traces" not in latest


def test_get_report_hydrates_fixed_expert_sections_for_legacy_reports():
    class ReportDB(RecordingDB):
        def execute_query(self, sql, params=None):
            payload = {
                "success": True,
                "id": "report-legacy",
                "table_names": "warehouse_stock_in_items",
                "depth": "expert",
                "summary": "分析遵循 descriptive -> diagnostic -> predictive 方法论。库存风险集中。",
                "insights": [
                    '{"category":"供应链风险","description":"单一供应商覆盖核心门店。","recommendation":"引入备份供应商。"}',
                    {"category": "库存价值集中", "description": "高价值商品波动明显。"},
                    {"category": "数据治理缺口", "description": "关键字段缺失。"},
                    {"category": "额外洞察", "description": "不应进入 top 3。"},
                ],
                "recommendations": [
                    "优先补齐主数据字段。",
                    {"title": "供应链韧性", "detail": "建立第二供应源。"},
                    "优化高价值库存阈值。",
                    "超出上限的建议。",
                ],
                "reasoning_traces": [{"round": 1, "trace": "secret"}],
            }
            return [{"report_json": payload}]

    agent = AnalystAgent(ReportDB(), lambda **kwargs: {"api_key": "key"})

    report = agent.get_report("report-legacy")

    assert report["executive_summary"] == "库存风险集中。"
    assert len(report["top_insights"]) == 3
    assert [item["title"] for item in report["top_insights"]] == ["供应链风险", "库存价值集中", "数据治理缺口"]
    assert len(report["action_items"]) == 3
    assert report["action_items"][0]["title"] == "动作建议 1"
    assert "优先补齐主数据字段" in report["action_items"][0]["detail"]
    assert report["action_items"][1]["title"] == "供应链韧性"
    assert report["recommendations"][1] == "供应链韧性：建立第二供应源。"
