from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from metadata_analyzer import MetadataAnalyzer
from vanna_doris import VannaDoris


class FakeAgentDb:
    def __init__(self):
        self.queries = []
        self.updates = []

    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        if "_sys_table_agents" in sql:
            return [
                {
                    "table_name": "institutions",
                    "agent_config": '{"fields":{"所在市":{"semantic":"geographic-city","match":"fuzzy"}}}',
                }
            ]
        if "SELECT * FROM `institutions` LIMIT 10" in sql:
            return [{"所在市": "广州市", "机构类型": "基金会", "年份": 2022}]
        return []

    def execute_update(self, sql, params=None):
        self.updates.append((sql, params))
        return 1

    def validate_identifier(self, identifier):
        return f"`{identifier}`"

    def get_table_schema(self, table_name):
        return [
            {"Field": "所在市", "Type": "VARCHAR"},
            {"Field": "机构类型", "Type": "VARCHAR"},
            {"Field": "年份", "Type": "INT"},
        ]


class SamplePhase2Vanna(VannaDoris):
    def system_message(self, message: str):
        return {"role": "system", "content": message}

    def user_message(self, message: str):
        return {"role": "user", "content": message}

    def assistant_message(self, message: str):
        return {"role": "assistant", "content": message}


def test_agent_endpoint_returns_table_agent(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.get_agent_config = lambda table_name: {
        "table_name": table_name,
        "agent_config": {"fields": {"所在市": {"semantic": "geographic-city", "match": "fuzzy"}}},
    }

    client = TestClient(main.app)
    response = client.get("/api/agents/institutions", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["agent"]["table_name"] == "institutions"


def test_feedback_endpoint_updates_quality_gate(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.update_query_feedback_async = AsyncMock(
        return_value={"success": True, "id": 1, "quality_gate": 2}
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/query/history/1/feedback",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"quality_gate": 2},
    )

    assert response.status_code == 200
    assert response.json()["quality_gate"] == 2


def test_get_related_documentation_includes_agent_config():
    vanna = SamplePhase2Vanna(doris_client=FakeAgentDb())

    docs = vanna.get_related_documentation("广州的机构")

    assert any("geographic-city" in doc for doc in docs)


def test_refresh_agent_assets_persists_agent_config_and_field_catalog(monkeypatch):
    analyzer = MetadataAnalyzer()
    analyzer.db = FakeAgentDb()
    analyzer.get_metadata = lambda table_name: {
        "table_name": table_name,
        "description": "机构表",
        "columns_info": {"所在市": "城市", "机构类型": "类型", "年份": "年份"},
    }
    analyzer._call_llm = lambda prompt: {
        "table_description": "机构表",
        "fields": {
            "所在市": {"semantic": "geographic-city", "match": "fuzzy", "values": ["广州市"]},
            "机构类型": {"semantic": "categorical", "match": "exact", "values": ["基金会"]},
            "年份": {"semantic": "temporal-year", "match": "range"},
        },
        "cot_template": "识别字段后生成 WHERE 条件",
    }

    result = analyzer.refresh_agent_assets("institutions", "excel")

    assert result["success"] is True
    assert len(analyzer.db.updates) >= 2
