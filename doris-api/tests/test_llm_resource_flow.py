from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main


def test_list_llm_configs_returns_normalized_resource_fields(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.doris_client.execute_query = lambda sql, params=None: [
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.provider_type", "Value": "DEEPSEEK"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.model_name", "Value": "deepseek-chat"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.endpoint", "Value": "https://api.deepseek.com/chat/completions"},
        {"Name": "Deepseek", "ResourceType": "ai", "Item": "ai.api_key", "Value": "******"},
    ]

    client = TestClient(main.app)
    response = client.get("/api/llm/config", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["resources"][0]["name"] == "Deepseek"
    assert payload["resources"][0]["provider"] == "DEEPSEEK"
    assert payload["resources"][0]["model"] == "deepseek-chat"
    assert payload["resources"][0]["endpoint"] == "https://api.deepseek.com/chat/completions"
    assert payload["resources"][0]["api_key_configured"] is True


def test_natural_query_route_uses_selected_resource_config(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-deepseek-key")
    captured = {}

    main.resolve_llm_resource_config = lambda resource_name=None: {
        "resource_name": resource_name,
        "provider": "DEEPSEEK",
        "model": "deepseek-chat",
        "base_url": "https://api.deepseek.com",
        "endpoint": "https://api.deepseek.com/chat/completions",
        "api_key_configured": True,
    }
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "institutions"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    main.PlannerAgent = type(
        "FakePlannerAgent",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "plan": lambda self, question: {
            "intent": "count",
            "tables": ["institutions"],
            "subtasks": [{"table": "institutions", "question": question}],
            "needs_join": False,
        }},
    )
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: (
                captured.setdefault("api_config", dict(api_config or {})),
                "SELECT COUNT(*) AS total FROM `institutions`",
            )[1],
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "coordinate": lambda self, plan, sql_map, relationships=None: next(iter(sql_map.values()))},
    )
    main.VannaDorisOpenAI = type(
        "FakeHistoryVanna",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "add_question_sql": lambda self, *args, **kwargs: {"status": "stored", "id": "history-1"},
        },
    )
    main.RepairAgent = type("FakeRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 1}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？", "resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    assert captured["api_config"]["resource_name"] == "Deepseek"
    assert captured["api_config"]["model"] == "deepseek-chat"
    assert captured["api_config"]["base_url"] == "https://api.deepseek.com"
    assert captured["api_config"]["api_key"] == "env-deepseek-key"
