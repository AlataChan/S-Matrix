import asyncio

import pytest
from fastapi.testclient import TestClient

from conftest import reload_main


class FakeAnalystAgent:
    def __init__(self):
        self.calls = []

    def analyze_table(self, table_name, depth="standard", resource_name=None):
        self.calls.append(("analyze_table", table_name, depth, resource_name))
        return {
            "success": True,
            "id": "report-1",
            "table_names": table_name,
            "depth": depth,
            "summary": "analysis ready",
            "insight_count": 1,
            "anomalies": [],
            "insights": [{"title": "Insight", "detail": "Example"}],
        }

    def replay_from_history(self, history_id, resource_name=None):
        self.calls.append(("replay_from_history", history_id, resource_name))
        return {
            "success": True,
            "id": "report-2",
            "history_id": history_id,
            "trigger_type": "history_replay",
            "note": "Replayed against current data.",
        }

    def list_reports(self, table_name=None, limit=20, offset=0):
        self.calls.append(("list_reports", table_name, limit, offset))
        return {
            "success": True,
            "reports": [{"id": "report-1", "table_names": "sales"}],
            "count": 1,
            "limit": limit,
            "offset": offset,
        }

    def get_report(self, report_id):
        self.calls.append(("get_report", report_id))
        return {"success": True, "id": report_id, "table_names": "sales"}

    def delete_report(self, report_id):
        self.calls.append(("delete_report", report_id))
        return {"success": True, "deleted": True, "id": report_id}

    def get_latest_report(self, table_name):
        self.calls.append(("get_latest_report", table_name))
        return {"success": True, "id": "latest-1", "table_names": table_name}


class FakeAnalysisScheduler:
    def __init__(self):
        self.calls = []

    def create_schedule(self, config):
        self.calls.append(("create_schedule", config))
        return {"success": True, "schedule": {"id": "schedule-1", **config}}

    def list_schedules(self):
        self.calls.append(("list_schedules",))
        return {"success": True, "count": 1, "schedules": [{"id": "schedule-1", "name": "Daily sales"}]}

    def update_schedule(self, schedule_id, config):
        self.calls.append(("update_schedule", schedule_id, config))
        return {"success": True, "schedule": {"id": schedule_id, **config}}

    def delete_schedule(self, schedule_id):
        self.calls.append(("delete_schedule", schedule_id))
        return {"success": True, "deleted": True, "id": schedule_id}

    def run_now(self, schedule_id):
        self.calls.append(("run_now", schedule_id))
        return {"success": True, "count": 2, "reports": [{"id": "report-1"}, {"id": "report-2"}]}

    def toggle_schedule(self, schedule_id):
        self.calls.append(("toggle_schedule", schedule_id))
        return {"success": True, "schedule": {"id": schedule_id, "enabled": False}}


def test_build_api_config_with_env_key(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("DEEPSEEK_API_KEY", "env-key")
    monkeypatch.setenv("DEEPSEEK_MODEL", "env-model")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://env.example.com")
    main.resolve_llm_resource_config = lambda resource_name=None: {
        "resource_name": resource_name,
        "provider": "DEEPSEEK",
        "model": "resource-model",
        "base_url": "https://resource.example.com",
        "endpoint": "https://resource.example.com/chat/completions",
    }

    config = main.build_api_config("Deepseek")

    assert config["api_key"] == "env-key"
    assert config["model"] == "resource-model"
    assert config["base_url"] == "https://resource.example.com"
    assert config["resource_name"] == "Deepseek"


def test_build_api_config_without_key_returns_none(monkeypatch):
    main = reload_main()
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    main.resolve_llm_resource_config = lambda resource_name=None: None

    config = main.build_api_config()

    assert config["api_key"] is None
    assert config["model"] == "deepseek-chat"
    assert config["base_url"] == "https://api.deepseek.com"


def test_app_uses_lifespan_context_manager():
    main = reload_main()

    assert main.app.router.lifespan_context is main.lifespan


def test_llm_config_request_uses_configdict_for_protected_namespaces():
    main = reload_main()

    assert main.LLMConfigRequest.model_config["protected_namespaces"] == ()


def test_analysis_table_endpoint_round_trip(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/sales",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick", "resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert main.analyst_agent.calls[0] == ("analyze_table", "sales", "quick", "Deepseek")


def test_analysis_replay_endpoint_documents_current_data(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/replay/history-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"resource_name": "Deepseek"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["history_id"] == "history-1"
    assert "current data" in payload["note"].lower()


def test_analysis_reports_list_supports_filters(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports?table_names=sales&limit=10&offset=5",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("list_reports", "sales", 10, 5)
    assert response.json()["count"] == 1


def test_analysis_report_detail_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/report-1",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_report", "report-1")
    assert response.json()["id"] == "report-1"


def test_analysis_report_delete_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.delete(
        "/api/analysis/reports/report-1",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("delete_report", "report-1")
    assert response.json()["deleted"] is True


def test_analysis_latest_report_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get(
        "/api/analysis/reports/latest/sales",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert main.analyst_agent.calls[0] == ("get_latest_report", "sales")
    assert response.json()["id"] == "latest-1"


def test_analysis_schedule_create_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/schedules",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "name": "Daily sales",
            "tables": ["sales", "orders"],
            "depth": "deep",
            "resource_name": "Deepseek",
            "schedule_type": "daily",
            "schedule_hour": 8,
            "schedule_minute": 0,
            "timezone": "UTC",
            "delivery": {"channels": [{"type": "webhook", "webhook_url": "***configured***"}]},
        },
    )

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0][0] == "create_schedule"
    assert response.json()["schedule"]["tables"] == ["sales", "orders"]


def test_analysis_schedule_list_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.get("/api/analysis/schedules", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("list_schedules",)
    assert response.json()["count"] == 1


def test_analysis_schedule_update_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.put(
        "/api/analysis/schedules/schedule-1",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"name": "Updated sales", "tables": ["sales"], "schedule_type": "weekly", "schedule_day_of_week": 1},
    )

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0][0] == "update_schedule"
    assert response.json()["schedule"]["name"] == "Updated sales"


def test_analysis_schedule_delete_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.delete("/api/analysis/schedules/schedule-1", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("delete_schedule", "schedule-1")
    assert response.json()["deleted"] is True


def test_analysis_schedule_run_now_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post("/api/analysis/schedules/schedule-1/run", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("run_now", "schedule-1")
    assert response.json()["count"] == 2


def test_analysis_schedule_toggle_endpoint(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = FakeAnalysisScheduler()

    client = TestClient(main.app)
    response = client.post("/api/analysis/schedules/schedule-1/toggle", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert main.analysis_scheduler.calls[0] == ("toggle_schedule", "schedule-1")
    assert response.json()["schedule"]["enabled"] is False


def test_analysis_requires_configured_api_key():
    main = reload_main()
    client = TestClient(main.app)

    response = client.get("/api/analysis/reports")

    assert response.status_code == 503


def test_analysis_requires_auth_header(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = FakeAnalystAgent()

    client = TestClient(main.app)
    response = client.get("/api/analysis/reports")

    assert response.status_code == 401


def test_analysis_endpoint_returns_503_when_agent_not_ready(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analyst_agent = None

    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/sales",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick"},
    )

    assert response.status_code == 503


def test_analysis_invalid_identifier_returns_400(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")

    class InvalidIdentifierAgent(FakeAnalystAgent):
        def analyze_table(self, table_name, depth="standard", resource_name=None):
            raise ValueError(f"Invalid identifier: {table_name}")

    main.analyst_agent = InvalidIdentifierAgent()
    client = TestClient(main.app)
    response = client.post(
        "/api/analysis/table/bad!name",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"depth": "quick"},
    )

    assert response.status_code == 400


def test_analysis_schedule_requires_ready_scheduler(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.analysis_scheduler = None

    client = TestClient(main.app)
    response = client.get("/api/analysis/schedules", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 503


def test_analysis_websocket_rejects_without_key(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis") as websocket:
        with pytest.raises(Exception):
            websocket.receive_text()


def test_analysis_websocket_rejects_when_gateway_key_is_not_configured(monkeypatch):
    main = reload_main()
    monkeypatch.delenv("SMATRIX_API_KEY", raising=False)
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis?api_key=secret-key") as websocket:
        with pytest.raises(Exception):
            websocket.receive_text()


def test_analysis_websocket_push_reaches_client(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    from analysis_dispatcher import AnalysisDispatcher

    main.analysis_dispatcher = AnalysisDispatcher()
    client = TestClient(main.app)

    with client.websocket_connect("/ws/analysis?api_key=secret-key") as websocket:
        asyncio.run(main.analysis_dispatcher._push_ws({"id": "report-1", "summary": "ready"}))
        payload = websocket.receive_json()

    assert payload["id"] == "report-1"
