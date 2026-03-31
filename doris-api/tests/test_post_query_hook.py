import threading
import time
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from vanna_doris import VannaDoris


class FakeDorisClient:
    def __init__(self, query_results=None):
        self.query_results = list(query_results or [])
        self.executed_queries = []
        self.executed_updates = []
        self.config = {"database": "doris_db"}

    def execute_query(self, sql, params=None):
        self.executed_queries.append((sql, params))
        if self.query_results:
            return self.query_results.pop(0)
        return []

    def execute_update(self, sql, params=None):
        self.executed_updates.append((sql, params))
        return 1

    async def execute_query_async(self, sql, params=None):
        return self.execute_query(sql, params)

    def get_tables(self):
        return ["institutions"]

    def get_table_schema(self, table_name):
        return [{"Field": "所在市", "Type": "VARCHAR"}]


class SampleVanna(VannaDoris):
    def system_message(self, message: str):
        return {"role": "system", "content": message}

    def user_message(self, message: str):
        return {"role": "user", "content": message}

    def assistant_message(self, message: str):
        return {"role": "assistant", "content": message}


def _configure_successful_query_route(main, history_result):
    main.resolve_llm_resource_config = lambda resource_name=None: None
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[{"table_name": "institutions"}])
    main.datasource_handler.list_relationships_async = AsyncMock(return_value=[])
    main.PlannerAgent = type(
        "FakePlannerAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "plan": lambda self, question: {
                "intent": "count",
                "tables": ["institutions"],
                "subtasks": [{"table": "institutions", "question": question}],
                "needs_join": False,
            },
        },
    )
    main.TableAdminAgent = type(
        "FakeTableAdminAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate": lambda self, plan, sql_map, relationships=None: next(iter(sql_map.values())),
        },
    )
    main.VannaDorisOpenAI = type(
        "FakeHistoryVanna",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "add_question_sql": lambda self, *args, **kwargs: history_result,
        },
    )
    main.RepairAgent = type("FakeRepairAgent", (), {"__init__": lambda self, *args, **kwargs: None})
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 42}])


def test_add_question_sql_returns_dict_with_id():
    client = FakeDorisClient(query_results=[[{"count": 0}], [{"table_name": "institutions"}], []])
    vanna = SampleVanna(doris_client=client)

    result = vanna.add_question_sql(
        question="广州有多少机构？",
        sql="SELECT COUNT(*) FROM `institutions`",
        row_count=1,
        is_empty_result=False,
    )

    assert result["status"] == "stored"
    assert result["id"]


def test_add_question_sql_duplicate_returns_existing_id():
    client = FakeDorisClient(query_results=[[{"count": 1}], [{"id": "existing-id"}]])
    vanna = SampleVanna(doris_client=client)

    result = vanna.add_question_sql(
        question="广州有多少机构？",
        sql="SELECT COUNT(*) FROM `institutions`",
    )

    assert result == {"status": "skipped", "id": "existing-id"}


def test_auto_analyze_fires_on_query_success(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("ANALYST_AUTO_ANALYZE", "true")
    _configure_successful_query_route(main, {"status": "stored", "id": "history-1"})
    calls = []

    class RecordingAnalystAgent:
        def replay_from_history(self, history_id, resource_name=None, **kwargs):
            calls.append((history_id, resource_name, kwargs))
            return {"success": True}

    main.analyst_agent = RecordingAnalystAgent()
    client = TestClient(main.app)

    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？", "resource_name": "Deepseek"},
    )

    deadline = time.time() + 1
    while time.time() < deadline and not calls:
        time.sleep(0.01)

    assert response.status_code == 200
    assert calls[0][0] == "history-1"
    assert calls[0][1] == "Deepseek"


def test_auto_analyze_does_not_fire_when_disabled(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.delenv("ANALYST_AUTO_ANALYZE", raising=False)
    _configure_successful_query_route(main, {"status": "stored", "id": "history-1"})
    calls = []

    class RecordingAnalystAgent:
        def replay_from_history(self, history_id, resource_name=None, **kwargs):
            calls.append((history_id, resource_name, kwargs))
            return {"success": True}

    main.analyst_agent = RecordingAnalystAgent()
    client = TestClient(main.app)

    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？"},
    )

    time.sleep(0.05)

    assert response.status_code == 200
    assert calls == []


def test_auto_analyze_is_fire_and_forget(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("ANALYST_AUTO_ANALYZE", "true")
    _configure_successful_query_route(main, {"status": "stored", "id": "history-1"})
    replay_started = threading.Event()

    class SlowAnalystAgent:
        def replay_from_history(self, history_id, resource_name=None, **kwargs):
            replay_started.set()
            time.sleep(0.3)
            return {"success": True}

    main.analyst_agent = SlowAnalystAgent()
    client = TestClient(main.app)

    started = time.perf_counter()
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？"},
    )
    elapsed = time.perf_counter() - started

    assert response.status_code == 200
    assert elapsed < 0.2
    assert replay_started.wait(timeout=1)
