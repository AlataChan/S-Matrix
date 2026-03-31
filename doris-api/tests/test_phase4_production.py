import importlib
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main
from vanna_doris import VannaDoris


class CacheAwareDorisClient:
    def __init__(self):
        self.executed_queries = []
        self.executed_updates = []
        self.config = {"database": "doris_db"}

    def execute_query(self, sql, params=None):
        self.executed_queries.append((sql, params))
        if "information_schema.COLUMNS" in sql:
            return [
                {
                    "TABLE_NAME": "institutions",
                    "COLUMN_NAME": "所在市",
                    "DATA_TYPE": "varchar",
                    "IS_NULLABLE": "YES",
                    "COLUMN_DEFAULT": None,
                    "COLUMN_COMMENT": "",
                }
            ]
        if "SELECT DISTINCT `所在市`" in sql:
            return [{"所在市": "广州市"}]
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


class SamplePhase4Vanna(VannaDoris):
    def system_message(self, message: str):
        return {"role": "system", "content": message}

    def user_message(self, message: str):
        return {"role": "user", "content": message}

    def assistant_message(self, message: str):
        return {"role": "assistant", "content": message}


def test_protected_api_accepts_bearer_token(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.list_all_metadata = lambda: []

    client = TestClient(main.app)
    response = client.get("/api/metadata", headers={"Authorization": "Bearer secret-key"})

    assert response.status_code == 200


def test_natural_query_auto_repairs_failed_sql(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    main.datasource_handler.list_table_registry = AsyncMock(return_value=[])
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
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: "SELECT bad_column FROM `institutions`",
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {"__init__": lambda self, *args, **kwargs: None, "coordinate": lambda self, plan, sql_map: "SELECT bad_column FROM `institutions`"},
    )
    main.RepairAgent = type(
        "FakeRepairAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "repair_sql": lambda self, question, failed_sql, error_message, ddl_list, api_config=None: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )

    class RecordingHistoryVanna:
        calls = []

        def __init__(self, *args, **kwargs):
            pass

        def add_question_sql(self, **kwargs):
            self.__class__.calls.append(kwargs)
            return {"status": "stored", "id": "history-1"}

    main.VannaDorisOpenAI = RecordingHistoryVanna
    main.doris_client.execute_query_async = AsyncMock(
        side_effect=[Exception("Unknown column 'bad_column'"), [{"total": 42}]]
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"Authorization": "Bearer secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？"},
    )

    assert response.status_code == 200
    assert response.json()["sql"] == "SELECT COUNT(*) AS total FROM `institutions`"
    assert main.doris_client.execute_query_async.await_count == 2
    assert RecordingHistoryVanna.calls[0]["sql"] == "SELECT COUNT(*) AS total FROM `institutions`"


def test_vanna_caches_ddl_and_enum_lookups():
    client = CacheAwareDorisClient()
    vanna = SamplePhase4Vanna(doris_client=client)

    vanna.get_related_ddl("广州的机构")
    vanna.get_related_ddl("广州的机构")
    vanna.get_column_sample_values("institutions", "所在市")
    vanna.get_column_sample_values("institutions", "所在市")

    ddl_queries = [sql for sql, _ in client.executed_queries if "information_schema.COLUMNS" in sql]
    enum_queries = [sql for sql, _ in client.executed_queries if "SELECT DISTINCT `所在市`" in sql]
    assert len(ddl_queries) == 1
    assert len(enum_queries) == 1


def test_doris_client_uses_pooled_connections(monkeypatch):
    import db

    db_module = importlib.reload(db)
    pool_config = {}

    class FakePool:
        def __init__(self, creator=None, maxconnections=None, **kwargs):
            pool_config["maxconnections"] = maxconnections
            pool_config["kwargs"] = kwargs

        def connection(self):
            return "pooled-connection"

    monkeypatch.setattr(db_module, "PooledDB", FakePool)
    monkeypatch.setenv("DORIS_POOL_SIZE", "10")

    client = db_module.DorisClient()

    assert client.get_connection() == "pooled-connection"
    assert pool_config["maxconnections"] == 10


def test_mcp_server_dispatches_query_tool(monkeypatch):
    import mcp_server

    class FakeApiClient:
        def query_natural(self, question):
            return {"success": True, "query": question, "sql": "SELECT 1", "data": [{"value": 1}]}

    monkeypatch.setattr(mcp_server, "build_api_client", lambda: FakeApiClient())

    response = mcp_server.handle_jsonrpc_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "query_natural", "arguments": {"question": "广州有多少机构？"}},
        }
    )

    assert response["result"]["structuredContent"]["sql"] == "SELECT 1"


def test_mcp_server_ignores_notification_without_response():
    import mcp_server

    response = mcp_server.handle_jsonrpc_request(
        {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
    )

    assert response is None
