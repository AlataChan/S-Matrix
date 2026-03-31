import hashlib
import uuid
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
            result = self.query_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
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


def test_protected_api_requires_x_api_key(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.metadata_analyzer.list_all_metadata = lambda: []

    client = TestClient(main.app)

    no_key = client.get("/api/metadata")
    assert no_key.status_code == 401

    health = client.get("/api/health")
    assert health.status_code == 200

    with_key = client.get("/api/metadata", headers={"X-API-Key": "secret-key"})
    assert with_key.status_code == 200


def test_history_endpoint_returns_records(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.list_query_history_async = AsyncMock(
        return_value=[
            {
                "id": 1,
                "question": "广州有多少机构？",
                "sql": "SELECT COUNT(*) FROM `institutions`",
                "table_names": "institutions",
                "is_empty_result": False,
                "row_count": 1,
                "created_at": "2026-03-28 00:00:00",
            }
        ]
    )

    client = TestClient(main.app)
    response = client.get("/api/query/history", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["history"][0]["table_names"] == "institutions"


def test_add_question_sql_persists_history_with_hash_and_empty_result():
    client = FakeDorisClient(query_results=[[{"count": 0}], [{"table_name": "institutions"}], []])
    vanna = SampleVanna(doris_client=client)

    result = vanna.add_question_sql(
        question="广州有多少机构？",
        sql="SELECT COUNT(*) FROM `institutions`",
        row_count=0,
        is_empty_result=True,
    )

    assert result["status"] == "stored"
    assert result["id"]
    assert client.executed_updates, "expected INSERT into _sys_query_history"
    _, params = client.executed_updates[0]
    uuid.UUID(params[0])
    assert params[1] == "广州有多少机构？"
    assert params[4] == hashlib.md5("广州有多少机构？SELECT COUNT(*) FROM `institutions`".encode("utf-8")).hexdigest()
    assert params[6] is True


def test_add_question_sql_softly_allows_unregistered_tables():
    client = FakeDorisClient(query_results=[[{"count": 0}], [{"table_name": "other_table"}]])
    vanna = SampleVanna(doris_client=client)

    result = vanna.add_question_sql(
        question="广州有多少机构？",
        sql="SELECT COUNT(*) FROM `institutions`",
        row_count=1,
        is_empty_result=False,
    )

    assert result["status"] == "stored"
    assert result["id"]
    assert client.executed_updates, "soft table validation should not block history writes"


def test_extract_table_names_handles_join():
    vanna = SampleVanna(doris_client=FakeDorisClient())

    tables = vanna.extract_table_names(
        "SELECT a.id FROM `institutions` a JOIN activities b ON a.id = b.org_id"
    )

    assert tables == ["activities", "institutions"]


def test_get_similar_question_sql_uses_like_fallback():
    client = FakeDorisClient(
        query_results=[
            Exception("Unknown column question_embedding"),
            [],
            [{"question": "广州有多少机构？", "sql": "SELECT COUNT(*) FROM `institutions`"}],
        ]
    )
    vanna = SampleVanna(doris_client=client)

    examples = vanna.get_similar_question_sql("广州的机构数量")

    assert len(examples) == 1
    assert "LIKE" in client.executed_queries[-1][0]


def test_generate_sql_injects_retrieved_examples():
    class RecordingVanna(SampleVanna):
        def __init__(self):
            super().__init__(doris_client=FakeDorisClient())
            self.prompt_examples = None

        def get_related_ddl(self, question, **kwargs):
            return ["CREATE TABLE `institutions` (`所在市` VARCHAR(100));"]

        def get_related_documentation(self, question, **kwargs):
            return []

        def get_similar_question_sql(self, question, **kwargs):
            return [{"question": "广州有多少机构？", "sql": "SELECT COUNT(*) FROM `institutions`"}]

        def get_sql_prompt(self, question, question_sql_list, ddl_list, doc_list, **kwargs):
            self.prompt_examples = question_sql_list
            return "SELECT 1"

        def submit_prompt(self, prompt, **kwargs):
            return "SELECT 1"

    vanna = RecordingVanna()
    vanna.generate_sql("广州的机构数量")

    assert vanna.prompt_examples == [{"question": "广州有多少机构？", "sql": "SELECT COUNT(*) FROM `institutions`"}]
