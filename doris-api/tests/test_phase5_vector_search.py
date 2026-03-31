import uuid

from datasource_handler import DataSourceHandler
from embedding import EmbeddingService
from vanna_doris import VannaDoris


class FakeVectorDorisClient:
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


class SamplePhase5Vanna(VannaDoris):
    def system_message(self, message: str):
        return {"role": "system", "content": message}

    def user_message(self, message: str):
        return {"role": "user", "content": message}

    def assistant_message(self, message: str):
        return {"role": "assistant", "content": message}


def test_embedding_service_hashing_fallback_is_deterministic():
    service = EmbeddingService(provider="hashing", dimension=16)

    first = service.embed_text("广州的机构数量")
    second = service.embed_text("广州的机构数量")

    assert len(first) == 16
    assert first == second
    assert any(value != 0.0 for value in first)


def test_add_question_sql_persists_question_embedding():
    client = FakeVectorDorisClient(query_results=[[{"count": 0}], [{"table_name": "institutions"}]])
    vanna = SamplePhase5Vanna(doris_client=client)
    vanna.embedding_service = EmbeddingService(provider="hashing", dimension=8)

    result = vanna.add_question_sql(
        question="广州有多少机构？",
        sql="SELECT COUNT(*) FROM `institutions`",
        row_count=1,
        is_empty_result=False,
    )

    assert result["status"] == "stored"
    assert result["id"]
    assert client.executed_updates, "expected INSERT into _sys_query_history"
    insert_sql, params = client.executed_updates[0]
    assert "question_embedding" in insert_sql
    uuid.UUID(params[0])


def test_get_similar_question_sql_prefers_vector_search():
    client = FakeVectorDorisClient(
        query_results=[
            [{"question": "广州的机构数量", "sql": "SELECT COUNT(*) FROM `institutions`", "score": 0.9}],
        ]
    )
    vanna = SamplePhase5Vanna(doris_client=client)
    vanna.embedding_service = EmbeddingService(provider="hashing", dimension=8)

    examples = vanna.get_similar_question_sql("在广州的组织有几家")

    assert len(examples) == 1
    assert "question_embedding" in client.executed_queries[0][0]


def test_ensure_query_history_vector_support_executes_alter_and_index():
    handler = DataSourceHandler()
    handler.db = FakeVectorDorisClient()

    result = handler.ensure_query_history_vector_support(dimension=8)

    assert result["success"] is True
    statements = [sql for sql, _ in handler.db.executed_updates]
    assert any("ALTER TABLE `_sys_query_history` ADD COLUMN `question_embedding` ARRAY<FLOAT>" in sql for sql in statements)
    assert any("USING ANN" in sql for sql in statements)
