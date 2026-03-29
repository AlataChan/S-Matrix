from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from conftest import reload_main


def test_relationship_override_endpoint_creates_manual_relationship(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    main.datasource_handler.create_relationship_async = AsyncMock(
        return_value={
            "success": True,
            "relationship": {
                "table_a": "institutions",
                "column_a": "id",
                "table_b": "activities",
                "column_b": "org_id",
                "is_manual": True,
                "confidence": 1.0,
            },
        }
    )

    client = TestClient(main.app)
    response = client.post(
        "/api/relationships",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={
            "table_a": "institutions",
            "column_a": "id",
            "table_b": "activities",
            "column_b": "org_id",
            "rel_type": "logical",
        },
    )

    assert response.status_code == 200
    assert response.json()["relationship"]["is_manual"] is True


def test_natural_query_route_uses_planner_pipeline(monkeypatch):
    main = reload_main()
    monkeypatch.setenv("SMATRIX_API_KEY", "secret-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
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
            "generate_sql_for_subtask": lambda self, subtask, question, api_config=None: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )
    main.CoordinatorAgent = type(
        "FakeCoordinatorAgent",
        (),
        {
            "__init__": lambda self, *args, **kwargs: None,
            "coordinate": lambda self, plan, sql_map: "SELECT COUNT(*) AS total FROM `institutions`",
        },
    )
    main.doris_client.execute_query_async = AsyncMock(return_value=[{"total": 42}])

    client = TestClient(main.app)
    response = client.post(
        "/api/query/natural",
        headers={"X-API-Key": "secret-key", "Content-Type": "application/json"},
        json={"query": "广州有多少机构？"},
    )

    assert response.status_code == 200
    assert response.json()["sql"] == "SELECT COUNT(*) AS total FROM `institutions`"
    assert response.json()["data"] == [{"total": 42}]


def test_coordinator_generates_join_sql_from_relationship():
    from coordinator_agent import CoordinatorAgent

    coordinator = CoordinatorAgent()
    sql = coordinator.coordinate(
        {
            "tables": ["institutions", "activities"],
            "needs_join": True,
            "subtasks": [
                {"table": "institutions", "question": "机构"},
                {"table": "activities", "question": "活动"},
            ],
        },
        {
            "institutions": "SELECT `id`, `name` FROM `institutions`",
            "activities": "SELECT `org_id`, `year` FROM `activities`",
        },
        relationships=[
            {
                "table_a": "institutions",
                "column_a": "id",
                "table_b": "activities",
                "column_b": "org_id",
                "is_manual": True,
            }
        ],
    )

    assert "JOIN" in sql
    assert "FROM (" in sql
    assert "SELECT `id`, `name` FROM `institutions`" in sql
    assert "SELECT `org_id`, `year` FROM `activities`" in sql
    assert "institutions_sub" in sql
    assert "activities_sub" in sql


def test_planner_marks_join_when_multiple_tables_match():
    from planner_agent import PlannerAgent

    planner = PlannerAgent(
        tables_context=[
            {"table_name": "institutions", "description": "机构基础信息"},
            {"table_name": "activities", "description": "机构活动记录"},
        ]
    )

    plan = planner.plan("哪些机构参加了2023年的活动？")

    assert plan["needs_join"] is True
    assert set(plan["tables"]) == {"institutions", "activities"}


def test_planner_routes_using_agent_config_and_columns_info():
    from planner_agent import PlannerAgent

    planner = PlannerAgent(
        tables_context=[
            {
                "table_name": "institutions",
                "description": "机构基础信息",
                "columns_info": {"所在市": "城市", "机构名称": "名称"},
            },
            {
                "table_name": "branches",
                "description": "网点记录",
                "agent_config": {
                    "fields": {
                        "分支情况": {"semantic": "branch-status"},
                        "所属机构": {"semantic": "institution-ref"},
                    }
                },
            },
        ]
    )

    plan = planner.plan("帮我看看广州的机构和分支情况")

    assert plan["needs_join"] is True
    assert set(plan["tables"]) == {"institutions", "branches"}
