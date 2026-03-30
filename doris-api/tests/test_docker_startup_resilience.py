import sys
from types import SimpleNamespace

from conftest import reload_main
from datasource_handler import DataSourceHandler


class RecordingInitClient:
    def __init__(self, failures=None):
        self.failures = list(failures or [])
        self.executed_updates = []

    def execute_update(self, sql, params=None):
        self.executed_updates.append((sql, params))
        if self.failures:
            failure = self.failures.pop(0)
            if failure is not None:
                raise failure
        return 1


def test_ensure_system_tables_retries_transient_backend_capacity_errors(monkeypatch):
    handler = DataSourceHandler()
    handler.db = RecordingInitClient(
        failures=[
            Exception(
                'errCode = 2, detailMessage = Failed to find enough backend, '
                'Backends details: backends with tag {"location" : "default"} '
                'is [[backendId=1, host=192.168.100.3, hdd disks count={}, ssd disk count={}]]'
            ),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ]
    )
    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda seconds: sleep_calls.append(seconds))

    result = handler._ensure_system_tables()

    assert result is True
    assert len(handler.db.executed_updates) > 1
    assert sleep_calls == [5]


def test_init_tables_only_marks_initialized_after_success(monkeypatch):
    handler = DataSourceHandler()
    handler.db = RecordingInitClient(
        failures=[Exception("errCode = 2, detailMessage = Failed to find enough backend") for _ in range(10)]
    )
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    result = handler.init_tables()

    assert result is False
    assert handler._tables_initialized is False


def test_system_table_ddl_avoids_boolean_default_literals():
    handler = DataSourceHandler()
    handler.db = RecordingInitClient()

    handler._ensure_system_tables()

    ddl_statements = [sql for sql, _ in handler.db.executed_updates]
    assert ddl_statements
    assert all("BOOLEAN DEFAULT FALSE" not in sql for sql in ddl_statements)
    assert all("BOOLEAN DEFAULT TRUE" not in sql for sql in ddl_statements)


def test_init_doris_sync_waits_until_system_tables_exist(monkeypatch):
    main = reload_main()

    class FakeCursor:
        def execute(self, sql):
            return None

        def fetchall(self):
            return [("backend",)]

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            return None

    init_results = iter([False, True])
    sleep_calls = []

    monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(connect=lambda **kwargs: FakeConnection()))
    monkeypatch.setattr(main.datasource_handler, "init_tables", lambda: next(init_results))
    main.analyst_agent = SimpleNamespace(init_tables=lambda: True)
    monkeypatch.setattr("time.sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setenv("DORIS_INIT_RETRY_INTERVAL", "2")

    result = main._init_doris_sync()

    assert result is True
    assert sleep_calls == [2]
