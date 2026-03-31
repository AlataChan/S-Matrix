import asyncio

from analysis_dispatcher import AnalysisDispatcher


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, headers=None):
        self.calls.append((url, json, headers))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeHttpResponse:
    def __init__(self, should_raise=False):
        self.should_raise = should_raise

    def raise_for_status(self):
        if self.should_raise:
            raise RuntimeError("boom")


class FakeWebSocket:
    def __init__(self):
        self.accepted = False
        self.closed = False
        self.messages = []

    async def accept(self):
        self.accepted = True

    async def send_json(self, payload):
        self.messages.append(payload)

    async def close(self, code=1000, reason=""):
        self.closed = True


def test_webhook_delivers_generic_payload(monkeypatch):
    dispatcher = AnalysisDispatcher()
    fake_client = FakeAsyncClient([FakeHttpResponse()])
    monkeypatch.setattr("analysis_dispatcher.httpx.AsyncClient", lambda timeout=None: fake_client)

    asyncio.run(
        dispatcher.dispatch(
            {"id": "report-1", "summary": "ok"},
            {"channels": [{"type": "webhook", "format": "generic", "webhook_url": "https://8.8.8.8/hook"}]},
        )
    )

    assert fake_client.calls[0][0] == "https://8.8.8.8/hook"
    assert fake_client.calls[0][1]["id"] == "report-1"


def test_webhook_retries_on_failure(monkeypatch):
    dispatcher = AnalysisDispatcher()
    fake_client = FakeAsyncClient([RuntimeError("down"), FakeHttpResponse()])
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("analysis_dispatcher.httpx.AsyncClient", lambda timeout=None: fake_client)
    monkeypatch.setattr("analysis_dispatcher.asyncio.sleep", fake_sleep)

    asyncio.run(
        dispatcher.dispatch(
            {"id": "report-1", "summary": "ok"},
            {"channels": [{"type": "webhook", "format": "generic", "webhook_url": "https://8.8.8.8/hook"}]},
        )
    )

    assert len(fake_client.calls) == 2
    assert sleeps == [1]


def test_webhook_includes_bearer_token(monkeypatch):
    dispatcher = AnalysisDispatcher()
    fake_client = FakeAsyncClient([FakeHttpResponse()])
    monkeypatch.setattr("analysis_dispatcher.httpx.AsyncClient", lambda timeout=None: fake_client)

    asyncio.run(
        dispatcher.dispatch(
            {"id": "report-1", "summary": "ok"},
            {
                "channels": [
                    {
                        "type": "webhook",
                        "format": "generic",
                        "webhook_url": "https://8.8.8.8/hook",
                        "webhook_token": "token-123",
                    }
                ]
            },
        )
    )

    assert fake_client.calls[0][2]["Authorization"] == "Bearer token-123"


def test_webhook_rejects_private_ip_targets(monkeypatch):
    dispatcher = AnalysisDispatcher()
    fake_client = FakeAsyncClient([FakeHttpResponse()])
    monkeypatch.setattr("analysis_dispatcher.httpx.AsyncClient", lambda timeout=None: fake_client)

    asyncio.run(
        dispatcher.dispatch(
            {"id": "report-1", "summary": "ok"},
            {
                "channels": [
                    {
                        "type": "webhook",
                        "format": "generic",
                        "webhook_url": "http://169.254.169.254/latest/meta-data",
                    }
                ]
            },
        )
    )

    assert fake_client.calls == []


def test_webhook_slack_format():
    dispatcher = AnalysisDispatcher()

    payload = dispatcher._format_payload({"id": "report-1", "summary": "Revenue rising"}, "slack")

    assert "blocks" in payload
    assert "Revenue rising" in str(payload["blocks"])


def test_webhook_dingtalk_format():
    dispatcher = AnalysisDispatcher()

    payload = dispatcher._format_payload({"id": "report-1", "summary": "Revenue rising", "table_names": ["sales", "orders"]}, "dingtalk")

    assert payload["msgtype"] == "markdown"
    assert "Revenue rising" in payload["markdown"]["text"]
    assert "sales, orders" in payload["markdown"]["text"]


def test_webhook_failure_does_not_raise(monkeypatch):
    dispatcher = AnalysisDispatcher()
    fake_client = FakeAsyncClient([RuntimeError("down"), RuntimeError("still down"), RuntimeError("down forever")])
    monkeypatch.setattr("analysis_dispatcher.httpx.AsyncClient", lambda timeout=None: fake_client)

    async def fake_sleep(delay):
        return None

    monkeypatch.setattr("analysis_dispatcher.asyncio.sleep", fake_sleep)

    asyncio.run(
        dispatcher.dispatch(
            {"id": "report-1", "summary": "ok"},
            {"channels": [{"type": "webhook", "format": "generic", "webhook_url": "https://8.8.8.8/hook"}]},
        )
    )

    assert len(fake_client.calls) == 3


def test_ws_push_reaches_connected_client():
    dispatcher = AnalysisDispatcher()
    websocket = FakeWebSocket()

    async def exercise():
        await dispatcher.ws_connect(websocket, "client-1")
        await dispatcher._push_ws({"id": "report-1"})

    asyncio.run(exercise())

    assert websocket.accepted is True
    assert websocket.messages == [{"id": "report-1"}]
