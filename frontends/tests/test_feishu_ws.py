"""Endpoint discovery contracts and recovery without a real Feishu account."""
import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("feishu_ws_under_test", ROOT / "frontends/feishu_ws.py")
feishu_ws = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feishu_ws)


def response(payload, status=200):
    return SimpleNamespace(status_code=status, content=json.dumps(payload).encode())


def test_endpoint_discovery_preserves_config_and_headers(monkeypatch):
    calls = []
    config = {"ReconnectCount": 7, "ReconnectInterval": 90, "ReconnectNonce": 3, "PingInterval": 120}

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return response({"code": 0, "data": {"URL": "ws://local/", "ClientConfig": config}})

    monkeypatch.setattr(feishu_ws.requests, "post", post)
    client = feishu_ws.FeishuWsClient("app", "secret", domain="https://example.test",
                                      headers={"x-test": "preserved"})
    assert client._get_conn_url() == "ws://local/"
    assert calls[0][0] == "https://example.test/callback/ws/endpoint"
    assert calls[0][1]["timeout"] == (10, 30)
    assert calls[0][1]["json"] == {"AppID": "app", "AppSecret": "secret"}
    assert calls[0][1]["headers"]["x-test"] == "preserved"
    assert calls[0][1]["headers"][feishu_ws.ws.USER_AGENT] == client._user_agent
    assert (client._reconnect_count, client._reconnect_interval, client._reconnect_nonce,
            client._ping_interval) == (7, 90, 3, 120)


@pytest.mark.parametrize("code,error", [(1, "ServerException"), (1000040343, "ServerException"),
                                       (1000040344, "ClientException")])
def test_api_errors_keep_sdk_retry_classification(monkeypatch, code, error):
    monkeypatch.setattr(feishu_ws.requests, "post", lambda *a, **kw: response({"code": code, "msg": "test"}))
    with pytest.raises(getattr(feishu_ws.ws, error)):
        feishu_ws.FeishuWsClient("app", "secret")._get_conn_url()


def test_http_failure_is_retryable(monkeypatch):
    monkeypatch.setattr(feishu_ws.requests, "post", lambda *a, **kw: response({}, 503))
    with pytest.raises(feishu_ws.ws.ServerException):
        feishu_ws.FeishuWsClient("app", "secret")._get_conn_url()


def test_missing_credentials_fail_before_network(monkeypatch):
    def post(*a, **kw):
        pytest.fail("invalid credentials must not make a network request")

    monkeypatch.setattr(feishu_ws.requests, "post", post)
    with pytest.raises(feishu_ws.ws.ClientException):
        feishu_ws.FeishuWsClient("app", "")._get_conn_url()


def test_stalled_endpoint_allows_next_sdk_reconnect_attempt(monkeypatch):
    """Real HTTP read stalls; the existing SDK reconnect loop must recover."""
    release = threading.Event()
    requests_seen = []
    connected = []

    class Endpoint(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests_seen.append(self.path)
            self.rfile.read(int(self.headers["Content-Length"]))
            if len(requests_seen) == 1:
                release.wait(2)
                return
            body = json.dumps({"code": 0, "data": {
                "URL": "ws://local/?device_id=local&service_id=1"
            }}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    async def connect(url, **kwargs):
        connected.append(url)
        return object()

    async def receive(self):
        pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Endpoint)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setattr(feishu_ws, "_ENDPOINT_TIMEOUT", (0.1, 0.1))
    monkeypatch.setattr(feishu_ws.ws.websockets, "connect", connect)
    monkeypatch.setattr(feishu_ws.FeishuWsClient, "_receive_message_loop", receive)

    async def exercise():
        monkeypatch.setattr(feishu_ws.ws, "loop", asyncio.get_running_loop())
        client = feishu_ws.FeishuWsClient("app", "secret", domain=f"http://127.0.0.1:{server.server_port}")
        client._reconnect_nonce = 0
        client._reconnect_interval = 0
        client._reconnect_count = 2
        started = time.monotonic()
        await asyncio.wait_for(client._reconnect(), timeout=1)
        # A synchronous HTTP call can block asyncio's timeout callback as well.
        assert time.monotonic() - started < 1
        assert client._conn is not None
        assert not client._lock.locked()

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert requests_seen == ["/callback/ws/endpoint"] * 2
    assert connected == ["ws://local/?device_id=local&service_id=1"]


def test_fsapp_constructs_the_guarded_client():
    """Exercise the production main() call site without loading credentials/Agent."""
    tree = ast.parse((ROOT / "frontends/fsapp.py").read_text())
    main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    constructed = []

    def factory(*args, **kwargs):
        constructed.append((args, kwargs))
        return SimpleNamespace(start=lambda: (_ for _ in ()).throw(KeyboardInterrupt()))

    handler = object()
    builder = SimpleNamespace(register_p2_im_message_receive_v1=lambda *_: SimpleNamespace(build=lambda: handler))
    namespace = {
        "_feishu_config": lambda: ("app", "secret", set(), False, "test-config"),
        "create_client": lambda: object(), "handle_message": lambda *_: None,
        "FeishuWsClient": factory,
        "lark": SimpleNamespace(
            EventDispatcherHandler=SimpleNamespace(builder=lambda *_: builder),
            LogLevel=SimpleNamespace(INFO="info")),
    }
    exec(compile(ast.Module(body=[main], type_ignores=[]), "fsapp.py", "exec"), namespace)
    with pytest.raises(KeyboardInterrupt):
        namespace["main"]()
    assert constructed == [(("app", "secret"), {"event_handler": handler, "log_level": "info"})]
