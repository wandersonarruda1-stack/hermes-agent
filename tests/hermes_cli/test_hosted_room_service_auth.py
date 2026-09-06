"""Real credential files, HTTP ticket gate, WS admission and durable lineage."""
from pathlib import Path
from types import SimpleNamespace
import secrets
import threading

import pytest
from fastapi.testclient import TestClient

from hermes_cli import web_server
from hermes_cli.dashboard_auth import clear_providers, register_provider, get_provider
from hermes_cli.dashboard_auth.service import (
    ProfileServiceProvider, TOKEN_FILE, TICKET_ROUTE, provision_token,
)
from hermes_cli.dashboard_auth.token_auth import register_token_route
from hermes_cli.dashboard_auth.ws_tickets import _reset_for_tests, consume_ticket
from tui_gateway.hosted_room_service import HostedRoomService
from tui_gateway.transport import bind_transport, reset_transport
import tui_gateway.server as server

pytestmark = pytest.mark.linux_only


@pytest.fixture
def auth(tmp_path, monkeypatch):
    root = tmp_path / "hermes"
    (root / "profiles" / "wanclone").mkdir(parents=True)
    (root / "profiles" / "other").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(root))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    clear_providers()
    _reset_for_tests()
    provider = ProfileServiceProvider(root)
    register_provider(provider)
    register_token_route(TICKET_ROUTE)
    register_token_route("/api/gateway/drain")
    path = provision_token("wanclone", root=root)
    token = path.read_text().strip()
    for key, value in {"auth_required": True, "bound_host": "auth.test", "bound_port": 443}.items():
        monkeypatch.setattr(web_server.app.state, key, value, raising=False)
    client = TestClient(web_server.app, base_url="https://auth.test")
    yield root, provider, token, client
    clear_providers()
    _reset_for_tests()


def test_files_bind_profile_and_reject_invalid_modes_duplicates_and_symlinks(auth):
    root, provider, token, _ = auth
    principal = provider.verify_token(token=token)
    assert (principal.provider, principal.principal) == ("service", "wanclone")
    assert provider.verify_token(token=secrets.token_urlsafe(32)) is None
    assert provider.verify_token(token="") is None
    path = root / "profiles" / "wanclone" / TOKEN_FILE
    path.chmod(0o644)
    assert provider.verify_token(token=token) is None
    path.chmod(0o600)
    other = root / "profiles" / "other" / TOKEN_FILE
    other.write_text(token)
    other.chmod(0o600)
    assert provider.verify_token(token=token) is None
    other.unlink()
    path.rename(root / "secret")
    path.symlink_to(root / "secret")
    assert provider.verify_token(token=token) is None


def test_provision_is_exclusive_and_does_not_accept_profile_traversal(auth):
    root, _, token, _ = auth
    with pytest.raises(FileExistsError):
        provision_token("wanclone", root=root)
    with pytest.raises(ValueError):
        provision_token("../other", root=root)
    assert (root / "profiles" / "wanclone" / TOKEN_FILE).read_text().strip() == token


def test_profile_credentials_stay_distinct_and_revocation_is_live(auth, monkeypatch):
    root, provider, token, _ = auth
    default = provision_token("default", root=root).read_text().strip()
    assert default != token
    monkeypatch.setenv("HERMES_HOME", str(root / "profiles" / "other"))
    assert provider.verify_token(token=default).principal == "default"
    assert provider.verify_token(token=token).principal == "wanclone"
    (root / "profiles" / "wanclone" / TOKEN_FILE).unlink()
    assert provider.verify_token(token=token) is None
    assert provider.verify_token(token=default).principal == "default"


def test_plugin_discovery_registers_service_provider(auth):
    from hermes_cli.plugins import discover_plugins
    discover_plugins(force=True)
    assert isinstance(get_provider("service"), ProfileServiceProvider)


def ticket(client, token):
    response = client.post(TICKET_ROUTE, headers={"Authorization": "Bearer " + token})
    assert response.status_code == 200
    return response.json()["ticket"]


def websocket(ticket_value, path="/api/ws"):
    return SimpleNamespace(query_params={"ticket": ticket_value}, headers={},
                           url=SimpleNamespace(path=path), client=SimpleNamespace(host="127.0.0.1"))


def test_http_rejects_anonymous_invalid_and_other_token_routes(auth):
    _, _, token, client = auth
    assert client.post(TICKET_ROUTE).status_code == 401
    assert client.post(TICKET_ROUTE, headers={"Authorization": "Bearer wrong"}).status_code == 401
    # A room token must never authenticate the existing gateway drain route.
    assert client.post("/api/gateway/drain", headers={"Authorization": "Bearer " + token}).status_code == 401
    info = consume_ticket(ticket(client, token))
    assert (info["provider"], info["user_id"]) == ("service", "wanclone")


@pytest.mark.parametrize("path", ["/api/pty", "/api/console", "/api/pub", "/api/events"])
def test_service_ticket_cannot_enter_other_websockets(auth, path):
    _, _, token, client = auth
    ws = websocket(ticket(client, token), path)
    assert web_server._ws_auth_reason(ws)[0] == "service_endpoint_forbidden"
    assert not hasattr(ws, "_hermes_auth_identity")


def test_real_ticket_to_rpc_to_store_and_founder_reopen(auth, monkeypatch):
    root, _, token, client = auth
    ws = websocket(ticket(client, token))
    assert web_server._ws_auth_reason(ws)[0] is None
    assert web_server._ws_auth_reason(ws)[0] == "ticket_invalid"  # one use
    transport = SimpleNamespace(auth_identity=ws._hermes_auth_identity)
    assert transport.auth_identity == {"provider": "service", "user_id": "wanclone"}
    service = HostedRoomService(SimpleNamespace(_methods={}, _sessions={}, _sessions_lock=threading.Lock()), db_path=root / "test-state.db")
    service.local_profiles = lambda: ("wanclone", "other")
    monkeypatch.setattr(server, "get_hosted_room_service", lambda: service)
    params = dict(room_id="r", name="Room", lineage_version=1,
                  members=[dict(member_id=p, profile=p, handle=p) for p in ["wanclone", "other"]],
                  principal="forged")
    binding = bind_transport(transport)
    try:
        assert "result" in server._methods["groups.create"](1, params)
        sent = server._methods["groups.send"](2, dict(room_id="r", event_id="first", principal="forged",
                 payload=dict(text="work", thread_id="thread", goal="Ship")))
        payload = sent["result"]["event"]["payload"]
        assert payload["founder"] == payload["requester"] == {"kind": "user", "id": "service:wanclone"}
        assert payload["depth"] == 0
        denied = server.dispatch({"id": 3, "method": "terminal.create", "params": {}}, transport)
        assert denied["error"]["code"] == 4403
        # Caller-supplied roster cannot authorize a room where this profile is absent.
        assert "error" in server._methods["groups.create"](4, {**params, "room_id": "excluded", "members": params["members"][1:]})
        assert "error" in server._methods["groups.create"](5, {**params, "room_id": "legacy", "lineage_version": 0})
    finally:
        reset_transport(binding)
    # An authenticated different member cannot impersonate the original founder.
    binding = bind_transport(SimpleNamespace(auth_identity={"provider": "service", "user_id": "other"}))
    try:
        result = server._methods["groups.send"](6, dict(room_id="r", event_id="reopen",
                    payload=dict(text="reopen", thread_id="thread", re="user:first")))
        assert "error" in result
    finally:
        reset_transport(binding)
    assert "error" in server._methods["groups.send"](7, dict(room_id="r", event_id="anonymous", payload=dict(text="no", thread_id="new", goal="Ship")))


def test_service_nonmember_cannot_send(auth):
    root, _, _, _ = auth
    service = HostedRoomService(SimpleNamespace(_methods={}, _sessions={}, _sessions_lock=threading.Lock()), db_path=root / "test-state.db")
    service.local_profiles = lambda: ("other", "third")
    service.create_room(room_id="excluded", name="Room", lineage_version=1, principal="human",
                        members=[dict(member_id=p, profile=p, handle=p) for p in ["other", "third"]])
    with pytest.raises(ValueError, match="local member"):
        service.send(room_id="excluded", event_id="user:x", principal="service:wanclone",
                     payload=dict(text="forbidden", thread_id="t", goal="Ship"))
