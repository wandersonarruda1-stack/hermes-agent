"""Admission authority and restart/checkpoint use real service/store paths."""

import threading
from types import SimpleNamespace
import pytest
from gateway import hosted_rooms as store
from gateway import hosted_room_discussion as policy
from tui_gateway.hosted_room_service import HostedRoomService
from tui_gateway.methods_groups import _lineage_principal
from tui_gateway.transport import bind_transport, reset_transport


def service(tmp_path):
    s = HostedRoomService(
        SimpleNamespace(_methods={}, _sessions={}, _sessions_lock=threading.Lock()),
        db_path=tmp_path / "state.db",
    )
    s.local_profiles = lambda: ("a", "b")
    return s


def test_authenticated_principal_and_reopen_with_real_service(tmp_path):
    s = service(tmp_path)
    token = bind_transport(
        SimpleNamespace(auth_identity={"user_id": "founder", "provider": "test"})
    )
    try:
        principal = _lineage_principal()
    finally:
        reset_transport(token)
    assert _lineage_principal() is None
    room = s.create_room(
        room_id="r",
        name="Test",
        members=[dict(member_id=p, profile=p, handle=p) for p in ("a", "b")],
        lineage_version=1,
        principal=principal,
    )
    assert room["lineage_version"] == 1
    args = dict(
        room_id="r",
        event_id="user:one",
        payload=dict(text="@a work", thread_id="t", goal="Ship"),
    )
    with pytest.raises(ValueError):
        s.send(**args)
    first = s.send(**args, principal=principal)
    assert first["payload"]["founder"] == {"kind": "user", "id": principal}
    assert s.send(**args, principal=principal)["seq"] == first["seq"]
    with pytest.raises(ValueError):
        s.send(
            room_id="r",
            event_id="user:two",
            principal="other",
            payload=dict(text="reopen", thread_id="t", re="user:one"),
        )
    second = s.send(
        room_id="r",
        event_id="user:two",
        principal=principal,
        payload=dict(text="@b reopen", thread_id="t", re="user:one"),
    )
    assert second["payload"]["goal"] == "Ship"
    assert second["payload"]["root_event_id"] == "user:two"
    # Cold service rebuild; full reference remains usable after checkpoint processing.
    s.policy_checkpoint.compact_completed(room_id="r")
    cold = service(tmp_path)
    assert (
        cold.send(
            room_id="r",
            event_id="user:two",
            principal=principal,
            payload=dict(text="@b reopen", thread_id="t", re="user:one"),
        )["seq"]
        == second["seq"]
    )


def test_receipt_append_has_no_execution_or_rpc_side_effect(tmp_path, monkeypatch):
    s = service(tmp_path)
    room = s.create_room(
        room_id="r",
        name="Test",
        members=[dict(member_id=p, profile=p, handle=p) for p in ("a", "b")],
        lineage_version=1,
        principal="founder",
    )
    from gateway import hosted_room_lineage as l

    value = dict(
        goal="Ship",
        root_event_id="root",
        re="parent",
        depth=5,
        requester={"kind": "member", "id": "a"},
        founder={"kind": "user", "id": "founder"},
    )
    rid, receipt = l.receipt("r", "b", value)
    d = policy.DiscussionDecision(
        status="bounded",
        reason="max_depth",
        thread_id="t",
        discussion_event_id="root",
        receipt=receipt,
        receipt_event_id=rid,
    )

    def forbidden(*a, **kw):
        raise AssertionError("receipt must not dispatch")

    monkeypatch.setattr(s.runtime, "wakeup", forbidden)
    first = s._append_room_status(room, d)
    assert s._append_room_status(room, d)["seq"] == first["seq"]
    assert first["payload"]["return_to"] == {"kind": "member", "id": "a"}
    assert first["kind"] == "room.activity"


def test_rpc_ignores_caller_supplied_principal(tmp_path, monkeypatch):
    import tui_gateway.server as server

    s = service(tmp_path)
    monkeypatch.setattr(server, "get_hosted_room_service", lambda: s)
    params = dict(
        room_id="rpc-room",
        name="Test",
        lineage_version=1,
        principal="forged",
        members=[dict(member_id=p, profile=p, handle=p) for p in ("a", "b")],
    )
    assert "error" in server._methods["groups.create"](1, params)
    token = bind_transport(
        SimpleNamespace(auth_identity={"user_id": "real", "provider": "test"})
    )
    try:
        actor = _lineage_principal()
        assert "result" in server._methods["groups.create"](2, params)
        send = dict(
            room_id="rpc-room",
            event_id="first",
            principal="forged",
            payload=dict(text="@a work", thread_id="t", goal="Ship"),
        )
        result = server._methods["groups.send"](3, send)
        assert result["result"]["event"]["payload"]["founder"]["id"] == actor
    finally:
        reset_transport(token)
    assert "error" in server._methods["groups.send"](4, send)
