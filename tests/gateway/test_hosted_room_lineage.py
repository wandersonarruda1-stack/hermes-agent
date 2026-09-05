"""Real store/planner/publication/replay tests for server-owned lineage."""

import copy
import pytest
from gateway import hosted_rooms as store
from gateway import hosted_room_discussion as policy
from gateway import hosted_room_lineage as lineage

PROFILES = ("a", "b", "c", "d", "e", "f")
PRINCIPAL = "principal:dashboard:founder"


def room(tmp_path):
    db = tmp_path / "state.db"
    value = store.create_room(
        db,
        room_id="room",
        name="Test",
        members=[dict(member_id=p, profile=p, handle=p) for p in PROFILES],
        authority_gateway_id="gateway",
        lineage_version=1,
    )
    return db, value


def events(db):
    return store.read_events(db, room_id="room", limit=store.MAX_LOG_LIMIT)["events"]


def user(db, event_id="user:one", principal=PRINCIPAL, **extra):
    payload = lineage.user_payload(
        dict(text="@a act", thread_id="thread", goal="Ship", **extra),
        event_id=event_id,
        principal=principal,
        events=events(db),
    )
    return store.append_event(
        db,
        room_id="room",
        event_id=event_id,
        kind="message.user",
        actor=dict(kind="user", id=principal),
        payload=payload,
        authority_gateway_id="gateway",
        authority_epoch=1,
    )


def decision(db, value):
    return policy.plan_next_task(value, events(db), local_profiles=PROFILES)


def publish(db, value, task, text):
    plan = policy.plan_publication(
        value,
        events(db),
        task,
        status="settled",
        result={"text": text},
        local_profiles=PROFILES,
    )
    for event in plan.events:
        store.append_event(db, **event.append_kwargs("room"))


def test_chain_depth_limit_replay_and_return_without_sixth_task(tmp_path):
    db, value = room(tmp_path)
    user(db)
    task_ids = []
    for depth in range(5):
        d = decision(db, value)
        assert d.status == "task"
        assert d.task.lineage["depth"] == depth
        assert d.task.lineage["goal"] == "Ship"
        task_ids.append(d.task.identity.task_id)
        rebuilt = policy.reconstruct_task_plan(
            value,
            events(db),
            {"identity": d.task.identity, "payload": d.task.payload},
            local_profiles=PROFILES,
        )
        assert rebuilt == d.task
        publish(db, value, d.task, "@" + PROFILES[depth + 1] + " continue")
    d = decision(db, value)
    assert d.status == "bounded" and d.reason == "max_depth" and d.task is None
    assert d.receipt["attempted_depth"] == 5
    assert d.receipt["return_to"] == {"kind": "member", "id": "e"}
    assert d.receipt["founder"] == {"kind": "user", "id": PRINCIPAL}
    assert decision(db, store.room_state(db, room_id="room")) == d
    kwargs = dict(
        room_id="room",
        event_id=d.receipt_event_id,
        kind="room.activity",
        actor={"kind": "gateway", "id": "gateway"},
        authority_gateway_id="gateway",
        authority_epoch=1,
        payload={
            "status": "bounded",
            "reason_code": "max_depth",
            "thread_id": "thread",
            "discussion_event_id": d.discussion_event_id,
            **d.receipt,
        },
    )
    first = store.append_event(db, **kwargs)
    assert store.append_event(db, **kwargs)["seq"] == first["seq"]
    assert decision(db, value).status == "idle"
    assert len(set(task_ids)) == 5
    forged = events(db)
    forged[-1]["payload"]["return_to"] = {"kind": "member", "id": "a"}
    with pytest.raises(ValueError):
        policy.plan_next_task(value, forged, local_profiles=PROFILES)
    user(db, "user:reopen", re=d.receipt_event_id)
    assert decision(db, value).task.lineage["depth"] == 0


def test_parent_founder_goal_and_conflicting_replay(tmp_path):
    db, value = room(tmp_path)
    root = user(db)
    assert user(db)["seq"] == root["seq"]
    with pytest.raises(ValueError):
        user(db, "user:other", principal="someone-else", re=root["event_id"])
    with pytest.raises(ValueError):
        user(db, "user:bad", re="foreign-event")
    with pytest.raises(ValueError):
        lineage.user_payload(
            {"text": "x", "thread_id": "other", "goal": "Ship", "re": root["event_id"]},
            event_id="x",
            principal=PRINCIPAL,
            events=events(db),
        )
    for spoof in (
        {"depth": 0},
        {"founder": {"kind": "user", "id": "fake"}},
        {"requester": "fake"},
    ):
        with pytest.raises(ValueError):
            lineage.user_payload(
                {"text": "x", "thread_id": "thread", "goal": "Ship", **spoof},
                event_id="x",
                principal=PRINCIPAL,
                events=events(db),
            )
    with pytest.raises(ValueError):
        lineage.user_payload(
            {"text": "x", "thread_id": "new"},
            event_id="x",
            principal=PRINCIPAL,
            events=[],
        )
    changed = copy.deepcopy(root["payload"])
    changed["goal"] = "Changed"
    changed.update(lineage.seal(changed))
    with pytest.raises(store.EventConflictError):
        store.append_event(
            db,
            room_id="room",
            event_id=root["event_id"],
            kind="message.user",
            actor=root["actor"],
            payload=changed,
            authority_gateway_id="gateway",
            authority_epoch=1,
        )


def test_forged_member_depth_is_rejected_even_with_recomputed_digest(tmp_path):
    db, value = room(tmp_path)
    user(db)
    publish(db, value, decision(db, value).task, "@b go")
    publish(db, value, decision(db, value).task, "@c go")
    log = events(db)
    member = [e for e in log if e["kind"] == "message.member"][-1]
    member["payload"]["depth"] = 0
    member["payload"].update(lineage.seal(member["payload"]))
    with pytest.raises(ValueError):
        policy.plan_next_task(value, log, local_profiles=PROFILES)


def test_branch_depth_is_parent_relative(tmp_path):
    db, value = room(tmp_path)
    user(db)
    publish(db, value, decision(db, value).task, "@b @c go")
    b = decision(db, value).task
    assert b.lineage["depth"] == 1
    publish(db, value, b, "done")
    c = decision(db, value).task
    assert c.lineage["depth"] == 1
    assert b.lineage["re"] == c.lineage["re"]


def test_replica_takeover_preserves_lineage_and_task(tmp_path, monkeypatch):
    from gateway import hosted_room_replicas as replicas

    db, value = room(tmp_path)
    user(db)
    first = decision(db, value).task
    replica_db = tmp_path / "replica.db"
    replicas.ingest_page(
        replica_db,
        room_id="room",
        room_name="Test",
        members=value["members"],
        page=store.read_events(db, room_id="room", limit=100),
    )
    monkeypatch.setattr(replicas, "local_authority_gateway_id", lambda: "gateway-next")
    replicas.promote_replica(replica_db, room_id="room")
    promoted = store.room_state(replica_db, room_id="room")
    assert promoted["lineage_version"] == 1
    replay = decision(replica_db, promoted).task
    assert replay.identity == first.identity and replay.lineage == first.lineage


def test_peer_target_at_depth_four_but_no_depth_five_plan(tmp_path):
    db, value = room(tmp_path)
    for member in value["members"][-2:]:
        member["target"] = dict(
            kind="peer",
            peer_id="brian",
            installation_id="remote",
            profile=member["profile"],
            capability_digest="a" * 64,
        )
    user(db)
    remote_plans = []
    for depth in range(5):
        task = decision(db, value).task
        if task.member.target["kind"] == "peer":
            remote_plans.append(task)
        publish(db, value, task, "@" + PROFILES[depth + 1] + " continue")
    bounded = decision(db, value)
    assert [t.lineage["depth"] for t in remote_plans] == [4]
    assert bounded.task is None and bounded.receipt["target_member_id"] == "f"


@pytest.mark.parametrize(
    "field,limit,reason",
    [
        ("max_turns_per_round", 1, "round_budget_too_small"),
        ("max_messages_total", 1, "max_messages"),
    ],
)
def test_budgets_still_bound_whole_event_with_lineage(tmp_path, field, limit, reason):
    db, value = room(tmp_path)
    value["discussion_policy"][field] = limit
    root = lineage.user_payload(
        dict(text="@a @b go", thread_id="t", goal="Ship"),
        event_id="budget-root",
        principal=PRINCIPAL,
        events=[],
    )
    store.append_event(
        db,
        room_id="room",
        event_id="budget-root",
        kind="message.user",
        actor={"kind": "user", "id": PRINCIPAL},
        payload=root,
        authority_gateway_id="gateway",
        authority_epoch=1,
    )
    d = decision(db, value)
    assert d.reason == reason and d.task is None


def test_checkpoint_retains_ancestors_after_completed_thread_compaction(tmp_path):
    from gateway.hosted_room_policy_checkpoint import HostedRoomPolicyCheckpoint

    db, value = room(tmp_path)
    user(db)
    for depth in range(5):
        publish(db, value, decision(db, value).task, "@" + PROFILES[depth + 1] + " go")
    bounded = decision(db, value)
    store.append_event(
        db,
        room_id="room",
        event_id=bounded.receipt_event_id,
        kind="room.activity",
        actor={"kind": "gateway", "id": "gateway"},
        authority_gateway_id="gateway",
        authority_epoch=1,
        payload={
            "status": "bounded",
            "reason_code": "max_depth",
            "thread_id": "thread",
            "discussion_event_id": bounded.discussion_event_id,
            **bounded.receipt,
        },
    )
    checkpoint = HostedRoomPolicyCheckpoint(db)
    checkpoint.sync(room_id="room", latest_seq=events(db)[-1]["seq"])
    checkpoint.compact_completed(room_id="room")
    user(db, "user:reopen", re=bounded.receipt_event_id)
    # Emulate transcript truncation to the newest old member + new root.
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute(
            "DELETE FROM hosted_room_policy_transcript WHERE seq < ?",
            (events(db)[-4]["seq"],),
        )
    snapshot = checkpoint.snapshot(room_id="room", latest_seq=events(db)[-1]["seq"])
    next_task = policy.plan_next_task(
        value,
        snapshot.events,
        local_profiles=PROFILES,
        initial_watermarks=snapshot.watermarks,
    ).task
    assert next_task.lineage["root_event_id"] == "user:reopen"
    assert next_task.lineage["depth"] == 0
