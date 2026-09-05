"""Frozen room budgets, whole-event rejection and deterministic larger rosters."""

import sqlite3
from dataclasses import FrozenInstanceError

import pytest

from gateway import hosted_room_discussion as discussion
from gateway import hosted_room_driver as driver
from gateway import hosted_rooms as rooms
from gateway import hosted_room_replicas as replicas
from gateway.config import GatewayConfig, load_gateway_config
from gateway.hosted_room_discussion_policy import DiscussionPolicy


def roster(n):
    return [
        {"member_id": f"m{i}", "profile": f"p{i}", "handle": f"p{i}"} for i in range(n)
    ]


def create(tmp_path, n=10, **budget):
    db = tmp_path / "state.db"
    policy = DiscussionPolicy.from_dict({
        "max_members": n,
        "max_turns_per_round": n,
        "max_messages_total": 30,
        **budget,
    })
    room = rooms.create_room(
        db,
        room_id="test",
        name="Test",
        members=roster(n),
        authority_gateway_id="host-a",
        discussion_policy=policy,
    )
    return db, room


def events(db):
    return rooms.read_events(db, room_id="test")["events"]


def user(db, text="Discuss", event_id="user-1"):
    return rooms.append_event(
        db,
        room_id="test",
        event_id=event_id,
        kind="message.user",
        actor={"kind": "user", "id": "tester"},
        authority_gateway_id="host-a",
        authority_epoch=1,
        payload={"text": text, "thread_id": "thread"},
    )


def plan(db, room):
    return discussion.plan_next_task(
        room, events(db), local_profiles=[m["profile"] for m in room["members"]]
    )


def settle(db, room, task, text="done"):
    publication = discussion.plan_publication(
        room,
        events(db),
        task,
        status="settled",
        result={"text": text},
        local_profiles=[m["profile"] for m in room["members"]],
    )
    for event in publication.events:
        rooms.append_event(db, **event.append_kwargs("test"))


@pytest.mark.parametrize("n", [7, 10])
def test_larger_roster_restart_reconstruction_and_replay(tmp_path, n):
    db, room = create(tmp_path, n)
    original = user(db)
    ids = []
    for index in range(n):
        task = plan(db, room).task
        assert task is not None
        assert task.identity.turn_id.split(".")[2] == f"p{index}"
        assert task.seen_through_seq >= original["seq"]
        assert len(task.payload["prompt"].encode()) <= driver.MAX_PROMPT_BYTES
        rebuilt = discussion.reconstruct_task_plan(
            room,
            events(db),
            {"identity": task.identity, "payload": task.payload},
            local_profiles=[m["profile"] for m in room["members"]],
        )
        assert rebuilt == task
        if index == 7:
            room = rooms.room_state(db, room_id="test")
            assert plan(db, room).task.identity == task.identity
        ids.append(task.identity.task_id)
        settle(db, room, task)
    assert len(set(ids)) == n
    assert len([e for e in events(db) if e["kind"] == "turn.settled"]) == n
    assert plan(db, room).task is None
    assert user(db)["seq"] == original["seq"]
    assert plan(db, room).task is None
    with pytest.raises(rooms.EventConflictError):
        user(db, "Changed")


def test_three_rounds_thirty_messages(tmp_path):
    db, room = create(tmp_path)
    user(db)
    tasks = []
    while (task := plan(db, room).task) is not None:
        assert len(tasks) < 30
        tasks.append(task)
        rebuilt = discussion.reconstruct_task_plan(
            room, events(db), {"identity": task.identity, "payload": task.payload},
            local_profiles=[f"p{i}" for i in range(10)],
        )
        assert rebuilt == task
        settle(db, room, task, "@everyone Continue")
    assert len(tasks) == 30
    assert {t.round_index for t in tasks} == {0, 1, 2}
    assert plan(db, room).reason == "max_messages"


def test_round_budget_refuses_entire_broadcast_but_admits_seven_mentions(tmp_path):
    db, room = create(tmp_path, max_turns_per_round=7)
    user(db)
    assert plan(db, room).reason == "round_budget_too_small"
    assert plan(db, room).task is None
    user(db, " ".join(f"@p{i}" for i in range(7)), "user-2")
    for _ in range(7):
        task = plan(db, room).task
        assert task is not None
        settle(db, room, task)
    assert plan(db, room).task is None


def test_total_budget_refuses_before_first_task(tmp_path):
    db, room = create(tmp_path, max_messages_total=9)
    user(db)
    assert plan(db, room).reason == "max_messages"
    assert plan(db, room).task is None


@pytest.mark.parametrize("field", list(DiscussionPolicy().to_dict()))
def test_overrides_only_reduce_and_are_strict(field):
    policy = DiscussionPolicy()
    with pytest.raises(ValueError, match="operator ceiling"):
        policy.reduce({field: getattr(policy, field) + 1})
    for invalid in (0, True, -1, "10", 1.5):
        with pytest.raises(ValueError):
            DiscussionPolicy.from_dict({field: invalid})
    with pytest.raises(FrozenInstanceError):
        policy.max_members = 10


def test_default_six_and_compiled_cap():
    assert (
        len(
            discussion.validate_roster(
                roster(6), local_profiles=[f"p{i}" for i in range(7)]
            )
        )
        == 6
    )
    with pytest.raises(discussion.DiscussionValidationError):
        discussion.validate_roster(
            roster(7), local_profiles=[f"p{i}" for i in range(7)]
        )
    assert DiscussionPolicy(max_members=32).max_members == 32
    with pytest.raises(ValueError):
        DiscussionPolicy(max_members=33)


def test_config_snapshot_identity_and_takeover(tmp_path, monkeypatch):
    db, room = create(tmp_path)
    policy = room["discussion_policy"]
    cfg = GatewayConfig.from_dict({"hosted_rooms": {"discussion": policy}})
    assert GatewayConfig.from_dict(cfg.to_dict()).hosted_rooms["discussion"] == policy
    user(db)
    for _ in range(7):
        settle(db, room, plan(db, room).task)
    original = plan(db, room).task
    page = rooms.read_events(db, room_id="test")
    replica_db = tmp_path / "replica.db"
    replicas.ingest_page(
        replica_db, room_id="test", room_name="Test", members=roster(10), page=page
    )
    assert (
        replicas.replica_state(replica_db, room_id="test")["discussion_policy"]
        == policy
    )
    monkeypatch.setattr(replicas, "local_authority_gateway_id", lambda: "host-b")
    replicas.promote_replica(replica_db, room_id="test")
    promoted = rooms.room_state(replica_db, room_id="test")
    assert promoted["discussion_policy"] == policy
    assert plan(replica_db, promoted).task.identity.task_id == original.identity.task_id
    from gateway.hosted_room_policy_checkpoint import HostedRoomPolicyCheckpoint

    snapshot = HostedRoomPolicyCheckpoint(replica_db).snapshot(
        room_id="test", latest_seq=promoted["latest_seq"]
    )
    resumed = discussion.plan_next_task(
        promoted,
        snapshot.events,
        local_profiles=[f"p{i}" for i in range(10)],
        initial_watermarks=snapshot.watermarks,
    )
    assert resumed.task.identity.task_id == original.identity.task_id
    assert rooms.create_room(
        db,
        room_id="test",
        name="Test",
        members=roster(10),
        authority_gateway_id="host-a",
        discussion_policy=policy,
    )["idempotent"]
    with pytest.raises(rooms.RoomConflictError):
        rooms.create_room(
            db,
            room_id="test",
            name="Test",
            members=roster(10),
            authority_gateway_id="host-a",
            discussion_policy={**policy, "max_messages_total": 29},
        )


def test_legacy_migration_does_not_rewrite_history(tmp_path):
    db = tmp_path / "state.db"
    old = rooms.create_room(
        db,
        room_id="test",
        name="Test",
        members=roster(2),
        authority_gateway_id="host-a",
    )
    user(db)
    original = plan(db, old).task
    before = events(db)
    with sqlite3.connect(db) as conn:
        conn.execute("ALTER TABLE hosted_rooms DROP COLUMN discussion_policy_json")
    migrated = rooms.room_state(db, room_id="test")
    assert migrated["discussion_policy"] == DiscussionPolicy().to_dict()
    assert events(db) == before
    assert plan(db, migrated).task == original


def test_yaml_config_load(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text(
        "gateway:\n  hosted_rooms:\n    discussion:\n      max_members: 10\n      max_turns_per_round: 10\n      max_messages_total: 30\n"
    )
    assert load_gateway_config().hosted_rooms["discussion"]["max_members"] == 10


def test_later_round_counts_already_executed_members(tmp_path):
    db, room = create(tmp_path, n=4, max_turns_per_round=2)
    user(db, "@p0 begin")
    for mention in ("@p1", "@p2", "@p3"):
        task = plan(db, room).task
        assert task is not None
        settle(db, room, task, mention)
    decision = plan(db, room)
    assert decision.task is None
    assert decision.reason == "round_budget_too_small"
    assert len([e for e in events(db) if e["kind"] == "turn.settled"]) == 3


def test_forged_position_and_round_fail_reconstruction(tmp_path):
    from dataclasses import replace

    db, room = create(tmp_path)
    user(db, "@p0 @p1 begin")
    task = plan(db, room).task
    for turn in (
        task.identity.turn_id.replace(".p0.", ".p9."),
        task.identity.turn_id.replace(".r0.", ".r32."),
    ):
        forged = {
            "identity": replace(task.identity, turn_id=turn),
            "payload": task.payload,
        }
        with pytest.raises(discussion.DiscussionReconstructionError):
            discussion.reconstruct_task_plan(
                room, events(db), forged, local_profiles=[f"p{i}" for i in range(10)]
            )


def test_ten_peer_prompt_is_byte_bounded_and_deterministic(tmp_path):
    db, room = create(tmp_path, max_delta_lines=2)
    user(db, "@everyone " + "x" * 65500)
    first = plan(db, room).task
    settle(db, room, first, "y" * 65500)
    second = plan(db, room).task
    assert len(second.payload["prompt"].encode()) <= driver.MAX_PROMPT_BYTES
    assert all(f"@p{i}" in second.payload["prompt"] for i in range(10))
    assert "Earlier content omitted" in second.payload["prompt"]
    assert plan(db, rooms.room_state(db, room_id="test")).task == second
