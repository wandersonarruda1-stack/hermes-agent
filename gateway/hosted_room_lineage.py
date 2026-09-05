"""Server-owned, replayable request edges. No model or transport side effects."""

import hashlib
import json
from collections.abc import Mapping

MAX_DEPTH = 4
FIELDS = frozenset({
    "goal",
    "root_event_id",
    "re",
    "parent_event_id",
    "depth",
    "requester",
    "founder",
    "lineage_digest",
})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def seal(value):
    result = {k: value[k] for k in FIELDS - {"lineage_digest"}}
    result["lineage_digest"] = hashlib.sha256(canonical(result).encode()).hexdigest()
    return result


def validate(value):
    if not FIELDS <= value.keys():
        raise ValueError("incomplete lineage")
    if (
        not isinstance(value["goal"], str)
        or not value["goal"].strip()
        or len(value["goal"]) > 2048
    ):
        raise ValueError("goal is required (maximum 2048 characters)")
    if type(value["depth"]) is not int or not 0 <= value["depth"] <= MAX_DEPTH:
        raise ValueError("invalid lineage depth")
    for key in ("root_event_id",):
        if not isinstance(value[key], str) or not value[key]:
            raise ValueError("invalid lineage reference")
    if value["re"] != value["parent_event_id"]:
        raise ValueError("parent aliases disagree")
    for key in ("requester", "founder"):
        actor = value[key]
        if (
            not isinstance(actor, Mapping)
            or set(actor) != {"kind", "id"}
            or actor["kind"] not in ("user", "member")
            or not isinstance(actor["id"], str)
            or not actor["id"]
        ):
            raise ValueError("invalid lineage principal")
    if (
        value["founder"]["kind"] != "user"
        or seal(value)["lineage_digest"] != value["lineage_digest"]
    ):
        raise ValueError("lineage digest/principal mismatch")
    return {k: value[k] for k in FIELDS}


def user_payload(payload, *, event_id, principal, events):
    if not isinstance(principal, str) or not principal:
        raise ValueError("authenticated RPC principal required")
    if not isinstance(payload, Mapping) or set(payload) - {
        "text",
        "thread_id",
        "goal",
        "re",
    }:
        raise ValueError("client cannot supply server-owned lineage")
    actor = {"kind": "user", "id": principal}
    thread = payload.get("thread_id")
    previous = [
        e
        for e in events
        if e["payload"].get("thread_id") == thread
        and e["kind"] == "message.user"
        and e["event_id"] != event_id
    ]
    # A new event in an existing thread is a reopen, even if the caller omits re.
    if previous and previous[0]["payload"].get("founder") != actor:
        raise ValueError("only the authenticated founder may reopen")
    parent_id = payload.get("re")
    if previous and not parent_id:
        raise ValueError("re is required to reopen an existing thread")
    parent = None
    if parent_id is not None:
        parent = next((e for e in events if e["event_id"] == parent_id), None)
        if not parent or parent["payload"].get("thread_id") != thread:
            raise ValueError("parent must exist in the same room/thread")
        if parent["payload"].get("founder") != actor:
            raise ValueError("only the authenticated founder may reopen")
    goal = payload.get("goal", parent["payload"].get("goal") if parent else None)
    result = seal(
        dict(
            goal=goal,
            root_event_id=event_id,
            re=parent_id,
            parent_event_id=parent_id,
            depth=0,
            requester=actor,
            founder=actor,
        )
    )
    validate(result)
    return {"text": payload.get("text"), "thread_id": thread, **result}


def edge(root, parent=None):
    if parent is None:
        value = dict(validate(root["payload"]))
        value.update(re=root["event_id"], parent_event_id=root["event_id"])
    else:
        value = dict(validate(parent["payload"]))
        value.update(
            re=parent["event_id"],
            parent_event_id=parent["event_id"],
            depth=value["depth"] + 1,
            requester={"kind": "member", "id": parent["payload"]["member_id"]},
        )
    return seal(value)


def receipt(room_id, target, value):
    seed = [room_id, value["root_event_id"], value["re"], target, MAX_DEPTH]
    event_id = (
        "lineage-return:" + hashlib.sha256(canonical(seed).encode()).hexdigest()[:48]
    )
    return event_id, {
        **{k: value[k] for k in ("goal", "root_event_id", "re", "founder")},
        "attempted_depth": value["depth"],
        "return_to": value["requester"],
        "requester": value["requester"],
        "target_member_id": target,
        "text": f"Delegation refused at depth 5; returned to @{value['requester']['id']}; founder must decide.",
    }
