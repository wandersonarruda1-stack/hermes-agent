# Hosted Discussion room budgets

The operator sets ceilings in `config.yaml`. Defaults remain six members,
three rounds, six eligible turns per round, ten member messages and 24 delta
lines. For a new ten-member room:

```yaml
gateway:
  hosted_rooms:
    discussion:
      max_members: 10
      max_rounds: 3
      max_turns_per_round: 10
      max_messages_total: 30
      max_delta_lines: 24
```

All fields require integers; booleans, zero, strings and unlimited values are
rejected. Compiled bounds are members 2–32, rounds 1–32, turns per round 1–32,
messages 1–1024 and delta lines 1–1024. Prompts retain the independent byte cap.

`groups.create` accepts an optional `discussion_policy` object with any subset
of these fields. Each supplied value can only reduce the operator ceiling.
The complete effective policy is persisted canonically in
`hosted_rooms.discussion_policy_json`, participates in idempotent room identity,
and appears in the atomic `room.created` event. Create/state return the frozen
policy; capabilities return operator ceilings. Log pages carry the policy for
replication. Takeover preserves it. Editing config never changes existing rooms.
Legacy rows receive compiled defaults, without changing their existing events.
The generic legacy storage API retains its event numbering when called without
an explicit Discussion policy.

A broadcast selects everyone; explicit mentions select their addressed members.
The selected set must fit both the per-round execution budget and the remaining
message budget before its first task. If it cannot fit, no subset is executed.
The gateway records a deterministic `room.activity` with `status=bounded`,
`reason_code=round_budget_too_small` or `max_messages`, `thread_id` and
`discussion_event_id`. For an initial admission refusal `groups.send` returns
that event in `event.receipt`. This control receipt never invokes a model.
Repeated sends reuse the same input and receipt; changed content conflicts.
Later rounds enforce the same budgets before admitting their selected work.

This intentionally replaces partial exhaustion behavior: with six replies
already published and four message slots left, a new five-member selection is
refused as a whole. The default ceiling values are unchanged.

The Desktop sync projection preserves up to the compiled 32-member bound and
carries the frozen policy. Its room header displays the effective cap when that
policy is present. Legacy Desktop-created rooms keep their six-member default.
All tests use temporary databases and mock transports; no real room is needed.
