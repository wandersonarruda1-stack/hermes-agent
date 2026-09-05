# Hosted Rooms: request lineage v1

This is an opt-in protocol for **new** rooms. Create with
`groups.create(..., lineage_version=1)` on an authenticated RPC transport.
`groups.capabilities.lineage` advertises the contract. Existing rooms and
clients remain on version 0; retrying creation cannot change a room's version.
The frozen version travels in `groups.state`, `groups.log` and replicas.

For v1, `groups.send.payload` accepts `text`, `thread_id`, required `goal`,
and optional `re`. A reopen must provide `re` in the same room/thread; omitting
`goal` on reopen explicitly inherits the referenced goal. Supplying a goal
replaces it. A new event ID is required for a new root.

The founder is a digest of the **server-authenticated transport identity**
(`provider`, `user_id`). RPC parameters, prompt text, members and peers cannot
set it. Unauthenticated/legacy-token/stdio transports without that identity
cannot create or send to v1 rooms. There is no fallback to `user:desktop` for
v1; deployment must supply the authenticated identity before enabling it.
Only the original founder may reopen an existing thread.

The gateway seals `goal`, `root_event_id`, `re`/`parent_event_id`, `depth`,
`requester`, `founder`, and a canonical `lineage_digest`. User roots have depth
0. Member replies inherit the request's depth; a subsequent member request
increments the depth of its actual parent, independently of the round number.
The metadata enters the prompt and task identity and is checked on replay.
Driver/RoomLink wire payloads stay unchanged; the prompt carries the sealed
context and the task ID binds its digest.

A request at depth 5 is bounded before creating any task/session/dispatch.
The gateway appends an idempotent `room.activity` with `reason_code=max_depth`,
`attempted_depth`, `return_to`, `founder`, `goal`, `re`, and the target member.
The receipt ID derives from room/root/parent/target/max-depth. It is a control
event, never a member message or a task to read the receipt. `groups.log`
exposes both structured addressing and human-readable `text` for clients to
display. No renderer-owned Discussion engine is changed by this protocol.

Admission and founder checks serialize with planning. Event conflicts keep
the store's canonical-payload rule. Replicas preserve the version and log;
takeover reproduces the task identity. Complete durable room history is used
for reopen authorization, including references no longer in an active policy
checkpoint; expired history fails closed.

The PR1 round, message and fan-out budgets remain independent limits. Defaults
remain six members and three rounds. Reaching depth 4 is not a promise that
another budget cannot bound the discussion earlier.

Local automated tests only. Deployment, live RoomLink tests and promotion of
the pilot remain the coordinator's separate gates.
