# Hosted-room profile service authentication

An agent can authenticate as `service:<profile>` without using the founder's
human Basic session. Human login, cookies and `/api/auth/ws-ticket` are unchanged.

Provision on the POSIX gateway host (including Linux/WSL), under the installation's existing profile home:

```sh
python -m hermes_cli.dashboard_auth.service --profile wanclone
```

This creates `hosted-room-service.token` with mode0600 inside that profile's
home (`default` uses the installation root). It refuses to replace an existing
file and does not print the token. Each profile needs its own random credential;
do not put credentials in config.yaml, shared configuration or Multica custom_env.
The provider rejects loose permissions, foreign ownership, links and duplicate
credentials. Files are read on verification; removing a file revokes new tickets.
Existing admitted WebSockets remain authenticated until disconnected.

With dashboard authentication enabled, read the token from its file into the
client's memory and POST `/api/auth/service-ws-ticket` using an Authorization
Bearer header. Never put the long-lived token in argv, URLs or logs. Connect to
`/api/ws` with the returned single-use ticket within30seconds, using the existing
ticket/subprotocol mechanism. Reconnects require a fresh ticket. Across machines,
use HTTPS/WSS or an authenticated tunnel.

Only `groups.create`, `groups.send`, `groups.capabilities` and `gateway.ping`
are available to a service connection. Service tickets cannot open terminal,
console or event WebSockets and cannot authenticate the drain endpoint. To
inspect room logs, use the existing authorized human inspection path.

The service profile must be a **local member** of a lineage-v1 room, including
when it creates the room. Membership in a peer installation with the same profile
name grants no local authority. The authenticated identity is server-owned:
`provider=service`, `user_id=<profile>`; lineage records `service:<profile>`.
Supplying principal/founder fields does not impersonate another profile. The
existing founder-only thread-reopen and depth/budget contracts still apply.

No live rollout is part of the phase-2 code delivery. Before deployment, obtain
the independent code review and the founder's service-specific maintenance
window; preserve database backups and serial reconnect/rollback procedures.
