# Control Plane API Notes

All `/api` endpoints require the scheduler session. Mutations additionally require same-origin, CSRF, rate-limit and explicit confirmation checks.

- `POST /api/session`: accepts the scheduler administrator secret, never the upstream API key.
- `POST /api/agent/chat`: persists a typed chat intent and goal; it does not wait for a model.
- `POST /api/agent/goals/{id}/confirm`: consumes a one-use confirmation token bound to the exact intent.
- `POST /api/agent/policies/{id}/activate`: approves and activates a validated, simulated proposal with base-version fencing.
- `POST /api/agent/policies/{id}/reject`: rejects a proposal with a reason.
- `POST /api/agent/policies/{id}/rollback`: restores the recorded previous active version.
- `POST /api/upstreams/{id}/keys/{keyID}/tier`: submits a typed main/backup/emergency transition to the Group Transition Executor.

There is no arbitrary group-ID mutation endpoint. Account mutations are not exposed as direct Sub2API writes; handlers delegate to AccountControl.
