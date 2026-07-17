# Core C Final Control Plane

## Product control chain

The deterministic scheduler remains the primary controller. It reads the last active policy from SQLite and continues to reconcile when every model provider is unavailable.

```text
Telemetry / policy activation / override expiry / lock change / periodic fallback
                                  |
                         Reconcile Coordinator
                                  |
                    deterministic policy + Arbiter
                                  |
                    AccountControl Mutation Executor
                                  |
                 journal -> transport -> readback -> audit
```

Administrator chat, the Agent Operator, the Agent Optimizer, failover evaluation, and scheduled commands are intent producers. They never own a transport writer.

## Agent runtime and chat

The persistent goal/step/checkpoint runtime is the only production Agent runtime. Interactive and background lanes have independent workers and model slots. `ChatAsync` classifies and validates a closed `ChatIntent`, persists the message and goal, and returns without waiting for a model.

Supported intent types are `query`, `analysis`, `direct_action`, `policy_change`, `scheduled_action`, and `ambiguous`. Query, analysis, and ambiguous intents are fail-closed read-only. High-risk intents create a five-minute, one-use confirmation whose random token is returned once and whose hash and semantic binding are stored.

The legacy `agent_runs` and `agent_tool_calls` tables remain available for historical reads. No production method writes them and they are never used to restore new work.

## Policy lifecycle

`score_policy_versions` is the only strategy version lifecycle: draft, simulated, pending approval, active, rejected, rolled back, or superseded. A proposal contains a typed patch, field diff, local deterministic simulation, risk, affected scope, base version, idempotency identity, and audit metadata.

Only low-risk proposals with sufficient passing simulation data, `optimizer_mode=auto`, and remaining daily budget can auto-activate. Other proposals require an administrator. Activation fences the base version and atomically updates the deterministic projection. Rollback can only restore the recorded previous active version and triggers Reconcile after commit.

## Independent modes

- `scheduler_mode`: `observe` or `control`.
- `agent_optimizer_mode`: `disabled`, `observe`, `propose`, or `auto`.
- `agent_operator_mode`: `disabled`, `confirm`, or `direct`.
- `failover_mode`: `disabled`, `observe`, or `control`.

The legacy Agent `enabled` and `mode` columns are retained as derived compatibility projections. They do not authorize capabilities, auto-promote, or control failover.

## Group transition executor

All token group mutations use `balance.Manager.TransitionGroupTier`. It takes a typed tier request, checks freeze, confirmed policy/version, current readback, manual protection and idempotency, serializes by source/key, writes the journal before the only `Fetcher.SwitchGroup` call, and verifies afterward. Different keys can proceed concurrently. Uncertain recovery only reads back; it never blindly replays the transport write.

The raw group HTTP endpoint and `balance.Manager.SwitchGroup` are removed. Failover evaluates only the current fixed level, then advances to the next configured and enabled level. It does not dynamically select or preflight an unobservable target group. Successful write/readback records `applied` and starts persisted post-switch validation; only new evidence above the transition watermarks can confirm health. Pool isolation and the explicit per-cycle mutation budget, defaulting to one, remain in force. See [fixed-three-level-failover.md](fixed-three-level-failover.md).

## Scheduled commands

Scheduled commands persist typed intent/resource/operation/desired-state fields, authority, timezone, missed-run policy and a stable occurrence identity. Execution rechecks grant, freeze, TTL, resource state and capability policy. Account actions use AccountControl; group actions use the Group Transition Executor.

## Security boundary

`SUB2API_ADMIN_API_KEY` is server-to-server only. Browser login uses `SCHEDULER_ADMIN_SECRET` and never validates or stores the upstream key. Legacy login requires the explicit `ALLOW_LEGACY_ADMIN_KEY_LOGIN=true` compatibility switch and emits a deprecation warning.

Forwarded IP and scheme headers are ignored unless the socket peer belongs to `TRUSTED_PROXY_CIDRS`. Client IP is resolved from the trusted chain from right to left. Session, CSRF, confirmation and idempotency randomness fails closed on `crypto/rand` errors. In-memory login and write limiters have TTL cleanup and a fixed 4096-key capacity.

## Frontend and retained scope

The Vue console displays chat intent/confirmation, persistent task state, policy risk/diff/simulation/lifecycle, independent operating modes, group transition readback, post-switch validation/evidence state, and activity. Arbitrary group selection is not exposed. An applied transition is explicitly shown as awaiting evidence rather than healthy. The UI reuses the existing event stream and polling fallback.

This stage does not add a message broker, microservice, workflow platform, arbitrary model actions, a second database, or a second control path.
