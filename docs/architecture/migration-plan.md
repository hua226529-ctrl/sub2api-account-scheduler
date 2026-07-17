# Control Plane Migration Plan

The core control-plane migration is complete through Core C. Every checkpoint remains independently reviewable and the final worktree is intentionally left uncommitted until the release gate.

| Checkpoint | Status | Result |
| --- | --- | --- |
| Stage 0 | Complete | Behavioral, query-count and approximately 10-account performance baselines. |
| Stage 1A/1B/1C | Complete | Typed Intent/Authority, pure Arbiter, stable identity, legacy adapters and read-only shadow. |
| Core A | Complete | AccountControl is the only account writer; durable overrides, journal, readback, keyed serialization and uncertain recovery. |
| Core B | Complete | Reconcile Coordinator, event-triggered targeted passes, periodic full fallback, telemetry isolation and interactive/background Agent lanes. |
| Core C1 | Complete | Persistent goal runtime is the only Agent runtime; typed chat intent and one-use high-risk confirmation. |
| Core C2 | Complete | One policy lifecycle with typed patch, simulation, deterministic risk, constrained auto-publish and rollback. |
| Core C3 | Complete | One Group Transition Executor, fixed three-level failover, persisted post-switch evidence validation, independent failover mode, per-pool isolation, mutation budget and typed scheduled occurrence identity. |
| Core C4 | Complete | Independent admin secret, trusted proxies, bounded limiters, frontend lifecycle controls, migration/operations documentation and legacy code removal. |

## Non-negotiable final constraints

- Deterministic scheduling continues with the last active policy when models are unavailable.
- All account writes go through AccountControl and all token group writes go through the Group Transition Executor.
- Reconcile passes start only through the Coordinator; periodic full reconciliation remains the final consistency fallback.
- Freeze, exact administrator authorization, freshness, locks, cooldown, rate limit, idempotency, journal, readback and audit cannot be bypassed.
- Agent autonomous actions remain TTL-bound and evidence/snapshot-bound.
- SQLite and the single-process architecture remain in place.
- Migrations are additive, repeatable and preserve all current and historical control data.
- Fixed failover targets are unobservable before switching; `applied` is separate from health and only post-switch evidence above persisted watermarks may validate a target.

## Legacy data

`agent_runs` and `agent_tool_calls` are retained for historical query/export only. Production has no write or recovery methods for those tables. A future optional cleanup must require a backup/export procedure and is not part of the core migration.

No further feature phase is implied by this document. The next step after review is the independent commit, Linux race CI and release gate.
