# RC.2 Agent Runtime Hardening

## Scope

This release candidate fixes the Runtime V2 failures observed in RC.1. It does not restore Agent V1, change deterministic account scoring, alter fixed three-level failover, add a second write path, or deploy the service. The deterministic scheduler remains independent of model availability.

## Final decision contract

Runtime V2 requests one strict `json_schema` object containing `summary`, `conclusion`, `confidence`, `no_change`, `actions`, `advice`, `data_limitations`, and `evidence_requests`. `EvidenceRequest` is a closed object with `tool` and optional typed resource selectors. The local decoder also uses `DisallowUnknownFields`, validates a single JSON value, and rejects missing arrays, string evidence requests, nonempty final evidence requests, invalid confidence, and `no_change` with actions.

A provider which explicitly rejects `json_schema` gets one downgrade to `json_object`; arbitrary text is never accepted. The first contract error receives one exact repair prompt in the same durable lease. A second error terminally fails and dead-letters the goal as `model_contract_invalid`.

## Bounded execution

Runtime errors use stable classes: model contract, tool arguments, no progress, provider rate limit, provider timeout, provider server, provider authentication, runtime internal, and external mutation uncertain. Provider 429, timeout, and 5xx failures use deterministic exponential backoff plus bounded jitter and stop after the goal's `max_attempts`, default five. Provider authentication/configuration failures are terminal. Independently, each goal has a hard ceiling of 16 model requests across native calls, compatibility fallback, providers, leases, and restarts, so changing invalid arguments cannot evade the same-fingerprint guard indefinitely.

Failed tool fingerprints use capability plus normalized JSON. One repeat receives a no-progress warning; the next identical repeat terminally fails as `model_no_progress`. External mutation uncertainty remains on the existing readback-only reconciliation path and is never mixed with model retry or blindly replayed.

## Decision and capability safety

`ValidateRuntimeDecision` is the single pure semantic gate before fallback actions are mapped or a final result completes. It checks immutable packet membership, chat intent operation and resources, read-only and ambiguous boundaries, conflict and scope limits, group confidence, and capability TTL. Capability execution independently enforces autonomous support, exact typed grants, confirmation, fresh snapshots, evidence, scope, default/maximum TTL, and scheduling support.

`transition_token_group_tier` is critical, not autonomous, and always requires a consumed administrator confirmation when invoked by Agent Runtime. Deterministic EmergencyAutomation remains a separate failover producer and still uses the one Group Transition Executor.

## Scope, telemetry, and no-op control

Operational events now use `account:<id>`, `monitor:<id>`, or `global`; monitor-only and global events never invent account zero. Emergency deduplication includes kind, event type, resource scope, and one-minute aggregation window. Merged goals append audit event references, while an active-goal cap records an explicit deferred event.

Telemetry filters targeted reconcile IDs against the current in-memory bindings after data commit. Invalid or unbound targets are omitted with one aggregate `telemetry_unbound_targets_ignored` log entry and no additional database or network read.

Automatic load decisions whose upstream state is already correct do not create `applied_noop` mutations. A consistent local projection records one deterministic `desired_already_applied` snapshot. A safely repairable projection uses a short SQLite projection update before that snapshot. Explicit administrator and Agent commands retain their mutation journals even when the upstream result is a no-op.

## Runtime V2 provenance

Runtime V2 authority is `GoalID + StepID`; evidence authority is `PacketID + PacketHash`. Legacy `RunID` remains readable and is zero for V2 records. Account mutations, group transitions, policy proposals, action outcomes, general audit events, Agent events, and evidence references now preserve the appropriate V2 identifiers. Outcome evaluation reads the stored packet directly for V2 and falls back to `agent_runs.packet_id` only for V1 history. The UI does not present zero as a run identifier.

## Additive migration

The migration adds bounded-attempt, error, warning, and dead-letter columns to `agent_goals`; recent error counters to `agent_providers`; V2 provenance to `account_mutations`, `upstream_group_transitions`, `score_policy_versions`, `decision_outcomes`, and `events`. Existing rows receive compatible zero/empty defaults. Legacy planned/running/waiting goals whose stored error matches the known final-contract failure are conservatively failed and dead-lettered; existing terminal goals are not reactivated. Migrations are idempotent and preserve goals, steps, checkpoints, events, V1 history, policies, and journals.

## Verification boundary

Regression tests use only temporary SQLite databases, fake model servers, and fake Sub2API. They cover the field failure payloads, bounded contract repair, normalized no-progress termination, timeout backoff, read-only action rejection, autonomous group blocking, scoped emergency goals, telemetry filtering, 50 no-op reconcile passes, local projection repair, and a logical thirty-minute terminal-goal simulation with no additional model or upstream writes.
