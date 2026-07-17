# Fixed Three-Level Failover

## Product contract

Each controlled upstream key has one operator-confirmed, fixed chain with at most three levels: `main`, `backup`, and `emergency`. Every level has an independent enabled flag and a configured group ID. A disabled or unconfigured level is skipped. There is no fourth level, dynamic candidate discovery, score, target readiness check, or model-selected group.

The target group is unobservable before the key is switched to it. Missing pre-switch target telemetry is therefore not a reason to block the transition and old target history is never evidence that the target is currently healthy.

```text
current level confirmed_failed
  -> next configured and enabled fixed level
  -> Group Transition Executor journal
  -> SwitchGroup transport
  -> target group readback
  -> transition=applied, validation=awaiting_evidence
  -> new post-switch evidence
       success threshold -> confirmed_healthy
       failure threshold -> confirmed_failed -> fixed next level
       timeout/no attributable evidence -> uncertain, no blind advance
```

Failure at `emergency` enters `exhausted`. Exhausted state is persisted, emits a critical event, and never cycles back to `main` or another group. Automatic return rules are not defined. The controller does not use historical or current data from an unselected main group to return automatically; an administrator may explicitly switch back through the executor.

## Failure and selection responsibilities

The controller evaluates only the currently selected level. Actionable failure is based on fresh current-key monitor evidence plus allowed hard traffic failures, or an explicit no-schedulable-channel condition. Credential, client, unknown, and ordinary model-capability errors cannot prove a group outage. Pool evaluation failures remain isolated and the explicit per-cycle mutation budget remains in force.

Current-level assessment uses stable reason codes: `all_channels_failed` for fresh allowed hard evidence, `all_channels_disabled` when every bound account is explicitly disabled, `no_schedulable_channels` when active accounts exist but none is schedulable, `evidence_insufficient` when the observable evidence cannot prove failure, `data_stale` when account or telemetry freshness gates fail, and `group_empty` when the configured pool has no bound channel. An invalid fixed next-level mapping is `configuration_invalid`. Only the first three are actionable outage results; stale, insufficient, empty, and invalid states never cause a blind advance.

Selection is a linear scan over the three configured slots after the current slot. It may skip disabled or empty slots, but it never scans `upstream_groups`, compares rate multipliers, ranks capacity, or checks target success rate. Upstream group metadata is used only to reject an invalid operator configuration before mutation.

## Applied versus healthy

`upstream_group_transitions` is the mutation journal. `applied` means that the journal was reserved before the transport write, the write returned, and readback matched the requested group. It says nothing about target availability.

`upstream_group_failover_states` is the validation source of truth. It persists:

- transition, source, key, from/target level, and from/target group;
- switch requested and verified times;
- `validation_not_before` and evidence deadline;
- monitor and traffic row-ID watermarks and processing cursors;
- validation mode, active probe attempts, success/failure counts;
- last evidence ID, source, reason, and time.

The state machine is `unknown`, `stable`, `transitioning`, `awaiting_evidence`, `probing`, `confirmed_healthy`, `confirmed_failed`, `uncertain`, and `exhausted`. Historical `completed` journal rows remain readable, but migration never promotes them to `confirmed_healthy`.

## Evidence boundary

Immediately before the network write, the executor records `MAX(rowid)` for `monitor_history_records` and `traffic_events`. After successful readback it sets each evidence cursor to that watermark, records a centralized five-second propagation delay, and sets a centralized ten-minute evidence deadline.

The timestamp contract was verified against Sub2API commit `57914967cbb127ff715719c3879d881c10d75274` rather than inferred from column names:

- `backend/internal/service/channel_monitor_checker.go` assigns `CheckedAt` immediately before the provider request. `checked_at` is therefore the monitor request occurrence/start, and is persisted unchanged by the monitor service and repository.
- `backend/internal/service/channel_monitor_const.go` bounds that provider request at 45 seconds. The scheduler centralizes the same bound as `GroupValidationMonitorRequestTimeout` for conservative compatibility with completion-only monitor records.
- successful request `created_at` is assigned while constructing the usage log after the gateway result; error `created_at` is assigned by the operations error logger after request handling. Neither traffic timestamp is a request start.
- local `ingested_at`, Telemetry pull time, processing time, and SQLite insertion order describe collection, not which group handled a request.

Passive validation accepts only committed rows that:

- have a row ID greater than the persisted cursor/watermark;
- belong to a monitor or account bound to the switched key;
- have a proven request start or monitor occurrence no earlier than `validation_not_before`;
- are not future-dated or stale;
- have an explicit success or an allowed hard-failure classification.

The row-ID boundary prevents already committed pre-switch rows from validating the target, while the request-start boundary rejects a request that began on the old group but completed or was inserted after the switch. The scheduler accepts an explicit upstream `request_started_at` when available and persists it in nullable form. It never derives a start from `created_at` and `duration_ms` because post-result processing makes that derivation non-authoritative.

Current Sub2API traffic responses do not expose a request start, so those records remain useful for operational statistics but cannot alone confirm either healthy or failed after a switch. A completion-only monitor record is accepted only at or after `switch_verified_at + propagation delay + 45 second maximum monitor request timeout`; before that conservative boundary, a pre-switch request could still be in flight. Native monitor history uses the stronger audited `checked_at` request-start semantic.

One successful post-switch monitor or traffic observation confirms healthy by the centralized default. Failure requires the existing configured monitor failure threshold; a single failure remains `awaiting_evidence`. Evidence timeout becomes `uncertain`, records `evidence_timeout`, and does not mark healthy, mark failed, or switch again.

An enabled fixed failover policy must have at least one enabled associated monitor whose propagation delay, configured interval, and request timeout fit inside the evidence deadline. Save, confirmation, and automatic transition fail closed with `validation_evidence_source_unavailable` when the source cannot be proven. The runtime `validation_mode` is not an operator-selectable probe configuration: production remains passive unless a real `PostSwitchProbe` implementation is explicitly installed.

## Optional active probe

Production defaults to passive validation because the repository has no existing safe, explicitly configured post-switch probe transport. `PostSwitchProbe` is a narrow optional interface, not a placeholder implementation. It may run only after readback has put the state in `awaiting_evidence`, outside SQLite transactions and the Group Transition Executor keyed lock.

The probe request and result carry the transition ID, source, key, target level, target group, and request start. Its start must be after switch readback and within the evidence window. Both the move to `probing` and the final result use a conditional SQLite write that compares the current transition/status/target, so a superseding transition makes the old result invalid. The persisted evidence ID includes the transition ID for restart diagnostics.

A successful probe confirms healthy. Failed probes increment the same bounded failure count; one failure does not advance. Probe errors, invalid binding, invalid time, or ambiguous results become `uncertain` without validating the target. No production implementation fabricates success.

## Recovery and integration

Pending/uncertain journal recovery remains readback-only. A recovered applied transition restores `awaiting_evidence` when its persisted watermark context is available. A legacy pending mutation without attributable watermarks becomes `uncertain`. Restart preserves `awaiting_evidence`, cursors, counts, `confirmed_failed`, and `exhausted`; it does not replay a switch or reuse pre-switch evidence.

Telemetry calls the evidence processor only after successful monitor/traffic commit. Evidence-processing errors are isolated as telemetry partial failures and cannot roll back or discard committed observations. Confirmed failure sends a bounded non-blocking wake to the failover controller; Telemetry never switches a group inside its transaction.

Administrator and Agent group actions still use the unique Group Transition Executor. An applied manual or Agent transition is shown as unverified until post-switch evidence arrives. Existing Agent TTL, evidence, snapshot, permission, failover-mode, freeze, cooldown, idempotency, journal, readback, uncertain recovery, source/key serialization, pool isolation, and mutation-budget controls remain unchanged.

## Migration and UI

The migration adds per-level enabled columns and validation-context columns to the existing policy/state tables, plus nullable `traffic_events.request_started_at`. It is additive and idempotent; existing traffic rows remain valid statistics with a null, non-attributable request start. Existing three-group policies are enabled at all three levels. Legacy state with a prior transition and no validation context migrates to `uncertain`; it is never silently marked healthy.

The console displays current level/group, transition status, validation status/mode, evidence source/time/counts/deadline, `uncertain`, and `exhausted`. `applied` is rendered as “已切换，等待新分组监控证据”, not as target health. The configuration remains a fixed three-level form with per-level enable switches; there is no candidate ranking UI.

## Removed paths

- dynamic candidate sorting by rate multiplier and rollback preference;
- pre-switch target verification windows and target traffic/readiness gates;
- automatic return to an unobservable main group;
- treating successful group readback as health confirmation;
- process-local post-switch verification counters that were lost on restart.

The unique production transport remains `balance.Fetcher.SwitchGroup`, called only by the Group Transition Executor.
