# Legacy Write Path Mapping

This document preserves the Stage 1B identifiers while recording their final Core C disposition. “Removed” means the production function or endpoint no longer exists; historical Git checkpoints retain the original implementation.

## StableSourceID 审计

Intent and mutation identities bind producer, authority, resource, operation, desired state, actor, time bounds, policy/snapshot version and evidence. Replays with the same idempotency key but different semantics conflict.

## Account paths

| IDs | Historical source | Final disposition |
| --- | --- | --- |
| WP-A01, WP-A02, WP-A03, WP-A04 | Policy pause/resume/load decisions | Produce typed account commands and enter AccountControl. |
| WP-A05, WP-A06, WP-A07 | `Engine.ManualPause`, `Engine.ManualResume`, `Engine.ForceResume` | Compatibility entry methods delegate to AccountControl; no transport write. |
| WP-A08, WP-A09 | `Engine.AgentPause`, `Engine.AgentResume` | Removed with Agent V1; Runtime V2 capabilities use AccountControl. |
| WP-A10, WP-A11 | `Engine.AgentSetLoadFactor`, `Engine.ForceSetLoadFactor`, `Engine.PinLoad` | Compatibility/domain entry methods delegate to AccountControl. |

The only production transport calls to `sub2api.Client.SetSchedulable` and `sub2api.Client.UpdateLoadFactor` are in `internal/accountcontrol/executor.go`. `Engine.reconcileAdaptiveLoad`, `Engine.applyPause` and `Engine.applyResume` can plan or delegate but cannot write transport directly.

## Group paths

| IDs | Historical source | Final disposition |
| --- | --- | --- |
| WP-G01 | `balance.Manager.SwitchGroup` | Removed. Arbitrary group IDs are not a production mutation contract. |
| WP-G02 | `httpserver.switchUpstreamKeyGroup` | Removed with its route and frontend API. |
| WP-G03 | `agent.Manager.executeAction` legacy group action | Removed with Agent V1. |
| WP-G04 | Runtime V2 group capability | Delegates to `balance.Manager.TransitionGroupTier`. |
| WP-G05 | Scheduled group command | Revalidates and delegates to `balance.Manager.TransitionGroupTier`. |
| WP-G06 | `failover.Controller.handleOutage` | Evaluates the current fixed level and requests only the next configured enabled level; mutation delegates to the executor and obeys per-cycle budget. Automatic return and dynamic candidate selection are absent. |
| WP-G07 | `balance.Manager.reconcileCostRouting` / historical `switchAutomatedGroup` | Cannot perform a token group transport write outside the executor. |

`balance.Manager.TransitionGroupTier` is the unique Group Transition Executor. `balance.Fetcher.SwitchGroup` is called from one executor transport method only, after journal and safety checks and before verified readback. Recovery is readback-only.

## Policy paths

Legacy `CreatePolicyVersion`, `ActivatePolicyVersion` and `PublishPolicyVersion` write APIs are removed. Policy changes create a proposal. Activation and rollback use `PublishPolicyProposal` and `RollbackPolicyProposal` in the policy lifecycle store with base-version fences and transactionally updated projections.

## Legacy Agent data

`Manager.Run`, `Manager.Chat`, `executeActions`, synchronous action loops and writes to `agent_runs` / `agent_tool_calls` are removed. Those two tables are historical read-only data sources; persistent goals, steps, checkpoints and events are the only active runtime.
