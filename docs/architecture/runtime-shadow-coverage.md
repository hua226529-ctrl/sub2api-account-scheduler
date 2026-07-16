# Runtime Shadow Coverage

阶段 1C 只在最终账号业务动作点观察一次。表中的 Gap 是生产旧路径在缺少完整真实上下文时首先暴露的主要缺口；受控测试可注入完整的真实形态上下文证明 mapped 行为。

| Path | 已接入 | 旧路径语义 | 预期状态 | 主要 Gap | 是否影响执行 |
|---|---|---|---|---|---|
| `reconcile_policy_pause` | 是，`applyPause` 的纯健康策略分支 | 健康策略设置 `schedulable=false` | 完整上下文 mapped，否则 incomplete | `missing_policy_version`、`missing_snapshot_version`、`missing_idempotency_source` | 否 |
| `reconcile_policy_resume` | 是，`applyResume` 的 automatic owner 纯健康分支 | 健康策略设置 `schedulable=true` | 完整上下文 mapped，否则 incomplete | `missing_policy_version`、`missing_snapshot_version`、`missing_idempotency_source` | 否 |
| `reconcile_policy_load` | 是，adaptive load 且无人工/余额/成本锁 | 健康策略设置或恢复 load factor | 完整上下文 mapped，否则 incomplete | `missing_policy_version`、`missing_snapshot_version`、`missing_idempotency_source` | 否 |
| `manual_pause` | 是，`ManualPause` | 永久人工暂停与 ManualHold；Agent grant 调用保留真实授权上下文 | web 通常 incomplete；完整 request/grant 上下文可 mapped | `missing_idempotency_source`，以及后续 `missing_created_at` | 否 |
| `manual_resume` | 是，`ManualResume` | 恢复写入、解除归属并重建保护状态 | 始终 incomplete，Intent 为空 | `ambiguous_manual_resume` | 否 |
| `force_resume` | 是，`ForceResume` | 管理员一次性恢复并建立保护窗口 | grant+TTL 完整时 mapped，否则 incomplete | `missing_ttl`、`missing_authority_context`、`missing_idempotency_source` | 否 |
| `force_set_load` | 是，`ForceSetLoadFactor` | 管理员精确设置负载并建立保护窗口 | grant+TTL 完整时 mapped，否则 incomplete | `missing_ttl`、`missing_authority_context`、`missing_idempotency_source` | 否 |
| `pin_load` | 是，`PinLoad` | 将负载固定到真实 `until` | 完整 admin 或 autonomous 上下文 mapped，否则 incomplete | `missing_idempotency_source`、`missing_snapshot_version`、`missing_evidence` | 否 |
| `agent_pause` | 是，`AgentPause` | Agent 自主暂停，不创建 ManualHold | 有限 TTL 和证据完整时 mapped；旧永久动作 incomplete | `legacy_permanent_agent_control`、`missing_snapshot_version`、`missing_evidence` | 否 |
| `agent_resume` | 是，`AgentResume` | Agent 自主恢复既有 Agent 归属 | 有限 TTL 和证据完整时 mapped；旧永久动作 incomplete | `legacy_permanent_agent_control`、`missing_snapshot_version`、`missing_evidence` | 否 |
| `agent_set_load` | 是，`AgentSetLoadFactor` | Agent 自主设置 load factor | 有限 TTL 和证据完整时 mapped；旧永久动作 incomplete | `legacy_permanent_agent_control`、`missing_snapshot_version`、`missing_evidence` | 否 |

HTTP handler 和 Sub2API transport 没有 observer 接入，因此不会与 Engine 重复记录。Balance/Cost 锁导致的 pause/resume 分支、rollback、compensation、readback、lock/freeze 和 uncertain reconciliation 都不生成新的业务 Observation。
