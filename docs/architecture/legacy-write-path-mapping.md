# Legacy Write Path Mapping

本文档记录阶段 1B 对当前外部账号与上游分组写路径的审计。范围只包括阶段 1A 支持的三种 Operation：账号 schedulable、账号 load factor、上游 key group tier。策略发布、上游配置和数据库内部状态写不属于这三个外部 Mutation，但它们作为入口或前置决策时会注明。

结论状态：`mapped` 表示旧上下文足以构造有效 Intent；`incomplete` 表示操作可表达但缺少真实上下文；`unsupported` 表示阶段 1A 没有等价 Operation；`invalid` 表示资源或目标状态本身无效。当前生产路径没有调用适配器。

## 入口路径

| 入口 | 调用关系 | 当前语义 | 映射结论 |
|---|---|---|---|
| `httpserver.accountAction` pause | `Engine.ManualPause` -> `Sub2API.SetSchedulable(false)` | 网页永久人工暂停 | `incomplete`：可表达为 ManualHold，但 HTTP 请求没有稳定幂等来源 |
| `httpserver.accountAction` resume | `Engine.ManualResume` -> `Sub2API.SetSchedulable(true)` | 强制开启、释放暂停归属，锁存在时再建保护期 | `incomplete/ambiguous_manual_resume` |
| `httpserver.switchUpstreamKeyGroup` | `balance.Manager.SwitchGroup` -> `balance.Fetcher.SwitchGroup` | 任意 group ID 直接切换 | `unsupported/unsupported_operation` |
| `httpserver.switchUpstreamKeyTier` | `balance.Manager.TransitionGroupTier` -> `switchAutomatedGroup` | 网页手动 tier 切换，随机生成 request key，完成后建立保护期 | `incomplete`：缺稳定幂等来源和显式 TTL |
| `agent.Manager.executeAction` | `AgentPause`、`AgentResume`、`AgentSetLoadFactor`、`TransitionGroupTier` | 旧 Agent 自主直接动作 | `incomplete/legacy_permanent_agent_control`；分组动作另有 packet evidence，但仍无 TTL |
| `agent.Manager.executeMutationCapability` | grant 路径调用 `ManualPause`、`ForceResume`、`ForceSetLoadFactor`；自主路径调用 Agent 方法；也调用 `PinLoad`、`TransitionGroupTier` | RuntimeV2 管理员或自主 capability | 管理员路径保留 Authority，但普通直接命令没有显式 TTL；自主路径缺统一 TTL/evidence/snapshot |
| `Engine.Reconcile` / `Engine.reconcileAccountWithFreeze` | `reconcileAdaptiveLoadWithFreeze`、`applyPause`、`applyResume` | 健康、余额、成本和人工锁合并后的确定性执行 | `incomplete`：动作没有携带统一 PolicyVersion、SnapshotVersion 和稳定幂等来源 |
| `balance.Manager.applySuccess` | `SyncBalanceLocks` -> Trigger -> Reconcile -> `applyPause/applyResume` | 余额硬锁 | `unsupported` 作为普通 Optimization：应成为未来 Safety Guard 输入 |
| `balance.Manager.reconcileCostRouting` | `SyncCostLocks` -> Trigger -> Reconcile -> `applyPause/applyResume` | 高倍率账号进入或离开待命 | `incomplete/missing_ttl`，同时缺 SnapshotVersion 和稳定幂等来源 |
| `failover.Controller.handleOutage` | `TransitionGroupTier` -> `switchAutomatedGroup` | 断流升级或主组试运行回滚 | `incomplete/missing_snapshot_version`；已有 source/key/tier/evidence/评估时间/idempotency key |
| `failover.Controller.handleRecovery` | `TransitionGroupTier` -> `switchAutomatedGroup` | 稳定窗口后试回主组 | `incomplete/missing_snapshot_version`；已有恢复 evidence 和 idempotency key |

HTTP handler 只作为入口别名；实际外部写入发生在以下业务写口和传输写口。

## 账号业务写入调用点

| ID | 文件与函数 | 来源 / Actor / 权限来源 | 资源与 Operation | TTL | Policy / Snapshot / Evidence / Idempotency | 前置回读 / 后置确认 / uncertain | 结论与 Semantic Gap |
|---|---|---|---|---|---|---|---|
| WP-A01 | `internal/reconcile/engine.go` `Engine.ManualPause` | HTTP 或管理员 grant；Actor 由入口传入；网页 session 或精确 grant | Account / SetAccountSchedulable(false) | 无，永久 | 无 / 无 / 无 / 无 | 无账号前读；使用写响应但未断言 false；错误标记 uncertain，持久化失败有补偿写 | `incomplete/missing_idempotency_source`；语义可表达为 ManualHold |
| WP-A02 | 同文件 `Engine.AgentPause` | 旧 Agent 或 RuntimeV2 自主；`agent:<run>` | Account / SetAccountSchedulable(false) | 无 | 无 / 当前 Snapshot 只用于前检 / 无结构化 evidence / run 可识别但没有动作级统一 key | ListAccounts；断言写响应 false；uncertain + 补偿写 | `incomplete/legacy_permanent_agent_control`，并缺 evidence/snapshot version |
| WP-A03 | 同文件 `Engine.AgentResume` | Agent 自主；Actor 为 agent | Account / SetAccountSchedulable(true) | 无 | 无 / 无稳定 snapshot version / 无 / 无统一 key | ListAccounts；断言 true；uncertain + 补偿写 | `incomplete/legacy_permanent_agent_control`；同时是释放 agent pause 归属 |
| WP-A04 | 同文件 `Engine.AgentSetLoadFactor` | Agent 自主 | Account / SetAccountLoadFactor | 无 | 无 / Engine snapshot 无版本 / 无 / 无统一 key | 使用 Engine snapshot；写响应未进行独立 GET；uncertain + 补偿写 | `incomplete/legacy_permanent_agent_control` |
| WP-A05 | 同文件 `Engine.ForceSetLoadFactor` | 管理员 grant；拒绝 agent Actor | Account / SetAccountLoadFactor | 写后从 settings 建 protection window，但调用上下文没有显式 ExpiresAt | 无 / 无 / 无 / grant/step 可作来源但未传入 Engine | ListAccounts；严格核对写响应；uncertain + 补偿写 | `incomplete/missing_ttl`；管理员 Authority 可证明时可转换 |
| WP-A06 | 同文件 `Engine.PinLoad` | 管理员或 Agent capability | Account / SetAccountLoadFactor | 有明确 until | 无 / 无 / 无 / capability key 未传入 Engine | ListAccounts；严格核对；uncertain + 补偿写 | 管理员路径缺稳定来源；Agent 路径缺 evidence 和 SnapshotVersion |
| WP-A07 | 同文件 `Engine.ManualResume` | HTTP `web` | Account / SetAccountSchedulable(true) + 归属释放 | 锁存在时写后创建 protection TTL，否则无 | 无 / 无 / 无 / 无 | ListAccounts；使用写响应但未严格断言；没有统一 uncertain wrapper | `incomplete/ambiguous_manual_resume`，不得当作单纯 ManualHold 撤销 |
| WP-A08 | 同文件 `Engine.ForceResume` | 管理员 grant；拒绝 agent Actor | Account / SetAccountSchedulable(true) | 仅自动锁存在时写后创建 protection TTL | 无 / 无 / 无 / grant/step 未传入 Engine | ListAccounts；严格核对；uncertain + 补偿写 | `incomplete/missing_ttl`，且直接恢复与撤销保持不是同一语义 |
| WP-A09 | 同文件 `Engine.reconcileAdaptiveLoadWithFreeze` | 确定性健康调度；Actor `system` | Account / SetAccountLoadFactor | 不适用长期 ActivePolicy | ScorePolicyVersionID 仅部分策略存在 / V3 DecisionSnapshot 已落库但 ID 未传入执行函数 / actionDetails 非稳定 EvidenceRefs / 无动作 key | Reconcile 已读取账号；严格核对响应；提交失败调用 rollback | `incomplete/missing_policy_version` 或 `missing_snapshot_version` |
| WP-A10 | 同文件 `Engine.applyPause` | Reconcile 合并健康、余额、成本、人工锁；Actor `system` | Account / SetAccountSchedulable(false) | 无独立 TTL | 策略版本不完整 / 无执行快照版本 / actionDetails / 无 | Reconcile 前读；严格核对；无统一 uncertain journal | 健康 ActivePolicy `incomplete`；余额硬锁应进入 Safety Guard；成本路径另缺 TTL |
| WP-A11 | 同文件 `Engine.applyResume` | Reconcile；Actor `system` | Account / SetAccountSchedulable(true) | 无独立 TTL | 同 WP-A10 | Reconcile 前读；严格核对；无统一 uncertain journal | `incomplete`；恢复原因可能来自健康、余额或成本锁释放 |

### 账号补偿写调用点

以下调用同样会写外部 API，但它们是对应主动作在本地提交失败后的补偿，不是新的业务 Intent Producer：

| 主函数 | 补偿调用 |
|---|---|
| `Engine.ManualPause` | `SetSchedulable(true)` |
| `Engine.AgentPause` | `SetSchedulable(true)` |
| `Engine.AgentResume` | `SetSchedulable(false)` |
| `Engine.AgentSetLoadFactor` | `UpdateLoadFactor(previous)` |
| `Engine.ForceSetLoadFactor` | `UpdateLoadFactor(previous)` |
| `Engine.PinLoad` | `UpdateLoadFactor(previous)` |
| `Engine.ForceResume` | `SetSchedulable(false)` |
| `Engine.rollbackLoadFactor` | `UpdateLoadFactor(previous)`，服务于 adaptive load 提交失败 |

补偿动作未来必须由 Mutation Executor 内部 journal 协调，不能作为独立高 Authority Intent 参与 Arbiter。

## 分组业务写入调用点

| ID | 文件与函数 | 来源 / Actor / 权限来源 | 资源与 Operation | TTL | Policy / Snapshot / Evidence / Idempotency | 前置回读 / 后置确认 / uncertain | 结论与 Semantic Gap |
|---|---|---|---|---|---|---|---|
| WP-G01 | `internal/balance/manager.go` `balance.Manager.SwitchGroup` | HTTP web；session 管理权限 | UpstreamKey / 任意 group ID | 无 | 无 / 无 / 无 / 无 | store 读取 source；Fetcher 内前读 token、写后 Fetch；mutation uncertain 分类 | `unsupported/unsupported_operation`：不能把任意 group ID 伪装成 tier |
| WP-G02 | `internal/balance/failover.go` `balance.Manager.TransitionGroupTier` | HTTP、Agent 或 Failover；Actor/Manual 来自 request | UpstreamKey / SetUpstreamKeyGroupTier | manual 完成后内部创建保护 TTL，request 无显式 ExpiresAt；自动救灾无 Intent TTL | 已确认 failover policy version / request 无 SnapshotVersion / request 可有 Evidence / 有 request key | live Fetch 前读；Fetcher 写后确认；transition 表记录 pending/completed/uncertain | 分来源：Failover 缺 SnapshotVersion；Agent 自主缺 TTL；管理员路径缺显式 TTL 或稳定来源 |
| WP-G03 | 同文件 `balance.Manager.switchAutomatedGroup` | WP-G02 内部；manual 或 automation barrier | UpstreamKey / 实际 group write | 继承 WP-G02 | 继承 WP-G02 | manual 直接调用 Fetcher；自动路径经过 freeze/barrier；Fetcher 负责后读；uncertain 返回 WP-G02 | 纯执行内部点，不创建第二个 Intent |
| WP-G04 | `internal/failover/controller.go` `failover.Controller.handleOutage` | `system:failover`，confirmed failover policy | UpstreamKey / target tier | 无 | failover policy version存在 / 无显式 SnapshotVersion / assessment.Evidence / 稳定 fallback key | Controller 多轮证据读取；WP-G02/WP-G03 前后读；uncertain transition | `incomplete/missing_snapshot_version` |
| WP-G05 | 同文件 `failover.Controller.handleRecovery` | `system:failover` | UpstreamKey / main tier | 无 | policy version存在 / 无显式 SnapshotVersion / recovery evidence / 稳定 recover key | 恢复窗口、监控、流量前读；WP-G02/WP-G03 后读；uncertain transition | `incomplete/missing_snapshot_version` |
| WP-G06 | `internal/agent/actions.go` `agent.Manager.executeAction` tier 分支 | legacy Agent run | UpstreamKey / target tier | 无 | 无长期 policy version / AnalysisPacket 可作 snapshot但未转换 / 有 packet/run evidence / 稳定 agent key | 执行前重建 packet 和 fence；WP-G02/WP-G03 后读 | `incomplete/legacy_permanent_agent_control` |
| WP-G07 | `internal/agent/capability_executor.go` `agent.Manager.executeMutationCapability` tier 分支 | RuntimeV2；管理员精确 grant 或 AutonomousAgent | UpstreamKey / target tier | 无 | 无 / autonomous 有 evidence fence但无显式版本 / request 未填 Evidence 字段 / invocation key | 前置 capability fence；WP-G02/WP-G03 后读 | 管理员：`missing_ttl`；自主：`legacy_permanent_agent_control` 或 `missing_evidence` |

## 传输层最终外部写入点

| ID | 文件与函数 | 外部请求 | 回读与 uncertain 行为 |
|---|---|---|---|
| TX-A01 | `internal/sub2api/client.go` `sub2api.Client.SetSchedulable` | `POST /api/v1/admin/accounts/{id}/schedulable` | 返回账号对象；最终严格确认由上层函数决定 |
| TX-A02 | 同文件 `sub2api.Client.UpdateLoadFactor` | `PUT /api/v1/admin/accounts/{id}` | 校验返回 account ID 和 load factor；nil 使用 0 清除并归一化 |
| TX-G01 | `internal/balance/fetcher.go` `balance.Fetcher.SwitchGroup` | New API `PUT /api/token/` 或 Sub2 `PUT /api/v1/keys/{id}` | 写前读取 token（New API）；写后完整 Fetch 并核对 group；提交后无法确认时返回 uncertain mutation |

传输函数不知道 Producer、Authority、PolicyVersion 或 TTL，不能自行创建 Intent。

## StableSourceID 审计

`CreatedAt`、目标状态或完整载荷哈希都不是 StableSourceID。下表只把可由业务入口重复提供、且不会因载荷变化而改变的真实动作标识计为“存在”。标记为“入口存在但未传递”的路径仍必须返回 `missing_idempotency_source`，适配器不得从周边字段补造。

| 路径 | 当前是否存在 StableSourceID | 审计结论 |
|---|---|---|
| WP-A01 `ManualPause` | 否 | HTTP 请求没有 command/request ID；Actor 和 CreatedAt 不构成稳定来源 |
| WP-A02 `AgentPause` | 部分 | run ID 可识别一次分析，但没有动作级 ID，且未形成 goal/step/action 稳定组合 |
| WP-A03 `AgentResume` | 部分 | 同 WP-A02；run ID 不能区分同一 run 内多个资源动作 |
| WP-A04 `AgentSetLoadFactor` | 部分 | 同 WP-A02；没有动作级来源传入 Engine |
| WP-A05 `ForceSetLoadFactor` | 入口存在但未传递 | capability/grant 层可识别 grant consumption 或 step，Engine 方法未接收该标识 |
| WP-A06 `PinLoad` | 入口存在但未传递 | capability invocation key 在上层存在，未传入 Engine 写边界 |
| WP-A07 `ManualResume` | 否 | 网页调用没有稳定 command/request ID，CreatedAt 不合格 |
| WP-A08 `ForceResume` | 入口存在但未传递 | grant/step 标识没有传入 Engine |
| WP-A09 `reconcileAdaptiveLoadWithFreeze` | 入口存在但未传递 | V3 DecisionID 已持久化，但执行函数没有携带 DecisionID/动作 occurrence；PolicyVersion 也并非所有路径都有 |
| WP-A10 `applyPause` | 否 | 多个决策来源在 Reconcile 内合并，没有最终动作 occurrence ID |
| WP-A11 `applyResume` | 否 | 同 WP-A10；时间戳和 actionDetails 不能代替稳定来源 |
| `balance.Manager.applySuccess` | 否 | 余额锁变化触发 Reconcile，没有独立动作 ID；未来应进入 Safety Guard |
| `balance.Manager.reconcileCostRouting` | 否 | 没有 optimization run ID + action ID，CostLock 时间不构成稳定来源 |
| WP-G01 `SwitchGroup` | 否 | HTTP 任意 group 切换没有稳定 command/request ID |
| WP-G02 `TransitionGroupTier` | 取决于调用方 | request 有 IdempotencyKey 字段，但 HTTP 路径随机生成的 UUID 不合格；Failover 和部分 Agent 调用方提供稳定 key |
| WP-G03 `switchAutomatedGroup` | 继承 WP-G02 | 内部执行点不得创建第二个业务动作身份 |
| WP-G04 `handleOutage` | 是 | confirmed evaluation occurrence 生成稳定 fallback key，可作为 failover transition source |
| WP-G05 `handleRecovery` | 是 | confirmed recovery occurrence 生成稳定 recover key，可作为 failover transition source |
| WP-G06 `executeAction` tier | 是 | run + resource/tier 动作 key 稳定，但旧动作仍缺 TTL 或版本上下文 |
| WP-G07 `executeMutationCapability` tier | 是 | capability invocation idempotency key 可作为来源；管理员与自主路径仍分别缺 TTL 或完整自动化上下文 |

传输层 TX-A01、TX-A02、TX-G01 只接收具体写请求，不拥有业务入口身份，不能自行生成 StableSourceID。

## 调用关系

```text
httpserver.accountAction
  -> Engine.ManualPause / Engine.ManualResume
  -> sub2api.Client.SetSchedulable

agent.Manager.executeAction / agent.Manager.executeMutationCapability
  -> Engine account methods
  -> sub2api.Client.SetSchedulable / UpdateLoadFactor

Engine.Reconcile
  -> Engine.reconcileAdaptiveLoadWithFreeze / applyPause / applyResume
  -> sub2api.Client.UpdateLoadFactor / SetSchedulable

balance.Manager.applySuccess / balance.Manager.reconcileCostRouting
  -> BalanceLock / CostLock
  -> Trigger -> Engine.Reconcile -> applyPause / applyResume

httpserver.switchUpstreamKeyGroup
  -> balance.Manager.SwitchGroup -> balance.Fetcher.SwitchGroup

httpserver.switchUpstreamKeyTier
agent.Manager.executeAction / agent.Manager.executeMutationCapability
failover.Controller.handleOutage / failover.Controller.handleRecovery
  -> balance.Manager.TransitionGroupTier
  -> balance.Manager.switchAutomatedGroup
  -> balance.Fetcher.SwitchGroup
```

## 映射结论

### 可以精确映射

当离线输入明确提供现有真实的 PolicyVersion、SnapshotVersion、EvidenceRefs、TTL 和稳定业务来源时，策略 pause/resume/load、永久 ManualPause、临时管理员账号动作、管理员聊天动作、Agent 临时动作、Failover tier transition 和 Cost 临时动作都能由 `internal/controlplanebridge` 构造有效 Intent。阶段 1B 的测试只验证这种类型映射能力，不代表当前运行时调用点已经具备全部字段。

### 部分可映射

- Reconcile 的三类账号动作缺统一 PolicyVersion、执行时 SnapshotVersion 和稳定业务动作 key。
- ManualPause 可表达为 ManualHold，但网页入口缺稳定幂等来源。
- 管理员聊天的即时 pause/resume/load/tier 具有精确 grant，但普遍没有明确 TTL。
- Agent 自主账号动作和 tier transition 没有统一 TTL；部分路径还缺 EvidenceRefs 或 SnapshotVersion。
- Failover 已有 source/key/tier/evidence/评估时间/idempotency key，但没有显式 SnapshotVersion。
- Cost routing 通过无期限 CostLock 间接改变 schedulable，缺 TTL、SnapshotVersion 和稳定动作来源。

### 无法映射

- `Engine.ManualResume` 是混合语义，返回 `ambiguous_manual_resume`。
- `balance.Manager.SwitchGroup` 接受任意 group ID，返回 `unsupported_operation`。
- 单纯解除 ManualHold 是 revocation，不生成 `SetAccountSchedulable(true)` Intent。
- BalanceLock、credential invalid 和全局冻结属于未来 Safety Guard 输入，不转换为普通 Optimization Intent。
- rollback 是 Mutation Executor 内部补偿语义，不作为独立 Intent。

## 迁移优先级

1. 阶段 1C 仅增加默认 no-op 的 shadow observer，把已有明确上下文传给离线适配器并统计 GapCode；旧 writer 继续执行。
2. 优先让 Reconcile action 携带已存在的 DecisionID、真实 policy version 和稳定 occurrence ID，不补造版本。
3. 为管理员即时命令和 AutonomousAgent 动作定义显式 TTL，并把 grant consumption、goal/step/packet ID 传到转换边界。
4. 保留 Failover 当前 transition idempotency、前后回读和 uncertain reconciliation，同时增加真实 snapshot reference。
5. 在 Safety Guard 合同建立前，不把 BalanceLock、credential invalid 或 freeze 降格为普通低 Authority Intent。
6. Mutation Executor 存在后再迁移主写与补偿写；阶段 1B/1C 都不得切换执行权。
