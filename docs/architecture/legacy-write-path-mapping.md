# Account And Group Write Path Mapping

本文档最初用于阶段 1B 的旧写入审计。核心阶段 A 完成后，账号写入已经迁移到统一执行链；分组写入仍保持原有实现，等待后续阶段。这里记录当前可执行调用关系，避免把历史直写路径误认为仍然有效。

## 当前账号生产调用链

```text
HTTP / Agent V1 / Runtime V2 / deterministic Reconcile
  -> typed controlplanebridge conversion
  -> accountcontrol.Service.Submit
  -> persisted account_mutations prepared row
  -> active account_overrides + controlplane.Arbiter
  -> account-specific Guard
  -> accountcontrol.executor
  -> sub2api.Client.SetSchedulable / UpdateLoadFactor
  -> upstream readback
  -> atomic journal + projection + override + event finalization
```

`internal/accountcontrol/executor.go` 是唯一调用账号写 transport 的生产文件。`internal/sub2api/client.go` 只实现 transport；业务包不能直接调用它的账号写方法。

## 账号入口映射

| ID | 当前入口 | Producer / Authority | StableSourceID | TTL 与证据 | 最终执行点 |
|---|---|---|---|---|---|
| WP-A01 | `httpserver.accountAction` | AdminUI / ManualHold 或 AdministratorCommand | 客户端 `Idempotency-Key`，缺失时入口用 `crypto/rand` 生成 | pause 无期限；resume 默认 30 分钟，可显式指定 | `accountcontrol.executor` |
| WP-A02 | `Engine.ManualPause` / `ManualPauseCommand` | AdminUI / ManualHold | 兼容方法生成随机 command ID；结构化方法接收 command ID | 无期限、显式 ManualHold | `accountcontrol.executor` |
| WP-A03 | `Engine.ManualResume` / `ManualResumeCommand` | AdminUI / AdministratorCommand | 同 WP-A02 | 默认 30 分钟；先撤销 ManualHold；硬锁阻止时不激活 Override | `accountcontrol.executor` |
| WP-A04 | `Engine.ReleaseManualHoldCommand` | 撤销 + 当前 ActivePolicy 候选 | command ID + account + release operation 的稳定请求键 | 不创建恢复 Override；重新仲裁 | `accountcontrol.executor`，或实际状态已满足时 `applied_noop` |
| WP-A05 | `Engine.ForceResume`、`Engine.ForceSetLoadFactor`、`Engine.PinLoad`、`Engine.ClearLoadPin` | AdminUI / AdministratorCommand 或 ManualHold | 结构化入口接收 command ID；兼容方法生成随机 ID | 临时默认 30 分钟；永久 PinLoad 显式 ManualHold | `accountcontrol.executor` |
| WP-A06 | `Engine.AgentPause`、`Engine.AgentResume`、`Engine.AgentSetLoadFactor` | AgentOperator / AdministratorCommand 或 AutonomousAgent | `CommandContext.CommandID` | 管理员默认 30 分钟；自主默认 15 分钟、最大 2 小时，并要求 EvidenceRefs 和 SnapshotVersion | `accountcontrol.executor` |
| WP-A07 | `agent.Manager.executeAction` | AgentOperator / AutonomousAgent | `agent-v1:run:<run>:tool:<tool>` | 分析 packet 作为 snapshot/evidence；15 分钟 | 经 WP-A06 |
| WP-A08 | `agent.Manager.executeMutationCapability` | AgentOperator / 精确管理员 grant 或 AutonomousAgent | grant consumption ID 或 capability invocation idempotency key | 保留真实 TTL、snapshot、evidence、run/goal/step；管理员 grant 单次消费 | 经 WP-A06 / WP-A05 |
| WP-A09 | `Engine.reconcileAdaptiveLoad` | ReconcilePolicy / ActivePolicy | 实际 PolicyVersion + SnapshotVersion | ActivePolicy 不持久化为 Override | `accountcontrol.executor` |
| WP-A10 | `Engine.applyPause` | ReconcilePolicy / ActivePolicy | 实际 PolicyVersion + SnapshotVersion | Guard 使用健康、余额、成本、冻结、新鲜度和冷却状态 | `accountcontrol.executor` |
| WP-A11 | `Engine.applyResume` | ReconcilePolicy / ActivePolicy | 实际 PolicyVersion + SnapshotVersion | 仅在策略拥有暂停且硬锁解除后提交 | `accountcontrol.executor` |

兼容方法只生成/接收命令身份、转换 Intent、提交 Service 并把结构化结果转换为旧 `error` 契约。它们不再执行前置读取、transport 写入、回读、补偿或审计提交。

## StableSourceID 审计

| 来源 | 当前规则 | 禁止替代物 |
|---|---|---|
| 管理员 HTTP | 原样使用有效 `Idempotency-Key`；缺失时用 `crypto/rand` command ID | 当前时间、账号 ID、IP、reason、desired state |
| 兼容管理员 Go API | 每次新业务调用生成随机 command ID；需要调用方重试语义时使用结构化 Command API | wall clock 或 payload hash |
| 确定性策略 | `stablePolicyVersion(policy)` + 真实 telemetry/account observation `stableSnapshotVersion(binding)` + 最近 Override 生命周期 revision | 单纯当前时间；Override revision 只用于区分新的仲裁事实，不替代 policy/snapshot |
| Agent V1 | 持久 run ID + tool call ID，packet ID 作为 evidence/snapshot | actor 或 reason |
| Runtime V2 管理员 | 已消费的 exact grant ID | 未消费 grant、会话文本 |
| Runtime V2 自主 | 持久 invocation idempotency key，并携带 run/goal/step、evidence、snapshot | 临时随机值 |

Intent ID、IdempotencyKey 和 Semantic Signature 继续由 `controlplane`/`controlplanebridge` 的稳定规则生成；相同键但语义签名不同返回 `idempotency_conflict`。

## 已删除的账号写入路径

- `Engine.ManualPause`、`ManualResume`、`AgentPause`、`AgentResume`、`ForceResume` 内的直接 `SetSchedulable`。
- `Engine.AgentSetLoadFactor`、`ForceSetLoadFactor`、`PinLoad`、`reconcileAdaptiveLoad` 内的直接 `UpdateLoadFactor`。
- `Engine.rollbackLoadFactor` 和本地提交失败后的反向账号写入。
- 各入口重复的前置读取、写响应判断、回读、uncertain 分类和 audit commit。
- 阶段 1C 的账号 Runtime Shadow observer、Engine option 和 `CONTROLPLANE_SHADOW_MODE`；生产 Intent 不再重复 shadow 自己。

## 分组业务写入调用点（本阶段未迁移）

| ID | 文件与函数 | 当前语义 | 本阶段状态 |
|---|---|---|---|
| WP-G01 | `balance.Manager.SwitchGroup` | 管理员直接指定 group | `unsupported_operation`，保持原路径 |
| WP-G02 | `balance.Manager.TransitionGroupTier` | 有 journal 的手动/自动 tier transition | 保持原路径 |
| WP-G03 | `balance.Manager.switchAutomatedGroup` | WP-G02 的内部执行点 | 保持原路径 |
| WP-G04 | `failover.Controller.handleOutage` | 断流切备组 | 继续使用现有 transition journal |
| WP-G05 | `failover.Controller.handleRecovery` | 稳定恢复后试回主组 | 继续使用现有 transition journal |
| WP-G06 | `agent.Manager.executeAction` 分组分支 | Agent V1 tier 动作 | 仍委托 balance transition |
| WP-G07 | `agent.Manager.executeMutationCapability` 分组分支 | Runtime V2 tier 动作 | 仍委托 balance transition |

入口别名包括 `httpserver.switchUpstreamKeyGroup` 和 `httpserver.switchUpstreamKeyTier`。最终分组 transport 仍是 `balance.Fetcher.SwitchGroup`。核心阶段 A 没有迁移或删除 `SwitchGroup`、`TransitionGroupTier`、`failover.Controller.handleOutage`、`failover.Controller.handleRecovery` 或 `balance.Manager.reconcileCostRouting`。

## 传输层最终写入点

| ID | 文件与函数 | 调用约束 |
|---|---|---|
| TX-A01 | `sub2api.Client.SetSchedulable` | 仅由 `accountcontrol.executor` 通过 `Transport` 调用；测试 fake 除外 |
| TX-A02 | `sub2api.Client.UpdateLoadFactor` | 仅由 `accountcontrol.executor` 通过 `Transport` 调用；测试 fake 除外 |
| TX-G01 | `balance.Fetcher.SwitchGroup` | 分组范围，保留原有回读和 uncertain transition 协调 |

静态测试 `internal/accountcontrol/unique_writer_test.go` 扫描生产 Go 文件，阻止账号业务包重新直接调用两个账号写 transport。
