# Intent Adapter Contract

本文档定义阶段 1B 的只读适配合同。适配器把明确提供的旧控制上下文转换成阶段 1A 的类型安全 Intent，或返回机器可读 Semantic Gap。它不执行动作、不调用 Arbiter 决定生产行为、不访问数据库或网络，也不记录运行时 shadow 数据。

## 边界

适配代码位于 `internal/controlplanebridge`，依赖方向只有 `controlplanebridge -> controlplane`。`internal/controlplane` 不依赖 reconcile、agent、balance、failover、httpserver 或 store。阶段 1B 的适配函数只被测试调用，现有 Engine、Agent、Balance、Failover 和 HTTP 路径没有调用点。

成功结果必须满足：

- `Status=mapped`；
- `Intent` 非空并通过 `Intent.Validate`；
- `GapCode` 为空。

失败结果必须满足：

- `Status` 为 `incomplete`、`unsupported` 或 `invalid`；
- `Intent` 必须为空；
- `GapCode` 必须是稳定值；
- 不返回缺字段的半成品 Intent。

## 不补造上下文

适配器不会调用 `time.Now()`，也不会为旧路径补造 TTL、Actor、EvidenceRefs、PolicyVersion、SnapshotVersion 或幂等来源。缺失值产生对应 Semantic Gap。调用方必须传入旧行为真实存在的时间和标识；测试使用固定时间。

所有成功映射还必须显式提供 `StableSourceNamespace` 和 `StableSourceID`。命名空间是封闭枚举，当前包括 policy decision、administrator request、administrator grant consumption、agent action、failover transition、schedule occurrence 和 optimization action。适配入口固定期望命名空间；来源缺失、未知或与入口不匹配都返回 `missing_idempotency_source`。适配器不得从 Reason、CreatedAt、DesiredState、载荷哈希、随机 UUID 或当前时间反推业务动作身份。

## Producer 与 Authority 来源

| 适配入口 | Producer | Authority |
|---|---|---|
| 确定性策略账号动作 | `PolicyScheduler` | `ActivePolicy` |
| 管理页面永久暂停 | `AdminUI` | `ManualHold` |
| 管理页面临时动作 | `AdminUI` | `AdministratorCommand` |
| 管理员聊天动作 | `AgentOperator` | `AdministratorCommand` |
| Agent 自主动作 | `AgentOperator` | `AutonomousAgent` |
| 自动救灾切组 | `FailoverController` | `EmergencyAutomation` |
| 成本调度动作 | `CostOptimizer` | `Optimization` |

调用者不能把 Producer、Authority 或整数 priority 传给通用转换器。每个公开适配函数在代码中固定二者。

管理员聊天只有同时具备已验证管理员身份、精确 grant、已消费 grant 和明确的 grant consumption ID 时，才能使用 `AdministratorCommand`。显式 `StableSourceID` 必须与该 grant consumption ID 一致；适配器不会从授权对象自动填充来源。消息来自聊天并不自动授予管理员权限；授权缺失或来源不一致返回 `missing_authority_context`，来源命名空间/ID 缺失返回 `missing_idempotency_source`。Producer 仍为 `AgentOperator`，不会错误降级为 `AutonomousAgent`。

## ManualHold 与管理员临时命令

永久人工暂停映射为无 TTL 的 `ManualHold`。临时管理动作映射为有明确 ExpiresAt 的 `AdministratorCommand`。临时命令没有 TTL 时返回 `missing_ttl`，适配器不会套用默认 30 分钟。

“解除 ManualHold”是 Override 撤销，不是 `SetAccountSchedulable(true)`。现有 `Engine.ManualResume` 同时写入 `schedulable=true`、释放暂停归属，并可能在余额锁或成本锁存在时建立新的保护期，因此返回 `ambiguous_manual_resume`。阶段 1B 不修改这项混合语义。

## Agent 旧永久控制

当前 `AgentPause`、`AgentResume`、`AgentSetLoadFactor` 和旧 Agent 分组动作都没有统一的有限 TTL。它们不能直接成为合法 `AutonomousAgent` Override；缺少 TTL 时返回 `legacy_permanent_agent_control`。即使有 TTL，仍必须显式提供 SnapshotVersion 和 EvidenceRefs。

## Semantic Gap

| GapCode | 含义 |
|---|---|
| `missing_ttl` | 临时管理员或 Optimization 动作没有有限过期时间 |
| `missing_actor` | 没有明确 Actor |
| `missing_authority_context` | 管理员身份、精确 grant 或消费上下文不完整 |
| `missing_snapshot_version` | 自动化动作没有可引用的快照版本 |
| `missing_policy_version` | ActivePolicy 动作没有策略版本 |
| `missing_evidence` | Agent 自主动作或救灾动作没有证据引用 |
| `ambiguous_manual_resume` | 旧 ManualResume 混合状态写和归属释放 |
| `legacy_permanent_agent_control` | 旧 Agent 动作会无限期保持 |
| `unsupported_operation` | 旧行为不是阶段 1A 支持的三种 Operation |
| `invalid_desired_state` | 目标负载或 tier 非法 |
| `missing_idempotency_source` | 没有稳定业务动作标识 |

实现还为缺 Reason、CreatedAt、非法过期时间和非法资源提供稳定 GapCode。Balance 硬锁、credential invalid、全局冻结等安全状态不转换成 Optimization Intent；它们是未来 Safety Guard 的输入。

## 三层身份模型

适配器使用标准库 SHA-256 和长度前缀字段编码，不使用随机 UUID、当前时间、map 序列化、内存地址或新依赖。

### IdempotencyKey

`IdempotencyKey` 是业务动作身份，使用 `cp-idem-v2-<sha256>`。摘要只包含 Producer、StableSourceNamespace、StableSourceID、Resource 和 Operation。Resource 与 Operation 允许一个业务来源安全地产生多个独立资源动作。

DesiredState、Authority、Actor、Reason、CreatedAt、ExpiresAt 和 EvidenceRefs 不得进入 IdempotencyKey。同一个业务动作的这些字段发生变化时，必须保留同一幂等键并由语义签名触发冲突，不能通过改变载荷生成新键绕过冲突检查。没有显式稳定来源时返回 `missing_idempotency_source`，不得退化为完整载荷哈希。

### Semantic Signature

Semantic Signature 使用 `cp-sem-v1-<sha256>`，覆盖 Producer、Authority、Resource、Operation、DesiredState、Actor、Reason、CreatedAt、ExpiresAt、PolicyVersion、SnapshotVersion 和 EvidenceRefs。它用于比较同一 IdempotencyKey 下的执行、仲裁、生命周期、权限与审计语义；签名不同必须由 Arbiter 返回 `idempotency_conflict`。

EvidenceRefs 按集合处理：签名前复制、去除首尾空白、排序并去重，不修改调用方 slice。因此仅顺序或重复项不同不会制造冲突，证据内容变化仍会改变签名。

### Intent ID

Intent ID 使用 `cp-intent-v2-<sha256>`，计算规则是 `hash(IdempotencyKey, Semantic Signature)`，代表完整 Intent 内容。同一稳定来源和完整语义得到相同 ID；同一来源的载荷变化保持 IdempotencyKey、改变 Intent ID 并触发冲突；不同来源即使载荷相同，也得到不同 IdempotencyKey 和 Intent ID。

所有三类值都只输出摘要。StableSourceID、upstream key、Reason、EvidenceRefs、token、管理员密钥和模型密钥不会以明文出现在输出中。

## 阶段状态

阶段 1B 没有运行时接入、feature flag、shadow observer、数据库表或持久化。阶段 1C 才能在默认 no-op、显式开启的前提下接入只读 runtime shadow observer；旧 writer 仍必须是唯一生产决策和执行路径。
