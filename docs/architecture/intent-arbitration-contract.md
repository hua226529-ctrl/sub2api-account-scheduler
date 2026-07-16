# Intent Arbitration Contract

本文档定义阶段 1A 的控制面领域合同。阶段 1A 只提供纯内存的 `Intent`、`OverrideLease` 和确定性 `Arbiter`；它没有接入现有生产 writer、调度循环、Agent、HTTP API 或 SQLite。

## Producer 与 Authority

`Producer` 只说明 Intent 来自哪个子系统，不决定胜负。当前 Producer 为：

- `PolicyScheduler`
- `AgentOperator`
- `AdminUI`
- `FailoverController`
- `CostOptimizer`

`Authority` 表示动作代表谁的授权。Authority 是封闭枚举，优先级只由代码中的集中映射定义，不接受整数 priority 或调用方自定义排序。

| 从高到低 | Authority | 典型语义 |
|---:|---|---|
| 1 | `ManualHold` | 管理员明确建立的持续保持 |
| 2 | `AdministratorCommand` | 管理员发出的临时直接命令 |
| 3 | `EmergencyAutomation` | 有证据和状态快照的自动救灾动作 |
| 4 | `AutonomousAgent` | 有证据、状态快照和 TTL 的自主 Agent 动作 |
| 5 | `ActivePolicy` | 当前生效策略版本产生的确定性动作 |
| 6 | `Optimization` | 有状态快照和 TTL 的优化建议或临时动作 |

Producer 与 Authority 彼此独立。例如，管理员通过聊天要求执行动作时，Intent 可以由 `AgentOperator` 产生，但 Authority 必须是 `AdministratorCommand`。Agent 不能仅凭 Producer 身份获得管理员权限。

## 支持的资源与动作

阶段 1A 只支持以下有类型的资源和动作：

| 资源 | 动作 | Desired State |
|---|---|---|
| Account | `SetAccountSchedulable` | `bool` |
| Account | `SetAccountLoadFactor` | `1..100`，或显式恢复默认值 |
| Upstream Key | `SetUpstreamKeyGroupTier` | `main`、`backup`、`emergency` |

资源、动作和目标状态通过构造函数创建。Intent 不接受任意 JSON payload，也不会把未知字段延迟到执行阶段解释。

## Intent 验证

有效 Intent 必须包含非空 ID、幂等键、Actor 和 Reason；有效的 Producer、Authority、资源、动作和目标状态；以及显式的 `CreatedAt`。存在 `ExpiresAt` 时，它必须严格晚于 `CreatedAt`。

自动化 Authority 必须携带状态快照。`ActivePolicy` 还必须包含策略版本；`EmergencyAutomation` 和 `AutonomousAgent` 必须携带证据；`AutonomousAgent` 与 `Optimization` 必须有 TTL。`AdministratorCommand` 也必须有 TTL，防止聊天或直接动作隐式成为永久配置。

真正需要持续生效的管理员动作必须显式使用 `ManualHold`。ManualHold 可以没有 TTL，并通过显式撤销结束，而不是伪装成一个无限期的临时 Override。

## Override Lease

Agent、救灾、优化和管理员直接动作默认建模为带 TTL 的 `OverrideLease`。到期判定为 `now >= ExpiresAt`；到期后 Override 不再参与仲裁，底层 `ActivePolicy` Intent 可重新胜出。Lease 也可以由 Actor 在明确时间、明确原因下撤销。

`ManualHold` 使用独立构造入口，允许无 TTL。普通 ActivePolicy Intent 不是 Override，也不能通过临时 Override 构造入口包装。

## 冲突与幂等

冲突组由 `(Resource, Operation)` 定义。不同账号、不同上游 key 或不同 Operation 彼此独立仲裁。

相同幂等键且 Semantic Signature 完全相同的 Intent 视为重复提交，只保留 ID 字典序最小的规范候选。Semantic Signature 包含 Producer、Authority、Resource、Operation、DesiredState、Actor、Reason、CreatedAt、ExpiresAt、PolicyVersion、SnapshotVersion 和 EvidenceRefs；EvidenceRefs 在比较前按集合复制、排序和去重。

对规范构造的 Intent，相同 IdempotencyKey 下完整内容变化会同时产生不同 Intent ID 和不同 Semantic Signature；Arbiter 将整组标记为 `idempotency_conflict`，不选出 winner，避免将同一次业务动作的载荷变化误当成合法重试。ID 本身不替代字段级语义比较：非规范调用方仅改写 ID、但完整语义完全相同时，仍按重复提交处理。DesiredState、Authority、TTL 或审计字段不能参与 IdempotencyKey；否则调用者可以通过修改载荷生成新键，绕过同一业务动作必须一致的冲突检查。

## 确定性仲裁

Arbiter 的固定顺序为：

1. 验证 Intent。
2. 排除在传入 `now` 已到期的 Intent。
3. 处理幂等重复或冲突。
4. 按 `(Resource, Operation)` 分组。
5. 选择 Authority 更高的候选。
6. Authority 相同时选择 `CreatedAt` 更新的候选。
7. 时间也相同时选择 ID 字典序更小的候选。

输入顺序不会影响结果，Arbiter 不修改调用方传入的 Intent，也不读取系统时钟、环境变量、数据库或网络。所有候选都会得到可枚举的结果和原因码，包括 selected、expired、invalid、lower authority、older、duplicate、idempotency conflict 和确定性 tie-break。

## Safety 边界

Arbiter 只回答“在同一冲突组中哪个有效 Intent 具有更高 Authority”，不负责判断外部写入是否安全。冻结、资源锁、新鲜度、管理员精确授权、冷却、限频、幂等执行、回读、uncertain mutation 协调和审计属于后续 Safety Guard 与唯一 Mutation Executor 的职责，不能因 Arbiter 已选出 winner 而绕过。

阶段 1A 不包含 Mutation Executor，也没有替换任何现有安全逻辑。现有生产调度和写入路径保持不变。

## 后续集成边界

下一小步只能以 feature flag 或 shadow 模式建立兼容适配器：把现有 Producer 的动作描述转换为 Intent，记录新旧决策差异，但仍由旧 writer 执行。进入实际写入切换前，必须先具备 Safety Guard、资源级串行、幂等、回读和审计的等价测试证明。

阶段 1A 没有数据库迁移、API 变更或前端变更，也未实现阶段 1B。
