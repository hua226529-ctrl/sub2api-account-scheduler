# Runtime Shadow Contract

阶段 1C 的 Runtime Shadow 只回答一个问题：账号旧路径准备执行的动作，能否被阶段 1A/1B 的 Intent 模型准确表达。它不是新的控制器，也不是执行器。

## 决策与执行边界

- 旧策略、人工和 Agent 路径仍是唯一生产决策来源。
- 旧 `reconcile.Engine` 路径仍是唯一生产执行来源。Shadow 的 Intent 和单 Intent Arbiter winner 永远不会调用 Sub2API。
- Shadow 的 conversion failure、validation failure、mismatch 或 panic 不会阻止、增加、删除或修改旧动作。
- Shadow 不观察 rollback、compensation、readback、锁状态提交、freeze、uncertain reconciliation 或 transport retry；这些不是新的业务 Intent。
- 本阶段只覆盖账号 `schedulable` 和 `load_factor`，不覆盖分组切换、Failover 或 Cost producer。

## 开关与生命周期

`CONTROLPLANE_SHADOW_MODE` 只接受 `off` 和 `log`。缺失、空值和 `off` 都关闭；非法值 fail closed 到 `off`，启动时只输出一次不含原值的警告。默认 constructor 注入 `NoopObserver`，因此关闭态不转换、不运行 Arbiter、不构造 Observation、不输出 shadow 日志，也不启动 goroutine。

测试通过 `reconcile.WithControlplaneShadow` 直接注入 `CaptureObserver`，不依赖环境变量。没有 package-level 可变 observer、全局 setter、后台 worker 或插件 registry。

## Observation

Observation 只包含下列安全、值类型字段：

- 稳定封闭的 `Path`、`Producer` 和可用时的 `Authority`；
- legacy 的 operation、resource、desired state；
- conversion status、GapCode；
- mapped 时的 Intent ID、IdempotencyKey、operation、resource、desired state；
- validation、单 Intent Arbiter、match、观察时间和安全 panic/detail 状态。

Path 只能取 `reconcile_policy_pause`、`reconcile_policy_resume`、`reconcile_policy_load`、`manual_pause`、`manual_resume`、`force_resume`、`force_set_load`、`pin_load`、`agent_pause`、`agent_resume`、`agent_set_load`。HTTP URL、函数指针、聊天内容和任意字符串不能作为 Path。

`Match=true` 仅当 conversion 成功、Intent 验证成功、单 Intent Arbiter 有 winner，并且 winner 的 resource、operation、desired state 分别与 legacy action 完全相同。转换失败、验证失败、无 winner 或任一字段不同都为 false。

## 真实上下文与 Semantic Gap

Shadow 只使用旧路径已经在内存中的上下文，不增加 API 或 SQLite 读取。`CreatedAt`、TTL、StableSourceID、PolicyVersion、SnapshotVersion、EvidenceRefs 和管理员授权都不得补造。`ObservedAt` 只是 shadow 观察时刻，不充当业务动作 ID、版本或证据。自动策略使用本轮真实决策时刻作为动作创建时刻，但不会把该时间冒充 snapshot 或稳定来源。

- `ManualResume` 始终为 `incomplete/ambiguous_manual_resume`，Intent 为空，因为旧路径同时包含恢复写入、归属解除和保护状态重建。
- 没有有限 TTL 的旧 Agent 自主暂停、恢复和负载动作保持 `incomplete/legacy_permanent_agent_control`。
- 管理员聊天只有在真实身份、精确 grant、已消费 grant、grant consumption ID、TTL、Actor、CreatedAt 和稳定来源全部存在时才能映射为 `AdministratorCommand`。
- 自主 Agent 只有在真实 TTL、SnapshotVersion、EvidenceRefs、Actor、CreatedAt 和稳定来源存在时才能映射为 `AutonomousAgent`。
- 策略动作缺少实际 policy、snapshot 或稳定决策来源时分别记录相应 Gap，不从当前账号状态或当前时间推导。

## 验证局限

单 Intent Arbiter 只验证该 Intent 在当前观察时刻能够成为一个结构有效的 winner，并用于比较 legacy 目标。它不模拟多 producer 冲突，不证明未来生产仲裁顺序，也不参与现有冻结、锁、权限、冷却、限频、回读或审计。

## 日志与敏感信息

生产日志只输出稳定 Path、枚举状态、账号资源、计数和安全 detail code。默认不输出 Reason、EvidenceRefs、StableSourceID、Intent ID、IdempotencyKey、grant 内容、原始聊天、原始错误、Cookie、Authorization、CSRF、任何 API key/token 或数据库连接信息。adapter/observer panic 只记录封闭状态，不记录 panic 文本。

## 存储、性能与移除

Shadow 不持久化 Intent 或 Observation，不创建表或迁移，不增加 transaction、SQL query/exec、上游请求或轮询。LoggingObserver 同步输出轻量累计计数；CaptureObserver 仅用于测试并复制值数据。

该实现通过 Engine option 和独立包隔离。未来可删除观察调用和 option，或替换 observer，而不迁移数据库、不回放数据，也不改变旧执行路径。本阶段没有 Mutation Executor、Safety Guard、Resource Lease、Event Bus 或多来源生产 Arbiter。
