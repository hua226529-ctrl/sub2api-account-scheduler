# Control Plane Contract

状态：阶段 0 基线合同，基准 `main@bbb318f` / `v1.0.0`；符合度已更新到核心阶段 A。

本文件定义后续控制平面重构不可回退的产品和可靠性边界。核心阶段 A 已实现账号范围的 Intent、Arbiter、持久 Override、资源锁和 Mutation Executor；分组与其他资源仍按后续小步迁移。

## 不可回退合同

1. 确定性策略调度器是主调度控制器。策略的采集、判定和执行不得依赖模型调用成功。
2. 模型不可用时，最后激活并持久化的确定性策略必须继续运行；Optimizer 或 Operator 故障不能停止 reconcile。
3. 智能体必须形成两个职责清晰的角色：Optimizer 定时分析、模拟、建议或发布版本化策略；Operator 根据管理员聊天或自主判断执行临时直接调度。
4. 策略调度、智能体、人工操作、余额保护和救灾都是 Intent Producer。它们不得各自拥有绕过控制面的外部写入捷径。
5. 所有外部 mutation 最终必须经过唯一执行通道，并依次具备：幂等键、前置回读、冻结和资源锁检查、精确权限检查、冷却和限频检查、外部写入、后置回读、持久审计、不确定状态协调。
6. 全局写锁不得跨越网络请求、模型调用或长数据库读取。锁内只能进行有明确上界的本地状态变更。
7. 同一个资源的 mutation 必须串行；不同资源可以在有界并发和限频约束下并行。资源身份必须稳定且可审计。
8. 智能体直接调度默认是有截止时间的 Override，而不是隐式永久策略变更。永久变更必须通过版本化策略发布流程。
9. 单进程和 SQLite 是当前部署合同。没有基准和兼容迁移证据，不替换数据库，也不拆分微服务。
10. 冻结、人工精确授权、敏感信息脱敏和 uncertain mutation 协调能力只能保持或增强，不能因统一执行路径而降低。
11. 数据库演进必须向后兼容；现有策略、会话、目标、步骤、checkpoint、事件、凭据和控制归属不能丢失。
12. 所有集成测试只能使用 fake Sub2API、fake 模型和临时数据库，不能访问真实上游。

## 当前符合度

| 合同 | 当前状态 | 阶段 0 证据 |
|---|---|---|
| 确定性调度器为主控制器 | 已满足 | `main` 独立启动 `reconcile.Engine`；engine 不依赖 Agent provider。现有 engine 测试覆盖暂停、恢复、负载档位。 |
| 模型不可用时最后策略继续运行 | 已满足 | 策略保存在 SQLite；`Reconcile` 不调用模型；`TestCurrentBehaviorInteractiveGoalWaitsForOccupiedRuntimeMutex` 中无 provider 只使 goal 等待。 |
| Optimizer / Operator 分责 | 部分满足 | 已有 scheduled/emergency/chat goal 和 capability，但共享一个 Manager、worker 和 `runtimeMu`，没有正式 lane/角色边界。 |
| 所有 producer 进入统一写入通道 | 部分满足 | 所有账号 producer 已进入 `accountcontrol`；SwitchGroup/TransitionGroupTier 仍保留现有分组通道。 |
| mutation 完整生命周期 | 账号已满足 | 账号具备 prepared journal、前后回读、Guard、原子审计和 restart recovery；三级切组继续使用既有 transition journal。 |
| 全局锁不跨网络/长查询 | 部分满足 | 账号 Reconcile 不再在 `runMu` 下执行网络写入；balance、Telemetry 和 Agent runtime 的其他 I/O 锁仍属后续范围。 |
| 同资源串行、异资源有界并行 | 账号已满足 | Service 实例持有可回收 keyed account lock；同账号串行，不同账号和恢复任务有限并行。 |
| Agent 直接动作默认 TTL Override | 账号已满足 | 管理员聊天默认 30 分钟；自主账号动作默认 15 分钟、最长 2 小时并要求 evidence/snapshot。分组动作尚未统一。 |
| 单进程 + SQLite | 已满足 | 当前 scheduler 为单进程，Store 使用 modernc SQLite。 |
| 安全能力不可降低 | 部分满足 | 精确 grant、单次消费、脱敏、freeze 和部分 uncertain 协调已有测试；写路径覆盖不一致。 |

## 当前必须保留的安全行为

- `writes_frozen` 阻止确定性自动写入，同时允许只读采集和 snapshot 更新。
- Agent mutation 必须经过 Agent freeze、精确管理员 grant、grant 单次消费及 capability 风险检查。
- 管理员授权必须绑定原始命令、capability、参数和资源，定时命令必须保留授权来源。
- 日志、prompt、消息和 API 响应继续执行现有敏感信息脱敏。
- 已支持的三级切组 mutation journal、幂等键、前后回读及 restart reconciliation 不得退化。
- 账号控制的人工归属、余额锁、成本锁、健康锁、抖动保护和新鲜度判断不得被统一通道忽略。

## 已知当前例外

阶段 0 的账号例外已经关闭：网页 pause/resume 受 `writes_frozen` 约束，所有账号入口共享资源锁和 journal，ManualResume 已改为有期限 Override。仍存在的例外是普通 `SwitchGroup` 尚未进入统一控制面、Agent V2 非账号工作仍全局串行、Telemetry 单 monitor 失败仍会终止整轮；后续阶段必须小步迁移且不得恢复账号直写。
