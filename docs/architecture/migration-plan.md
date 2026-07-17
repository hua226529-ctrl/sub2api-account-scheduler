# Control Plane Migration Plan

本计划只定义阶段 1 及以后的小步迁移顺序。阶段 0 没有实现下列组件。每一步必须独立部署、可关闭、可回滚，并继续使用单进程和 SQLite。

| 顺序 | 阶段 | 输入 | 输出 | 兼容策略与回滚点 | 不允许破坏的行为 | 所需测试 |
|---:|---|---|---|---|---|---|
| 1 | Intent 与 Arbiter（1A、1B、1B.5 已完成） | 当前 Policy、人工、Agent、Balance、Failover 的动作描述和安全检查 | 类型安全 Intent、纯函数 Arbiter、离线适配、稳定 identity | 账号 1C shadow 已在核心阶段 A 迁移完成后删除；领域模型和 bridge 继续生产使用。 | 判定结果、freeze、人工优先级、精确 grant、脱敏 | 领域验证、Authority、幂等、确定性、适配和稳定 identity |
| 2 | 账号 Mutation Executor（核心阶段 A 已完成） | 账号 Intent、account_controls、freeze/lock/freshness 规则 | SetSchedulable/UpdateLoadFactor 唯一 executor，持久 Override、幂等、readback journal 和 restart recovery | 旧公开 Engine 方法仅作委托包装；没有旧账号外部 writer 或双写回退路径。数据库回滚点是部署前备份，不做破坏性降级。 | 暂停/恢复阈值、人工/余额/成本/健康锁、uncertain 分类 | fake 写成功超时、本地提交失败不回滚、同资源并发、重启协调、静态唯一 writer |
| 3 | 分组 Mutation Executor | 普通 SwitchGroup、三级 Transition、已有 transitions 表 | 普通和自动切组统一 executor | 先包裹现有 `TransitionGroupTier`，普通 SwitchGroup feature flag；保留旧 HTTP 契约。 | 已确认 policy/version、余额和新鲜度、幂等、前后回读、人工精确授权 | 三级切换矩阵、普通切组兼容、unknown group freeze、restart readback |
| 4 | Reconcile 热路径 | 当前单体 Reconcile、账号 executor | 明确 Collect/Evaluate/Plan/Execute 边界，执行只产 Intent | 必须以当前统一 Executor 为边界小步拆分，不能恢复旧账号 writer。 | 确定性策略主导、模型不可用继续、freeze 下采集不写、stale fail-closed | 约 10 账号等价、事件到动作延迟、并发人工动作、故障注入、阶段 duration benchmark |
| 5 | SQLite 与批量查询 | 实测 `5N+4` SQL、单连接配置 | 批量读取控制/锁/状态，经过基准验证的连接策略 | Store 保留逐项 API 兼容包装；每个批量函数可回退。迁移只能向后兼容。 | 事务原子性、事件顺序、凭据和历史数据、WAL 语义 | query count 上限、事务失败、旧库升级/降级、并发读写和 race |
| 6 | 事件驱动 | poll/trigger、持久 events/agent events | 进程内 domain event bus 和定向 reconcile 提示 | 50 秒全量轮询保留为兜底；事件消费 shadow 后逐类启用。 | 丢事件时最终一致、去重、顺序、审计完整 | 丢失/重复/乱序事件、重启、背压、全量兜底 |
| 7 | Agent lanes | 单 `runtimeMu`、planned priority、scheduled worker | interactive/emergency/background lanes，有界并发与资源 lease | 先只分队列不并行 mutation；flag 回退单 worker。 | interactive 优先级、checkpoint、grant 单次消费、同资源串行 | 饥饿/抢占、队列等待、取消、模型超时、资源冲突、restart |
| 8 | 策略 Optimizer | score policy versions、分析 packet、outcomes | 独立 Optimizer：create/simulate/suggest/publish policy version | 默认 suggest/shadow；人工确认后有限池发布；旧 active policy 始终可回退。 | 模型失败不改变 active policy、版本原子发布、审计 | 仿真等价、发布原子性、坏模型输出、回滚旧版本 |
| 9 | 其他旧路径删除 | 分组 executor 覆盖率和稳定期指标 | 删除剩余分组直写和旧 Agent 分组 action 路径 | 账号直写已在核心阶段 A 删除；其他资源只有新路径覆盖 100% 且安全测试等价或更强后才能删除。 | 不能以减代码为由删除安全逻辑 | 静态禁止直写、端到端 mutation 矩阵、升级/回滚演练 |
| 10 | 安全与前端收尾 | 统一控制面 API、运行指标、旧 UI | 权限/审计复核，前端显示 Intent、Override、uncertain 状态 | API 兼容期内同时提供旧字段；前端开关独立回滚。 | 管理员精确授权、CSRF/session、脱敏、确认流程、无凭据泄漏 | HTTP auth、浏览器工作流、可访问性、poll/SSE fallback、敏感信息扫描 |

## 依赖关系

账号 executor 已具备 Intent/Arbiter、幂等和资源级串行。分组 executor 仍需在后续阶段独立迁移；Reconcile 拆分必须依赖现有账号 executor，SQLite 批量化只在约 10 账号基线证明收益后进行；事件驱动依赖 executor 的幂等和定向资源身份；Agent lanes 必须在对应资源串行机制存在后才允许并行；Optimizer 必须在策略版本发布和回滚已经证明原子后启用。

任何后续阶段若 uncertain backlog、审计缺口或授权不等价超过验收阈值，应停止该阶段的新入口并保留 journal 用于诊断，不得恢复已删除的账号直写路径，也不进行破坏性数据库降级。

## 当前状态

阶段 1A、1B 和 1B.5 已完成领域模型、确定性仲裁、类型安全适配、Semantic Gap 和稳定 identity。核心阶段 A 已完成账号 `schedulable`/`load_factor` 的持久 Override、生产 Arbiter、账号 Safety Guard、唯一 Mutation Executor、keyed account lock、journal 与重启恢复；账号 Runtime Shadow 和旧直写已删除。

核心阶段 B 已完成：Reconcile Coordinator 是唯一生产 pass loop，保留周期 full fallback，并接入 Telemetry、策略发布、Override 到期、余额锁、成本锁和 startup recovery；Telemetry 支持最多 4 路有限并发及部分成功；Agent 使用持久 interactive/background lane、独立 worker 和独立模型 slot，旧全局 runtime lock 与单 worker 已删除。这里没有引入通用 Event Bus。分组 Mutation Executor、完整 Agent Optimizer、策略模拟和自动回滚、完整聊天意图分类及前端整体重构仍未开始，也不因本阶段标记完成。
