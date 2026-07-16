# Control Plane Migration Plan

本计划只定义阶段 1 及以后的小步迁移顺序。阶段 0 没有实现下列组件。每一步必须独立部署、可关闭、可回滚，并继续使用单进程和 SQLite。

| 顺序 | 阶段 | 输入 | 输出 | 兼容策略与回滚点 | 不允许破坏的行为 | 所需测试 |
|---:|---|---|---|---|---|---|
| 1 | Intent 与 Arbiter（1A、1B 已完成；1C 未开始） | 当前 Policy、人工、Agent、Balance、Failover 的动作描述和安全检查 | 1A：有类型的 Intent、OverrideLease、纯判定 Arbiter；1B：离线旧路径适配与 Semantic Gap 审计；1C：runtime shadow observer | 1A/1B 均未接入生产路径。1C 才以默认 no-op、显式开启方式接入 shadow，旧 writer 仍执行并可完全关闭。 | 判定结果、freeze、人工优先级、精确 grant、脱敏 | 1A：领域验证、Authority、幂等、确定性；1B：适配、稳定 identity、调用点覆盖；1C：shadow 等价、旧数据库重启、权限矩阵 |
| 2 | 账号 Mutation Executor | 账号 Intent、account_controls、freeze/lock/freshness 规则 | SetSchedulable/UpdateLoadFactor 唯一 executor，幂等和 readback journal | 逐 capability/账号池切换；旧 Engine wrapper 可回退。双写只允许审计，不允许双外部写。 | 暂停/恢复阈值、人工/余额/成本/健康锁、uncertain 分类 | fake 写成功超时、提交失败回滚、同资源并发、重启协调、限频 |
| 3 | 分组 Mutation Executor | 普通 SwitchGroup、三级 Transition、已有 transitions 表 | 普通和自动切组统一 executor | 先包裹现有 `TransitionGroupTier`，普通 SwitchGroup feature flag；保留旧 HTTP 契约。 | 已确认 policy/version、余额和新鲜度、幂等、前后回读、人工精确授权 | 三级切换矩阵、普通切组兼容、unknown group freeze、restart readback |
| 4 | Reconcile 热路径 | 当前单体 Reconcile、账号 executor | 明确 Collect/Evaluate/Plan/Execute 边界，执行只产 Intent | shadow plan 与旧 decision 对比；先对单账号池启用，开关回退旧循环。 | 确定性策略主导、模型不可用继续、freeze 下采集不写、stale fail-closed | 10/100/500 等价、并发人工动作、故障注入、阶段 duration benchmark |
| 5 | SQLite 与批量查询 | 实测 `5N+4` SQL、单连接配置 | 批量读取控制/锁/状态，经过基准验证的连接策略 | Store 保留逐项 API 兼容包装；每个批量函数可回退。迁移只能向后兼容。 | 事务原子性、事件顺序、凭据和历史数据、WAL 语义 | query count 上限、事务失败、旧库升级/降级、并发读写和 race |
| 6 | 事件驱动 | poll/trigger、持久 events/agent events | 进程内 domain event bus 和定向 reconcile 提示 | 50 秒全量轮询保留为兜底；事件消费 shadow 后逐类启用。 | 丢事件时最终一致、去重、顺序、审计完整 | 丢失/重复/乱序事件、重启、背压、全量兜底 |
| 7 | Agent lanes | 单 `runtimeMu`、planned priority、scheduled worker | interactive/emergency/background lanes，有界并发与资源 lease | 先只分队列不并行 mutation；flag 回退单 worker。 | interactive 优先级、checkpoint、grant 单次消费、同资源串行 | 饥饿/抢占、队列等待、取消、模型超时、资源冲突、restart |
| 8 | 策略 Optimizer | score policy versions、分析 packet、outcomes | 独立 Optimizer：create/simulate/suggest/publish policy version | 默认 suggest/shadow；人工确认后有限池发布；旧 active policy 始终可回退。 | 模型失败不改变 active policy、版本原子发布、审计 | 仿真等价、发布原子性、坏模型输出、回滚旧版本 |
| 9 | 旧路径删除 | executor 覆盖率、shadow 差异、稳定期指标 | 删除直写和旧 Agent action 路径 | 只有新路径覆盖 100% 且安全测试等价或更强后删除；前一版本仍可回滚。 | 不能以减代码为由删除安全逻辑 | 静态禁止直写、端到端 mutation 矩阵、升级/回滚演练 |
| 10 | 安全与前端收尾 | 统一控制面 API、运行指标、旧 UI | 权限/审计复核，前端显示 Intent、Override、uncertain 状态 | API 兼容期内同时提供旧字段；前端开关独立回滚。 | 管理员精确授权、CSRF/session、脱敏、确认流程、无凭据泄漏 | HTTP auth、浏览器工作流、可访问性、poll/SSE fallback、敏感信息扫描 |

## 依赖关系

账号和分组 executor 都依赖 Intent/Arbiter 的稳定 envelope；Reconcile 拆分依赖账号 executor，SQLite 批量化依赖拆分后可识别的读集合；事件驱动依赖 executor 的幂等和定向资源身份；Agent lanes 必须在资源级串行机制存在后才允许并行；Optimizer 必须在策略版本发布和回滚已经证明原子后启用；旧路径删除是最后一步。

任何阶段若 shadow 差异、uncertain backlog、审计缺口或授权不等价超过验收阈值，回滚到旧执行路径并保留新数据用于诊断，不进行破坏性数据库降级。

## 当前状态

阶段 1A 已完成纯领域模型、确定性仲裁、单元测试和领域合同。阶段 1B 已完成离线类型安全适配、Semantic Gap、稳定 identity 和旧写路径审计。两者都没有生产调用方、数据库表、外部写入或 feature flag。阶段 1C 的 runtime shadow observer 尚未开始。
