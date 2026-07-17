# Core B Low-Latency Control Paths

核心阶段 B 在单进程和 SQLite 边界内缩短状态提交到调度、以及管理员聊天到 Agent claim 的本地等待。账号外部写入边界没有变化：所有 `schedulable` 和 `load_factor` mutation 仍由 AccountControl 的唯一 Mutation Executor 执行。

## Reconcile Coordinator

`internal/reconcile.Coordinator` 是一个聚焦的进程内请求协调器，不是通用 Event Bus。它只接收账号 ID 集合或 full 请求，使用容量 1 的 wake channel 和内存集合合并重复请求。默认 debounce 为 100ms，可在测试中调整，生产上限为 500ms。

Coordinator 串行执行 Reconcile pass，但在执行 pass、网络请求或数据库操作期间不持有自身 mutex。pass 运行期间的新请求进入下一批；当前 pass 返回后立即继续，不等待下一次 ticker。full 请求覆盖尚未开始的 targeted 请求。日志记录 full/targeted、账号数、合并数、queue wait 和 run duration。

没有引入 `internal/eventbus`、任意 payload、反射订阅、持久消息队列或工作流引擎。系统约有 10 个账号，集合加单执行循环足以满足事件合并和最终一致性。

## Targeted And Full Reconcile

`ReconcileAccounts` 可以复用完整 settings、freeze、monitor、account、policy 和公共状态读取，但只有目标账号允许进入策略 action 和 AccountControl submission。目标不存在只记录稳定日志；单账号失败进入聚合错误，其他目标继续。

`ReconcileFull` 处理全部账号，并继续承担 pending mutation runtime recovery。targeted pass 不会恢复非目标账号的 pending mutation，从而保持“只有目标账号产生动作”的边界。Account Mutation Executor 继续按账号串行，不同账号和管理员直接命令不被 Coordinator 的 pass mutex 串行。

现有可配置周期没有删除。周期 ticker 现在调用 `RequestFull`，与 startup、事件 targeted/full 共用同一个 pass loop，不再存在第二个直接 Reconcile ticker。

## Trigger Sources

- Startup：先完成 pending account mutation recovery，再启动 Coordinator，积累恢复账号并请求一次 full。
- Telemetry：traffic 或 monitor history 成功提交后，按本轮已有 account/monitor 关联请求目标账号。没有新增记录时不触发。
- Policy：account scope 发布后请求对应账号；global、pool 或无法可靠解析的 scope 请求 full。触发发生在原子发布提交之后。
- Override：创建、撤销或替换 Override 后唤醒 expiry worker 重算最近 deadline；到期事务提交后请求受影响账号。
- BalanceLock / CostLock：同步函数返回实际发生状态变化的账号集合；创建、更新或解除后定向请求，集合无变化时不触发。
- Periodic fallback：可配置 ticker 始终请求 full，保证进程内 wake 遗漏时最终收敛。

## Telemetry

Telemetry round 不再跨网络和数据库工作持有整轮 mutex。monitor history 使用固定最多 4 个 worker；worker 从有界工作源领取 monitor，不为每个 monitor 创建永久 goroutine，也不在网络期间持有 SQLite transaction。

monitor 结果独立保存。一个 monitor 的 fetch、validation 或 store 失败不再终止后续 monitor；成功 monitor 的数据照常提交并触发其账号。round 返回 `telemetry_partial_success` 聚合错误，单项机器码包括 `monitor_fetch_failed`、`monitor_history_invalid` 和 `monitor_store_failed`。traffic 顶层失败仍是整轮无法继续的明确错误。

阶段 0 的“首个 monitor 错误中止整轮”characterization 已更新为有意的产品行为修复。10 monitor fake benchmark 为 7.43–7.51ms/op，并发峰值测试确认不超过 4。

## Override Expiry Worker

expiry worker 只维护一个 timer。它查询 active 且具有 `expires_at` 的最近 deadline；无记录时等待 wake，不轮询。创建、撤销或替换 Override 的 wake 只重算 timer，不会被误认为到期。

timer 到期后，Store 在单个事务中把符合条件的 active 行标记为 expired，并在 commit 后返回去重账号 ID。随后 worker 调用 `RequestAccounts`。该过程不直接写 Sub2API；ActivePolicy、其他 Override、Authority 和锁仍由 Reconcile + Arbiter + AccountControl 决定。无 `expires_at` 的 ManualHold 不进入 timer。

## Agent Lanes

Agent 只有两个持久 lane：`interactive` 和 `background`。管理员聊天进入 interactive；定时分析、日报、结果评估和维护任务进入 background。没有新增 emergency lane，紧急管理员请求仍通过 interactive lane 内的 priority 排序。

`agent_goals` 向后兼容增加：

- `lane TEXT NOT NULL DEFAULT 'background'`
- `lease_owner TEXT NOT NULL DEFAULT ''`
- `lease_until TEXT`
- `next_runnable_at TEXT`
- `idx_agent_goals_claim` claim 索引

旧 administrator/conversation goal 回填 interactive；无法识别的旧 goal 保持 background。迁移可重复运行，不删除 goal、step、checkpoint、scheduled command 或 lease 状态。

运行时使用一个 interactive worker 和一个 background worker。每个 worker 只 claim 自己的 lane，claim 顺序为 priority 降序、created_at 升序、ID 升序，并使用持久 lease。interactive fallback 为 1 秒，background fallback 为 10 秒；正常 enqueue 使用容量 1 的非阻塞 wake，不依赖 fallback 才能响应。

旧单 `runtimeWorker`、覆盖完整 goal 生命周期的 `runtimeMu`、`beginRun` 和全局 active-run 门闩已删除。Overview 的 running 状态改为读取持久 running goal。worker 在 goal 边界恢复 panic；被中断的 goal 依靠 lease 到期恢复。

模型并发采用两个独立容量 1 的 slot：interactive 最大 1，background 最大 1，总计最大 2。background 模型请求或 capability 生命周期不会持有 Agent 全局 mutex，也不能占用 interactive slot。已经开始的 background 请求不做危险抢占。

retry/wait 会保存 `next_runnable_at`、释放 lease 和 worker；等待期间不占用模型 slot。到期后由 lane claim query 重新领取。账号 capability 无论来自哪个 lane，都继续调用现有 AccountControl，lane 不改变 AdministratorCommand、AutonomousAgent、TTL、EvidenceRefs、SnapshotVersion 或 grant 语义。

## ChatAsync And Frontend

ChatAsync 在一个 SQLite transaction 中提交 conversation 更新、新 user message 和 interactive goal。commit 后才非阻塞唤醒 interactive worker，然后返回 conversation ID 和 goal ID；HTTP 请求不等待模型或 capability。

本阶段没有新增 SSE、WebSocket、状态框架或页面。仓库原有 Agent SSE 保持兼容；SSE 不可用时现有未完成任务约 1.5 秒 fallback polling 保留，完成后停止任务轮询。

## Performance

Windows 本地 fake + 临时 SQLite，未运行 100/500 账号场景：

| 项目 | 核心阶段 B 结果 | 验收 |
|---|---:|---:|
| 10 账号 full Reconcile | 12.58–12.89ms/op，2 upstream calls/op | <20ms，通过 |
| 10 monitor Telemetry | 7.43–7.51ms/op，13 upstream calls/op | 不退化，通过 |
| ChatAsync durable enqueue | 0.74–1.03ms/op，4 exec/op | <200ms，通过 |
| Telemetry commit 到 targeted pass 开始 | 100.92ms | <1s，通过 |
| Policy commit 到 Reconcile 开始 | 100.59ms | <500ms，通过 |
| Override expiry commit 到 targeted pass 开始 | 100.20ms | <1s，通过 |
| 空闲 interactive claim | 1.58ms | <500ms，通过 |
| background 模型槽阻塞时 interactive claim | 1.00ms | <500ms，通过 |

真实模型推理时间、真实上游网络时间和 Telemetry 采集周期不包含在这些本地排队结果中。

## Removed Production Paths

- Engine 启动/周期/trigger 直接调用 Reconcile 的旧循环和旧 trigger channel。
- Telemetry 整轮 mutex 和 monitor 首错立即返回路径。
- 单 Agent runtime worker、15 秒统一 fallback、`runtimeMu`、`beginRun/endRun` 和全局 running gate。
- ChatAsync 依赖统一 worker 轮询才能开始的旧路径。

本阶段没有接入 SwitchGroup Mutation Executor，没有修改 Failover 分组写入或成本路由业务算法，没有实现完整 Optimizer、策略模拟、自动回滚、自然语言策略 DSL 或前端重构。
