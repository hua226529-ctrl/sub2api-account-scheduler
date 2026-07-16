# Current Bottlenecks

状态：阶段 0，基准 `main@bbb318f` / `v1.0.0`。完整环境和原始数据见 [baseline-report.md](baseline-report.md)。本文件严格区分实测结果与静态推断。

## A. 已由测试或 benchmark 证实

| 项目 | 实测证据 | 结论边界 |
|---|---|---|
| Reconcile 随资源数增长 | 10/100/500 账号分别为 7.38/72.10/350.66 ms/op；SQL query 为 33/303/1503，exec 为 21/201/1001。 | 当前健康输入下 SQL 操作约为 `5N+4`，总耗时近似线性增长。尚未证明由哪条 SQL 或连接等待主导。 |
| Reconcile 的 Sub2API 请求数 | 10/100/500 场景均为 2 calls/op：`ListMonitors=1`、`ListAccounts=1`。 | 账号数不会使本轮只读 Sub2API 请求线性增长；有 mutation 的异常场景未纳入该 benchmark。 |
| 纯本地计算 | Health Evaluate 10/100/500 为 2.61/25.45/132.94 us；ResolveBindings 为 15.59/175.44/1022.08 us。 | 本地计算随规模增长，但比包含 SQLite 的 Reconcile 小两个到三个数量级。不能据此断言生产 I/O 占比完全相同。 |
| Store 当前控制读取 | 100/500 控制分别 2.18/10.99 ms/op，严格为 100/500 query/op。 | `GetControl` 逐账号读取是线性数据库访问。未测量并发连接等待。 |
| 固定上游延迟 | 100 账号 0ms fake 为 72.10 ms/op；每个 fake API 固定 1ms 后为 78.21 ms/op，增加 6.12 ms（8.5%）。 | Windows timer 调度使增量大于名义 2ms；证明外部读取位于整轮同步关键路径，不代表生产 RTT。 |
| ChatAsync 入队 | 1.504 ms/op，9,292 B/op，195 allocs/op，5 SQL exec/op，无模型调用。 | 这是 SQLite 持久化 benchmark 平均值，不是 p50/p95，也不含 HTTP/auth。 |
| Agent interactive 等待 | 配置后台持锁 1ms，测得 `runtimeMu` 等待 1.518 ms/op；characterization test 证明 interactive goal 在 mutex 释放前保持 planned。 | 合成的有界等待，不是生产 background goal 时长分布。模型时间未测。 |
| Telemetry 串行规模 | 10/100 monitors 最新明细复测为 7.73/70.14 ms/op；上游 calls 为 13/103，SQL query 为 10/100，exec 为 34/313。 | 上游请求为 `N+3`：success/error/monitors 各 1 次，history 为 N 次；当前逐 monitor 串行采集。 |
| Telemetry 单点失败 | `TestCurrentBehaviorSingleMonitorFailureStopsTelemetryRound` 在第 2 次 history 调用失败后返回，第 3 个 monitor 未请求。 | 已证实 stop-on-first-error 行为；未测量生产失败率和影响频次。 |
| 救灾多池首错中止 | `TestCurrentBehaviorFirstPoolAssessmentFailureStopsLaterPools` 注入 `pool-a` 评估错误后，`pool-b` 的账号没有被读取。 | 已证实当前一池评估错误会终止本轮后续池；未测量生产触发频次和持续时间。 |
| 人工动作与 Reconcile 并发 | channel/hook characterization test 记录 fake 最大并发调用数为至少 2。 | `ManualPause`/`ManualResume` 当前能在 Reconcile 网络读取时进入外部写入；测试不量化数据竞争发生率。 |

增长趋势：Reconcile 100/10 耗时为 9.77 倍，500/100 为 4.86 倍；每账号约 0.70-0.74 ms。Telemetry 最新明细复测 100/10 为 9.08 倍。Store 500/100 为 5.04 倍。上述比值来自本机基准，不是容量承诺。

## B. 仅由静态代码分析推断

| 项目 | 静态证据 | 尚缺测量 |
|---|---|---|
| `reconcile.runMu` 是全局阻塞边界 | `Engine.Reconcile` 从读取 settings 起持锁，跨 `ListMonitors`、`ListAccounts`、所有逐资源 SQL 和外部 mutation。 | 未做并发 Reconcile/Agent mutation 的等待分布和锁 profile，不能称为已证实性能瓶颈。 |
| `balance.runMu` 是全局阻塞边界 | Balance refresh/transition 在全局 manager 锁下读取 SQLite、调用 fetcher、协调成本和救灾状态。 | 未有 10/100 upstream 并发基准或 mutex profile。 |
| SQLite 单连接可能放大排队 | `store.Open` 配置 `SetMaxOpenConns(1)`。 | 当前 benchmark 证明 SQL 次数线性增长，但没有证明连接等待时间或更大连接池会更快、更安全。禁止据此替换配置。 |
| Agent V2 全局串行会形成长队 | `processNextRuntimeGoal` 持有 `runtimeMu` 运行完整 goal，模型和 tool 调用在锁内。 | 只测了 1ms 合成持锁；缺少真实模型延迟、队列深度和优先级反转分布。 |
| 前端刷新可见延迟 | `App.vue` 的全量 `refreshAll` 使用 50 秒 timeout；Agent stream 失败时回退 polling。 | 未做浏览器端事件到 UI 可见的 p50/p95、网络 waterfall 或渲染 profile。 |
| 普通 SwitchGroup 阻塞其他 balance 工作 | `Manager.SwitchGroup` 和 `TransitionGroupTier` 的锁/网络边界宽，HTTP 可直接调用。 | 没有并发切组与余额 refresh 基准。 |
| Reconcile mutation 场景可能增加上游请求 | 健康 benchmark 只有两次读取；每个需暂停/恢复/调载账号会触发写和可能回滚。 | 缺少 10/100 个同时 mutation 的基准；阶段 0 不制造生产语义变化来拆分测量。 |

## 测量限制

- 未启用 SQLite trace duration、mutex profile 或 block profile；SQL counter 只统计真正交给 driver 的 query/exec 次数。
- Reconcile 尚未拆成 Collect/Evaluate/Execute，因此没有三个阶段的独立 duration。
- benchmark 使用内存 fake 加临时磁盘 SQLite；无真实网络、TLS、模型响应、生产数据分布和并发 HTTP 流量。
- 所有结论只适用于当前提交和本机环境，后续优化必须在相同命令下对比，并保留原始结果。
