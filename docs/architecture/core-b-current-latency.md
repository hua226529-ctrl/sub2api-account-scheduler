# Core B Current Latency

基线提交：`ed73c3aa831c41b23cd7b74132d6aa112adff364`。本文件记录核心阶段 B 编码前的实际控制路径。除阶段 0 benchmark 外，下列延迟均由调用图推断，不作为生产 SLO。

## 已测基线

| 项目 | 本地 fake / 临时 SQLite 结果 | 包含范围 |
|---|---:|---|
| 10 账号全量 Reconcile | 7.38 ms/op | 两次 fake 只读上游调用、33 query、21 exec；不含真实网络 |
| 10 monitor Telemetry | 7.73 ms/op | 13 次 fake 调用、10 query、34 exec；逐 monitor 串行 |
| ChatAsync 持久化 | 1.50 ms/op | conversation/message/goal/event SQLite；不含 worker、模型和 HTTP |
| interactive 等待 runtimeMu | 1.52 ms（后台合成持锁 1 ms） | 只证明等待全局 runtimeMu；不是生产模型时长 |

阶段 0 的 100/500 账号数据只用于复杂度趋势。本阶段以约 10 个账号和 10 个 monitor 为性能边界，不为大规模资源数引入缓存、连接池或批量框架。

## Reconcile 路径

```text
Engine.Start
  -> 启动时直接 Reconcile
  -> POLL_INTERVAL ticker（默认 50 秒）
  -> Engine.trigger（容量 1）
  -> 每个分支直接调用同一个全量 Engine.Reconcile
```

`Engine.Reconcile` 每轮读取 settings、freeze、全部 monitors、全部 accounts 和全部 policies，更新 monitor/health 状态，解析全部 binding，然后逐账号读取 control、balance lock、cost lock 并可能提交 AccountControl mutation。生产代码没有定向 Reconcile 入口。

当前 Reconcile 本身已不持有 `runMu` 执行网络和账号 mutation。`runMu` 仍用于设置/策略发布和 `RunExclusive`；账号外部写入继续由 Account Mutation Executor 按账号串行。

## 状态到调度的等待

| 状态变化 | 当前提交路径 | 本地排队/采集等待 | 模型耗时 |
|---|---|---|---|
| Telemetry 新数据 | Telemetry 写 SQLite 后不触发 Engine；等待独立 50 秒 Reconcile ticker 或其他 full trigger | 最坏接近一个 Reconcile 周期；Telemetry 自身默认每 2 分钟采集 | 无 |
| Policy 激活 | 原子发布后 `Engine.Trigger()` | trigger 非阻塞，随后运行全量 Reconcile；不能定向账号 | 发布动作可能由 Agent 模型产生，但发布后的排队不包含模型耗时 |
| 临时 Override 到期 | 读取 active override 时惰性标记 expired | 没有独立 timer；必须等待某个账号命令或下一轮全量 Reconcile 读取该账号 | 无 |
| BalanceLock 变化 | `SyncBalanceLocks` 后 `Engine.Trigger()` | 全量触发；即使锁集合未变化也可能触发 | 无 |
| CostLock 变化 | `SyncCostLocks` 后 `Engine.Trigger()` | 全量触发；已有相等检查，但不能定向账号 | 无 |
| 启动 mutation recovery | `Engine.Start` 同步 recovery 后直接启动旧 Reconcile loop | recovery 完成后由启动全量扫描收敛 | 无 |

## Telemetry 调用链

```text
Manager.Start / ticker（默认 2 分钟）
  -> RunOnce 持有 Manager.mu 整轮
  -> traffic success
  -> traffic errors
  -> InsertTrafficBatch
  -> ListMonitors
  -> 对每个 monitor 串行 ListMonitorHistory + InsertMonitorHistoryBatch
  -> refresh capabilities
  -> cleanup
```

网络调用和数据库操作都位于同一把 Telemetry mutex 内。任一 monitor history 读取或保存失败会立即返回，后续 monitor 本轮不再执行。阶段 B 编码前的 characterization 已证明第 2 个 monitor 失败时第 3 个 monitor 不会被读取；该测试随后被更新为新的部分成功合同。

## Agent 交互调用链

```text
HTTP POST /api/agent/chat
  -> ChatAsync
  -> 持久化 conversation/message/goal/event
  -> runtimeWake（容量 1，非阻塞）
  -> 返回 conversation ID / goal ID

单 runtimeWorker
  -> 15 秒 fallback 或 runtimeWake
  -> List planned goals（priority DESC, created_at, id）
  -> runtimeMu
  -> beginRun 全局 running 标记
  -> 完整 goal：packet -> model -> capability -> checkpoint/result
  -> endRun / runtimeMu unlock
```

ChatAsync 不等待模型。UI 已优先使用 Agent SSE；SSE 不可用时，未完成任务每 1.5 秒轮询一次，完成后停止任务轮询。全页面数据仍按 50 秒刷新。

`runtimeMu` 和 `beginRun` 覆盖完整 goal 生命周期。background goal 在模型或 capability 调用中阻塞时，interactive goal 即使已持久化并唤醒 worker，也只能保持 planned，等待前一 goal 释放全局门。这里的等待属于本地队列等待；模型服务响应时间必须单独统计。

## 风险边界

- 旧 Engine ticker 与 trigger 都只能启动全量 pass，事件无法表达账号范围。
- pass 运行期间的 trigger 只能在容量 1 channel 中合并为一次全量；没有明确 pending account 集合。
- Override 到期依赖后续读取，不能保证到期后快速重新仲裁。
- Telemetry 首错中止会同时扩大数据采集等待和调度等待。
- Agent goal 没有持久 lane、goal lease 或 next-runnable 时间；重启恢复依赖把 running goal 改回 planned。
- SQLite 仍为单连接 WAL。本阶段不改变连接模型，也不以消除 `5N+4` 查询为目标。
