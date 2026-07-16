# Stage 0 Baseline Report

测量日期：2026-07-16（Asia/Shanghai）。基准：`main@bbb318f`，版本 `v1.0.0`。阶段 0 只增加测试基础设施、characterization tests、benchmarks 和合同文档，没有改变产品控制流或数据库 schema。

## 环境

| 项目 | 值 |
|---|---|
| OS | Microsoft Windows 11 家庭版 中文版，10.0.26200 build 26200 |
| Go | go1.26.3 |
| GOOS / GOARCH | windows / amd64 |
| CGO_ENABLED | 0 |
| CPU | AMD Ryzen 9 7945HX with Radeon Graphics，16 cores / 32 logical processors |
| 内存 | 33,518,592,000 bytes（约 31.2 GiB） |
| Node.js | v24.15.0 |
| npm | 11.12.1 |
| SQLite driver | `modernc.org/sqlite v1.53.0` |

## 旧路径与调用图

```text
cmd/scheduler/main.go
  -> store.Open (SQLite, WAL, MaxOpenConns=1)
  -> reconcile.Engine.Start
       -> Reconcile [runMu]
          -> settings/freeze -> ListMonitors -> ListAccounts
          -> monitor state/health SQL -> ResolveBindings
          -> per-account control/lock/event SQL -> optional account mutation
  -> balance.Manager.Start [runMu/source locks]
       -> Fetcher network -> balance/cost locks -> optional account/group mutation
  -> telemetry.Manager.Start [mu]
       -> traffic endpoints -> ListMonitors -> per-monitor history -> SQLite
  -> agent.Manager.Start
       -> ChatAsync -> conversation/message/goal/event SQLite
       -> runtimeWorker -> processNextRuntimeGoal [runtimeMu]
          -> packet -> model -> capability -> Engine/Balance direct methods
  -> failover.Controller.Start [mu]
       -> snapshot/policies/evidence -> Balance.TransitionGroupTier
  -> httpserver.Server
       -> manual account methods / balance methods / agent chat
```

本阶段直接涉及的现有表按所有权分组如下：

- 调度：`settings`, `account_policies`, `monitor_states`, `monitor_observations`, `monitor_health_states`, `account_controls`, `events`, `sessions`。
- 余额和救灾：`upstream_sources`, `upstream_key_rates`, `upstream_groups`, `upstream_group_failover_policies`, `upstream_group_failover_accounts`, `upstream_group_failover_states`, `upstream_group_transitions`, `balance_account_locks`, `cost_account_locks`。
- Telemetry：`monitor_history_records`, `traffic_events`, `account_model_capabilities`, `decision_snapshots`。
- Agent：`agent_providers`, `agent_settings`, `analysis_packets`, `availability_assessments`, `agent_conversations`, `agent_runs`, `agent_messages`, `agent_tool_calls`, `score_policy_versions`, `decision_outcomes`, `agent_daily_reports`, `agent_goals`, `agent_steps`, `agent_v2_events`, `agent_checkpoints`, `agent_memories`, `agent_scheduled_commands`, `agent_freeze_states`, `agent_administrator_grant_consumptions`。

没有新增、删除或修改任何表、索引或迁移语句。

## Benchmark 命令

```powershell
go test -buildvcs=false -run '^$' -bench . -benchmem ./...
go test -buildvcs=false -run '^$' -bench BenchmarkTelemetryRunOnceCurrentBehavior -benchmem ./internal/telemetry
```

第二条命令是在增加分接口 custom metrics 后进行的最新 Telemetry 明细复测；表格采用该次结果，其余项目采用第一条全仓命令结果。

## Characterization 测试清单

| 领域 | 固化的当前行为 |
|---|---|
| 确定性调度 | 三次不同检查暂停、只恢复本调度器归属、adaptive load 档位、stale 不写、freeze 下采集不写、余额/健康锁共同阻止恢复 |
| 人工操作 | pause/resume 在 writes_frozen 下仍写；二者都可与 Reconcile 网络读取重叠；write-success/response-error 后重启可见本地与上游分歧 |
| Agent | pause 归属和 resume 锁、精确 grant scope、grant 单次消费、敏感信息脱敏、capability 风险、ChatAsync 落库、interactive 等待、checkpoint/restart readback |
| 余额和救灾 | 余额双阈值、成本两阶段锁、普通 SwitchGroup、三级切组版本确认与幂等、全局 outage 后切 backup、stale fail-closed、observe 模式 dry-run、多池首错中止、重复/stale monitor 不形成 streak |
| Telemetry | 10/100 串行周期；单 monitor history 失败立即终止后续采集 |
| 测试基础设施 | fake 写成功但响应失败、陈旧回读、延迟取消、调用顺序、分组幂等/readback、临时库 reopen 和真实 SQL 计数、固定 clock、确定性数据维度 |

## Benchmark 原始输出

```text
goos: windows
goarch: amd64
cpu: AMD Ryzen 9 7945HX with Radeon Graphics
BenchmarkChatAsyncEnqueueCurrentBehavior-32              792   1504481 ns/op  5.000 sql_execs/op  0 sql_queries/op  9292 B/op  195 allocs/op
BenchmarkAgentInteractiveQueueWaitCurrentBehavior-32     802   1518062 ns/op  1000000 configured_background_hold_ns/op  1518488 queue_wait_ns/op  305 B/op  3 allocs/op
BenchmarkPolicyEvaluationLocal/monitors_10-32          471140      2608 ns/op  1682 B/op  40 allocs/op
BenchmarkPolicyEvaluationLocal/monitors_100-32          45734     25446 ns/op  16827 B/op  400 allocs/op
BenchmarkPolicyEvaluationLocal/monitors_500-32           9192    132942 ns/op  84170 B/op  2000 allocs/op
BenchmarkReconcileCurrentBehavior/accounts_10-32          165   7380512 ns/op  1.000 list_accounts/op  1.000 list_monitors/op  21.00 sql_execs/op  33.00 sql_queries/op  2.000 upstream_calls/op  126181 B/op  2561 allocs/op
BenchmarkReconcileCurrentBehavior/accounts_100-32          16  72097425 ns/op  1.000 list_accounts/op  1.000 list_monitors/op  201.0 sql_execs/op  303.0 sql_queries/op  2.000 upstream_calls/op  1103097 B/op  21506 allocs/op
BenchmarkReconcileCurrentBehavior/accounts_500-32           3 350660133 ns/op  1.000 list_accounts/op  1.000 list_monitors/op  1001 sql_execs/op  1503 sql_queries/op  2.000 upstream_calls/op  5408616 B/op  102859 allocs/op
BenchmarkReconcileCurrentBehavior/accounts_100_upstream_delay_1ms-32  15  78214313 ns/op  1.000 list_accounts/op  1.000 list_monitors/op  201.0 sql_execs/op  303.0 sql_queries/op  2.000 upstream_calls/op  1102867 B/op  21505 allocs/op
BenchmarkResolveBindingsLocal/accounts_10-32             82340     15588 ns/op  19880 B/op  92 allocs/op
BenchmarkResolveBindingsLocal/accounts_100-32             7786    175440 ns/op  182248 B/op  801 allocs/op
BenchmarkResolveBindingsLocal/accounts_500-32             1734   1022078 ns/op  944106 B/op  3913 allocs/op
BenchmarkStoreReadCurrentControls/accounts_100-32          550   2180518 ns/op  0 sql_execs/op  100.0 sql_queries/op  338401 B/op  9200 allocs/op
BenchmarkStoreReadCurrentControls/accounts_500-32          109  10994368 ns/op  0 sql_execs/op  500.0 sql_queries/op  1695908 B/op  46490 allocs/op
BenchmarkTelemetryRunOnceCurrentBehavior/monitors_10-32    154   7728444 ns/op  1.000 list_errors/op  10.00 list_history/op  1.000 list_monitors/op  1.000 list_success/op  34.00 sql_execs/op  10.00 sql_queries/op  13.00 upstream_calls/op  66235 B/op  1281 allocs/op
BenchmarkTelemetryRunOnceCurrentBehavior/monitors_100-32    15  70139807 ns/op  1.000 list_errors/op  100.0 list_history/op  1.000 list_monitors/op  1.000 list_success/op  313.0 sql_execs/op  100.0 sql_queries/op  103.0 upstream_calls/op  630178 B/op  12093 allocs/op
```

## 性能基线汇总

| 场景 | 账号/监控数 | ns/op | B/op | allocs/op | DB query + exec/op | fake 上游请求/op |
|---|---:|---:|---:|---:|---:|---:|
| Reconcile | 10 | 7,380,512 | 126,181 | 2,561 | 33 + 21 | 2 |
| Reconcile | 100 | 72,097,425 | 1,103,097 | 21,506 | 303 + 201 | 2 |
| Reconcile | 500 | 350,660,133 | 5,408,616 | 102,859 | 1,503 + 1,001 | 2 |
| Reconcile + 每个 fake API 1ms | 100 | 78,214,313 | 1,102,867 | 21,505 | 303 + 201 | 2 |
| Telemetry | 10 | 7,728,444 | 66,235 | 1,281 | 10 + 34 | 13 |
| Telemetry | 100 | 70,139,807 | 630,178 | 12,093 | 100 + 313 | 103 |
| Store GetControl | 100 | 2,180,518 | 338,401 | 9,200 | 100 + 0 | 0 |
| Store GetControl | 500 | 10,994,368 | 1,695,908 | 46,490 | 500 + 0 | 0 |

| 指标 | 当前基线 |
|---|---:|
| ChatAsync enqueue benchmark | 1,504,481 ns/op |
| interactive queue wait（后台持锁配置 1ms） | 1,518,488 ns/op wait；总 benchmark 1,518,062 ns/op |
| Telemetry 10 monitors | 7,728,444 ns/op |
| Telemetry 100 monitors | 70,139,807 ns/op |
| Reconcile 10 accounts | 7,380,512 ns/op |
| Reconcile 100 accounts | 72,097,425 ns/op |
| Reconcile 500 accounts | 350,660,133 ns/op |

## 包含与不包含

- Local Health/ResolveBindings 不含 SQLite 和 fake I/O。
- Reconcile 含生产 schema 的临时 SQLite 和内存 fake Sub2API；0ms 场景无主动延迟，delay 场景对两次 fake 读取各配置 1ms。
- ChatAsync 含临时 SQLite，不含 HTTP、鉴权、worker、模型或 tool 时间。
- Agent queue wait 是 `runtimeMu` 的 1ms 有界合成持锁，不包含真实模型响应。
- Telemetry 含临时 SQLite 和内存 fake；每 monitor 一条 history，无主动延迟。
- 全部测量都不包含真实 Sub2API、真实模型、TLS、跨机网络、生产磁盘争用、生产并发和生产数据倾斜，因此不能直接作为 SLO 或容量上限。

## 风险基线

- `runMu`/`runtimeMu`/Telemetry `mu` 跨 I/O，可能导致全局排队；阶段 0 只记录，不移除。
- ManualPause/ManualResume 在 writes_frozen 下仍写，并可与 Reconcile I/O 重叠；characterization test 固化这一 known issue。
- 健康 Reconcile 的上游读取恒为 2，但 SQL 操作线性增长；mutation 密集场景尚未测。
- Agent 无 provider 时 goal 转 waiting，确定性 Reconcile 不受影响。
- 普通账号 mutation 发生“上游成功、本地未持久化”后没有统一 restart journal；fake restart test 显示上游和本地可分歧。
- 三级切组已有更强的 transition/idempotency/readback 协调；统一控制面不得降低它。
- Telemetry 单 monitor 失败中止整轮，后续 monitor 本轮无数据。
- 前端全量刷新默认为 50 秒；尚无端到端 UI 可见延迟测量。

## 测量可复现性

测试数据由无网络的确定性生成器产生，支持 10/100/500 账号、monitor 数、基础设施/凭据/限流信号、池、人工/余额/成本锁和 policy。SQL 数由 `database/sql/driver` 包装器计数，Store 业务函数未散布计数代码；计数器在迁移完成后复位。每个测试使用独立 TempDir，可关闭并按同一路径重新打开。

## 验证结果

| 命令 | 结果 |
|---|---|
| `gofmt -w`（仅本阶段修改的 Go 文件） | 通过 |
| `go test -buildvcs=false -count=1 ./...` | 通过 |
| `go vet ./...` | 通过 |
| `go build -buildvcs=false ./cmd/scheduler` | 通过 |
| `go test -buildvcs=false -run '^$' -bench . -benchmem ./...` | 通过，原始结果见上文 |
| `frontend: npm test -- --run` | 通过，1 file / 21 tests |
| `frontend: npm run build` | 通过，1770 modules transformed |
| `go test -race -buildvcs=false ./...` | 未运行成功：`CGO_ENABLED=0`，Go 报告 `-race requires cgo` |
| `CGO_ENABLED=1 go test -race -buildvcs=false ./...` | 未运行成功：`gcc` 不在 `%PATH%`，失败发生在 `runtime/cgo` 编译前 |

Race 结果是环境限制，不是通过。阶段 0 没有安装编译器，也没有为此修改生产代码。

## 数据库迁移

无数据库迁移。
