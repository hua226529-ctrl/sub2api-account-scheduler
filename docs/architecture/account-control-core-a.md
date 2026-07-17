# Account Control Core A

核心阶段 A 将账号 `schedulable` 和 `load_factor` 的所有生产写入收敛到 `internal/accountcontrol`。确定性 Reconcile 仍是主控制器；Agent 不参与策略循环，模型不可用不会停止策略执行。

## Ownership And Persistence

- `account_overrides` 是有效账号 Override 的唯一事实来源。ActivePolicy 是每轮候选，不写入该表。
- `account_mutations` 是账号 mutation journal 的唯一事实来源。
- `account_controls` 保留为兼容读模型和健康/余额/成本/抖动状态投影，不再作为 Override 的仲裁事实来源。
- `events` 保留审计记录；它不决定仲裁。
- 旧 Agent 专用 runtime/mutation 表继续服务 Agent 工作流，但不记录或执行账号 mutation journal。

`account_controls` 的 `manual_locked`、`manual_override_until`、`load_override_until`、`load_pin_*` 等字段在新写入中只由 AccountControl 最终事务更新。生产仲裁只读取 `account_overrides`。这是迁移兼容期：旧 API/UI 仍可读投影字段，但不能用它们创建第二条执行路径。

## Submission Flow

1. 入口构造类型安全 Intent 和真实 command identity。
2. Service 将候选登记到实例级等待队列，然后获取 `account:<id>` keyed lock 和 4 路 semaphore。
3. 获锁后重新读取当前有效 Override，并把仍在等待的同资源候选加入纯函数 Arbiter；重新确认提交 Intent 是 winner 且未过期。
4. 依据 IdempotencyKey 和 Semantic Signature，在一个短事务内同时创建 `pending` Override（如需要）和 `prepared` journal。
5. 前置 `ListAccounts` 获取实际上游状态，读取本地 control/安全锁并重新执行账号 Guard。
6. 实际状态已满足时提交 `applied_noop`，不调用写 API。
7. 否则 journal 进入 `executing`，唯一 executor 调用写 transport。
8. journal 进入 `verifying`，再次 `ListAccounts` 并严格比较目标状态。
9. 一个短 SQLite 事务完成 journal、Override 激活/撤销、account_controls 投影和 event。
10. 返回统一 `accountcontrol.Result`，并在交出账号锁前移除已完成的等待候选。

本地最终提交失败时不反向写上游。journal 保持 `verifying` 并由恢复流程收敛。

## Atomic State Boundary

接收命令到完成的原子边界如下：

| 阶段 | 持久状态 | 是否可仲裁 | 恢复依据 |
|---|---|---|---|
| 尚未 Prepare | 无 Override、无 journal | 否 | 无外部写入，无需恢复 |
| Prepare 事务内 | `pending` Override 与 `prepared` journal 同事务写入 | 否 | 事务整体提交或整体回滚 |
| prepared/validating | journal 包含 requested/winning Intent、before（读取后补齐）和安全上下文 | 否 | 可重新 Guard、过期或受控执行 |
| executing/verifying/uncertain | journal 保留 before、desired、attempt 和 winner 元数据 | 否 | 回读后证明 applied、未写入或分叉 |
| 最终事务 | journal 终态、Override 状态、撤销、投影、event 同事务提交 | 仅最终为 `active` 时是 | 最终事务整体提交或回滚 |

不存在“Override 已有效但 journal 不存在”的提交窗口。`pending`、`blocked`、`failed`、`superseded`、`expired`、`revoked` 和 `legacy_disabled` 都不参与 Arbiter；只有未过期的 `active` Override 参与。blocked ManualResume 的恢复 Override 保持 `blocked`，且撤销 ManualHold 的更新不会发生。failed 临时命令同样不会留下隐藏 `active` Override。

## Safety Matrix

| 条件 | 暂停/降低负载 | 恢复/增加负载 |
|---|---|---|
| 全局 `writes_frozen` | 阻止 | 阻止 |
| Agent paused/read-only，Producer=AgentOperator | 阻止 | 阻止 |
| credential invalid | 允许 | 阻止 |
| health/balance/cost hard lock | 允许 | 阻止 |
| 上游 rate-limit/overload/temp-unschedulable 窗口 | 允许 | 阻止 |
| stale telemetry | ActivePolicy/AutonomousAgent 阻止；管理员允许 | 同左，管理员仍受硬锁 |
| cooldown/anti-flap | ActivePolicy/AutonomousAgent 阻止；管理员允许 | 同左，管理员仍受硬锁 |

blocked 结果写入 journal 并返回稳定 `BlockReason`。会导致不安全恢复/增载的 blocked Override 不会激活为稍后生效的隐藏动作。

## Override Semantics

- `ManualPause`: AdminUI / ManualHold / `schedulable=false` / 无过期时间。
- `ReleaseManualHold`: 只撤销 hold，重新加载当前策略和其他 Override 后仲裁；不等价于强制恢复。
- `ManualResume`: 撤销 schedulable hold，并创建默认 30 分钟的 AdministratorCommand；硬锁阻止时不保留恢复 Override。
- `ForceResume` 和普通直接负载调整: 默认 30 分钟 AdministratorCommand，不能绕过 freeze、credential、health、balance 或 cost hard lock。
- `PinLoad`: 临时 pin 使用期限 AdministratorCommand；明确永久 pin 使用 ManualHold；`OverrideKindLoadPin` 与普通临时负载调整分离。
- 管理员聊天: AgentOperator producer + AdministratorCommand authority，绑定已消费的 exact grant，默认 30 分钟。
- 自主 Agent: AutonomousAgent，默认 15 分钟、最大 2 小时，必须携带 EvidenceRefs、SnapshotVersion 和稳定动作身份。

## Recovery

`ReconcilePendingAccountMutations` 在 Engine worker 启动前和每轮全量 Reconcile 前运行。它处理 `prepared`、`executing`、`verifying`、`uncertain`：

- 同账号仍使用同一个 keyed lock；不同账号最多 4 个并行 worker。
- 每轮只读取最多 100 条 pending journal，先按账号分组，再交给固定 worker；不会按 mutation 数量创建等待 goroutine。
- 上游已等于 desired 时完成本地事务。
- 上游仍等于 before 且状态允许时做最多 2 次受控尝试。
- 回读失败、状态分叉或无法证明前置条件时保持 uncertain。
- 单账号失败被聚合报告，不阻止其他账号恢复。
- context 取消后 feeder 停止派发，worker 不再领取下一条 mutation；下一轮继续处理超过本轮上限的记录。

## Concurrency

keyed lock map 属于 Service 实例，没有 package-level 可变全局状态。等待锁使用可取消 context；最后一个使用者释放后回收 map entry。同一账号的 schedulable、load 和 recovery 严格串行，不同账号可并行。

`Engine.Reconcile` 不再用 `runMu` 包围 collect/evaluate/network mutation。`runMu` 仅保留给短的设置/策略发布协调；AccountControl 的网络请求只持有该账号的资源锁。

唯一锁顺序为：需要冻结协调的入口先取得 automation barrier 的共享 mutation lease；登记候选时短暂取得 candidate queue mutex 并立即释放；随后取得 account keyed lock，再取得 4 路 semaphore，最后才允许进入 Store 的短 SQLite transaction。automation barrier、account lock 和 semaphore 的等待都接受 context 取消，冻结 waiter 对新 mutation 有优先级。等待 account lock 或 semaphore 时没有 transaction，Sub2API 前置读取、写入和后置回读期间也没有 transaction。AccountControl 不回调 Reconcile 或 Agent，不会在 account lock 内获取 `runMu` 或 Agent runtime lock。Store transaction 是最内层资源。

排队候选在释放 account lock 前完成生命周期登记。blocked、failed、superseded 和 expired 候选立即移除；已应用或 uncertain 的候选只保留到与它重叠的 waiter 排空，使“新策略先拿锁”和“旧策略先拿锁”得到相同仲裁结果，之后不会留下长期内存状态。新的 winner 不由旧提交自动执行，仍由其自身提交或下一轮 Reconcile 负责。

## Reconcile Error Isolation

全局设置、冻结状态、账号/监控器快照和策略读取失败仍会终止整轮。进入账号循环后，单账号 blocked、failed、uncertain、数据不完整、写入或回读失败会记录带 account ID、mutation status 和稳定 error code 的事件与聚合错误，然后继续处理其他账号。整轮仍发布部分快照并返回 `AccountReconcileErrors`，因此错误不会被吞掉。

## HTTP Contract

账号 action 接受 `Idempotency-Key` 和可选 `ttl_minutes`（1 到 1440）。缺少 key 时入口使用 `crypto/rand` 生成，并显式处理失败。响应返回 command、mutation、intent、operation、requested/winner、before/after、expiry、blocked、replay 和 uncertain 字段。

- 同 key 同语义返回原结果且不重复写。
- 同 key 不同语义返回 HTTP 409 `idempotency_conflict`。
- blocked 返回 HTTP 409 和稳定 reason。
- uncertain 返回 HTTP 503，不伪装成成功。

策略动作身份由真实 PolicyVersion、telemetry/account SnapshotVersion 和最近 Override 生命周期 revision 共同构成。revision 来自持久 Override 的 ID/status，只用于让激活、撤销或到期后的重新仲裁成为新业务事实；它不使用 wall clock 冒充策略或快照版本。临时 schedulable/load Override 到期后，即使 telemetry 水位未变化，策略也能生成新的幂等动作并接回控制。

## Readback Limitation

当前 Sub2API client 只提供 `ListAccounts`，没有精确按 ID 获取单账号的接口。AccountControl 因此前置读取、uncertain 协调和后置验证都使用完整 `ListAccounts` 后按 ID 选择账号。该回读保持真实上游一致性，不以缓存替代；在当前约 10 个账号的产品规模下保留此限制，待上游提供精确读取能力后再评估。

## Race Gate

Linux CI 的 `race` job 执行 `go test -race -buildvcs=false -count=1 ./...`，不排除 accountcontrol、reconcile 或 agent。测试使用 fake Sub2API 和临时 SQLite；本地 Windows 缺少 race 所需的 cgo/gcc，因此本地结果不能替代实际 CI 结果。

## Migration

初始化增量创建 `account_overrides`、`account_mutations` 及索引；`ensureColumn` 兼容已见过中间 schema 的数据库。一次性事务迁移使用 settings marker `account_control_core_a_migrated`：

- 旧人工暂停转为永久 ManualHold。
- 尚未过期的旧人工恢复/负载保护转为有期限 AdministratorCommand。
- 旧 load pin 保留 permanent/temporary 语义和 `load_pin` kind。
- 旧 Agent 永久 pause/load ownership 不转为 ManualHold 或有效 AutonomousAgent；写入按 operation 区分的 `legacy_disabled` 记录，清空兼容投影 ownership，并产生 `legacy_agent_control_disabled` 审计事件。上游现状不在迁移中改写，等待管理员检查。
- marker、回填和审计同事务提交，失败可重试，重复打开幂等。

迁移不删除 account_controls、events、policies、conversations、goals、steps、credentials 或任何 Agent 状态。

## Out Of Scope

`SwitchGroup`、`TransitionGroupTier`、分组 Failover、余额采集、成本路由、Agent lane、聊天快车道、事件总线和 Optimizer 没有在本阶段统一。它们不能借用账号 Executor 绕过自己的既有安全与 journal。
