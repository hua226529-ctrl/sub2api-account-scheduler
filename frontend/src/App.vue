<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from "vue";
import {
  Activity, AlertTriangle, ArrowRight, Check, CirclePause, Clock3, Database,
  CalendarClock, ChevronDown, ChevronRight, KeyRound, Link2, ListRestart, ListTodo, LoaderCircle, LockKeyhole, LogOut,
  BrainCircuit, MessageSquareText, Pencil, Pause, Play, Plus, RefreshCw, Save, ServerCog, Settings2, ShieldAlert,
  ShieldCheck, ShieldOff, Snowflake, Sparkles, Target, Trash2, Unlink, WalletCards, Wrench, X
} from "@lucide/vue";
import { accountAction, activateAgentPolicy, chatAgent, confirmUpstreamFailoverPolicy, createUpstream, deleteUpstream, getAgentCapabilities, getAgentFreezeState, getAgentGoals, getAgentMemories, getAgentMessages, getAgentOverview, getAgentRuntimeEvents, getAgentTasks, getEvents, getOverview, getUpstreamFailoverTransitions, getUpstreams, login, logout, openAgentStream, refreshUpstream, restoreSession, runAgent, saveAgentProvider, saveUpstreamFailoverPolicy, setAgentFreezeState, switchUpstreamKeyGroup, switchUpstreamKeyTier, triggerReconcile, updateAgentSettings, updatePolicy, updateSettings, updateUpstream, validateAgentProvider, validateUpstream } from "./api";
import { getDiagnostics } from "./api";
import type { AgentCapability, AgentCommandReceipt, AgentFreezeMode, AgentFreezeState, AgentGoal, AgentMemory, AgentMessage, AgentOverview, AgentProvider, AgentProviderInput, AgentRun, AgentRuntimeEvent, AgentScheduledTask, AgentSettings, AgentStep, Binding, Diagnostics, EventItem, FailoverTier, GroupFailoverPolicy, GroupTierTransition, Monitor, Settings, UpstreamFailoverPolicyInput, UpstreamInput, UpstreamPreview, UpstreamSource } from "./types";

type BalanceRow = UpstreamSource & { row_key: string; discovered: boolean };

const authenticated = ref(false);
const booting = ref(true);
const apiKey = ref("");
const loginError = ref("");
const loading = ref(false);
const snapshot = ref<Awaited<ReturnType<typeof getOverview>> | null>(null);
const diagnostics = ref<Diagnostics | null>(null);
const events = ref<EventItem[]>([]);
const upstreams = ref<UpstreamSource[]>([]);
const agentOverview = ref<AgentOverview | null>(null);
const activeTab = ref<"overview" | "agent" | "balances" | "events" | "diagnostics">("overview");
const toast = ref("");
const modal = ref<"policy" | "settings" | "upstream" | "agent-provider" | "agent-settings" | "confirm" | null>(null);
const selected = ref<Binding | null>(null);
const detailBinding = ref<Binding | null>(null);
const selectedUpstream = ref<UpstreamSource | null>(null);
const upstreamAccountOptions = ref<UpstreamSource["matched_accounts"]>([]);
const discoveredUpstream = ref(false);
const expandedSource = ref<number | null>(null);
const upstreamPreview = ref<UpstreamPreview | null>(null);
const groupSelections = reactive<Record<string, string>>({});
const failoverTransitions = reactive<Record<string, GroupTierTransition[]>>({});
const failoverTransitionLoading = reactive<Record<string, boolean>>({});
const failoverTransitionErrors = reactive<Record<string, string>>({});
const selectedAgentProvider = ref<AgentProvider | null>(null);
const agentProviderValidated = ref(false);
const agentConversationID = ref(0);
const agentMessages = ref<AgentMessage[]>([]);
const agentMessage = ref("");
const agentCapabilities = ref<AgentCapability[]>([]);
const agentGoals = ref<AgentGoal[]>([]);
const agentSteps = ref<AgentStep[]>([]);
const agentRuntimeEvents = ref<AgentRuntimeEvent[]>([]);
const agentScheduledTasks = ref<AgentScheduledTask[]>([]);
const agentMemories = ref<AgentMemory[]>([]);
const agentFreeze = ref<AgentFreezeState>({ scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "system" });
const agentV2Available = ref(false);
const agentStreamState = ref<"idle" | "connected" | "polling">("idle");
const trackedAgentGoalID = ref(0);
const trackedAgentRunID = ref(0);
const trackedAgentStatus = ref("");
const confirmState = reactive({ title: "", message: "", action: async () => {} });
const policyForm = reactive({
  monitor_id: "", excluded: false, enabled: true, failure_threshold: "", recovery_threshold: "",
  flap_enabled: "inherit", flap_window_minutes: "", flap_pause_threshold: "", flap_recovery_threshold: "",
  healthy_score_threshold: "", watch_score_threshold: "", quarantine_score_threshold: "", minimum_samples: "",
  latency_warning_ms: "", latency_critical_ms: "", traffic_pause_below: "", traffic_healthy_at: "",
  hard_failures_10_threshold: "", persistent_slow_rate: ""
});
const settingsForm = reactive<Settings>({
  dry_run: true, failure_threshold: 3, recovery_threshold: 3, manual_hold_minutes: 10,
  flap_window_minutes: 60, flap_pause_threshold: 3, flap_recovery_threshold: 10,
  health_engine_mode: "observe", healthy_score_threshold: 80, watch_score_threshold: 60,
  quarantine_score_threshold: 35, latency_warning_ms: 8000, latency_critical_ms: 15000,
  traffic_pause_below: 80, traffic_healthy_at: 95, hard_failures_10_threshold: 5, persistent_slow_rate: 40,
  minimum_samples: 10, quarantine_minutes: 5, recovery_window_size: 10,
  recovery_required_successes: 8, degraded_load_percent: 50, recovery_initial_percent: 25,
  recovery_mid_percent: 50, recovery_stage_minutes: 5, load_manual_hold_minutes: 30
});
const upstreamForm = reactive<UpstreamInput>({
  name: "", provider: "newapi", base_url: "", username: "", password: "",
  pause_below: 5, resume_at: 10, enabled: true, selected_key_id: "", routing_enabled: false, routing_pool: ""
});
const failoverForm = reactive<UpstreamFailoverPolicyInput>({
  enabled: false, key_id: "", main_group_id: "", backup_group_id: "", emergency_group_id: "", account_ids: [], pool: ""
});
const failoverTiers: FailoverTier[] = ["main", "backup", "emergency"];
const agentProviderForm = reactive<AgentProviderInput>({
  slot: "primary", base_url: "", api_key: "", model: "", enabled: true,
  timeout_seconds: 90, max_output_tokens: 4096, temperature: 0.1
});
const agentSettingsForm = reactive<AgentSettings>({
  enabled: false, mode: "observe", analysis_interval_minutes: 30, emergency_cooldown_minutes: 5,
  context_token_budget: 16000, max_anomalies: 20, max_drilldowns: 8, retention_days: 90,
  successful_observation_runs: 0
});
let timer: number | undefined;
let agentCommandTimer: number | undefined;
let agentStreamRefreshTimer: number | undefined;
let agentEventStream: EventSource | null = null;

const bindings = computed(() => snapshot.value?.bindings ?? []);
const unmatchedMonitors = computed(() => snapshot.value?.unmatched_monitors ?? []);
const monitors = computed<Monitor[]>(() => {
  const map = new Map<number, Monitor>();
  for (const binding of bindings.value) if (binding.monitor) map.set(binding.monitor.id, binding.monitor);
  for (const monitor of unmatchedMonitors.value) map.set(monitor.id, monitor);
  return [...map.values()].sort((a, b) => a.id - b.id);
});
const balanceRows = computed<BalanceRow[]>(() => {
  const rows: BalanceRow[] = upstreams.value.map((source) => ({ ...source, row_key: `source:${source.id}`, discovered: false }));
  const configuredURLs = new Set(upstreams.value.map((source) => source.normalized_url));
  const pending = new Map<string, BalanceRow>();
  for (const binding of bindings.value) {
    const rawURL = typeof binding.account.credentials?.base_url === "string" ? binding.account.credentials.base_url : "";
    const normalizedURL = normalizeAccountURL(rawURL);
    if (!normalizedURL || configuredURLs.has(normalizedURL)) continue;
    const account = { id: binding.account.id, name: binding.account.name, schedulable: binding.account.schedulable };
    const existing = pending.get(normalizedURL);
    if (existing) {
      existing.matched_accounts.push(account);
      continue;
    }
    pending.set(normalizedURL, {
      id: 0, name: binding.account.name, provider: "newapi", base_url: rawURL, normalized_url: normalizedURL,
      credential_configured: false, username_hint: "", credential_hint: "待填写登录账号密码", pause_below: 5, resume_at: 10,
      enabled: false, unit: "", low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false,
      key_rates: [], groups: [], selected_key_id: "", routing_enabled: false, routing_pool: "", failover_policies: [], matched_accounts: [account], row_key: `account:${normalizedURL}`, discovered: true
    });
  }
  rows.push(...pending.values());
  return rows.sort((left, right) => {
    const leftID = Math.min(...left.matched_accounts.map((account) => account.id), Number.MAX_SAFE_INTEGER);
    const rightID = Math.min(...right.matched_accounts.map((account) => account.id), Number.MAX_SAFE_INTEGER);
    return leftID - rightID;
  });
});
const counts = computed(() => {
  const result = { healthy: 0, watch: 0, degraded: 0, quarantined: 0, recovering: 0, frozen: 0, unmatched: 0, paused: 0 };
  for (const binding of bindings.value) {
    if (binding.state !== "bound") result.unmatched++;
    else result[healthStage(binding)]++;
    if (binding.control.owns_pause) result.paused++;
  }
  result.unmatched += unmatchedMonitors.value.length;
  return result;
});
const averageHealthScore = computed(() => {
  const scores = bindings.value.map(qualityScore).filter((score): score is number => typeof score === "number");
  return scores.length ? Math.round(scores.reduce((total, score) => total + score, 0) / scores.length) : undefined;
});
const averageHardSuccess = computed(() => averageDecisionMetric("hard_success_rate_60"));
const averageDegradedRate = computed(() => averageDecisionMetric("degraded_rate_60"));
const averageTrafficSuccess = computed(() => averageDecisionMetric("traffic_success_rate"));
const hardFailureAccounts = computed(() => bindings.value.filter((binding) => (binding.decision?.hard_failure_streak ?? 0) > 0 || (binding.decision?.hard_failures_10 ?? 0) > 0).length);
const disagreementCount = computed(() => bindings.value.filter((binding) => binding.decision?.disagreement === true).length);
const capabilityWarningCount = computed(() => bindings.value.filter((binding) => capabilityWarnings(binding).length > 0).length);
const effectiveEngineMode = computed(() => snapshot.value?.settings.health_engine_mode ?? (snapshot.value?.settings.dry_run ? "observe" : "legacy"));
const upstreamFormRates = computed(() => upstreamPreview.value?.key_rates ?? selectedUpstream.value?.key_rates ?? []);
const upstreamFormGroups = computed(() => upstreamPreview.value?.groups ?? selectedUpstream.value?.groups ?? []);
const failoverConfirmationLines = computed(() => buildFailoverConfirmationLines({ ...failoverForm, account_ids: [...failoverForm.account_ids] }));
const latestAgentRun = computed(() => agentOverview.value?.runs?.[0]);
const latestAgentPacket = computed(() => agentOverview.value?.packets?.[0]);
const latestDailyReport = computed(() => agentOverview.value?.daily_reports?.[0]);
const agentAvailabilityCounts = computed(() => {
  const result = { available: 0, degraded: 0, unavailable: 0, insufficient_data: 0 };
  for (const item of agentOverview.value?.assessments ?? []) result[item.state]++;
  return result;
});
const activeAgentGoal = computed(() => {
  if (trackedAgentGoalID.value) {
    const tracked = agentGoals.value.find((goal) => goal.id === trackedAgentGoalID.value);
    if (tracked) return tracked;
  }
  return agentGoals.value.find((goal) => !isTerminalAgentStatus(goal.status)) ?? agentGoals.value[0];
});
const activeAgentSteps = computed(() => {
  const goal = activeAgentGoal.value;
  if (!goal) return [];
  const items = agentSteps.value.filter((step) => step.goal_id === goal.id);
  return (items.length ? items : goal.steps ?? []).slice().sort((left, right) => left.sequence - right.sequence);
});
const activeAgentStep = computed(() => activeAgentSteps.value.find((step) => !isTerminalAgentStatus(step.status)) ?? activeAgentSteps.value.at(-1));
const observationProgress = computed(() => {
  const settings = agentOverview.value?.settings;
  const runs = settings?.successful_observation_runs ?? 0;
  const runPercent = Math.min(100, Math.round(runs * 100 / 40));
  const started = settings?.observation_started_at ? Date.parse(settings.observation_started_at) : NaN;
  const elapsedHours = Number.isFinite(started) ? Math.max(0, (Date.now() - started) / 3_600_000) : 0;
  const timePercent = Math.min(100, Math.round(elapsedHours * 100 / 24));
  const proposed = settings?.observation_proposed_actions ?? 0;
  const executable = settings?.observation_executable_actions ?? 0;
  const actionRate = proposed > 0 ? executable * 100 / proposed : 100;
  const clean = (settings?.observation_violations ?? 0) === 0 && (settings?.observation_structure_errors ?? 0) === 0;
  return {
    runs, runPercent, timePercent, proposed, executable, actionRate, clean,
    percent: settings?.mode === "control" ? 100 : Math.min(runPercent, timePercent, Math.round(actionRate)),
    timeLabel: settings?.mode === "control" ? "已转正式控制" : `${Math.min(24, elapsedHours).toFixed(elapsedHours >= 10 ? 0 : 1)} / 24 小时`
  };
});
const trackedAgentPending = computed(() => {
  if (trackedAgentGoalID.value) {
    const goal = agentGoals.value.find((item) => item.id === trackedAgentGoalID.value);
    if (goal) return !isTerminalAgentStatus(goal.status);
    return !isTerminalAgentStatus(trackedAgentStatus.value);
  }
  if (trackedAgentRunID.value) {
    const run = agentOverview.value?.runs.find((item) => item.id === trackedAgentRunID.value);
    if (run) return !isTerminalAgentStatus(run.status);
    return !isTerminalAgentStatus(trackedAgentStatus.value);
  }
  return false;
});

onMounted(async () => {
  authenticated.value = await restoreSession();
  booting.value = false;
  if (authenticated.value) await refreshAll();
});
onUnmounted(() => {
  if (timer) window.clearTimeout(timer);
  if (agentCommandTimer) window.clearTimeout(agentCommandTimer);
  if (agentStreamRefreshTimer) window.clearTimeout(agentStreamRefreshTimer);
  agentEventStream?.close();
});

async function submitLogin() {
  loginError.value = "";
  loading.value = true;
  try {
    await login(apiKey.value);
    apiKey.value = "";
    authenticated.value = true;
    await refreshAll();
  } catch (error) {
    loginError.value = messageOf(error);
  } finally { loading.value = false; }
}

async function refreshAll() {
  if (!authenticated.value) return;
  loading.value = true;
  try {
    const [nextSnapshot, nextEvents, nextDiagnostics, nextUpstreams, nextAgent] = await Promise.all([
      getOverview(), getEvents(), getDiagnostics(), getUpstreams(), Promise.resolve(getAgentOverview()).catch(() => null)
    ]);
    snapshot.value = nextSnapshot;
    events.value = nextEvents.items ?? [];
    diagnostics.value = nextDiagnostics;
    upstreams.value = nextUpstreams.items;
    if (nextAgent) {
      agentOverview.value = nextAgent;
      Object.assign(agentSettingsForm, nextAgent.settings);
    }
    await refreshAgentControlRoom();
    ensureAgentStream();
    Object.assign(settingsForm, nextSnapshot.settings);
  } catch (error) {
    if (messageOf(error).includes("登录") || messageOf(error).includes("会话")) authenticated.value = false;
    else showToast(messageOf(error));
  } finally {
    loading.value = false;
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(refreshAll, 50_000);
  }
}

async function refreshAgentControlRoom() {
  const results = await Promise.allSettled([
    getAgentCapabilities(), getAgentGoals(), getAgentRuntimeEvents(), getAgentTasks(), getAgentMemories(), getAgentFreezeState()
  ]);
  let successful = 0;
  const [capabilitiesResult, goalsResult, eventsResult, tasksResult, memoriesResult, freezeResult] = results;
  if (capabilitiesResult.status === "fulfilled" && capabilitiesResult.value?.items) {
    agentCapabilities.value = capabilitiesResult.value.items;
    successful++;
  }
  if (goalsResult.status === "fulfilled" && goalsResult.value?.items) {
    agentGoals.value = goalsResult.value.items;
    agentSteps.value = goalsResult.value.steps ?? [];
    successful++;
  }
  if (eventsResult.status === "fulfilled" && eventsResult.value?.items) {
    mergeAgentRuntimeEvents(eventsResult.value.items);
    successful++;
  }
  if (tasksResult.status === "fulfilled" && tasksResult.value?.items) {
    agentScheduledTasks.value = tasksResult.value.items;
    successful++;
  }
  if (memoriesResult.status === "fulfilled" && memoriesResult.value?.items) {
    agentMemories.value = memoriesResult.value.items;
    successful++;
  }
  if (freezeResult.status === "fulfilled" && freezeResult.value?.mode) {
    agentFreeze.value = freezeResult.value;
    successful++;
  }
  agentV2Available.value = successful > 0;
}

function mergeAgentRuntimeEvents(items: AgentRuntimeEvent[]) {
  const merged = new Map<number | string, AgentRuntimeEvent>();
  for (const item of [...items, ...agentRuntimeEvents.value]) merged.set(item.id || item.event_key, item);
  agentRuntimeEvents.value = [...merged.values()]
    .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at))
    .slice(0, 100);
}

function ensureAgentStream() {
  if (!authenticated.value || !agentV2Available.value || agentEventStream) return;
  agentEventStream = openAgentStream((event) => {
    mergeAgentRuntimeEvents([event]);
    agentStreamState.value = "connected";
    if (agentStreamRefreshTimer) window.clearTimeout(agentStreamRefreshTimer);
    agentStreamRefreshTimer = window.setTimeout(async () => {
      await refreshAgentExecution();
    }, 120);
  }, () => {
    agentEventStream?.close();
    agentEventStream = null;
    agentStreamState.value = "polling";
    if (trackedAgentPending.value) scheduleAgentCommandPoll();
  });
  agentStreamState.value = agentEventStream ? "connected" : "polling";
}

function openAgentProvider(slot: "primary" | "fallback") {
  const provider = agentOverview.value?.providers.find((item) => item.slot === slot) ?? null;
  selectedAgentProvider.value = provider;
  agentProviderValidated.value = false;
  Object.assign(agentProviderForm, {
    slot, base_url: provider?.base_url ?? "", api_key: "", model: provider?.model ?? "", enabled: provider?.enabled ?? true,
    timeout_seconds: provider?.timeout_seconds ?? 90, max_output_tokens: provider?.max_output_tokens ?? 4096,
    temperature: provider?.temperature ?? 0.1
  });
  modal.value = "agent-provider";
}

async function testAgentProvider() {
  await runTask(async () => {
    await validateAgentProvider({ ...agentProviderForm });
    agentProviderValidated.value = true;
  }, "模型连接及结构化输出验证成功");
}

async function persistAgentProvider() {
  if (!agentProviderValidated.value && agentProviderForm.api_key) {
    showToast("请先测试模型连接");
    return;
  }
  await runTask(async () => {
    await saveAgentProvider({ ...agentProviderForm });
    modal.value = null;
    await refreshAll();
  }, "模型配置已加密保存");
}

async function persistAgentSettings() {
  await runTask(async () => {
    await updateAgentSettings({ ...agentSettingsForm });
    modal.value = null;
    await refreshAll();
  }, "智能体运行设置已更新");
}

function requestAgentRun() {
  confirmAction("立即运行智能分析", "确认使用最新统计数据包调用智能体？观察模式下只记录拟执行动作。", async () => {
    const result = await runAgent();
    trackAgentCommand(result);
    await refreshAgentExecution();
  });
}

async function sendAgentMessage() {
  const message = agentMessage.value.trim();
  if (!message) return;
  await runTask(async () => {
    const result = await chatAgent(message, agentConversationID.value);
    agentConversationID.value = result.conversation_id;
    trackAgentCommand(result);
    agentMessage.value = "";
    agentMessages.value = (await getAgentMessages(result.conversation_id)).items ?? [];
    await refreshAgentExecution();
  }, "命令已提交智能体任务队列");
}

function trackAgentCommand(result: AgentRun | AgentCommandReceipt) {
  const receipt = result as AgentCommandReceipt;
  trackedAgentGoalID.value = receipt.goal_id ?? 0;
  trackedAgentRunID.value = receipt.run_id ?? receipt.run?.id ?? ("id" in result ? result.id : 0);
  const status = receipt.status ?? receipt.run?.status ?? ("status" in result ? result.status : undefined) ?? "queued";
  trackedAgentStatus.value = status;
  if (!isTerminalAgentStatus(status) || trackedAgentGoalID.value > 0) scheduleAgentCommandPoll();
}

async function refreshAgentExecution() {
  const [overviewResult] = await Promise.allSettled([getAgentOverview(), refreshAgentControlRoom()]);
  if (overviewResult.status === "fulfilled" && overviewResult.value) {
    agentOverview.value = overviewResult.value;
    Object.assign(agentSettingsForm, overviewResult.value.settings);
  }
  if (agentConversationID.value > 0) {
    const messages = await getAgentMessages(agentConversationID.value).catch(() => null);
    if (messages) agentMessages.value = messages.items ?? [];
  }
  const trackedGoal = agentGoals.value.find((item) => item.id === trackedAgentGoalID.value);
  const trackedRun = agentOverview.value?.runs.find((item) => item.id === trackedAgentRunID.value);
  if (trackedGoal) trackedAgentStatus.value = trackedGoal.status;
  else if (trackedRun) trackedAgentStatus.value = trackedRun.status;
  if (trackedAgentPending.value) scheduleAgentCommandPoll();
  else {
    if (agentCommandTimer) window.clearTimeout(agentCommandTimer);
    agentCommandTimer = undefined;
  }
}

function scheduleAgentCommandPoll() {
  if (agentCommandTimer) window.clearTimeout(agentCommandTimer);
  agentCommandTimer = window.setTimeout(async () => {
    agentCommandTimer = undefined;
    await refreshAgentExecution();
  }, agentStreamState.value === "connected" ? 5000 : 1500);
}

function isTerminalAgentStatus(status?: string) {
  return ["completed", "failed", "cancelled", "canceled", "rejected", "interrupted", "expired"].includes((status ?? "").toLowerCase());
}

function rollbackAgentPolicy(id: number, label: string) {
  confirmAction("激活历史策略", `确认重新激活${label}？该版本将立即成为当前活动策略。`, async () => {
    await activateAgentPolicy(id);
  });
}

function assessmentReasons(value: string) {
  try { return (JSON.parse(value) as string[]).join("；") || "暂无补充原因"; }
  catch { return "暂无补充原因"; }
}

function availabilityLabel(state?: string) {
  return ({ available: "可用", degraded: "性能下降", unavailable: "不可用", insufficient_data: "数据不足" } as Record<string, string>)[state ?? ""] ?? "未知";
}

function agentRunKind(kind?: string) {
  return ({ scheduled: "定时分析", emergency: "紧急分析", manual: "手动分析", chat: "对话命令", daily: "每日总结" } as Record<string, string>)[kind ?? ""] ?? kind ?? "等待运行";
}

function requestAgentFreeze(mode: "agent_paused" | "writes_frozen") {
  const releasing = agentFreeze.value.mode === mode;
  const label = mode === "agent_paused" ? "智能体" : "全部自动化";
  const effect = mode === "agent_paused"
    ? "确定性调度仍会每 50 秒运行，智能体不会分析或执行新动作。"
    : "数据采集与只读分析继续，确定性调度和智能体都不能向 Sub2API 或上游写入。";
  const target: AgentFreezeMode = releasing ? "active" : mode;
  confirmAction(
    releasing ? `解除${label}冻结` : `冻结${label}`,
    releasing
      ? `确认解除${label}冻结？自动执行能力将立即恢复，请确认当前渠道和策略状态已经安全。`
      : `确认冻结${label}？${effect}`,
    async () => {
      await setAgentFreezeState(target, releasing ? `管理员解除${label}冻结` : `管理员冻结${label}`);
      await refreshAgentControlRoom();
    }
  );
}

function agentGoalStatusLabel(status?: string) {
  return ({ pending: "待规划", planned: "待规划", planning: "规划中", queued: "排队中", running: "执行中", waiting: "等待条件", checkpointed: "已保存检查点", blocked: "已阻塞", completed: "已完成", failed: "失败", cancelled: "已取消", canceled: "已取消", interrupted: "已中断", expired: "已过期" } as Record<string, string>)[status ?? ""] ?? status ?? "未知";
}

function agentStepStatusLabel(status?: string) {
  return ({ pending: "待执行", scheduled: "等待到期", queued: "排队中", leased: "已领取", running: "执行中", verifying: "回读确认中", reconciling: "结果核对中", compensating: "正在补偿", waiting: "等待条件", completed: "已完成", compensated: "已补偿", failed: "失败", skipped: "已跳过", blocked: "已阻塞", cancelled: "已取消", expired: "已过期" } as Record<string, string>)[status ?? ""] ?? status ?? "未知";
}

function agentTaskTime(task: AgentScheduledTask) {
  if (task.status === "running") return "正在执行";
  if (isTerminalAgentStatus(task.status)) return task.completed_at ? formatTime(task.completed_at) : agentStepStatusLabel(task.status);
  return formatTime(task.execute_at);
}

function runtimeEventText(event: AgentRuntimeEvent) {
  if (typeof event.payload === "string") return event.payload || event.type;
  const payload = event.payload ?? {};
  for (const key of ["message", "summary", "reason", "result", "error"]) {
    const value = payload[key];
    if (typeof value === "string" && value) return value;
  }
  const compact = JSON.stringify(payload);
  return compact === "{}" ? event.type : compact.slice(0, 180);
}

function capabilityTitle(name: string) {
  return agentCapabilities.value.find((item) => item.name === name)?.title || name;
}

function memoryScope(memory: AgentMemory) {
  if (memory.scope_type === "global") return "全局";
  return `${memory.scope_type} · ${memory.scope_id || "默认"}`;
}

async function rematchNow() {
  await triggerReconcile();
  await new Promise<void>((resolve) => window.setTimeout(resolve, 700));
}

async function signOut() {
  try { await logout(); } finally {
    authenticated.value = false;
    snapshot.value = null;
    agentEventStream?.close();
    agentEventStream = null;
    agentStreamState.value = "idle";
  }
}

function openPolicy(binding: Binding) {
  selected.value = binding;
  policyForm.monitor_id = binding.policy.monitor_id ? String(binding.policy.monitor_id) : "";
  policyForm.excluded = binding.policy.excluded;
  policyForm.enabled = binding.policy.enabled;
  policyForm.failure_threshold = binding.policy.failure_threshold ? String(binding.policy.failure_threshold) : "";
  policyForm.recovery_threshold = binding.policy.recovery_threshold ? String(binding.policy.recovery_threshold) : "";
  policyForm.flap_enabled = binding.policy.flap_enabled === undefined ? "inherit" : binding.policy.flap_enabled ? "enabled" : "disabled";
  policyForm.flap_window_minutes = binding.policy.flap_window_minutes ? String(binding.policy.flap_window_minutes) : "";
  policyForm.flap_pause_threshold = binding.policy.flap_pause_threshold ? String(binding.policy.flap_pause_threshold) : "";
  policyForm.flap_recovery_threshold = binding.policy.flap_recovery_threshold ? String(binding.policy.flap_recovery_threshold) : "";
  policyForm.healthy_score_threshold = binding.policy.healthy_score_threshold ? String(binding.policy.healthy_score_threshold) : "";
  policyForm.watch_score_threshold = binding.policy.watch_score_threshold ? String(binding.policy.watch_score_threshold) : "";
  policyForm.quarantine_score_threshold = binding.policy.quarantine_score_threshold ? String(binding.policy.quarantine_score_threshold) : "";
  policyForm.minimum_samples = binding.policy.minimum_samples ? String(binding.policy.minimum_samples) : "";
  policyForm.latency_warning_ms = binding.policy.latency_warning_ms ? String(binding.policy.latency_warning_ms) : "";
  policyForm.latency_critical_ms = binding.policy.latency_critical_ms ? String(binding.policy.latency_critical_ms) : "";
  policyForm.traffic_pause_below = binding.policy.traffic_pause_below ? String(binding.policy.traffic_pause_below) : "";
  policyForm.traffic_healthy_at = binding.policy.traffic_healthy_at ? String(binding.policy.traffic_healthy_at) : "";
  policyForm.hard_failures_10_threshold = binding.policy.hard_failures_10_threshold ? String(binding.policy.hard_failures_10_threshold) : "";
  policyForm.persistent_slow_rate = binding.policy.persistent_slow_rate ? String(binding.policy.persistent_slow_rate) : "";
  modal.value = "policy";
}

async function savePolicy() {
  if (!selected.value) return;
  await runTask(async () => {
    await updatePolicy(selected.value!.account.id, {
      monitor_id: policyForm.monitor_id ? Number(policyForm.monitor_id) : undefined,
      excluded: policyForm.excluded,
      enabled: policyForm.enabled,
      failure_threshold: policyForm.failure_threshold ? Number(policyForm.failure_threshold) : undefined,
      recovery_threshold: policyForm.recovery_threshold ? Number(policyForm.recovery_threshold) : undefined,
      flap_enabled: policyForm.flap_enabled === "inherit" ? undefined : policyForm.flap_enabled === "enabled",
      flap_window_minutes: policyForm.flap_window_minutes ? Number(policyForm.flap_window_minutes) : undefined,
      flap_pause_threshold: policyForm.flap_pause_threshold ? Number(policyForm.flap_pause_threshold) : undefined,
      flap_recovery_threshold: policyForm.flap_recovery_threshold ? Number(policyForm.flap_recovery_threshold) : undefined,
      healthy_score_threshold: policyForm.healthy_score_threshold ? Number(policyForm.healthy_score_threshold) : undefined,
      watch_score_threshold: policyForm.watch_score_threshold ? Number(policyForm.watch_score_threshold) : undefined,
      quarantine_score_threshold: policyForm.quarantine_score_threshold ? Number(policyForm.quarantine_score_threshold) : undefined,
      minimum_samples: policyForm.minimum_samples ? Number(policyForm.minimum_samples) : undefined,
      latency_warning_ms: policyForm.latency_warning_ms ? Number(policyForm.latency_warning_ms) : undefined,
      latency_critical_ms: policyForm.latency_critical_ms ? Number(policyForm.latency_critical_ms) : undefined,
      traffic_pause_below: policyForm.traffic_pause_below ? Number(policyForm.traffic_pause_below) : undefined,
      traffic_healthy_at: policyForm.traffic_healthy_at ? Number(policyForm.traffic_healthy_at) : undefined,
      hard_failures_10_threshold: policyForm.hard_failures_10_threshold ? Number(policyForm.hard_failures_10_threshold) : undefined,
      persistent_slow_rate: policyForm.persistent_slow_rate ? Number(policyForm.persistent_slow_rate) : undefined
    });
    modal.value = null;
    await refreshAll();
  }, "绑定规则已保存");
}

async function saveSettings() {
  await runTask(async () => {
    settingsForm.dry_run = settingsForm.health_engine_mode === "observe";
    await updateSettings({ ...settingsForm });
    modal.value = null;
    await refreshAll();
  }, "调度策略已更新");
}

function openUpstream(source?: UpstreamSource) {
  const configured = source && source.credential_configured && source.id > 0 ? source : null;
  const legacyKeyID = configured?.selected_key_id || "";
  const policies = configured?.failover_policies ?? [];
  const policy = policies.find((item) => item.key_id === legacyKeyID) ?? policies[0];
  const legacyKey = configured?.key_rates.find((item) => item.external_id === legacyKeyID);
  selectedUpstream.value = configured;
  upstreamAccountOptions.value = source?.matched_accounts ?? [];
  discoveredUpstream.value = Boolean(source && !configured);
  upstreamPreview.value = null;
  Object.assign(upstreamForm, source ? {
    name: source.name, provider: configured ? source.provider : "newapi", base_url: source.base_url,
    username: "", password: "", pause_below: source.pause_below || 5,
    resume_at: source.resume_at || 10, enabled: configured ? source.enabled : true,
    selected_key_id: source.selected_key_id || "", routing_enabled: source.routing_enabled || false, routing_pool: source.routing_pool || ""
  } : { name: "", provider: "newapi", base_url: "", username: "", password: "", pause_below: 5, resume_at: 10, enabled: true, selected_key_id: "", routing_enabled: false, routing_pool: "" });
  Object.assign(failoverForm, {
    enabled: policy?.enabled ?? Boolean(configured?.routing_enabled),
    key_id: policy?.key_id ?? legacyKeyID,
    main_group_id: policy?.main_group_id ?? legacyKey?.group_id ?? "",
    backup_group_id: policy?.backup_group_id ?? "",
    emergency_group_id: policy?.emergency_group_id ?? "",
    account_ids: [...(policy?.account_ids ?? source?.matched_accounts.map((account) => account.id) ?? [])],
    pool: policy?.pool ?? configured?.routing_pool ?? ""
  });
  modal.value = "upstream";
}

async function testUpstreamConnection() {
  if (selectedUpstream.value && !upstreamForm.password) {
    showToast("密码留空将保留服务器中的加密密码；无需重复测试即可保存");
    return;
  }
  if (!upstreamForm.username || !upstreamForm.password) {
    showToast("请填写上游登录账号和密码");
    return;
  }
  await runTask(async () => {
    upstreamPreview.value = await validateUpstream({ ...upstreamForm });
    if (!failoverForm.key_id && upstreamPreview.value.key_rates.length) {
      failoverForm.key_id = upstreamPreview.value.key_rates[0].external_id;
    }
    selectFailoverKey();
  }, "连接成功，已读取余额与倍率");
}

function saveUpstream() {
  const editing = selectedUpstream.value;
  if (!editing && (!upstreamForm.username || !upstreamForm.password)) {
    showToast("请填写上游登录账号和密码");
    return;
  }
  if (editing && usesLegacyCredential(editing) && (!upstreamForm.username || !upstreamForm.password)) {
    showToast("旧访问密钥配置需要填写账号和密码后迁移");
    return;
  }
  if (!selectedUpstream.value && !upstreamPreview.value) {
    showToast("请先测试连接，确认能够读取余额和倍率");
    return;
  }
  if (!validateFailoverForm()) return;
  const policyCopy = { ...failoverForm, account_ids: [...failoverForm.account_ids] };
  const policySummary = policyCopy.enabled
    ? buildFailoverConfirmationLines(policyCopy).join("\n")
    : "三级故障转移：未启用\n保存后余额规则将自动关联对应账号。";
  confirmAction(editing ? "保存并确认上游策略" : "添加并确认上游策略", policySummary, async () => {
    const saved = editing ? await updateUpstream(editing.id, { ...upstreamForm }) : await createUpstream({ ...upstreamForm });
    const existingPolicy = editing?.failover_policies?.find((item) => item.key_id === policyCopy.key_id);
    if (policyCopy.enabled || existingPolicy) {
      const policy = await saveUpstreamFailoverPolicy(saved.id, policyCopy);
      if (policyCopy.enabled) await confirmUpstreamFailoverPolicy(saved.id, policy.key_id, policy.version);
    }
  });
}

function buildFailoverConfirmationLines(policy: UpstreamFailoverPolicyInput) {
  if (!policy.enabled) return ["三级故障转移：未启用"];
  const key = upstreamFormRates.value.find((item) => item.external_id === policy.key_id);
  const accountNames = policy.account_ids.map((id) => {
    const account = upstreamAccountOptions.value.find((item) => item.id === id);
    return account ? `#${id} ${account.name}` : `#${id}`;
  });
  return [
    `受控令牌：${key?.name || "未命名令牌"}（${key?.key_hint || "已脱敏"}）`,
    `绑定账号：${accountNames.join("、") || "未选择"}`,
    `策略池：${policy.pool || "未填写"}`,
    `主用分组：${failoverGroupSummary(policy.main_group_id)}`,
    `备用分组：${failoverGroupSummary(policy.backup_group_id)}`,
    `应急分组：${failoverGroupSummary(policy.emergency_group_id)}`,
    "确认规则：修改受控令牌、绑定账号、任一分组或策略池都会使原确认失效；保存后将确认服务端生成的新版本。"
  ];
}

function failoverGroupSummary(groupID: string) {
  const group = upstreamFormGroups.value.find((item) => item.external_id === groupID);
  return group ? `${group.name}（${group.rate_multiplier.toFixed(2)} 倍）` : "未选择";
}

function selectFailoverKey() {
  const existing = selectedUpstream.value?.failover_policies?.find((item) => item.key_id === failoverForm.key_id);
  if (existing) {
    Object.assign(failoverForm, {
      enabled: existing.enabled, main_group_id: existing.main_group_id, backup_group_id: existing.backup_group_id,
      emergency_group_id: existing.emergency_group_id, account_ids: [...existing.account_ids], pool: existing.pool
    });
    return;
  }
  const key = upstreamFormRates.value.find((item) => item.external_id === failoverForm.key_id);
  failoverForm.main_group_id = key?.group_id ?? "";
  failoverForm.backup_group_id = "";
  failoverForm.emergency_group_id = "";
  failoverForm.account_ids = upstreamAccountOptions.value.map((account) => account.id);
}

function validateFailoverForm() {
  if (!failoverForm.enabled) return true;
  if (!failoverForm.key_id || !failoverForm.main_group_id || !failoverForm.backup_group_id || !failoverForm.emergency_group_id) {
    showToast("启用三级故障转移时必须选择受控令牌及主用、备用、应急分组");
    return false;
  }
  if (!failoverForm.pool.trim()) {
    showToast("启用三级故障转移时必须填写策略池名称");
    return false;
  }
  if (!failoverForm.account_ids.length) {
    showToast("受控令牌至少需要绑定一个关联账号");
    return false;
  }
  const groups = [failoverForm.main_group_id, failoverForm.backup_group_id, failoverForm.emergency_group_id];
  if (new Set(groups).size !== groups.length) {
    showToast("主用、备用、应急必须配置为三个不同分组");
    return false;
  }
  return true;
}

function transitionListKey(sourceID: number, keyID: string) {
  return `${sourceID}:${keyID}`;
}

async function toggleSourceExpansion(source: BalanceRow) {
  if (expandedSource.value === source.id) {
    expandedSource.value = null;
    return;
  }
  expandedSource.value = source.id;
  await Promise.all((source.failover_policies ?? []).map((policy) => loadFailoverTransitions(source.id, policy.key_id)));
}

async function loadFailoverTransitions(sourceID: number, keyID: string) {
  const listKey = transitionListKey(sourceID, keyID);
  if (failoverTransitionLoading[listKey]) return;
  failoverTransitionLoading[listKey] = true;
  failoverTransitionErrors[listKey] = "";
  try {
    const result = await getUpstreamFailoverTransitions(sourceID, keyID);
    failoverTransitions[listKey] = (result.items ?? []).slice(0, 5);
  } catch (error) {
    failoverTransitionErrors[listKey] = messageOf(error);
  } finally {
    failoverTransitionLoading[listKey] = false;
  }
}

function refreshBalance(source: UpstreamSource) {
	confirmAction("立即刷新余额", `确认立即连接「${source.name}」读取余额和密钥倍率？`, async () => {
		await refreshUpstream(source.id);
	});
}

function selectedGroup(source: UpstreamSource, keyID: string, currentGroup: string) {
  return groupSelections[`${source.id}:${keyID}`] ?? currentGroup;
}

function setSelectedGroup(source: UpstreamSource, keyID: string, value: string) {
  groupSelections[`${source.id}:${keyID}`] = value;
}

function changeKeyGroup(source: UpstreamSource, keyID: string, keyName: string, currentGroup: string) {
  const groupID = selectedGroup(source, keyID, currentGroup);
  const group = source.groups.find((item) => item.external_id === groupID);
  if (!groupID || groupID === currentGroup || !group) return;
  confirmAction("切换令牌分组", `确认将「${keyName || `密钥 #${keyID}`}」切换到「${group.name}（${group.rate_multiplier.toFixed(2)} 倍）」？系统会写入后重新读取确认。`, async () => {
    await switchUpstreamKeyGroup(source.id, keyID, groupID);
  });
}

function switchKeyTier(source: UpstreamSource, policy: GroupFailoverPolicy, tier: FailoverTier) {
  if (!policy.enabled || !policy.confirmed || policy.state?.frozen || !policy.key_id || policy.state?.current_tier === tier) return;
  const groupID = tier === "main" ? policy.main_group_id : tier === "backup" ? policy.backup_group_id : policy.emergency_group_id;
  const group = source.groups.find((item) => item.external_id === groupID);
  confirmAction("人工切换故障层级", `确认将受控令牌切换到${tierLabel(tier)}「${group?.name ?? groupID}」？该操作会覆盖当前自动层级。`, async () => {
    await switchUpstreamKeyTier(source.id, policy.key_id, tier);
    await loadFailoverTransitions(source.id, policy.key_id);
  });
}

function removeUpstream(source: UpstreamSource) {
  confirmAction("删除上游账户", `确认删除「${source.name}」？对应余额锁会解除，但其他控制锁仍然保留。`, async () => {
    await deleteUpstream(source.id);
  });
}

function confirmAction(title: string, message: string, action: () => Promise<void>) {
  confirmState.title = title;
  confirmState.message = message;
  confirmState.action = action;
  modal.value = "confirm";
}

async function executeConfirmed() {
  await runTask(async () => {
    await confirmState.action();
    modal.value = null;
    await refreshAll();
  }, "操作已完成");
}

async function runTask(task: () => Promise<void>, success: string) {
  loading.value = true;
  try { await task(); showToast(success); }
  catch (error) { showToast(messageOf(error)); }
  finally { loading.value = false; }
}

function healthStage(binding: Binding): "healthy" | "watch" | "degraded" | "quarantined" | "recovering" | "frozen" {
  const loadStage = binding.control.load_stage;
  if (loadStage === "recovering_25" || loadStage === "recovering_50" || loadStage === "recovering_80") return "recovering";
  if (binding.decision?.action === "pause") return "quarantined";
  if (binding.decision?.action?.startsWith("reduce_")) return "degraded";
  if (binding.decision?.action === "full") return "healthy";
  const stage = binding.health_state?.stage;
	if (stage === "recovering" || stage === "recovering_25" || stage === "recovering_50" || stage === "recovering_80") return "recovering";
  if (stage === "healthy" || stage === "watch" || stage === "degraded" || stage === "quarantined" || stage === "frozen") return stage;
  return ({ healthy: "healthy", degraded: "degraded", unhealthy: "quarantined", frozen: "frozen", unknown: "frozen" } as const)[binding.monitor_state.phase as "healthy" | "degraded" | "unhealthy" | "frozen" | "unknown"] ?? "frozen";
}

function phaseLabel(binding: Binding) {
  if (binding.state === "excluded") return "已排除";
  if (binding.state === "conflict") return "存在冲突";
  if (binding.state !== "bound") return "未匹配";
  return ({ healthy: "健康", watch: "观察", degraded: "性能下降", quarantined: "已隔离", recovering: "恢复试运行", frozen: "数据冻结" })[healthStage(binding)];
}

function phaseClass(binding: Binding) {
  if (binding.state !== "bound") return "neutral";
  return healthStage(binding);
}

function formatPercent(value?: number) {
  if (value === undefined || value === null) return "--";
  const percentage = value >= 0 && value <= 1 ? value * 100 : value;
  return `${percentage.toFixed(percentage >= 99.95 ? 0 : 1)}%`;
}

function formatLatency(value?: number) {
  if (value === undefined || value === null) return "--";
  return value >= 1000 ? `${(value / 1000).toFixed(1)} 秒` : `${Math.round(value)} 毫秒`;
}

function qualityScore(binding: Binding) {
  return binding.decision?.quality_score ?? binding.health_state?.score;
}

function averageDecisionMetric(key: "hard_success_rate_60" | "degraded_rate_60" | "traffic_success_rate") {
  const values = bindings.value.map((binding) => binding.decision?.[key]).filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  return values.length ? values.reduce((total, value) => total + value, 0) / values.length : undefined;
}

function metricOrEmpty(value?: number) {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function hardAvailabilityLabel(binding: Binding, window: 10 | 60) {
  return formatPercent(metricOrEmpty(window === 10 ? binding.decision?.hard_success_rate_10 : binding.decision?.hard_success_rate_60));
}

function degradedRateLabel(binding: Binding, window: 10 | 60) {
  return formatPercent(metricOrEmpty(window === 10 ? binding.decision?.degraded_rate_10 : binding.decision?.degraded_rate_60));
}

function disagreementLabel(binding: Binding) {
  if (binding.decision?.disagreement === undefined) return "等待真实流量";
  return binding.decision.disagreement ? "监控与业务不一致" : "监控与业务一致";
}

function capabilityWarnings(binding: Binding): string[] {
  const capabilities = binding.decision?.model_capabilities;
  if (!capabilities) return [];
  if (Array.isArray(capabilities)) {
    return capabilities.flatMap((item) => {
      if (typeof item === "string") return item ? [item] : [];
      const unhealthy = item.supported === false || (item.status !== undefined && !["supported", "available", "operational", "healthy"].includes(item.status.toLowerCase()));
      return unhealthy ? [`${item.model || "未知模型"}${item.reason ? `：${item.reason}` : "不可用"}`] : [];
    });
  }
  return Object.entries(capabilities).flatMap(([model, status]) => {
    const healthy = status === true || (typeof status === "string" && ["supported", "available", "operational", "healthy"].includes(status.toLowerCase()));
    return healthy ? [] : [`${model}：${typeof status === "string" ? status : "不可用"}`];
  });
}

function modelCapabilityLabel(binding: Binding) {
  if (binding.decision?.model_capabilities === undefined) return "等待能力数据";
  const warnings = capabilityWarnings(binding);
  return warnings.length ? `${warnings.length} 项模型告警` : "模型能力正常";
}

function reasonCodeLabel(code: string) {
  const labels: Record<string, string> = {
    credential_failure: "凭据故障", infrastructure_failure: "基础设施故障", capacity_pressure: "上游容量不足",
    semantic_mismatch: "语义校验异常", client_error_ignored: "已排除客户端错误", model_unsupported: "模型能力不足",
    latency_high: "响应时间偏高", latency_vs_baseline_high: "响应明显高于自身基线", degraded_rate_high: "黄色结果占比偏高",
    traffic_healthy_override: "真实流量健康，取消暂停", monitor_traffic_disagreement: "监控与真实业务不一致",
		hard_failure_threshold: "达到硬故障暂停门槛", manual_lock: "被人工锁阻止", frozen: "监控数据冻结",
		credential_failure_pause: "凭据失效，立即暂停", real_traffic_unhealthy: "真实请求成功率低于八成",
		consecutive_hard_failures: "连续三次基础设施故障", hard_failures_in_window: "近十次硬故障达到暂停门槛",
		real_traffic_healthy_override: "真实请求健康，压制监控暂停", semantic_mismatch_overridden: "语义异常已被真实请求纠正",
		semantic_mismatch_observe: "语义异常仅进入观察", latency_quality_penalty: "响应性能偏离正常范围",
		slow_without_latency: "性能下降但缺少响应时间", long_term_quality: "长期成功率仍需观察",
		recent_hard_failure: "近期出现基础设施故障", repeated_hard_failure: "近期基础设施故障反复出现",
		repeated_capacity_pressure: "近期容量不足反复出现", persistent_performance_slow: "黄色慢响应持续偏多", healthy: "硬可用性与性能正常"
  };
  return labels[code] ?? code.replaceAll("_", " ");
}

function decisionReasons(binding: Binding) {
  const reasons = binding.decision?.reason_codes;
  return reasons?.length ? reasons.map(reasonCodeLabel) : [healthReason(binding)];
}

function decisionActionLabel(binding: Binding) {
  const action = binding.decision?.action;
  const labels: Record<string, string> = {
    none: "保持现状", hold: "保持现状", reduce_load: "降低负载", adjust_load: "调整负载", pause: "暂停账号",
    resume: "恢复账号", recover: "分阶段恢复", increase_load: "提升负载", freeze: "冻结控制", warn: "仅告警",
		blocked: "被控制锁阻止", full: "保持满载", reduce_80: "降至八成负载", reduce_50: "降至半载",
		reduce_25: "降至四分之一负载"
  };
  if (action) return labels[action] ?? action;
  if (binding.decision?.suggested_load_percent !== undefined) return `建议负载 ${binding.decision.suggested_load_percent}%`;
  return "等待第三版判定";
}

function errorCategoryRows(binding: Binding) {
  const labels: Record<string, string> = {
    credential: "凭据故障", credential_failure: "凭据故障", infrastructure: "基础设施故障", infrastructure_failure: "基础设施故障",
    capacity: "容量不足", capacity_pressure: "容量不足", semantic: "语义校验异常", semantic_mismatch: "语义校验异常",
		client: "客户端错误", client_error: "客户端错误", model_capability: "模型能力不足", model_unsupported: "模型能力不足", unknown: "未分类",
		operational: "正常", performance_slow: "性能慢"
  };
  const counts = binding.decision?.error_category_counts;
  return counts ? Object.entries(counts).map(([key, count]) => ({ key, label: labels[key] ?? reasonCodeLabel(key), count })) : [];
}

function latencyRatio(binding: Binding) {
  const current = binding.decision?.response_p90_ms;
  const baseline = binding.decision?.baseline_latency_ms ?? binding.health_state?.baseline_latency_ms;
  return typeof current === "number" && typeof baseline === "number" && baseline > 0 ? `${(current / baseline).toFixed(2)} 倍` : "--";
}

function safeEventDetails(event: EventItem): Record<string, unknown> {
  if (event.details && typeof event.details === "object" && !Array.isArray(event.details)) return event.details;
  if (typeof event.details !== "string" || !event.details.trim()) return {};
  try {
    const value = JSON.parse(event.details);
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
  } catch { return {}; }
}

function eventKind(event: EventItem): "decision" | "proposed" | "actual" | "failed" | "blocked" | "other" {
  const type = event.type.toLowerCase();
  const details = safeEventDetails(event);
	if (/(failed|failure|write_error|query_failed)/.test(type)) return "failed";
  if (/(blocked|manual_lock|protected)/.test(type) || details.blocked === true || details.blocked_reason) return "blocked";
  if (/(would_|proposed|dry_run)/.test(type)) return "proposed";
  if (/(decision_snapshot|health_decision|assessment)/.test(type)) return "decision";
  if (/(pause|resume|load|recover|restore|override|lock|action)/.test(type)) return "actual";
  return "other";
}

function eventKindLabel(event: EventItem) {
  return ({ decision: "决策快照", proposed: "拟执行", actual: "实际动作", failed: "执行失败", blocked: "人工锁阻止", other: "系统记录" })[eventKind(event)];
}

function eventTypeLabel(type: string) {
  const labels: Record<string, string> = {
    decision_snapshot: "健康判定", health_decision_snapshot: "第三版健康判定",
    would_pause: "拟暂停账号", would_resume: "拟恢复账号", would_quarantine: "拟隔离渠道", would_reduce_load: "拟降低负载",
    automatic_pause: "自动暂停账号", automatic_resume: "自动恢复账号", account_action_failed: "账号操作失败",
    health_stage_changed: "健康阶段变化", health_recovery_started: "开始分级恢复",
    load_adjusted: "负载调整", load_factor_adjusted: "负载调整", load_factor_restored: "负载恢复",
    manual_load_override: "人工负载保护", manual_override_cleared: "解除人工保护",
    manual_pause: "人工暂停账号", manual_resume: "人工恢复账号", manual_resume_confirmed: "人工恢复确认",
    balance_low_detected: "检测到余额不足", balance_pause: "余额不足暂停", balance_recovered: "余额恢复", balance_resume: "余额恢复账号",
    balance_query_failed: "余额查询失败", balance_manual_override: "余额控制人工处理",
    cost_tier_disabled: "高倍率来源待命", cost_tier_enabled: "启用备用倍率来源", cost_pause: "倍率策略暂停", cost_resume: "倍率策略恢复", cost_manual_override: "倍率控制人工处理",
    upstream_key_group_changed: "令牌分组已切换", upstream_key_group_change_failed: "令牌分组切换失败",
    flap_protection_activated: "启用抖动保护", flap_protection_cleared: "解除抖动保护",
    binding_updated: "绑定关系更新", binding_conflict: "发现绑定冲突", binding_conflict_cleared: "绑定冲突解除",
    upstream_created: "新增上游账户", upstream_updated: "更新上游账户", upstream_deleted: "删除上游账户",
    telemetry_sync_failed: "遥测同步失败", telemetry_sync_recovered: "遥测同步恢复", sync_failed: "调度同步失败",
    settings_updated: "策略设置更新", action_blocked: "操作被保护锁阻止", health_action_failed: "健康操作失败"
  };
  return labels[type.toLowerCase()] ?? "其他记录";
}

function eventStateLabel(value?: string) {
  if (!value) return "-";
  const direct: Record<string, string> = {
    true: "允许调度", false: "暂停调度", enabled: "已启用", disabled: "已停用",
    healthy: "健康", watch: "观察", degraded: "性能下降", quarantined: "暂停隔离", frozen: "冻结控制",
    full: "满载运行", reduce_80: "八成负载", reduce_50: "半载运行", reduce_25: "四分之一负载",
    limited_80: "八成负载", limited_50: "半载运行", limited_25: "四分之一负载",
    recovering_25: "四分之一试运行", recovering_50: "半载试运行", recovering_80: "八成试运行",
    cost_locked: "倍率待命", schedulable: "允许调度"
  };
  if (direct[value]) return direct[value];
  const [stage, percent] = value.split(":", 2);
  if (percent && direct[stage]) return `${direct[stage]}（${percent}）`;
  return value;
}

function eventEvidence(event: EventItem) {
  const details = safeEventDetails(event);
  const reasons = Array.isArray(details.reason_codes) ? details.reason_codes.filter((item): item is string => typeof item === "string").map(reasonCodeLabel) : [];
  if (reasons.length) return reasons.join("；");
  if (typeof details.reason === "string") return reasonCodeLabel(details.reason);
  if (event.before_state || event.after_state) return `${eventStateLabel(event.before_state)} → ${eventStateLabel(event.after_state)}`;
  return "-";
}

function actorLabel(actor: string) {
  return ({ web: "管理端", system: "调度器", scheduler: "调度器", sub2api: "外部人工" } as Record<string, string>)[actor] ?? (actor || "系统");
}

function controlOwnerLabel(binding: Binding) {
  if (!binding.control.owns_pause) return binding.control.owns_load_factor ? "健康负载策略" : "无控制归属";
  const owner = binding.control.owner;
  if (owner === "operator") return "管理端";
  if (owner === "balance") return "余额策略";
  if (owner === "cost") return "倍率策略";
  if (owner === "combined") return "组合控制策略";
  return "健康策略";
}

function healthReason(binding: Binding) {
  const value = binding.health_state?.reason_json;
  if (!value) return binding.reason || "等待健康引擎生成判定原因";
  if (typeof value === "string") {
    try { return healthReasonValue(JSON.parse(value)); }
    catch { return value; }
  }
  return healthReasonValue(value);
}

function healthReasonValue(value: unknown): string {
  if (Array.isArray(value)) return value.map((item) => typeof item === "string" ? item : healthReasonValue(item)).filter(Boolean).join("；");
  if (value && typeof value === "object") return Object.values(value as Record<string, unknown>).map(healthReasonValue).filter(Boolean).join("；");
  return value === undefined || value === null ? "" : String(value);
}

function currentLoad(binding: Binding) {
  return binding.account.load_factor ?? binding.account.concurrency;
}

function loadStageLabel(stage?: string) {
  return ({
    healthy: "满载运行", full: "满载运行", restored: "原始负载", watch: "八成观察", watch_80: "八成观察", reduced_80: "八成观察",
    degraded: "半载运行", congested_50: "半载运行", recovering_25: "四分之一试运行", severe_25: "四分之一负载",
    limited_80: "八成观察", limited_50: "半载运行", limited_25: "四分之一负载",
    recovering_50: "半载试运行", recovering_80: "八成试运行", quarantined: "暂停隔离", frozen: "冻结控制"
  } as Record<string, string>)[stage ?? ""] ?? "未接管负载";
}

function engineModeLabel(mode?: string) {
  return ({ legacy: "旧判定", observe: "只观察", adaptive: "智能调度" } as Record<string, string>)[mode ?? ""] ?? "旧判定";
}

function formatTime(value?: string) {
  return value ? new Intl.DateTimeFormat("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(new Date(value)) : "-";
}
function formatBalance(source: UpstreamSource) {
  if (source.balance === undefined) return "--";
  const digits = source.unit === "TOKENS" ? 0 : 2;
  return `${new Intl.NumberFormat("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(source.balance)} ${source.unit}`;
}
function usesLegacyCredential(source: UpstreamSource) {
  if (source.credential_mode) return source.credential_mode === "access_key";
  return source.credential_configured && !source.username_hint && source.credential_hint?.includes("访问密钥");
}
function tierLabel(tier?: FailoverTier) {
  return ({ main: "主用层", backup: "备用层", emergency: "应急层" } as Record<string, string>)[tier ?? ""] ?? "等待判定";
}
function transitionStatusLabel(status: string) {
  return ({ pending: "等待回读", completed: "回读确认", failed: "执行失败", simulated: "观察记录" } as Record<string, string>)[status] ?? status;
}
function transitionReadback(item: GroupTierTransition) {
  if (item.error) return `错误：${item.error}`;
  if (item.status === "completed") return `回读已确认目标分组 ${item.to_group_id}`;
  if (item.status === "pending") return "已提交写入，等待上游回读确认";
  if (item.status === "simulated" || item.dry_run) return "观察模式，未向上游写入";
  return "未返回回读详情";
}
function primaryFailoverPolicy(source: UpstreamSource) {
  return source.failover_policies?.find((policy) => policy.enabled) ?? source.failover_policies?.[0];
}
function failoverStatusLabel(policy?: GroupFailoverPolicy) {
  if (!policy) return "未配置三级策略";
  if (!policy.enabled) return "三级策略已停用";
  if (!policy.confirmed || policy.confirmed_version !== policy.version) return `策略 v${policy.version} 待确认`;
  if (policy.state?.frozen) return "策略已冻结";
  if (policy.state?.last_error) return "层级切换异常";
  return `当前${tierLabel(policy.state?.current_tier)}`;
}
function failoverRuntimeDetail(policy: GroupFailoverPolicy) {
  if (policy.state?.freeze_reason) return policy.state.freeze_reason;
  if (policy.state?.last_error) return policy.state.last_error;
  if (policy.state?.manual_override_until) return `人工层级保持至 ${formatTime(policy.state.manual_override_until)}`;
  if (policy.state?.cooldown_until) return `切层冷却至 ${formatTime(policy.state.cooldown_until)}`;
  const expectedGroup = policy.state?.current_tier === "main" ? policy.main_group_id : policy.state?.current_tier === "backup" ? policy.backup_group_id : policy.state?.current_tier === "emergency" ? policy.emergency_group_id : "";
  if (policy.state?.observed_group_id && expectedGroup && policy.state.observed_group_id !== expectedGroup) return `观察分组 ${policy.state.observed_group_id}，等待确认 ${expectedGroup}`;
  return `${policy.key_hint || `令牌 #${policy.key_id}`} · 绑定 ${policy.account_ids.length} 个账号 · 策略池 ${policy.pool}`;
}
function upstreamState(source: UpstreamSource) {
	if (!source.credential_configured) return { label: "待配置", tone: "neutral" };
	if (!source.enabled) return { label: "规则关闭", tone: "neutral" };
  if (source.stale) return { label: "数据失联", tone: "unhealthy" };
  if (source.last_error) return { label: "读取异常", tone: "degraded" };
  if (source.balance_locked) return { label: "余额锁定", tone: "unhealthy" };
  return { label: "余额正常", tone: "healthy" };
}
function normalizeAccountURL(raw: string) {
  try { return new URL(raw).origin.toLowerCase(); }
  catch { return ""; }
}
function rateText(rate?: number, dynamic = false) { return dynamic || rate === undefined ? "动态" : `${rate.toFixed(2)}x`; }
function formatDuration(start?: string) {
  if (!start) return "-";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(start).getTime()) / 1000));
  if (seconds < 60) return `${seconds} 秒`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} 小时`;
  return `${Math.floor(seconds / 86400)} 天`;
}
function messageOf(error: unknown) { return error instanceof Error ? error.message : "操作失败"; }
function showToast(message: string) { toast.value = message; window.setTimeout(() => { if (toast.value === message) toast.value = ""; }, 3200); }
</script>

<template>
  <div v-if="booting" class="center-state"><LoaderCircle class="spin" :size="24" /> 正在连接调度服务</div>

  <main v-else-if="!authenticated" class="login-shell">
    <section class="login-panel">
      <div class="brand-mark"><ShieldCheck :size="24" /></div>
      <div>
        <p class="eyebrow">SUB2API CONTROL</p>
        <h1>账号调度中心</h1>
        <p class="login-copy">输入 Sub2API 全局管理员密钥以进入独立调度控制台。</p>
      </div>
      <form @submit.prevent="submitLogin" class="login-form">
        <label for="api-key">管理员密钥</label>
        <div class="secret-input"><LockKeyhole :size="18" /><input id="api-key" v-model="apiKey" type="password" autocomplete="off" autofocus required /></div>
        <p v-if="loginError" class="form-error">{{ loginError }}</p>
        <button class="primary-button" :disabled="loading"><LoaderCircle v-if="loading" class="spin" :size="17" /><ArrowRight v-else :size="17" />进入控制台</button>
      </form>
      <p class="security-note">密钥仅用于本次登录验证，不会保存在浏览器中。</p>
    </section>
  </main>

  <div v-else class="app-shell">
    <header class="topbar">
      <div class="brand-row"><div class="brand-mark small"><ServerCog :size="20" /></div><div><strong>账号调度中心</strong><span>Sub2API 独立控制器</span></div></div>
      <div class="header-actions">
        <span class="sync-state"><span :class="['dot', snapshot?.last_sync_error ? 'danger' : 'ok']"></span>{{ snapshot?.last_sync_error ? '同步异常' : '运行正常' }}</span>
        <button class="icon-button" title="重新读取并匹配" @click="confirmAction('重新读取并匹配', '确认立即读取最新账号和渠道监控并重新计算映射？', rematchNow)"><RefreshCw :class="{ spin: loading }" :size="18" /></button>
        <button class="icon-button" title="退出登录" @click="signOut"><LogOut :size="18" /></button>
      </div>
    </header>

    <nav class="tabs">
      <button :class="{ active: activeTab === 'overview' }" @click="activeTab = 'overview'"><Activity :size="17" />调度总览</button>
      <button :class="{ active: activeTab === 'agent' }" @click="activeTab = 'agent'"><BrainCircuit :size="17" />智能调度</button>
      <button :class="{ active: activeTab === 'balances' }" @click="activeTab = 'balances'"><WalletCards :size="17" />余额中心</button>
      <button :class="{ active: activeTab === 'events' }" @click="activeTab = 'events'"><Clock3 :size="17" />操作记录</button>
      <button :class="{ active: activeTab === 'diagnostics' }" @click="activeTab = 'diagnostics'"><Database :size="17" />诊断状态</button>
    </nav>

    <div v-if="effectiveEngineMode === 'observe' || snapshot?.settings.dry_run" class="mode-banner"><AlertTriangle :size="18" /><div><strong>当前处于只观察模式</strong><span>系统计算健康评分并记录拟执行动作，不会修改账号调度状态或负载。</span></div><button @click="modal = 'settings'">调整策略</button></div>
    <div v-if="snapshot?.last_sync_error" class="error-banner"><AlertTriangle :size="18" />{{ snapshot.last_sync_error }}</div>

    <section v-if="activeTab === 'overview'" class="workspace">
      <div class="summary-strip">
        <div><span class="summary-icon healthy"><Check :size="18" /></span><p>健康</p><strong>{{ counts.healthy }}</strong></div>
        <div><span class="summary-icon degraded"><AlertTriangle :size="18" /></span><p>性能下降</p><strong>{{ counts.degraded }}</strong></div>
        <div><span class="summary-icon quarantined"><Pause :size="18" /></span><p>已隔离</p><strong>{{ counts.quarantined }}</strong></div>
        <div><span class="summary-icon frozen"><CirclePause :size="18" /></span><p>数据冻结</p><strong>{{ counts.frozen }}</strong></div>
        <div><span class="summary-icon healthy"><ShieldCheck :size="18" /></span><p>硬可用率</p><strong>{{ formatPercent(averageHardSuccess) }}</strong></div>
        <div><span class="summary-icon degraded"><AlertTriangle :size="18" /></span><p>黄色率</p><strong>{{ formatPercent(averageDegradedRate) }}</strong></div>
        <div><span class="summary-icon score"><Activity :size="18" /></span><p>真实成功率</p><strong>{{ formatPercent(averageTrafficSuccess) }}</strong></div>
        <div><span class="summary-icon score"><Activity :size="18" /></span><p>平均质量分</p><strong>{{ averageHealthScore ?? '--' }}</strong></div>
        <div><span class="summary-icon paused"><ShieldAlert :size="18" /></span><p>存在硬故障</p><strong>{{ hardFailureAccounts }}</strong></div>
        <div><span class="summary-icon watch"><Unlink :size="18" /></span><p>判定不一致</p><strong>{{ disagreementCount }}</strong></div>
        <div><span class="summary-icon neutral"><AlertTriangle :size="18" /></span><p>模型能力告警</p><strong>{{ capabilityWarningCount }}</strong></div>
      </div>

      <div class="section-heading"><div><h2>账号与渠道映射</h2><p>当前模式：{{ engineModeLabel(effectiveEngineMode) }}。硬故障决定暂停，性能质量决定负载，真实流量负责纠偏。</p></div><button class="secondary-button" @click="modal = 'settings'"><Settings2 :size="16" />策略设置</button></div>
      <div class="table-wrap health-table">
        <table>
          <thead><tr><th>账号</th><th>上游与监控</th><th>状态与质量</th><th>硬可用性</th><th>黄色率</th><th>真实请求</th><th>负载阶段</th><th>风险校验</th><th>调度控制</th><th></th></tr></thead>
          <tbody>
            <tr v-for="binding in bindings" :key="binding.account.id">
              <td><div class="primary-cell"><strong>{{ binding.account.name }}</strong><span>#{{ binding.account.id }} · {{ binding.account.platform }}</span></div></td>
              <td><div class="endpoint-cell"><code>{{ binding.normalized_endpoint || '-' }}</code><div v-if="binding.monitor" class="primary-cell"><strong>{{ binding.monitor.name }}</strong><span>#{{ binding.monitor.id }} · {{ binding.source === 'manual' ? '显式绑定' : '自动匹配' }}</span></div><span v-else class="muted">{{ binding.reason }}</span></div></td>
              <td><div class="health-decision"><span :class="['status-chip', phaseClass(binding)]"><span></span>{{ phaseLabel(binding) }}</span><strong>{{ qualityScore(binding) ?? '--' }} 分</strong><small>质量评分 · {{ binding.decision?.checked_at ? formatTime(binding.decision.checked_at) : '等待第三版数据' }}</small><small v-if="!binding.decision && binding.health_state" class="legacy-health-data">置信度 {{ formatPercent(binding.health_state.confidence) }} · 15 分钟{{ formatPercent(binding.health_state.availability_15m) }} · 当前 {{ formatLatency(binding.health_state.current_latency_ms) }}</small><div class="streaks"><span class="bad">连续硬故障 {{ binding.decision?.hard_failure_streak ?? '--' }}</span><span>近十次 {{ binding.decision?.hard_failures_10 ?? '--' }}</span></div></div></td>
              <td><div class="metric-pair"><span>近 10 次<strong>{{ hardAvailabilityLabel(binding, 10) }}</strong></span><span>近 60 次<strong>{{ hardAvailabilityLabel(binding, 60) }}</strong></span><small>只统计真正不可用</small></div></td>
              <td><div class="metric-pair"><span>近 10 次<strong>{{ degradedRateLabel(binding, 10) }}</strong></span><span>近 60 次<strong>{{ degradedRateLabel(binding, 60) }}</strong></span><small>黄色只影响负载</small></div></td>
              <td><div class="metric-pair"><span>成功率<strong>{{ formatPercent(binding.decision?.traffic_success_rate) }}</strong></span><span>样本量<strong>{{ binding.decision?.traffic_sample_count ?? '--' }}</strong></span><small>{{ disagreementLabel(binding) }}</small></div></td>
              <td><div class="load-cell"><span><small>原始</small><strong>{{ binding.control.original_load_factor ?? binding.account.concurrency ?? '--' }}</strong></span><ArrowRight :size="13" /><span><small>当前</small><strong>{{ currentLoad(binding) ?? '--' }}</strong></span><small>{{ loadStageLabel(binding.control.load_stage) }}</small><em v-if="binding.decision?.suggested_load_percent !== undefined">建议 {{ binding.decision.suggested_load_percent }}%</em><em v-else-if="binding.control.load_override_until">人工保护至 {{ formatTime(binding.control.load_override_until) }}</em></div></td>
              <td><div class="risk-cell"><span :class="['risk-line', binding.decision?.disagreement ? 'warning' : 'neutral']">{{ disagreementLabel(binding) }}</span><span :class="['risk-line', capabilityWarnings(binding).length ? 'danger' : 'neutral']">{{ modelCapabilityLabel(binding) }}</span><small>{{ decisionActionLabel(binding) }}</small><small v-if="!binding.decision">{{ healthReason(binding) }}</small><small v-if="!binding.decision && binding.health_state?.next_recovery_condition">{{ binding.health_state.next_recovery_condition }}</small></div></td>
              <td><div class="control-cell"><span :class="['switch-state', binding.account.schedulable ? 'on' : 'off']">{{ binding.account.schedulable ? '参与调度' : '已暂停' }}</span><div class="primary-cell"><strong>{{ controlOwnerLabel(binding) }}</strong><span v-if="binding.control.manual_override_until">操作保护至 {{ formatTime(binding.control.manual_override_until) }}</span></div><div class="lock-chips"><span v-if="binding.control.health_locked">健康锁</span><span v-if="binding.control.balance_locked">余额锁</span><span v-if="binding.control.cost_locked">倍率待命 · {{ binding.control.cost_pool }}</span><span v-if="binding.control.flap_active">抖动保护</span></div></div></td>
              <td class="row-actions">
                <button class="icon-button compact" title="查看健康详情" @click="detailBinding = binding"><ChevronRight :size="16" /></button>
                <button class="icon-button compact" title="编辑绑定" @click="openPolicy(binding)"><Link2 :size="16" /></button>
                <button v-if="binding.account.schedulable" class="icon-button compact danger-hover" title="暂停调度" @click="confirmAction('暂停账号调度', `确认暂停账号「${binding.account.name}」接收新请求？`, async () => { await accountAction(binding.account.id, 'pause'); })"><Pause :size="16" /></button>
                <button v-else class="icon-button compact success-hover" :disabled="!binding.control.owns_pause" title="恢复调度" @click="confirmAction('恢复账号调度', `确认恢复账号「${binding.account.name}」接收新请求？`, async () => { await accountAction(binding.account.id, 'resume'); })"><Play :size="16" /></button>
                <button v-if="binding.control.manual_override_until" class="icon-button compact" title="解除人工保护" @click="confirmAction('解除人工保护', `确认立即解除账号「${binding.account.name}」的人工保护期？`, async () => { await accountAction(binding.account.id, 'clear-override'); })"><ShieldOff :size="16" /></button>
                <button v-if="binding.control.flap_active" class="icon-button compact flap-hover" title="解除抖动保护" @click="confirmAction('解除抖动保护', `确认仅解除账号「${binding.account.name}」当前这次抖动保护？未来再次达到条件时仍会重新触发。`, async () => { await accountAction(binding.account.id, 'clear-flap'); })"><ShieldAlert :size="16" /></button>
              </td>
            </tr>
            <tr v-if="bindings.length === 0"><td colspan="10" class="empty-row">尚未发现带上游地址的账号</td></tr>
          </tbody>
        </table>
      </div>

      <div v-if="unmatchedMonitors.length" class="unmatched-band"><div><Unlink :size="18" /><strong>未匹配监控</strong></div><span v-for="monitor in unmatchedMonitors" :key="monitor.id">#{{ monitor.id }} {{ monitor.name }}</span></div>
    </section>

    <section v-else-if="activeTab === 'agent'" class="workspace agent-workspace">
      <div class="agent-heading">
        <div><p class="eyebrow">INTELLIGENCE CONTROL</p><h2>智能调度控制室</h2><span>统计器整理全局摘要与变化增量，模型只分析压缩后的不可变数据包。</span></div>
        <div class="agent-heading-actions">
          <button :class="['freeze-button', { active: agentFreeze.mode === 'agent_paused' }]" :disabled="agentFreeze.mode === 'writes_frozen'" @click="requestAgentFreeze('agent_paused')"><CirclePause :size="16" />{{ agentFreeze.mode === 'agent_paused' ? '解除智能体冻结' : '冻结智能体' }}</button>
          <button :class="['freeze-button', 'critical', { active: agentFreeze.mode === 'writes_frozen' }]" @click="requestAgentFreeze('writes_frozen')"><Snowflake :size="16" />{{ agentFreeze.mode === 'writes_frozen' ? '解除自动化冻结' : '冻结全部自动化' }}</button>
          <button class="secondary-button" @click="openAgentProvider('primary')"><ServerCog :size="16" />主模型</button>
          <button class="secondary-button" @click="openAgentProvider('fallback')"><ShieldCheck :size="16" />备用模型</button>
          <button class="secondary-button" @click="Object.assign(agentSettingsForm, agentOverview?.settings ?? agentSettingsForm); modal = 'agent-settings'"><Settings2 :size="16" />运行设置</button>
          <button class="primary-button inline" :disabled="agentOverview?.running" @click="requestAgentRun"><Sparkles :size="16" />立即分析</button>
        </div>
      </div>

      <div class="agent-status-band">
        <div class="agent-orbit"><BrainCircuit :size="24" /></div>
        <div><span>当前状态</span><strong>{{ agentOverview?.running ? '正在分析' : agentOverview?.settings.enabled ? '等待下一轮' : '尚未启用' }}</strong></div>
        <div><span>控制模式</span><strong>{{ agentFreeze.mode === 'writes_frozen' ? '全部自动化已冻结' : agentFreeze.mode === 'agent_paused' ? '智能体已冻结' : agentOverview?.settings.mode === 'control' ? '完全控制' : '24小时观察' }}</strong></div>
        <div><span>观察进度</span><strong>{{ agentOverview?.settings.successful_observation_runs ?? 0 }} / 40</strong></div>
        <div><span>下次分析</span><strong>{{ agentOverview?.next_run_at ? formatTime(agentOverview.next_run_at) : '启用后计算' }}</strong></div>
        <div><span>上下文估算</span><strong>{{ latestAgentPacket?.token_estimate?.toLocaleString() ?? 0 }} 令牌</strong></div>
      </div>

      <div v-if="agentFreeze.mode !== 'active'" :class="['agent-freeze-banner', agentFreeze.mode]">
        <LockKeyhole :size="18" />
        <div><strong>{{ agentFreeze.mode === 'agent_paused' ? '智能体暂停，确定性调度继续' : '自动化写入已冻结' }}</strong><span>{{ agentFreeze.reason || (agentFreeze.mode === 'agent_paused' ? '50 秒确定性调度周期不受影响。' : '数据采集和只读分析继续，所有自动化写操作停止。') }}</span></div>
        <small>{{ agentFreeze.actor || 'system' }} · {{ formatTime(agentFreeze.updated_at) }}</small>
      </div>

      <section class="agent-observation-progress" aria-label="观察转正进度">
        <div><span>观察转正</span><strong>{{ observationProgress.percent }}%</strong><small>{{ observationProgress.timeLabel }} · {{ observationProgress.runs }} / 40 次有效分析</small></div>
        <div class="observation-track"><i :style="{ width: `${observationProgress.percent}%` }"></i></div>
        <div class="observation-checks"><span :class="{ done: observationProgress.timePercent >= 100 }">连续 24 小时</span><span :class="{ done: observationProgress.runPercent >= 100 }">40 次有效分析</span><span :class="{ done: observationProgress.actionRate >= 95 }">模拟可执行率 {{ observationProgress.actionRate.toFixed(1) }}%</span><span :class="{ done: observationProgress.clean }">零越权与结构错误</span><span :class="{ done: agentOverview?.settings.mode === 'control' }">正式控制</span></div>
      </section>

      <div class="agent-metrics">
        <div><span class="agent-metric-mark available"></span><p>可用</p><strong>{{ agentAvailabilityCounts.available }}</strong></div>
        <div><span class="agent-metric-mark degraded"></span><p>性能下降</p><strong>{{ agentAvailabilityCounts.degraded }}</strong></div>
        <div><span class="agent-metric-mark unavailable"></span><p>不可用</p><strong>{{ agentAvailabilityCounts.unavailable }}</strong></div>
        <div><span class="agent-metric-mark unknown"></span><p>数据不足</p><strong>{{ agentAvailabilityCounts.insufficient_data }}</strong></div>
        <div><span class="agent-metric-mark score"></span><p>平均可用分</p><strong>{{ latestAgentPacket?.system_summary.average_availability?.toFixed(1) ?? '--' }}</strong></div>
        <div><span class="agent-metric-mark confidence"></span><p>平均可信度</p><strong>{{ latestAgentPacket?.system_summary.average_confidence?.toFixed(1) ?? '--' }}%</strong></div>
      </div>

      <div class="agent-runtime-grid">
        <section class="agent-goal-panel">
          <div class="section-heading"><div><h2>当前目标与步骤</h2><p>目标 #{{ activeAgentGoal?.id ?? '--' }} · {{ activeAgentGoal?.source || '等待任务' }}</p></div><span :class="['agent-run-state', activeAgentGoal?.status]">{{ agentGoalStatusLabel(activeAgentGoal?.status) }}</span></div>
          <div v-if="activeAgentGoal" class="goal-summary">
            <div class="goal-mark"><Target :size="19" /></div><div><strong>{{ activeAgentGoal.title }}</strong><p>{{ activeAgentGoal.objective }}</p><small>优先级 {{ activeAgentGoal.priority }} · 风险 {{ activeAgentGoal.risk_level || '未标记' }}<template v-if="activeAgentGoal.deadline_at"> · 截止 {{ formatTime(activeAgentGoal.deadline_at) }}</template></small></div>
          </div>
          <div v-else class="agent-empty compact"><Target :size="20" /><span>暂无活动目标</span></div>
          <ol v-if="activeAgentSteps.length" class="goal-step-list">
            <li v-for="step in activeAgentSteps" :key="step.id" :class="[step.status, { current: activeAgentStep?.id === step.id }]">
              <span>{{ step.sequence }}</span><div><strong>{{ capabilityTitle(step.capability) }}</strong><small>{{ agentStepStatusLabel(step.status) }} · 尝试 {{ step.attempt_count }}/{{ step.max_attempts }}</small><p v-if="step.last_error">{{ step.last_error }}</p><p v-else-if="step.result">{{ step.result }}</p></div>
            </li>
          </ol>
        </section>

        <section class="agent-event-panel">
          <div class="section-heading"><div><h2>实时事件队列</h2><p>{{ agentStreamState === 'connected' ? '事件流已连接' : '轮询同步' }} · {{ agentRuntimeEvents.length }} 条</p></div><Activity :size="19" /></div>
          <div class="agent-event-queue">
            <article v-for="event in agentRuntimeEvents.slice(0, 9)" :key="event.id || event.event_key">
              <i :class="event.severity"></i><time>{{ formatTime(event.created_at) }}</time><div><strong>{{ event.type }}</strong><p>{{ runtimeEventText(event) }}</p></div><span v-if="event.goal_id">#{{ event.goal_id }}</span>
            </article>
            <div v-if="!agentRuntimeEvents.length" class="agent-empty compact"><Activity :size="18" /><span>事件队列为空</span></div>
          </div>
        </section>
      </div>

      <div class="agent-runtime-support">
        <section>
          <div class="section-heading"><div><h2>定时任务</h2><p>等待条件与重试进度</p></div><CalendarClock :size="18" /></div>
          <div class="runtime-compact-list"><article v-for="task in agentScheduledTasks.slice(0, 6)" :key="task.id"><span :class="['runtime-dot', task.status]"></span><div><strong>{{ capabilityTitle(task.capability) }}</strong><small>{{ agentTaskTime(task) }} · {{ task.attempt_count }}/{{ task.max_attempts }}</small></div><em>{{ agentStepStatusLabel(task.status) }}</em></article><div v-if="!agentScheduledTasks.length" class="agent-empty compact">暂无定时任务</div></div>
        </section>
        <section>
          <div class="section-heading"><div><h2>能力清单</h2><p>{{ agentCapabilities.length }} 项已注册能力</p></div><Wrench :size="18" /></div>
          <div class="capability-list"><div v-for="capability in agentCapabilities.slice(0, 12)" :key="`${capability.name}:${capability.version}`"><strong>{{ capability.title || capability.name }}</strong><span>{{ capability.auto_executable ? '可自动执行' : '需人工触发' }}</span><small>{{ capability.approval_required ? '需要审批' : capability.supports_compensation ? '支持补偿' : capability.risk_level }}</small></div><div v-if="!agentCapabilities.length" class="agent-empty compact">暂无已注册能力</div></div>
        </section>
        <section>
          <div class="section-heading"><div><h2>运行记忆</h2><p>高价值结论与人工约束</p></div><ListTodo :size="18" /></div>
          <div class="memory-list"><article v-for="memory in agentMemories.slice(0, 6)" :key="memory.id"><span>{{ memory.pinned ? '固定' : memory.kind }}</span><div><strong>{{ memory.summary || memory.key }}</strong><small>{{ memoryScope(memory) }} · 重要度 {{ memory.importance }}</small></div></article><div v-if="!agentMemories.length" class="agent-empty compact">暂无运行记忆</div></div>
        </section>
      </div>

      <section v-if="latestDailyReport" class="daily-report-band">
        <div class="daily-date"><span>系统日报</span><strong>{{ latestDailyReport.report_date }}</strong></div>
        <div class="daily-copy"><p>{{ latestDailyReport.summary }}</p><div v-if="latestDailyReport.advice?.length" class="daily-advice"><span v-for="item in latestDailyReport.advice" :key="item">{{ item }}</span></div></div>
      </section>

      <div class="agent-split">
        <section class="agent-analysis-panel">
          <div class="section-heading"><div><h2>最新分析</h2><p>{{ agentRunKind(latestAgentRun?.kind) }} · {{ latestAgentRun?.model || '等待配置模型' }}</p></div><span :class="['agent-run-state', latestAgentRun?.status]">{{ latestAgentRun?.status === 'completed' ? '已完成' : latestAgentRun?.status === 'failed' ? '失败' : latestAgentRun?.status === 'running' ? '分析中' : '等待' }}</span></div>
          <div v-if="latestAgentRun" class="agent-analysis-copy">
            <strong>{{ latestAgentRun.summary || latestAgentRun.error }}</strong>
            <p>{{ latestAgentRun.conclusion || '本轮没有补充结论。' }}</p>
            <div><span>可信度 {{ ((latestAgentRun.confidence ?? 0) * 100).toFixed(0) }}%</span><span>触发：{{ latestAgentRun.trigger }}</span><span>{{ formatTime(latestAgentRun.started_at) }}</span></div>
          </div>
          <div v-else class="agent-empty"><BrainCircuit :size="22" /><span>配置模型并启用后生成第一份分析结论</span></div>
          <div v-if="agentOverview?.tool_calls?.length" class="tool-call-list"><div v-for="call in agentOverview.tool_calls" :key="call.id"><span :class="['tool-status', call.status]">{{ call.status === 'completed' ? '已执行' : call.status === 'proposed' ? '拟执行' : call.status === 'failed' ? '失败' : call.status }}</span><strong>{{ call.tool }}</strong><small>{{ call.result }}</small></div></div>
        </section>

        <section class="agent-chat-panel">
          <div class="section-heading"><div><h2>智能体对话</h2><p>命令会使用最新数据包分析；正式控制模式下可直接执行。</p></div><MessageSquareText :size="20" /></div>
          <div class="chat-transcript">
            <div v-for="message in agentMessages" :key="message.id" :class="['chat-message', message.role]"><span>{{ message.role === 'user' ? '管理员' : '智能体' }}</span><p>{{ message.content }}</p></div>
            <div v-if="agentMessages.length === 0" class="agent-empty"><MessageSquareText :size="20" /><span>可以询问账号异常原因、要求调整负载或切换策略。</span></div>
          </div>
          <div v-if="trackedAgentGoalID || trackedAgentRunID" class="agent-command-receipt"><span :class="{ active: trackedAgentPending }"></span><strong>{{ trackedAgentPending ? '任务已进入队列' : '最近任务已结束' }}</strong><small><template v-if="trackedAgentGoalID">目标 #{{ trackedAgentGoalID }}</template><template v-if="trackedAgentRunID"> · 运行 #{{ trackedAgentRunID }}</template></small></div>
          <form class="agent-chat-form" @submit.prevent="sendAgentMessage"><textarea v-model="agentMessage" rows="3" maxlength="4000" placeholder="输入分析问题或调度命令"></textarea><button class="primary-button inline" :disabled="loading || !agentMessage.trim()"><ArrowRight :size="16" />发送</button></form>
        </section>
      </div>

      <div class="section-heading"><div><h2>账号可用性</h2><p>监控状态、真实流量和控制状态交叉验证；黄色成功请求不会被直接判为不可用。</p></div><span>数据包 #{{ latestAgentPacket?.id ?? '--' }}</span></div>
      <div class="table-wrap agent-availability-table">
        <table><thead><tr><th>账号</th><th>结论</th><th>可用性</th><th>性能</th><th>稳定性</th><th>容量</th><th>成本</th><th>可信度</th><th>证据</th></tr></thead>
          <tbody><tr v-for="item in agentOverview?.assessments ?? []" :key="item.account_id">
            <td><strong>#{{ item.account_id }}</strong></td><td><span :class="['availability-state', item.state]">{{ availabilityLabel(item.state) }}</span></td>
            <td><strong>{{ item.availability_score.toFixed(1) }}</strong></td><td>{{ item.performance_score.toFixed(1) }}</td><td>{{ item.stability_score.toFixed(1) }}</td><td>{{ item.capacity_score.toFixed(1) }}</td><td>{{ item.cost_score.toFixed(1) }}</td>
            <td>{{ (item.confidence * 100).toFixed(0) }}%</td><td><span v-if="item.evidence_conflict" class="evidence-conflict">证据冲突</span><small>{{ assessmentReasons(item.reasons_json) }}</small></td>
          </tr><tr v-if="!(agentOverview?.assessments?.length)"><td colspan="9" class="empty-row">尚未生成可用性分析数据包</td></tr></tbody>
        </table>
      </div>

      <div class="agent-bottom-grid">
        <section><div class="section-heading"><div><h2>数据包记录</h2><p>相同哈希表示核心状态没有重要变化。</p></div></div><div class="compact-list"><div v-for="packet in agentOverview?.packets?.slice(0, 8) ?? []" :key="packet.id"><span>#{{ packet.id }} · {{ agentRunKind(packet.kind) }}</span><strong>{{ packet.token_estimate.toLocaleString() }} 令牌</strong><small>{{ packet.no_material_change ? '无重要变化' : `${packet.changes?.length ?? 0} 项变化` }} · {{ formatTime(packet.created_at) }}</small></div><p v-if="!agentOverview?.packets?.length" class="agent-empty">暂无数据包</p></div></section>
        <section><div class="section-heading"><div><h2>策略版本</h2><p>全局、池和账号三级版本均可追溯。</p></div></div><div class="compact-list"><div v-for="policy in agentOverview?.policy_versions?.slice(0, 8) ?? []" :key="policy.id"><span>{{ policy.scope_type }} {{ policy.scope_id || '默认' }} · v{{ policy.version }}</span><strong>{{ policy.status === 'active' ? '当前活动' : '历史版本' }}</strong><small>{{ policy.reason || '未填写原因' }}</small><button v-if="policy.status !== 'active'" class="text-button" @click="rollbackAgentPolicy(policy.id, `${policy.scope_type} v${policy.version}`)">激活</button></div><p v-if="!agentOverview?.policy_versions?.length" class="agent-empty">暂无策略版本</p></div></section>
      </div>
    </section>

    <section v-else-if="activeTab === 'balances'" class="workspace balance-workspace">
      <div class="balance-heading">
        <div><p class="eyebrow">UPSTREAM FINANCE</p><h2>上游余额与故障转移</h2><span>账号密码加密保存；定时刷新余额与分组状态，并按主用、备用、应急三级策略切换受控令牌。</span></div>
        <button class="secondary-button" @click="openUpstream()"><Plus :size="16" />添加其他上游</button>
      </div>
      <div class="balance-summary">
		<div><span>已发现账号组</span><strong>{{ balanceRows.length }}</strong></div>
		<div><span>已配置</span><strong>{{ upstreams.length }}</strong></div>
		<div><span>余额正常</span><strong>{{ upstreams.filter(item => item.enabled && !item.balance_locked && !item.stale).length }}</strong></div>
		<div><span>余额锁定</span><strong>{{ upstreams.filter(item => item.balance_locked).length }}</strong></div>
		<div><span>待配置</span><strong>{{ balanceRows.filter(item => !item.credential_configured).length }}</strong></div>
      </div>
		<div class="section-heading"><div><h2>账号余额</h2><p>每个账户可绑定一个受控令牌，并为其配置三个互异分组；自动运行状态和人工切层都在账户下展开查看。</p></div><span class="last-balance-run">最近轮询 {{ formatTime(diagnostics?.balance_last_run_at) }}</span></div>
      <div class="table-wrap balance-table">
        <table>
          <thead><tr><th></th><th>站点</th><th>余额 / 密钥倍率</th><th>控制阈值</th><th>关联账号</th><th>状态与进度</th><th>最近成功</th><th></th></tr></thead>
          <tbody>
			<template v-for="source in balanceRows" :key="source.row_key">
			  <tr>
				<td><button v-if="source.credential_configured" class="expand-button" :title="expandedSource === source.id ? '收起密钥倍率' : '展开密钥倍率'" @click="toggleSourceExpansion(source)"><ChevronDown v-if="expandedSource === source.id" :size="16" /><ChevronRight v-else :size="16" /></button></td>
				<td><div class="source-name"><span :class="['provider-mark', source.discovered ? 'pending' : source.provider]">{{ source.discovered ? '?' : source.provider === 'newapi' ? 'N' : 'S2' }}</span><div><strong>{{ source.name }}</strong><small>{{ source.credential_hint }} · {{ source.normalized_url }}</small><small v-if="usesLegacyCredential(source)" class="migration-label">旧访问密钥待迁移为账号密码</small><small v-else-if="primaryFailoverPolicy(source)" class="routing-label">{{ source.failover_policies.length }} 枚受控令牌 · {{ failoverStatusLabel(primaryFailoverPolicy(source)) }}</small><small v-else-if="source.routing_enabled" class="migration-label">旧倍率策略待迁移</small></div></div></td>
				<td><div class="balance-value"><strong>{{ formatBalance(source) }}</strong><small v-if="!source.credential_configured">配置后显示余额与密钥倍率</small><small v-else-if="source.last_error">{{ source.last_error }}</small><div v-else class="key-rate-summary"><small>{{ source.key_rates.length }} 个密钥</small><button v-if="source.key_rates.length" @click="toggleSourceExpansion(source)">{{ expandedSource === source.id ? '收起倍率' : '查看倍率' }}</button></div></div></td>
				<td><div v-if="source.credential_configured" class="thresholds"><span>停用 &lt; {{ source.pause_below }} {{ source.unit }}</span><span>恢复 ≥ {{ source.resume_at }} {{ source.unit }}</span></div><span v-else class="muted">配置后启用</span></td>
				<td><div class="account-tags"><span v-for="account in source.matched_accounts" :key="account.id">#{{ account.id }} {{ account.name }}</span><em v-if="!source.matched_accounts.length">未匹配</em></div></td>
				<td><div class="balance-state"><span :class="['status-chip', upstreamState(source).tone]"><i></i>{{ upstreamState(source).label }}</span><small v-if="source.balance_locked">恢复 {{ source.recovery_streak }}/2</small><small v-else-if="source.credential_configured">低额 {{ source.low_streak }}/2</small><small v-else>等待账号密码</small><small v-if="primaryFailoverPolicy(source)">{{ failoverStatusLabel(primaryFailoverPolicy(source)) }}</small></div></td>
				<td><div v-if="source.credential_configured" class="primary-cell"><strong>{{ formatTime(source.last_success_at) }}</strong><span>{{ source.stale ? '超过 30 分钟未更新' : '数据有效' }}</span></div><span v-else class="muted">-</span></td>
				<td v-if="source.credential_configured" class="row-actions"><button class="icon-button compact" title="立即刷新" :disabled="!source.enabled" @click="refreshBalance(source)"><RefreshCw :size="15" /></button><button class="icon-button compact" title="编辑账户" @click="openUpstream(source)"><Pencil :size="15" /></button><button class="icon-button compact danger-hover" title="删除账户" @click="removeUpstream(source)"><Trash2 :size="15" /></button></td><td v-else class="row-actions"><button class="icon-button compact configure-button" title="配置账号密码" @click="openUpstream(source)"><KeyRound :size="15" /></button></td>
			  </tr>
			  <tr v-if="source.credential_configured && expandedSource === source.id" class="rate-row"><td></td><td colspan="7"><div class="rate-panel">
                <section v-for="policy in source.failover_policies ?? []" :key="policy.key_id" class="failover-policy-block">
                  <div class="failover-runtime"><div><span>三级故障转移 · v{{ policy.version }} {{ policy.confirmed ? '已确认' : '待确认' }}</span><strong>{{ policy.key_name || `令牌 #${policy.key_id}` }} · {{ failoverStatusLabel(policy) }}</strong><small>{{ failoverRuntimeDetail(policy) }}</small></div><div class="tier-actions"><button v-for="tier in failoverTiers" :key="tier" :class="['tier-button', tier, { active: policy.state?.current_tier === tier }]" :disabled="!policy.enabled || !policy.confirmed || policy.state?.frozen || policy.state?.current_tier === tier" @click="switchKeyTier(source, policy, tier)">{{ tierLabel(tier) }}</button></div></div>
                  <div class="transition-heading"><strong>最近切换流水</strong><button class="text-button" :disabled="failoverTransitionLoading[transitionListKey(source.id, policy.key_id)]" @click="loadFailoverTransitions(source.id, policy.key_id)">刷新</button></div>
                  <div v-if="failoverTransitionLoading[transitionListKey(source.id, policy.key_id)]" class="transition-empty">正在读取切换记录...</div>
                  <div v-else-if="failoverTransitionErrors[transitionListKey(source.id, policy.key_id)]" class="transition-empty error">{{ failoverTransitionErrors[transitionListKey(source.id, policy.key_id)] }}</div>
                  <div v-else-if="failoverTransitions[transitionListKey(source.id, policy.key_id)]?.length" class="transition-list">
                    <article v-for="item in failoverTransitions[transitionListKey(source.id, policy.key_id)]" :key="item.id">
                      <time>{{ formatTime(item.completed_at || item.created_at) }}</time>
                      <div><strong>{{ tierLabel(item.from_tier) }} → {{ tierLabel(item.to_tier) }}</strong><small>{{ item.trigger || item.reason || '未记录触发原因' }}</small></div>
                      <span :class="['transition-status', item.status]">{{ transitionStatusLabel(item.status) }}</span>
                      <p>{{ transitionReadback(item) }}</p>
                    </article>
                  </div>
                  <div v-else class="transition-empty">暂无切换流水</div>
                </section>
                <div v-if="source.routing_enabled && !source.failover_policies?.length" class="legacy-policy-warning"><AlertTriangle :size="16" />旧倍率路由尚未配置完整三级分组，请编辑账户完成迁移。</div>
                <div class="rate-panel-title"><KeyRound :size="16" /><strong>密钥实际分组倍率</strong><span>高级操作会直接修改令牌分组</span></div><div v-if="source.key_rates.length" class="rate-grid"><div v-for="rate in source.key_rates" :key="rate.external_id"><span><strong>{{ rate.name || `密钥 #${rate.external_id}` }}</strong><small>{{ rate.key_hint || '已脱敏' }} · 当前 {{ rate.group_name || '动态分组' }}</small></span><div class="rate-actions"><b :class="{ dynamic: rate.dynamic }">{{ rateText(rate.rate_multiplier, rate.dynamic) }}</b><select :value="selectedGroup(source, rate.external_id, rate.group_id)" :aria-label="`${rate.name || rate.external_id}目标分组`" @change="setSelectedGroup(source, rate.external_id, ($event.target as HTMLSelectElement).value)"><option value="">选择分组</option><option v-for="group in source.groups" :key="group.external_id" :value="group.external_id">{{ group.name }} · {{ group.rate_multiplier.toFixed(2) }} 倍</option></select><button class="secondary-button rate-change-button" :disabled="!selectedGroup(source, rate.external_id, rate.group_id) || selectedGroup(source, rate.external_id, rate.group_id) === rate.group_id" @click="changeKeyGroup(source, rate.external_id, rate.name, rate.group_id)">切换</button></div></div></div><p v-else class="empty-rates">该账户没有可展示的密钥</p>
              </div></td></tr>
			</template>
			<tr v-if="!balanceRows.length"><td colspan="8" class="empty-row">尚未发现带上游地址的账号</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-else-if="activeTab === 'events'" class="workspace">
      <div class="section-heading"><div><h2>操作记录</h2><p>每条新检测保存决策快照，并明确区分拟执行、实际动作、执行失败和被人工锁阻止。</p></div><button class="secondary-button" @click="refreshAll"><RefreshCw :size="16" />刷新</button></div>
      <div class="table-wrap event-table">
        <table><thead><tr><th>时间</th><th>记录类型</th><th>账号 / 监控</th><th>说明</th><th>判定证据</th><th>状态变化</th><th>来源</th></tr></thead>
          <tbody>
            <tr v-for="event in events" :key="event.id">
              <td>{{ formatTime(event.created_at) }}</td>
              <td><div class="event-type"><span :class="['event-kind', eventKind(event)]">{{ eventKindLabel(event) }}</span><code>{{ eventTypeLabel(event.type) }}</code></div></td>
              <td><div class="event-target"><span>{{ event.account_id ? `账号 #${event.account_id}` : '全局' }}</span><small>{{ event.monitor_id ? `监控 #${event.monitor_id}` : '无监控' }}</small></div></td>
              <td><div class="event-message"><strong>{{ event.message }}</strong><span :class="['severity', event.severity]">{{ event.severity === 'error' ? '错误' : event.severity === 'warning' ? '警告' : '信息' }}</span></div></td>
              <td><div class="event-evidence">{{ eventEvidence(event) }}</div></td>
              <td><div class="state-change"><span>{{ eventStateLabel(event.before_state) }}</span><ArrowRight :size="13" /><strong>{{ eventStateLabel(event.after_state) }}</strong></div></td>
              <td>{{ actorLabel(event.actor) }}</td>
            </tr>
            <tr v-if="events.length === 0"><td colspan="7" class="empty-row">暂无操作记录</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-else class="workspace diagnostics-workspace">
      <div class="section-heading"><div><h2>诊断状态</h2><p>调度进程、数据库和最近一次 Sub2API 读取状态。</p></div><button class="secondary-button" @click="refreshAll"><ListRestart :size="16" />刷新状态</button></div>
      <div class="diagnostic-grid">
        <article><span :class="['diagnostic-icon', diagnostics?.alive ? 'ok' : 'danger']"><ServerCog :size="19" /></span><div><p>服务存活</p><strong>{{ diagnostics?.alive ? '正常' : '异常' }}</strong><small>已运行 {{ formatDuration(diagnostics?.service_started_at) }}</small></div></article>
        <article><span :class="['diagnostic-icon', diagnostics?.ready ? 'ok' : 'warning']"><Activity :size="19" /></span><div><p>读取就绪</p><strong>{{ diagnostics?.ready ? '就绪' : '等待同步' }}</strong><small>{{ diagnostics?.last_sync_at ? formatTime(diagnostics.last_sync_at) : '尚无成功读取' }}</small></div></article>
        <article><span :class="['diagnostic-icon', diagnostics?.database === 'ok' ? 'ok' : 'danger']"><Database :size="19" /></span><div><p>SQLite 状态</p><strong>{{ diagnostics?.database === 'ok' ? '正常' : '异常' }}</strong><small>独立状态与审计数据库</small></div></article>
        <article><span class="diagnostic-icon neutral"><Clock3 :size="19" /></span><div><p>读取周期</p><strong>{{ diagnostics?.poll_interval_seconds ?? 50 }} 秒</strong><small>{{ diagnostics?.dry_run ? '观察模式' : '正式控制模式' }}</small></div></article>
      </div>
      <div v-if="diagnostics?.last_sync_error" class="diagnostic-error"><AlertTriangle :size="17" />{{ diagnostics.last_sync_error }}</div>
    </section>

    <div v-if="modal" class="modal-backdrop" @mousedown.self="modal = null">
      <section v-if="modal === 'policy' && selected" class="modal-panel">
        <header><div><h3>编辑账号规则</h3><p>{{ selected.account.name }} · #{{ selected.account.id }}</p></div><button class="icon-button" @click="modal = null"><X :size="18" /></button></header>
        <div class="form-grid">
          <label class="full">指定监控<select v-model="policyForm.monitor_id"><option value="">自动匹配</option><option v-for="monitor in monitors" :key="monitor.id" :value="String(monitor.id)">#{{ monitor.id }} {{ monitor.name }} · {{ monitor.endpoint }}</option></select></label>
          <label>异常阈值<input v-model="policyForm.failure_threshold" type="number" min="1" placeholder="使用全局值" /></label>
          <label>恢复阈值<input v-model="policyForm.recovery_threshold" type="number" min="1" placeholder="使用全局值" /></label>
          <label class="full">抖动保护<select v-model="policyForm.flap_enabled"><option value="inherit">继承全局（启用）</option><option value="enabled">为此账号启用</option><option value="disabled">为此账号关闭</option></select></label>
          <label>滚动窗口（分钟）<input v-model="policyForm.flap_window_minutes" type="number" min="1" placeholder="使用全局值" /></label>
          <label>窗口暂停次数<input v-model="policyForm.flap_pause_threshold" type="number" min="1" placeholder="使用全局值" /></label>
          <label class="full">保护恢复次数<input v-model="policyForm.flap_recovery_threshold" type="number" min="1" placeholder="使用全局值" /></label>
          <label>健康分数线<input v-model="policyForm.healthy_score_threshold" type="number" min="1" max="100" placeholder="继承全局" /></label>
          <label>观察分数线<input v-model="policyForm.watch_score_threshold" type="number" min="1" max="100" placeholder="继承全局" /></label>
          <label>隔离分数线<input v-model="policyForm.quarantine_score_threshold" type="number" min="1" max="100" placeholder="继承全局" /></label>
          <label>最低样本数<input v-model="policyForm.minimum_samples" type="number" min="1" placeholder="继承全局" /></label>
          <label>响应缓慢（毫秒）<input v-model="policyForm.latency_warning_ms" type="number" min="100" step="100" placeholder="继承全局" /></label>
          <label>严重缓慢（毫秒）<input v-model="policyForm.latency_critical_ms" type="number" min="100" step="100" placeholder="继承全局" /></label>
          <label>真实流量暂停线（%）<input v-model="policyForm.traffic_pause_below" type="number" min="1" max="99" placeholder="继承全局" /></label>
          <label>真实流量纠偏线（%）<input v-model="policyForm.traffic_healthy_at" type="number" min="2" max="100" placeholder="继承全局" /></label>
          <label>近十次硬故障上限<input v-model="policyForm.hard_failures_10_threshold" type="number" min="1" max="10" placeholder="继承全局" /></label>
          <label>黄色降载比例线（%）<input v-model="policyForm.persistent_slow_rate" type="number" min="1" max="100" placeholder="继承全局" /></label>
          <label class="toggle-row full"><input v-model="policyForm.enabled" type="checkbox" />启用该账号的调度规则</label>
          <label class="toggle-row full"><input v-model="policyForm.excluded" type="checkbox" />排除自动调度与自动匹配</label>
        </div>
        <footer><button class="secondary-button" @click="modal = null">取消</button><button class="primary-button inline" @click="savePolicy"><Save :size="16" />保存规则</button></footer>
      </section>

      <section v-else-if="modal === 'upstream'" class="modal-panel upstream-modal">
		<header><div><h3>{{ selectedUpstream ? '编辑上游账户' : discoveredUpstream ? '配置账号余额' : '添加上游账户' }}</h3><p>{{ discoveredUpstream ? '账号名称和地址已从 Sub2API 自动带入。' : '登录密码会加密保存在服务器，页面不会回显明文。' }}</p></div><button class="icon-button" @click="modal = null"><X :size="18" /></button></header>
		<div class="form-grid upstream-form">
		  <label>站点名称<input v-model.trim="upstreamForm.name" type="text" placeholder="例如：主力 New API" :readonly="discoveredUpstream" /></label>
		  <label>上游类型<select v-model="upstreamForm.provider"><option value="newapi">New API</option><option value="sub2">Sub2</option></select></label>
		  <label class="full">站点地址<input v-model.trim="upstreamForm.base_url" type="url" placeholder="https://api.example.com" :readonly="discoveredUpstream" /></label>
          <div v-if="selectedUpstream && usesLegacyCredential(selectedUpstream)" class="credential-migration full"><AlertTriangle :size="17" /><div><strong>旧访问密钥配置需要迁移</strong><span>填写可登录上游管理后台的账号和密码，保存后旧访问密钥将被替换。</span></div></div>
          <label>登录账号<input v-model.trim="upstreamForm.username" type="text" autocomplete="username" :placeholder="selectedUpstream?.username_hint ? `留空保留 ${selectedUpstream.username_hint}` : (upstreamForm.provider === 'sub2' ? '邮箱或用户名' : '用户名')" /></label>
          <label>登录密码<input v-model="upstreamForm.password" type="password" autocomplete="new-password" :placeholder="selectedUpstream && !usesLegacyCredential(selectedUpstream) ? '留空保留服务器中的加密密码' : '输入登录密码'" /></label>
          <label>停用阈值<input v-model.number="upstreamForm.pause_below" type="number" min="0" step="0.01" /></label>
          <label>恢复阈值<input v-model.number="upstreamForm.resume_at" type="number" min="0" step="0.01" /></label>
          <label class="toggle-row full"><input v-model="upstreamForm.enabled" type="checkbox" />启用余额读取与自动控制</label>
          <div v-if="selectedUpstream?.routing_enabled && !selectedUpstream.failover_policies?.length" class="legacy-policy-warning full"><AlertTriangle :size="16" />已读取旧倍率路由配置。请选择备用与应急分组，保存后迁移为三级故障转移。</div>
          <div class="failover-config full">
            <label class="toggle-row"><input v-model="failoverForm.enabled" type="checkbox" />启用三级故障转移</label>
            <div v-if="failoverForm.enabled" class="failover-grid">
              <label class="full">受控令牌<select v-model="failoverForm.key_id" @change="selectFailoverKey"><option value="">选择要绑定此账户的固定倍率令牌</option><option v-for="rate in upstreamFormRates" :key="rate.external_id" :value="rate.external_id" :disabled="rate.dynamic">{{ rate.name || `密钥 #${rate.external_id}` }} · 当前 {{ rate.group_name || '未分组' }}</option></select></label>
              <label class="full">策略池<input v-model.trim="failoverForm.pool" type="text" placeholder="例如：GPT 主线路" /></label>
              <label>主用分组<select v-model="failoverForm.main_group_id"><option value="">请选择</option><option v-for="group in upstreamFormGroups" :key="group.external_id" :value="group.external_id">{{ group.name }} · {{ group.rate_multiplier.toFixed(2) }} 倍</option></select></label>
              <label>备用分组<select v-model="failoverForm.backup_group_id"><option value="">请选择</option><option v-for="group in upstreamFormGroups" :key="group.external_id" :value="group.external_id">{{ group.name }} · {{ group.rate_multiplier.toFixed(2) }} 倍</option></select></label>
              <label class="full">应急分组<select v-model="failoverForm.emergency_group_id"><option value="">请选择</option><option v-for="group in upstreamFormGroups" :key="group.external_id" :value="group.external_id">{{ group.name }} · {{ group.rate_multiplier.toFixed(2) }} 倍</option></select></label>
              <div class="account-bindings full"><span>绑定关联账号</span><div v-if="upstreamAccountOptions.length" class="account-binding-list"><label v-for="account in upstreamAccountOptions" :key="account.id"><input v-model="failoverForm.account_ids" type="checkbox" :value="account.id" />#{{ account.id }} {{ account.name }}</label></div><small v-else>该上游尚未关联 Sub2API 账号，无法启用受控令牌策略。</small></div>
              <div class="failover-confirmation-summary full"><strong>保存前确认摘要</strong><span v-for="line in failoverConfirmationLines" :key="line">{{ line }}</span></div>
            </div>
          </div>
        </div>
		<div v-if="upstreamPreview" class="connection-preview">
		  <div class="preview-summary"><span class="preview-ok"><Check :size="16" />连接成功</span><strong>{{ upstreamPreview.balance.toFixed(upstreamPreview.unit === 'TOKENS' ? 0 : 2) }} {{ upstreamPreview.unit }}</strong><small>{{ upstreamPreview.key_rates.length }} 个密钥 · {{ upstreamPreview.username || '已验证' }}</small><small v-if="upstreamPreview.credential_warning" class="credential-warning">{{ upstreamPreview.credential_warning }}</small></div>
		  <div v-if="upstreamPreview.key_rates.length" class="preview-rates"><div v-for="rate in upstreamPreview.key_rates.slice(0, 12)" :key="rate.external_id"><span><b>{{ rate.name || `密钥 #${rate.external_id}` }}</b><small>{{ rate.key_hint || '已脱敏' }} · {{ rate.group_name || '未绑定分组' }}</small></span><strong :class="{ dynamic: rate.dynamic }">{{ rateText(rate.rate_multiplier, rate.dynamic) }}</strong></div><p v-if="upstreamPreview.key_rates.length > 12">另有 {{ upstreamPreview.key_rates.length - 12 }} 个密钥，保存后可展开查看</p></div>
		  <p v-else class="preview-empty">该账户暂未返回可展示的密钥</p>
		</div>
        <div v-else class="modal-warning"><AlertTriangle :size="17" />使用可登录上游管理后台的账号密码读取余额、令牌和分组。编辑既有账号时，密码留空会保留服务器中的加密密码。</div>
        <footer><button class="secondary-button" @click="testUpstreamConnection"><RefreshCw :size="16" />测试连接</button><span class="footer-spacer"></span><button class="secondary-button" @click="modal = null">取消</button><button class="primary-button inline" @click="saveUpstream"><Save :size="16" />保存账户</button></footer>
      </section>

      <section v-else-if="modal === 'settings'" class="modal-panel settings-modal">
        <header><div><h3>全局调度策略</h3><p>评分、隔离和分阶段恢复应用到所有未单独覆盖的账号。</p></div><button class="icon-button" @click="modal = null"><X :size="18" /></button></header>
        <div class="settings-groups">
          <fieldset><legend>运行方式</legend><div class="form-grid compact-grid">
            <label class="full">判定模式<select v-model="settingsForm.health_engine_mode"><option value="legacy">旧判定</option><option value="observe">只观察</option><option value="adaptive">智能调度</option></select></label>
          </div><p class="field-help">只观察会计算评分并记录拟执行动作；智能调度会自动降载、隔离和恢复。</p></fieldset>
          <fieldset><legend>评分与延迟</legend><div class="form-grid compact-grid">
            <label>健康分数线<input v-model.number="settingsForm.healthy_score_threshold" type="number" min="1" max="100" /></label>
            <label>观察分数线<input v-model.number="settingsForm.watch_score_threshold" type="number" min="1" max="100" /></label>
            <label>隔离分数线<input v-model.number="settingsForm.quarantine_score_threshold" type="number" min="0" max="100" /></label>
            <label>最低样本数<input v-model.number="settingsForm.minimum_samples" type="number" min="1" /></label>
            <label>响应缓慢（毫秒）<input v-model.number="settingsForm.latency_warning_ms" type="number" min="100" step="100" /></label>
            <label>严重缓慢（毫秒）<input v-model.number="settingsForm.latency_critical_ms" type="number" min="100" step="100" /></label>
            <label>真实流量暂停线（%）<input v-model.number="settingsForm.traffic_pause_below" type="number" min="1" max="99" /></label>
            <label>真实流量纠偏线（%）<input v-model.number="settingsForm.traffic_healthy_at" type="number" min="2" max="100" /></label>
            <label>近十次硬故障上限<input v-model.number="settingsForm.hard_failures_10_threshold" type="number" min="1" max="10" /></label>
            <label>黄色降载比例线（%）<input v-model.number="settingsForm.persistent_slow_rate" type="number" min="1" max="100" /></label>
          </div></fieldset>
          <fieldset><legend>隔离与恢复</legend><div class="form-grid compact-grid">
            <label>最低隔离分钟<input v-model.number="settingsForm.quarantine_minutes" type="number" min="1" /></label>
            <label>恢复观察样本<input v-model.number="settingsForm.recovery_window_size" type="number" min="1" /></label>
            <label>所需正常样本<input v-model.number="settingsForm.recovery_required_successes" type="number" min="1" /></label>
            <label>降载比例（%）<input v-model.number="settingsForm.degraded_load_percent" type="number" min="1" max="100" /></label>
            <label>首段试运行（%）<input v-model.number="settingsForm.recovery_initial_percent" type="number" min="1" max="100" /></label>
            <label>次段试运行（%）<input v-model.number="settingsForm.recovery_mid_percent" type="number" min="1" max="100" /></label>
            <label>每阶段分钟<input v-model.number="settingsForm.recovery_stage_minutes" type="number" min="1" /></label>
            <label>人工负载保护分钟<input v-model.number="settingsForm.load_manual_hold_minutes" type="number" min="1" /></label>
          </div></fieldset>
          <fieldset><legend>兼容与抖动保护</legend><div class="form-grid compact-grid">
            <label>连续异常次数<input v-model.number="settingsForm.failure_threshold" type="number" min="1" /></label>
            <label>连续恢复次数<input v-model.number="settingsForm.recovery_threshold" type="number" min="1" /></label>
            <label>人工操作保护分钟<input v-model.number="settingsForm.manual_hold_minutes" type="number" min="1" /></label>
            <label>抖动窗口分钟<input v-model.number="settingsForm.flap_window_minutes" type="number" min="1" /></label>
            <label>窗口暂停次数<input v-model.number="settingsForm.flap_pause_threshold" type="number" min="1" /></label>
            <label>抖动恢复次数<input v-model.number="settingsForm.flap_recovery_threshold" type="number" min="1" /></label>
          </div></fieldset>
        </div>
        <div class="modal-warning"><AlertTriangle :size="17" />选择智能调度后，系统会按健康评分实际修改账号负载和参与调度状态。</div>
        <footer><button class="secondary-button" @click="modal = null">取消</button><button class="primary-button inline" @click="confirmAction('保存全局策略', '确认应用新的全局调度策略？', saveSettings)"><Save :size="16" />保存策略</button></footer>
      </section>

      <section v-else-if="modal === 'agent-provider'" class="modal-panel agent-provider-modal">
        <header><div><h3>{{ agentProviderForm.slot === 'primary' ? '主模型配置' : '备用模型配置' }}</h3><p>兼容 OpenAI 对话补全接口；API 密钥使用独立主密钥加密保存。</p></div><button class="icon-button" @click="modal = null"><X :size="18" /></button></header>
        <div class="form-grid">
          <label class="full">接口地址<input v-model.trim="agentProviderForm.base_url" type="url" placeholder="https://api.example.com/v1" /></label>
          <label class="full">模型名称<input v-model.trim="agentProviderForm.model" type="text" placeholder="例如：gpt-5.4-mini" /></label>
          <label class="full">API 密钥<input v-model="agentProviderForm.api_key" type="password" autocomplete="new-password" :placeholder="selectedAgentProvider?.api_key_configured ? '留空保留服务器中的加密密钥' : '输入模型 API 密钥'" /></label>
          <label>请求超时（秒）<input v-model.number="agentProviderForm.timeout_seconds" type="number" min="10" max="300" /></label>
          <label>最大输出令牌<input v-model.number="agentProviderForm.max_output_tokens" type="number" min="512" max="32768" step="256" /></label>
          <label>分析温度<input v-model.number="agentProviderForm.temperature" type="number" min="0" max="1" step="0.1" /></label>
          <label class="toggle-row"><input v-model="agentProviderForm.enabled" type="checkbox" />启用这个模型</label>
        </div>
        <div :class="['connection-state', agentProviderValidated ? 'success' : 'neutral']"><Check v-if="agentProviderValidated" :size="17" /><AlertTriangle v-else :size="17" />{{ agentProviderValidated ? '连接、鉴权和结构化输出均已验证' : '保存前会调用一次模型验证结构化输出能力' }}</div>
        <footer><button class="secondary-button" @click="testAgentProvider"><RefreshCw :size="16" />测试模型</button><span class="footer-spacer"></span><button class="secondary-button" @click="modal = null">取消</button><button class="primary-button inline" @click="persistAgentProvider"><Save :size="16" />加密保存</button></footer>
      </section>

      <section v-else-if="modal === 'agent-settings'" class="modal-panel settings-modal">
        <header><div><h3>智能体运行设置</h3><p>控制统计包大小、运行周期、紧急唤醒和观察期。</p></div><button class="icon-button" @click="modal = null"><X :size="18" /></button></header>
        <div class="settings-groups">
          <fieldset><legend>运行状态</legend><div class="form-grid compact-grid">
            <label class="toggle-row full"><input v-model="agentSettingsForm.enabled" type="checkbox" />启用智能体定时分析</label>
            <label>有效模式<select v-model="agentSettingsForm.mode" disabled><option value="observe">24小时观察</option><option value="control">完全控制</option></select></label>
            <label>全量分析周期（分钟）<input v-model.number="agentSettingsForm.analysis_interval_minutes" type="number" min="5" /></label>
            <label>紧急分析冷却（分钟）<input v-model.number="agentSettingsForm.emergency_cooldown_minutes" type="number" min="1" /></label>
            <label>记录保留天数<input v-model.number="agentSettingsForm.retention_days" type="number" min="7" /></label>
          </div></fieldset>
          <fieldset><legend>分析数据包</legend><div class="form-grid compact-grid">
            <label>上下文令牌预算<input v-model.number="agentSettingsForm.context_token_budget" type="number" min="2000" step="1000" /></label>
            <label>展开异常账号数<input v-model.number="agentSettingsForm.max_anomalies" type="number" min="1" max="100" /></label>
            <label>单轮追查次数<input v-model.number="agentSettingsForm.max_drilldowns" type="number" min="0" max="20" /></label>
            <label>成功观察次数<input :value="agentSettingsForm.successful_observation_runs" type="number" readonly /></label>
          </div></fieldset>
        </div>
        <div class="modal-warning"><AlertTriangle :size="17" />模式由服务端控制：至少连续观察24小时并完成40次有效分析后才会自动进入完全控制；停用或更换模型会重新观察。</div>
        <footer><button class="secondary-button" @click="modal = null">取消</button><button class="primary-button inline" @click="confirmAction('保存智能体设置', '确认更新智能体运行模式和分析参数？', persistAgentSettings)"><Save :size="16" />保存设置</button></footer>
      </section>

      <section v-else-if="modal === 'confirm'" :class="['modal-panel', 'confirm-panel', { 'has-summary': confirmState.message.includes('\n') }]">
        <div class="confirm-icon"><AlertTriangle :size="24" /></div><h3>{{ confirmState.title }}</h3><p>{{ confirmState.message }}</p>
        <footer><button class="secondary-button" @click="modal = null">取消</button><button class="danger-button" @click="executeConfirmed">确认执行</button></footer>
      </section>
    </div>

    <div v-if="detailBinding" class="detail-backdrop" @mousedown.self="detailBinding = null">
      <aside class="detail-drawer" aria-label="健康判定详情">
        <header>
          <div><p>账号 #{{ detailBinding.account.id }}</p><h3>{{ detailBinding.account.name }}</h3><span>{{ detailBinding.monitor?.name || '未绑定监控' }} · {{ detailBinding.normalized_endpoint || '-' }}</span></div>
          <button class="icon-button" title="关闭详情" @click="detailBinding = null"><X :size="18" /></button>
        </header>
        <div class="detail-status-band">
          <span :class="['status-chip', phaseClass(detailBinding)]"><span></span>{{ phaseLabel(detailBinding) }}</span>
          <div><small>质量分</small><strong>{{ qualityScore(detailBinding) ?? '--' }}</strong></div>
          <div><small>建议动作</small><strong>{{ decisionActionLabel(detailBinding) }}</strong></div>
          <div><small>建议负载</small><strong>{{ detailBinding.decision?.suggested_load_percent !== undefined ? `${detailBinding.decision.suggested_load_percent}%` : '--' }}</strong></div>
        </div>

        <section class="detail-section">
          <div class="detail-section-title"><h4>最近窗口证据</h4><span>{{ detailBinding.decision?.checked_at ? formatTime(detailBinding.decision.checked_at) : '等待第三版数据' }}</span></div>
          <div class="detail-table-wrap"><table class="detail-metric-table"><thead><tr><th>指标</th><th>近 10 次</th><th>近 60 次</th></tr></thead><tbody>
            <tr><td>硬成功率</td><td>{{ hardAvailabilityLabel(detailBinding, 10) }}</td><td>{{ hardAvailabilityLabel(detailBinding, 60) }}</td></tr>
            <tr><td>黄色率</td><td>{{ degradedRateLabel(detailBinding, 10) }}</td><td>{{ degradedRateLabel(detailBinding, 60) }}</td></tr>
            <tr><td>硬故障</td><td>{{ detailBinding.decision?.hard_failures_10 ?? '--' }}</td><td>连续 {{ detailBinding.decision?.hard_failure_streak ?? '--' }}</td></tr>
            <tr><td>真实请求</td><td colspan="2">{{ formatPercent(detailBinding.decision?.traffic_success_rate) }} · {{ detailBinding.decision?.traffic_sample_count ?? '--' }} 个样本</td></tr>
          </tbody></table></div>
        </section>

        <section class="detail-section">
          <div class="detail-section-title"><h4>响应性能</h4><span>绝对响应与自身基线只取较严重项</span></div>
          <dl class="detail-definition-list">
            <div><dt>九成响应</dt><dd>{{ formatLatency(detailBinding.decision?.response_p90_ms) }}</dd></div>
            <div><dt>自身基线</dt><dd>{{ formatLatency(detailBinding.decision?.baseline_latency_ms ?? detailBinding.health_state?.baseline_latency_ms) }}</dd></div>
            <div><dt>基线倍率</dt><dd>{{ latencyRatio(detailBinding) }}</dd></div>
          </dl>
        </section>

        <section class="detail-section">
          <div class="detail-section-title"><h4>错误分类</h4><span>客户端错误不参与暂停</span></div>
          <div v-if="errorCategoryRows(detailBinding).length" class="error-count-list"><div v-for="item in errorCategoryRows(detailBinding)" :key="item.key"><span>{{ item.label }}</span><strong>{{ item.count }}</strong></div></div>
          <p v-else class="detail-empty">尚无第三版错误分类数据</p>
        </section>

        <section class="detail-section">
          <div class="detail-section-title"><h4>交叉校验</h4><span>真实流量用于纠正监控误判</span></div>
          <div class="cross-check-lines">
            <div><span>监控与业务</span><strong :class="{ warning: detailBinding.decision?.disagreement }">{{ disagreementLabel(detailBinding) }}</strong></div>
            <div><span>模型能力</span><strong :class="{ danger: capabilityWarnings(detailBinding).length }">{{ modelCapabilityLabel(detailBinding) }}</strong></div>
          </div>
          <ul v-if="capabilityWarnings(detailBinding).length" class="capability-list"><li v-for="warning in capabilityWarnings(detailBinding)" :key="warning">{{ warning }}</li></ul>
        </section>

        <section class="detail-section decision-section">
          <div class="detail-section-title"><h4>决策原因</h4><span>{{ loadStageLabel(detailBinding.control.load_stage) }}</span></div>
          <ol class="decision-reasons"><li v-for="reason in decisionReasons(detailBinding)" :key="reason">{{ reason }}</li></ol>
          <div class="suggested-action"><span>建议动作</span><strong>{{ decisionActionLabel(detailBinding) }}</strong><small>{{ detailBinding.health_state?.next_recovery_condition || '当前没有待完成的恢复条件' }}</small></div>
        </section>
      </aside>
    </div>

    <div v-if="toast" class="toast">{{ toast }}</div>
  </div>
</template>
