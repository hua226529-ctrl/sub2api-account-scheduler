import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App.vue";

const api = vi.hoisted(() => ({
  restoreSession: vi.fn(), login: vi.fn(), logout: vi.fn(), getOverview: vi.fn(), getEvents: vi.fn(), getDiagnostics: vi.fn(),
  getUpstreams: vi.fn(), getUpstreamFailoverTransitions: vi.fn(), validateUpstream: vi.fn(), createUpstream: vi.fn(), updateUpstream: vi.fn(), deleteUpstream: vi.fn(), refreshUpstream: vi.fn(),
	  saveUpstreamFailoverPolicy: vi.fn(), confirmUpstreamFailoverPolicy: vi.fn(), switchUpstreamKeyTier: vi.fn(),
  triggerReconcile: vi.fn(), updatePolicy: vi.fn(), updateSettings: vi.fn(), accountAction: vi.fn(),
  getAgentOverview: vi.fn(), updateAgentSettings: vi.fn(), validateAgentProvider: vi.fn(), saveAgentProvider: vi.fn(),
	  runAgent: vi.fn(), chatAgent: vi.fn(), confirmAgentGoal: vi.fn(), getAgentMessages: vi.fn(), activateAgentPolicy: vi.fn(), rejectAgentPolicy: vi.fn(), rollbackAgentPolicy: vi.fn(),
  getAgentCapabilities: vi.fn(), getAgentGoals: vi.fn(), getAgentRuntimeEvents: vi.fn(), getAgentTasks: vi.fn(),
  getAgentMemories: vi.fn(), getAgentFreezeState: vi.fn(), setAgentFreezeState: vi.fn(), openAgentStream: vi.fn()
}));

vi.mock("./api", () => api);

const adaptiveSettings = {
	  dry_run: false, scheduler_mode: "control" as const, failover_mode: "observe" as const, group_failover_mutation_budget: 1,
	  failure_threshold: 3, recovery_threshold: 3, manual_hold_minutes: 10,
  flap_window_minutes: 60, flap_pause_threshold: 3, flap_recovery_threshold: 10,
  health_engine_mode: "adaptive" as const, healthy_score_threshold: 80, watch_score_threshold: 60,
  quarantine_score_threshold: 35, latency_warning_ms: 8000, latency_critical_ms: 15000,
  minimum_samples: 10, quarantine_minutes: 5, recovery_window_size: 10,
  recovery_required_successes: 8, degraded_load_percent: 50, recovery_initial_percent: 25,
  recovery_mid_percent: 50, recovery_stage_minutes: 5, load_manual_hold_minutes: 30
};

const agentSettings = {
	  enabled: true, mode: "observe" as const, optimizer_mode: "propose" as const, operator_mode: "confirm" as const, daily_policy_change_budget: 2,
	  analysis_interval_minutes: 30, emergency_cooldown_minutes: 5,
  context_token_budget: 16000, max_anomalies: 20, max_drilldowns: 8, retention_days: 90,
  successful_observation_runs: 17, observation_started_at: "2026-07-13T00:00:00Z"
};

function mockAuthenticatedConsole(agentOverview: Record<string, unknown>) {
  api.restoreSession.mockResolvedValue(true);
  api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: "2026-07-14T00:00:00Z" });
  api.getEvents.mockResolvedValue({ items: [] });
  api.getUpstreams.mockResolvedValue({ items: [] });
  api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: "2026-07-14T00:00:00Z", poll_interval_seconds: 50, dry_run: false });
  api.getAgentOverview.mockResolvedValue(agentOverview);
}

function populatedAgentOverview() {
  return {
    settings: agentSettings,
    providers: [{ slot: "primary", base_url: "https://model.example/v1", model: "analysis-model", api_key_configured: true, enabled: true, timeout_seconds: 90, max_output_tokens: 4096, temperature: 0.1 }],
    runs: [{ id: 41, kind: "scheduled", trigger: "周期分析", status: "completed", provider_slot: "primary", model: "analysis-model", packet_id: 81, summary: "整体可用，示例账号上游性能下降", conclusion: "维持当前负载并继续观察真实流量。", confidence: 0.91, started_at: "2026-07-14T10:00:00Z", completed_at: "2026-07-14T10:00:05Z" }],
    assessments: [
      { id: 1, packet_id: 81, account_id: 225, state: "available", availability_score: 98, performance_score: 91, stability_score: 96, capacity_score: 88, cost_score: 82, confidence: 0.94, evidence_conflict: false, reasons_json: '["真实请求持续成功"]', created_at: "2026-07-14T10:00:00Z" },
      { id: 2, packet_id: 81, account_id: 298, state: "degraded", availability_score: 91, performance_score: 63, stability_score: 84, capacity_score: 77, cost_score: 69, confidence: 0.82, evidence_conflict: true, reasons_json: '["监控黄色但真实请求成功"]', created_at: "2026-07-14T10:00:00Z" }
    ],
    daily_reports: [{ id: 7, report_date: "2026-07-13", status: "completed", summary: "昨日可用率稳定，成本下降。", metrics: {}, advice: ["继续观察高倍率备用池"], created_at: "2026-07-14T00:10:00Z" }],
    policy_versions: [
		  { id: 12, scope_type: "global", scope_id: "", version: 3, status: "active", config: {}, previous_active_version_id: 10, risk_level: "low", reason: "降低单次性能下降影响", created_by: "agent", created_at: "2026-07-14T09:30:00Z" },
		  { id: 11, scope_type: "global", scope_id: "", version: 4, status: "simulated", config: {}, diff: { failure_threshold: { before: 3, after: 4 } }, simulation: { passed: true, data_sufficient: true, sample_count: 120, summary: "动作次数未增加" }, risk_level: "low", affected_account_ids: [225], reason: "减少抖动", created_by: "agent", created_at: "2026-07-14T09:40:00Z" }
    ],
    packets: [{ id: 81, kind: "scheduled", cutoff_at: "2026-07-14T10:00:00Z", hash: "packet-hash", token_estimate: 4280, no_material_change: false, system_summary: { accounts: 2, schedulable: 2, available: 1, degraded: 1, unavailable: 0, insufficient_data: 0, average_availability: 94.5, average_performance: 77, average_confidence: 88, critical_anomalies: 1, data_fresh: true }, changes: ["账号298性能下降"], created_at: "2026-07-14T10:00:00Z" }],
    tool_calls: [{ id: 6, run_id: 41, tool: "set_load_factor", arguments: { account_id: 298, value: 50 }, status: "proposed", result: "观察模式，仅记录拟执行动作", created_at: "2026-07-14T10:00:04Z" }],
    running: false,
    next_run_at: "2026-07-14T10:30:00Z"
  };
}

describe("App", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    Object.values(api).forEach((mock) => mock.mockReset());
    api.getAgentOverview.mockResolvedValue({
		  settings: { enabled: false, mode: "observe", optimizer_mode: "disabled", operator_mode: "disabled", daily_policy_change_budget: 2, analysis_interval_minutes: 30, emergency_cooldown_minutes: 5, context_token_budget: 16000, max_anomalies: 20, max_drilldowns: 8, retention_days: 90, successful_observation_runs: 0 },
      providers: [], runs: [], assessments: [], daily_reports: [], policy_versions: [], packets: [], tool_calls: [], running: false
    });
    api.getAgentMessages.mockResolvedValue({ items: [] });
    api.getAgentCapabilities.mockResolvedValue({ items: [] });
    api.getAgentGoals.mockResolvedValue({ items: [], steps: [] });
    api.getAgentRuntimeEvents.mockResolvedValue({ items: [] });
    api.getAgentTasks.mockResolvedValue({ items: [] });
    api.getAgentMemories.mockResolvedValue({ items: [] });
    api.getAgentFreezeState.mockResolvedValue({ scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "system" });
    api.setAgentFreezeState.mockResolvedValue({ scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "operator" });
    api.openAgentStream.mockReturnValue(null);
    api.getUpstreamFailoverTransitions.mockResolvedValue({ items: [] });
  });

  it("requires the administrator key when there is no session", async () => {
    api.restoreSession.mockResolvedValue(false);
    const wrapper = mount(App);
    await flushPromises();
    expect(wrapper.text()).toContain("账号调度中心");
    expect(wrapper.text()).toContain("管理员密钥");
    wrapper.unmount();
  });

  it("renders resolved bindings and observation mode", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: true });
	api.validateUpstream.mockResolvedValue({ balance: 25, unit: "USD", username: "owner", fetched_at: new Date().toISOString(), groups: [{ external_id: "vip", name: "vip", rate_multiplier: 0.8 }], key_rates: [{ external_id: "1", name: "vip-key", key_hint: "sk-1...7890", group_id: "vip", group_name: "vip", rate_multiplier: 0.8, dynamic: false, status: "active" }] });
    api.getOverview.mockResolvedValue({
      bindings: [{
        account: { id: 225, name: "上游账号", platform: "openai", type: "apikey", status: "active", schedulable: true, error_message: "", credentials: { base_url: "https://example.com/v1" }, load_factor: 5, concurrency: 10 },
        monitor: { id: 2, name: "渠道监控", provider: "openai", endpoint: "https://example.com", primary_model: "gpt", enabled: true, interval_seconds: 60, primary_status: "operational" },
        policy: { account_id: 225, excluded: false, enabled: true }, source: "auto", state: "bound", reason: "", normalized_endpoint: "https://example.com",
        monitor_state: { monitor_id: 2, last_status: "operational", healthy_streak: 3, unhealthy_streak: 0, phase: "healthy" },
        health_state: { stage: "recovering_50", score: 76, confidence: 0.92, current_latency_ms: 1250, baseline_latency_ms: 800, availability_15m: 0.9, availability_1h: 0.95, availability_24h: 0.975, sample_count: 18, recovery_healthy_count: 7, last_two_healthy: true, recovery_eligible: false, reason_json: ["最近十五分钟可用率偏低", "响应时间超过基准"], next_recovery_condition: "再获得 1 次正常结果后恢复原始负载" },
        control: { account_id: 225, owns_pause: false, owner: "", last_decision: "", flap_active: true, flap_recovery_required: 10, recent_automatic_pauses: 3, health_locked: false, manual_locked: false, balance_locked: false, owns_load_factor: true, original_load_factor: 10, expected_load_factor: 5, load_stage: "recovering_50", recovery_step: 2 },
        failure_threshold: 3, base_recovery_threshold: 3, recovery_threshold: 10, flap_enabled: true, flap_window_minutes: 60, flap_pause_threshold: 3, flap_recovery_threshold: 10
      }],
      unmatched_monitors: [], conflicts: [], settings: { ...adaptiveSettings, dry_run: true, health_engine_mode: "observe" }, service_started_at: new Date().toISOString()
    });
    const wrapper = mount(App);
    await flushPromises();
    expect(wrapper.text()).toContain("当前处于只观察模式");
    expect(wrapper.text()).toContain("上游账号");
    expect(wrapper.text()).toContain("渠道监控");
    expect(wrapper.text()).toContain("恢复试运行");
    expect(wrapper.text()).toContain("76 分");
    expect(wrapper.text()).toContain("置信度 92.0%");
    expect(wrapper.text()).toContain("15 分钟90.0%");
    expect(wrapper.text()).toContain("1.3 秒");
    expect(wrapper.text()).toContain("原始10");
    expect(wrapper.text()).toContain("当前5");
    expect(wrapper.text()).toContain("最近十五分钟可用率偏低");
    expect(wrapper.text()).toContain("再获得 1 次正常结果后恢复原始负载");
	await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
	expect(wrapper.text()).toContain("待填写登录账号密码");
	expect(wrapper.text()).toContain("待配置");
	expect(wrapper.find('button[title="配置账号密码"]').exists()).toBe(true);
	await wrapper.find('button[title="配置账号密码"]').trigger("click");
	expect(wrapper.text()).not.toContain("管理访问密钥");
	expect(wrapper.text()).not.toContain("用户编号");
	await wrapper.find('input[placeholder="用户名"]').setValue("owner");
	await wrapper.find('input[placeholder="输入登录密码"]').setValue("secret-password");
	await wrapper.findAll("button").find((button) => button.text().includes("测试连接"))!.trigger("click");
	await flushPromises();
	expect(api.validateUpstream).toHaveBeenCalledWith(expect.objectContaining({ username: "owner", password: "secret-password" }));
	expect(api.validateUpstream.mock.calls[0][0]).not.toHaveProperty("access_key");
	expect(api.validateUpstream.mock.calls[0][0]).not.toHaveProperty("user_id");
	expect(wrapper.text()).toContain("vip-key");
	expect(wrapper.text()).toContain("0.80x");
    wrapper.unmount();
  });

  it("renders the console when optional list responses are null", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: null });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({
      bindings: null,
      unmatched_monitors: null,
      conflicts: null,
      settings: adaptiveSettings,
      service_started_at: new Date().toISOString()
    });
    const wrapper = mount(App);
    await flushPromises();
    expect(wrapper.text()).toContain("账号与渠道映射");
    expect(wrapper.text()).toContain("尚未发现带上游地址的账号");
    wrapper.unmount();
  });

  it("keeps rendering when third-version decision fields are absent", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({
      bindings: [{
        account: { id: 9, name: "旧版账号", platform: "openai", type: "apikey", status: "active", schedulable: true, error_message: "", credentials: {}, concurrency: 3 },
        policy: { account_id: 9, excluded: false, enabled: true }, source: "auto", state: "bound", reason: "", normalized_endpoint: "https://legacy.example",
        monitor_state: { monitor_id: 9, last_status: "operational", healthy_streak: 1, unhealthy_streak: 0, phase: "healthy" },
        control: { account_id: 9, owns_pause: false, owner: "", last_decision: "", flap_active: false, flap_recovery_required: 10, recent_automatic_pauses: 0, health_locked: false, manual_locked: false, balance_locked: false },
        failure_threshold: 3, base_recovery_threshold: 3, recovery_threshold: 3, flap_enabled: true, flap_window_minutes: 60, flap_pause_threshold: 3, flap_recovery_threshold: 10
      }],
      unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString()
    });

    const wrapper = mount(App);
    await flushPromises();
    expect(wrapper.text()).toContain("旧版账号");
    expect(wrapper.text()).toContain("等待第三版数据");
    expect(wrapper.text()).toContain("等待真实流量");
    expect(wrapper.text()).toContain("等待能力数据");
    wrapper.unmount();
  });

  it("shows hard availability, quality evidence and the compact detail drawer", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({
      bindings: [{
        account: { id: 12, name: "示例账号", platform: "openai", type: "apikey", status: "active", schedulable: true, error_message: "", credentials: {}, load_factor: 5, concurrency: 10 },
        monitor: { id: 8, name: "示例账号监控", provider: "openai", endpoint: "https://duck.example", primary_model: "gpt-5.5", enabled: true, interval_seconds: 60, primary_status: "degraded" },
        policy: { account_id: 12, excluded: false, enabled: true }, source: "auto", state: "bound", reason: "", normalized_endpoint: "https://duck.example",
        monitor_state: { monitor_id: 8, last_status: "degraded", healthy_streak: 0, unhealthy_streak: 0, phase: "degraded" },
        health_state: { stage: "degraded", score: 61, baseline_latency_ms: 4000 },
        decision: {
          quality_score: 70, hard_success_rate_10: 1, hard_success_rate_60: 1, degraded_rate_10: 0.4, degraded_rate_60: 0.3167,
          traffic_success_rate: 0.8745, traffic_sample_count: 251, hard_failure_streak: 0, hard_failures_10: 0,
          suggested_load_percent: 50, action: "reduce_load", disagreement: true, response_p90_ms: 8000, baseline_latency_ms: 4000,
          reason_codes: ["latency_high", "traffic_healthy_override"], error_category_counts: { infrastructure: 4, capacity: 2, client: 7 },
          model_capabilities: [{ model: "gpt-5.4-mini", supported: false, reason: "上游不支持" }], checked_at: "2026-07-14T01:02:03Z"
        },
        control: { account_id: 12, owns_pause: false, owner: "", last_decision: "reduce_load", flap_active: false, flap_recovery_required: 10, recent_automatic_pauses: 0, health_locked: false, manual_locked: false, balance_locked: false, owns_load_factor: true, original_load_factor: 10, expected_load_factor: 5, load_stage: "congested_50" },
        failure_threshold: 3, base_recovery_threshold: 3, recovery_threshold: 3, flap_enabled: true, flap_window_minutes: 60, flap_pause_threshold: 3, flap_recovery_threshold: 10
      }],
      unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString()
    });

    const wrapper = mount(App);
    await flushPromises();
    expect(wrapper.text()).toContain("100%");
    expect(wrapper.text()).toContain("31.7%");
    expect(wrapper.text()).toContain("87.5%");
    expect(wrapper.text()).toContain("监控与业务不一致");
    expect(wrapper.text()).toContain("1 项模型告警");
    expect(wrapper.text()).toContain("建议 50%");

    await wrapper.find('button[title="查看健康详情"]').trigger("click");
    expect(wrapper.text()).toContain("最近窗口证据");
    expect(wrapper.text()).toContain("九成响应");
    expect(wrapper.text()).toContain("2.00 倍");
    expect(wrapper.text()).toContain("基础设施故障");
    expect(wrapper.text()).toContain("gpt-5.4-mini：上游不支持");
    expect(wrapper.text()).toContain("降低负载");
    wrapper.unmount();
  });

  it("distinguishes decision, proposed, actual, failed and blocked audit records", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getEvents.mockResolvedValue({ items: [
      { id: 1, type: "decision_snapshot", severity: "info", account_id: 12, monitor_id: 8, message: "完成判定", details: JSON.stringify({ reason_codes: ["latency_high"] }), actor: "scheduler", created_at: "2026-07-14T01:00:00Z" },
      { id: 2, type: "would_pause", severity: "warning", account_id: 12, message: "观察模式拟暂停", details: "{}", actor: "system", created_at: "2026-07-14T01:01:00Z" },
      { id: 3, type: "load_adjusted", severity: "info", account_id: 12, message: "负载调整成功", before_state: "100%", after_state: "50%", actor: "scheduler", created_at: "2026-07-14T01:02:00Z" },
      { id: 4, type: "health_action_failed", severity: "error", account_id: 12, message: "写入失败", details: "不是合法 JSON", actor: "scheduler", created_at: "2026-07-14T01:03:00Z" },
      { id: 5, type: "action_blocked", severity: "warning", account_id: 12, message: "人工保护中", details: { blocked_reason: "manual_lock" }, actor: "web", created_at: "2026-07-14T01:04:00Z" }
    ] });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("操作记录"))!.trigger("click");
    expect(wrapper.text()).toContain("决策快照");
    expect(wrapper.text()).toContain("拟执行");
    expect(wrapper.text()).toContain("实际动作");
    expect(wrapper.text()).toContain("执行失败");
    expect(wrapper.text()).toContain("人工锁阻止");
    expect(wrapper.text()).toContain("响应时间偏高");
    expect(wrapper.text()).toContain("100% → 50%");
    expect(wrapper.text()).toContain("管理端");
    expect(wrapper.text()).toContain("拟暂停账号");
    expect(wrapper.text()).not.toContain("would_pause");
    expect(wrapper.text()).not.toContain("load_adjusted");
    wrapper.unmount();
  });

  it("shows and submits the adaptive health policy in Chinese", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getUpstreams.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    api.updateSettings.mockResolvedValue(adaptiveSettings);

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("button").find((button) => button.text().includes("策略设置"))!.trigger("click");
    expect(wrapper.text()).toContain("旧判定");
    expect(wrapper.text()).toContain("只观察");
    expect(wrapper.text()).toContain("智能调度");
    expect(wrapper.text()).toContain("健康分数线");
    expect(wrapper.text()).toContain("隔离分数线");
    expect(wrapper.text()).toContain("响应缓慢（毫秒）");
    expect(wrapper.text()).toContain("恢复观察样本");
    expect(wrapper.text()).toContain("首段试运行（%）");
    expect(wrapper.text()).toContain("人工负载保护分钟");

    await wrapper.findAll("button").find((button) => button.text().includes("保存策略"))!.trigger("click");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.updateSettings).toHaveBeenCalledWith(expect.objectContaining({
      health_engine_mode: "adaptive", healthy_score_threshold: 80, watch_score_threshold: 60,
      quarantine_score_threshold: 35, degraded_load_percent: 50, recovery_initial_percent: 25,
      recovery_mid_percent: 50, load_manual_hold_minutes: 30, dry_run: false
    }));
    wrapper.unmount();
  });

	it("requires confirmation before manually refreshing an upstream", async () => {
		api.restoreSession.mockResolvedValue(true);
		api.getEvents.mockResolvedValue({ items: [] });
		api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
		api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
		api.getUpstreams.mockResolvedValue({ items: [{
			id: 1, name: "主力站", provider: "newapi", base_url: "https://upstream.example", normalized_url: "https://upstream.example",
			credential_configured: true, username_hint: "o***r", pause_below: 5, resume_at: 10, enabled: true, balance: 20, unit: "USD",
			low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false, key_rates: [], matched_accounts: [], created_at: new Date().toISOString(), updated_at: new Date().toISOString()
		}] });
		api.refreshUpstream.mockResolvedValue({});

		const wrapper = mount(App);
		await flushPromises();
		await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
		await wrapper.find('button[title="立即刷新"]').trigger("click");
		expect(wrapper.text()).toContain("确认立即连接「主力站」读取余额和密钥倍率？");
		expect(api.refreshUpstream).not.toHaveBeenCalled();
		await wrapper.find(".danger-button").trigger("click");
		await flushPromises();
		expect(api.refreshUpstream).toHaveBeenCalledWith(1);
		wrapper.unmount();
	});

		it("does not expose an arbitrary token group writer", async () => {
		api.restoreSession.mockResolvedValue(true);
		api.getEvents.mockResolvedValue({ items: [] });
		api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
		api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
		api.getUpstreams.mockResolvedValue({ items: [{
			id: 2, name: "倍率站", provider: "newapi", base_url: "https://rate.example", normalized_url: "https://rate.example",
			credential_configured: true, username_hint: "", credential_hint: "访问密钥 ab***yz", pause_below: 5, resume_at: 10, enabled: true, balance: 20, unit: "USD",
			low_streak: 0, recovery_streak: 2, balance_locked: false, stale: false, selected_key_id: "11", routing_enabled: true, routing_pool: "主池",
			groups: [{ external_id: "cheap", name: "低价组", rate_multiplier: 0.5 }, { external_id: "backup", name: "备用组", rate_multiplier: 1.2 }],
			key_rates: [{ external_id: "11", name: "调度令牌", key_hint: "sk-1...7890", group_id: "cheap", group_name: "低价组", rate_multiplier: 0.5, dynamic: false, status: "active" }], matched_accounts: []
		}] });
			const wrapper = mount(App);
		await flushPromises();
		await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
		await wrapper.find('button[title="展开密钥倍率"]').trigger("click");
			expect(wrapper.text()).toContain("分组写入只允许通过已确认的三级层级控制");
			expect(wrapper.find('select[aria-label="调度令牌目标分组"]').exists()).toBe(false);
			expect(wrapper.find(".rate-change-button").exists()).toBe(false);
		wrapper.unmount();
	});

  it("creates an account with password credentials and a distinct three-tier policy", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    api.getUpstreams.mockResolvedValue({ items: [{
      id: 0, name: "主力站", provider: "newapi", base_url: "https://main.example", normalized_url: "https://main.example",
      credential_configured: false, username_hint: "", credential_hint: "待填写登录账号密码", pause_below: 5, resume_at: 10, enabled: false,
      unit: "", low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false, selected_key_id: "", routing_enabled: false, routing_pool: "",
      groups: [], key_rates: [], failover_policies: [], matched_accounts: [{ id: 225, name: "主账号", schedulable: true }]
    }] });
    api.validateUpstream.mockResolvedValue({
      balance: 80, unit: "USD", username: "operator", fetched_at: new Date().toISOString(),
      groups: [
        { external_id: "main-group", name: "主用组", rate_multiplier: 0.5 },
        { external_id: "backup-group", name: "备用组", rate_multiplier: 1 },
        { external_id: "emergency-group", name: "应急组", rate_multiplier: 2 }
      ],
      key_rates: [{ external_id: "key-7", name: "受控令牌", key_hint: "sk-7...7890", group_id: "main-group", group_name: "主用组", rate_multiplier: 0.5, dynamic: false, status: "active" }]
    });
    api.createUpstream.mockResolvedValue({ id: 44 });
    api.saveUpstreamFailoverPolicy.mockResolvedValue({ key_id: "key-7", version: 3 });
    api.confirmUpstreamFailoverPolicy.mockResolvedValue({});

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
    await wrapper.find('button[title="配置账号密码"]').trigger("click");
    await wrapper.find('input[placeholder="用户名"]').setValue("operator");
    await wrapper.find('input[placeholder="输入登录密码"]').setValue("secret-password");
    await wrapper.findAll("button").find((button) => button.text().includes("测试连接"))!.trigger("click");
    await flushPromises();
    await wrapper.find(".failover-config input[type=checkbox]").setValue(true);
    const policySelects = wrapper.findAll(".failover-grid select");
    await policySelects[0].setValue("key-7");
    await wrapper.find('input[placeholder="例如：GPT 主线路"]').setValue("主池");
    await policySelects[1].setValue("main-group");
    await policySelects[2].setValue("backup-group");
    await policySelects[3].setValue("emergency-group");
    await wrapper.findAll("button").find((button) => button.text().includes("保存账户"))!.trigger("click");
    expect(api.createUpstream).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("受控令牌：受控令牌（sk-7...7890）");
    expect(wrapper.text()).toContain("绑定账号：#225 主账号");
    expect(wrapper.text()).toContain("策略池：主池");
    expect(wrapper.text()).toContain("主用分组：主用组（0.50 倍）");
    expect(wrapper.text()).toContain("备用分组：备用组（1.00 倍）");
    expect(wrapper.text()).toContain("应急分组：应急组（2.00 倍）");
    expect(wrapper.text()).toContain("修改受控令牌、绑定账号、任一分组或策略池都会使原确认失效");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.createUpstream).toHaveBeenCalledWith(expect.objectContaining({ username: "operator", password: "secret-password" }));
    expect(api.saveUpstreamFailoverPolicy).toHaveBeenCalledWith(44, {
		enabled: true, key_id: "key-7", main_enabled: true, backup_enabled: true, emergency_enabled: true,
		main_group_id: "main-group", backup_group_id: "backup-group", emergency_group_id: "emergency-group",
      account_ids: [225], pool: "主池"
    });
    expect(api.confirmUpstreamFailoverPolicy).toHaveBeenCalledWith(44, "key-7", 3);
    wrapper.unmount();
  });

  it("requires legacy credentials to migrate", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    const source = {
      id: 8, name: "旧配置站", provider: "newapi", base_url: "https://legacy.example", normalized_url: "https://legacy.example",
      credential_configured: true, credential_mode: "access_key", username_hint: "", credential_hint: "访问密钥 ab...yz", pause_below: 5, resume_at: 10, enabled: true,
      balance: 20, unit: "USD", low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false, selected_key_id: "", routing_enabled: false, routing_pool: "", groups: [], key_rates: [], failover_policies: [], matched_accounts: []
    };
    api.getUpstreams.mockResolvedValue({ items: [source] });
    api.updateUpstream.mockResolvedValue(source);

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
    expect(wrapper.text()).toContain("旧访问密钥待迁移为账号密码");
    await wrapper.find('button[title="编辑账户"]').trigger("click");
    expect(wrapper.text()).toContain("旧访问密钥配置需要迁移");
    await wrapper.findAll("button").find((button) => button.text().includes("保存账户"))!.trigger("click");
    expect(wrapper.text()).toContain("需要填写账号和密码后迁移");
    expect(api.updateUpstream).not.toHaveBeenCalled();
    await wrapper.find('input[placeholder="用户名"]').setValue("operator");
    await wrapper.find('input[placeholder="输入登录密码"]').setValue("replacement-password");
    await wrapper.findAll("button").find((button) => button.text().includes("保存账户"))!.trigger("click");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.updateUpstream).toHaveBeenCalledWith(8, expect.objectContaining({ username: "operator", password: "replacement-password" }));
    wrapper.unmount();
  });

  it("keeps the encrypted password when an account edit leaves it blank", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    const source = {
      id: 18, name: "密码站", provider: "newapi", base_url: "https://password.example", normalized_url: "https://password.example",
      credential_configured: true, credential_mode: "password", username_hint: "o***r", credential_hint: "账号 o***r", pause_below: 5, resume_at: 10, enabled: true,
      balance: 20, unit: "USD", low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false, selected_key_id: "", routing_enabled: false, routing_pool: "",
      groups: [], key_rates: [], failover_policies: [], matched_accounts: []
    };
    api.getUpstreams.mockResolvedValue({ items: [source] });
    api.updateUpstream.mockResolvedValue(source);

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
    await wrapper.find('button[title="编辑账户"]').trigger("click");
    expect((wrapper.find('input[placeholder="留空保留服务器中的加密密码"]').element as HTMLInputElement).value).toBe("");
    await wrapper.findAll("button").find((button) => button.text().includes("保存账户"))!.trigger("click");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.updateUpstream).toHaveBeenCalledWith(18, expect.objectContaining({ username: "", password: "" }));
    wrapper.unmount();
  });

  it("shows failover runtime state and confirms a manual tier switch", async () => {
    api.restoreSession.mockResolvedValue(true);
    api.getEvents.mockResolvedValue({ items: [] });
    api.getDiagnostics.mockResolvedValue({ alive: true, ready: true, database: "ok", service_started_at: new Date().toISOString(), poll_interval_seconds: 50, dry_run: false });
    api.getOverview.mockResolvedValue({ bindings: [], unmatched_monitors: [], conflicts: [], settings: adaptiveSettings, service_started_at: new Date().toISOString() });
    api.getUpstreams.mockResolvedValue({ items: [{
      id: 9, name: "三级站", provider: "newapi", base_url: "https://tiers.example", normalized_url: "https://tiers.example",
      credential_configured: true, credential_mode: "password", username_hint: "o***r", credential_hint: "账号 o***r", pause_below: 5, resume_at: 10, enabled: true,
      balance: 20, unit: "USD", low_streak: 0, recovery_streak: 0, balance_locked: false, stale: false, selected_key_id: "", routing_enabled: false, routing_pool: "",
      groups: [{ external_id: "g1", name: "主用组", rate_multiplier: 0.5 }, { external_id: "g2", name: "备用组", rate_multiplier: 1 }, { external_id: "g3", name: "应急组", rate_multiplier: 2 }],
      key_rates: [{ external_id: "key-9", name: "受控令牌", key_hint: "sk-9...7890", group_id: "g1", group_name: "主用组", rate_multiplier: 0.5, dynamic: false, status: "active" }], matched_accounts: [],
		failover_policies: [{ source_id: 9, enabled: true, key_id: "key-9", key_name: "受控令牌", key_hint: "sk-9...7890", main_enabled: true, backup_enabled: true, emergency_enabled: true, main_group_id: "g1", backup_group_id: "g2", emergency_group_id: "g3", account_ids: [225], pool: "主池", version: 2, confirmed_version: 2, confirmed: true, state: { source_id: 9, key_id: "key-9", current_tier: "backup", observed_group_id: "g2", frozen: false, recovery_healthy_count: 0, validation_status: "awaiting_evidence", validation_mode: "passive", successful_evidence_count: 0, failed_evidence_count: 0, evidence_deadline: "2026-07-14T10:10:03Z" } }]
    }] });
    api.getUpstreamFailoverTransitions.mockResolvedValue({ items: [
		{ id: 31, idempotency_key: "switch-31", source_id: 9, key_id: "key-9", from_tier: "main", to_tier: "backup", from_group_id: "g1", to_group_id: "g2", status: "applied", actor: "deterministic", reason: "主池完全不可用", trigger: "连续 3 次硬失败", manual: false, dry_run: false, created_at: "2026-07-14T10:00:00Z", completed_at: "2026-07-14T10:00:03Z" },
      { id: 30, idempotency_key: "switch-30", source_id: 9, key_id: "key-9", from_tier: "backup", to_tier: "emergency", from_group_id: "g2", to_group_id: "g3", status: "failed", actor: "agent", reason: "备用组验证失败", trigger: "真实请求成功率低于 20%", error: "上游回读仍为备用组", manual: false, dry_run: false, created_at: "2026-07-14T09:50:00Z", completed_at: "2026-07-14T09:50:05Z" }
    ] });
    api.switchUpstreamKeyTier.mockResolvedValue({});

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("余额中心"))!.trigger("click");
	expect(wrapper.text()).toContain("等待切换后证据");
    await wrapper.find('button[title="展开密钥倍率"]').trigger("click");
    await flushPromises();
    expect(api.getUpstreamFailoverTransitions).toHaveBeenCalledWith(9, "key-9");
	expect(wrapper.text()).toContain("当前 备用层 · g2");
	expect(wrapper.text()).toContain("成功 0 / 失败 0");
    expect(wrapper.text()).toContain("最近切换流水");
    expect(wrapper.text()).toContain("主用层 → 备用层");
    expect(wrapper.text()).toContain("连续 3 次硬失败");
    expect(wrapper.text()).toContain("回读已确认目标分组 g2");
	expect(wrapper.text()).toContain("已切换，等待新分组监控证据");
    expect(wrapper.text()).toContain("备用层 → 应急层");
    expect(wrapper.text()).toContain("真实请求成功率低于 20%");
    expect(wrapper.text()).toContain("错误：上游回读仍为备用组");
	await wrapper.find(".tier-button.emergency").trigger("click");
    expect(api.switchUpstreamKeyTier).not.toHaveBeenCalled();
	expect(wrapper.text()).toContain("确认将受控令牌切换到应急层「应急组」");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
	expect(api.switchUpstreamKeyTier).toHaveBeenCalledWith(9, "key-9", "emergency");
    wrapper.unmount();
  });

  it("renders the intelligent scheduling control room with compact evidence", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");

    expect(wrapper.text()).toContain("智能调度控制室");
    expect(wrapper.text()).toContain("17 / 40");
    expect(wrapper.text()).toContain("4,280 令牌");
    expect(wrapper.text()).toContain("整体可用，示例账号上游性能下降");
    expect(wrapper.text()).toContain("监控黄色但真实请求成功");
    expect(wrapper.text()).toContain("证据冲突");
    expect(wrapper.text()).toContain("昨日可用率稳定，成本下降");
    expect(wrapper.text()).toContain("观察模式，仅记录拟执行动作");
    expect(wrapper.text()).toContain("降低单次性能下降影响");
    wrapper.unmount();
  });

  it("renders V2 goals, steps, events, schedules, capabilities and memory", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.getAgentCapabilities.mockResolvedValue({ items: [{ name: "pause_account", version: "1", title: "暂停账号", description: "暂停异常账号", risk_level: "medium", input_schema: {}, scopes: ["account"], auto_executable: true, approval_required: false, supports_schedule: true, supports_compensation: true }] });
    api.getAgentGoals.mockResolvedValue({
      items: [{ id: 71, title: "恢复示例账号上游容量", objective: "验证稳定流量后分阶段恢复账号", status: "running", priority: 80, risk_level: "medium", source: "chat", context: {}, created_by: "operator", created_at: "2026-07-14T10:06:00Z", updated_at: "2026-07-14T10:07:00Z" }],
      steps: [
        { id: 711, goal_id: 71, sequence: 1, capability: "pause_account", arguments: {}, preconditions: {}, compensation: {}, status: "completed", risk_level: "medium", idempotency_key: "step-711", attempt_count: 1, max_attempts: 3, result: "异常账号已隔离", created_at: "2026-07-14T10:06:00Z", updated_at: "2026-07-14T10:06:30Z" },
        { id: 712, goal_id: 71, sequence: 2, depends_on_step_id: 711, capability: "verify_recovery", arguments: {}, preconditions: {}, compensation: {}, status: "running", risk_level: "low", idempotency_key: "step-712", attempt_count: 1, max_attempts: 3, created_at: "2026-07-14T10:06:30Z", updated_at: "2026-07-14T10:07:00Z" }
      ]
    });
    api.getAgentRuntimeEvents.mockResolvedValue({ items: [{ id: 91, event_key: "evt-91", goal_id: 71, step_id: 712, type: "step_started", severity: "info", actor: "agent", payload: { message: "开始验证连续正常样本" }, created_at: "2026-07-14T10:07:00Z" }] });
    api.getAgentTasks.mockResolvedValue({ items: [{ id: 81, goal_id: 71, step_id: 712, capability: "verify_recovery", arguments: {}, conditions: {}, status: "queued", timezone: "Asia/Shanghai", execute_at: "2026-07-14T10:10:00Z", idempotency_key: "task-81", attempt_count: 0, max_attempts: 3, created_by: "agent", created_at: "2026-07-14T10:07:00Z", updated_at: "2026-07-14T10:07:00Z" }] });
    api.getAgentMemories.mockResolvedValue({ items: [{ id: 61, scope_type: "account", scope_id: "298", kind: "constraint", key: "manual_hold", summary: "人工保护期内不覆盖负载", content: "", importance: 90, pinned: true, created_at: "2026-07-14T09:00:00Z", updated_at: "2026-07-14T09:00:00Z" }] });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");

    expect(wrapper.text()).toContain("当前目标与步骤");
    expect(wrapper.text()).toContain("恢复示例账号上游容量");
    expect(wrapper.text()).toContain("开始验证连续正常样本");
    expect(wrapper.text()).toContain("定时任务");
    expect(wrapper.text()).toContain("暂停账号");
    expect(wrapper.text()).toContain("人工保护期内不覆盖负载");
    expect(wrapper.text()).toContain("17 / 40 次有效分析");
    wrapper.unmount();
  });

  it("requires confirmation before releasing either global freeze", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.getAgentFreezeState.mockResolvedValueOnce({ scope_type: "global", scope_id: "", mode: "agent_paused", reason: "人工检查", actor: "operator" }).mockResolvedValue({ scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "operator" });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");

    const releaseAgent = wrapper.findAll("button").find((button) => button.text().includes("解除智能体冻结"));
    expect(releaseAgent).toBeTruthy();
    await releaseAgent!.trigger("click");
    expect(api.setAgentFreezeState).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("自动执行能力将立即恢复");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.setAgentFreezeState).toHaveBeenCalledWith("active", expect.stringContaining("解除智能体冻结"));

    const freezeWrites = wrapper.findAll("button").find((button) => button.text().includes("冻结全部自动化"));
    await freezeWrites!.trigger("click");
    expect(wrapper.text()).toContain("确定性调度和智能体都不能向 Sub2API 或上游写入");
    wrapper.unmount();
  });

  it("requires a second confirmation before releasing the write freeze", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.getAgentFreezeState.mockResolvedValueOnce({ scope_type: "global", scope_id: "", mode: "writes_frozen", reason: "变更窗口", actor: "operator" }).mockResolvedValue({ scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "operator" });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
    await wrapper.findAll("button").find((button) => button.text().includes("解除自动化冻结"))!.trigger("click");

    expect(api.setAgentFreezeState).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("解除全部自动化冻结");
    expect(wrapper.text()).toContain("自动执行能力将立即恢复");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.setAgentFreezeState).toHaveBeenCalledWith("active", expect.stringContaining("解除全部自动化冻结"));
    wrapper.unmount();
  });

	  it("tracks an asynchronous chat goal and polls until runtime state changes", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
	  api.chatAgent.mockResolvedValue({ conversation_id: 9, goal_id: 71, run_id: 43, status: "queued", intent: { intent_type: "analysis", resource_type: "account", resource_ids: ["225"], operation: "analyze", read_only: true, requires_confirmation: false, risk_level: "low", user_facing_summary: "只读分析账号" } });
    api.getAgentMessages.mockResolvedValue({ items: [{ id: 1, conversation_id: 9, role: "user", content: "恢复示例账号上游", created_at: "2026-07-14T10:05:00Z" }] });
    api.getAgentGoals.mockResolvedValue({ items: [{ id: 71, title: "恢复示例账号上游", objective: "等待数据后恢复", status: "queued", priority: 70, risk_level: "medium", source: "chat", context: {}, created_by: "operator", created_at: "2026-07-14T10:05:00Z", updated_at: "2026-07-14T10:05:00Z" }], steps: [] });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
    await wrapper.find('textarea[placeholder="输入分析问题或调度命令"]').setValue("恢复示例账号上游");
    await wrapper.find(".agent-chat-form").trigger("submit");
    await flushPromises();

    expect(wrapper.text()).toContain("任务已进入队列");
    expect(wrapper.text()).toContain("目标 #71");
    const callsBeforePoll = api.getAgentGoals.mock.calls.length;
    await vi.advanceTimersByTimeAsync(1600);
    await flushPromises();
    expect(api.getAgentGoals.mock.calls.length).toBeGreaterThan(callsBeforePoll);
    wrapper.unmount();
	  });

	  it("shows a high-risk chat preview and consumes confirmation explicitly", async () => {
		mockAuthenticatedConsole(populatedAgentOverview());
		api.chatAgent.mockResolvedValue({ conversation_id: 9, goal_id: 72, status: "waiting", confirmation_token: "one-use-token", confirmation_expires_at: "2026-07-14T10:10:00Z", intent: { intent_type: "direct_action", resource_type: "account", resource_ids: ["225", "298"], operation: "bulk_pause", read_only: false, requires_confirmation: true, risk_level: "critical", user_facing_summary: "暂停 2 个账号" } });
		api.confirmAgentGoal.mockResolvedValue({ conversation_id: 9, goal_id: 72, status: "planned", intent: { intent_type: "direct_action", resource_type: "account", resource_ids: ["225", "298"], operation: "bulk_pause", read_only: false, requires_confirmation: true, risk_level: "critical", user_facing_summary: "暂停 2 个账号" } });
		const wrapper = mount(App);
		await flushPromises();
		await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
		await wrapper.find('.agent-chat-form textarea').setValue("暂停所有账号。");
		await wrapper.find(".agent-chat-form").trigger("submit");
		await flushPromises();
		expect(wrapper.text()).toContain("暂停 2 个账号");
		expect(wrapper.text()).toContain("影响 2 个资源");
		await wrapper.find(".agent-intent-receipt .danger-button").trigger("click");
		await flushPromises();
		expect(api.confirmAgentGoal).not.toHaveBeenCalled();
		await wrapper.find(".confirm-panel .danger-button").trigger("click");
		await flushPromises();
		expect(api.confirmAgentGoal).toHaveBeenCalledWith(72, "one-use-token");
		wrapper.unmount();
	  });

	  it("renders policy simulation and requires confirmation before activation", async () => {
		mockAuthenticatedConsole(populatedAgentOverview());
		api.activateAgentPolicy.mockResolvedValue({ activated: true });
		const wrapper = mount(App);
		await flushPromises();
		await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
		expect(wrapper.text()).toContain("动作次数未增加");
		await wrapper.findAll("button").find((button) => button.text().includes("批准并激活"))!.trigger("click");
		expect(api.activateAgentPolicy).not.toHaveBeenCalled();
		await wrapper.find(".confirm-panel .danger-button").trigger("click");
		await flushPromises();
		expect(api.activateAgentPolicy).toHaveBeenCalledWith(11);
		wrapper.unmount();
	  });

  it("validates and saves the primary model configuration", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.validateAgentProvider.mockResolvedValue({ valid: true });
    api.saveAgentProvider.mockResolvedValue({});

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
    await wrapper.findAll("button").find((button) => button.text().includes("主模型"))!.trigger("click");

    expect(wrapper.text()).toContain("主模型配置");
    await wrapper.find('input[placeholder="https://api.example.com/v1"]').setValue("https://gateway.example/v1");
    await wrapper.find('input[placeholder="例如：gpt-5.4-mini"]').setValue("reasoning-model");
    await wrapper.find('input[type="password"]').setValue("secret-model-key");

    await wrapper.findAll("button").find((button) => button.text().includes("加密保存"))!.trigger("click");
    expect(api.saveAgentProvider).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("请先测试模型连接");

    await wrapper.findAll("button").find((button) => button.text().includes("测试模型"))!.trigger("click");
    await flushPromises();
    expect(api.validateAgentProvider).toHaveBeenCalledWith(expect.objectContaining({
      slot: "primary", base_url: "https://gateway.example/v1", model: "reasoning-model", api_key: "secret-model-key"
    }));
    expect(wrapper.text()).toContain("连接、鉴权和结构化输出均已验证");

    await wrapper.findAll("button").find((button) => button.text().includes("加密保存"))!.trigger("click");
    await flushPromises();
    expect(api.saveAgentProvider).toHaveBeenCalledWith(expect.objectContaining({
      slot: "primary", base_url: "https://gateway.example/v1", model: "reasoning-model", api_key: "secret-model-key"
    }));
    wrapper.unmount();
  });

  it("updates agent runtime settings only after confirmation", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.updateAgentSettings.mockResolvedValue({ ...agentSettings, mode: "control", analysis_interval_minutes: 45 });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");
    await wrapper.findAll("button").find((button) => button.text().includes("运行设置"))!.trigger("click");

    expect(wrapper.text()).toContain("智能体运行设置");
	  const intervalLabel = wrapper.findAll("label").find((label) => label.text().includes("全量分析周期"))!;
	  await intervalLabel.find('input[type="number"]').setValue("45");
    await wrapper.findAll("button").find((button) => button.text().includes("保存设置"))!.trigger("click");
    expect(api.updateAgentSettings).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("确认更新智能体运行模式和分析参数？");

    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.updateAgentSettings).toHaveBeenCalledWith(expect.objectContaining({
		  optimizer_mode: "propose", operator_mode: "confirm", daily_policy_change_budget: 2,
		  analysis_interval_minutes: 45, context_token_budget: 16000, max_drilldowns: 8
    }));
    wrapper.unmount();
  });

  it("confirms a manual analysis and keeps the conversation transcript", async () => {
    mockAuthenticatedConsole(populatedAgentOverview());
    api.runAgent.mockResolvedValue({ id: 42, status: "completed" });
	  api.chatAgent.mockResolvedValue({ conversation_id: 9, run: { id: 43, status: "completed" }, intent: { intent_type: "query", resource_type: "account", resource_ids: ["225"], operation: "query", read_only: true, requires_confirmation: false, risk_level: "low", user_facing_summary: "查询账号状态" } });
    api.getAgentMessages.mockResolvedValue({ items: [
      { id: 1, conversation_id: 9, role: "user", content: "为什么示例账号上游降级？", created_at: "2026-07-14T10:05:00Z" },
      { id: 2, conversation_id: 9, role: "assistant", content: "真实请求成功，当前仅降低性能质量分。", run_id: 43, created_at: "2026-07-14T10:05:03Z" }
    ] });

    const wrapper = mount(App);
    await flushPromises();
    await wrapper.findAll("nav button").find((button) => button.text().includes("智能调度"))!.trigger("click");

    await wrapper.findAll("button").find((button) => button.text().includes("立即分析"))!.trigger("click");
    expect(api.runAgent).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("确认使用最新统计数据包调用智能体？");
    await wrapper.find(".danger-button").trigger("click");
    await flushPromises();
    expect(api.runAgent).toHaveBeenCalledTimes(1);

    const textarea = wrapper.find('textarea[placeholder="输入分析问题或调度命令"]');
    await textarea.setValue("为什么示例账号上游降级？");
    await textarea.trigger("keydown.enter");
    await wrapper.find(".agent-chat-form").trigger("submit");
    await flushPromises();
    expect(api.chatAgent).toHaveBeenCalledWith("为什么示例账号上游降级？", 0);
    expect(api.getAgentMessages).toHaveBeenCalledWith(9);
    expect(wrapper.text()).toContain("管理员");
    expect(wrapper.text()).toContain("真实请求成功，当前仅降低性能质量分。");
    wrapper.unmount();
  });
});
