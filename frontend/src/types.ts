export interface Monitor {
  id: number;
  name: string;
  provider: string;
  endpoint: string;
  primary_model: string;
  enabled: boolean;
  interval_seconds: number;
  last_checked_at?: string;
  primary_status: string;
}

export interface Account {
  id: number;
  name: string;
  platform: string;
  type: string;
  status: string;
  schedulable: boolean;
  error_message: string;
  credentials: Record<string, unknown>;
  load_factor?: number;
  concurrency?: number;
}

export interface Policy {
  account_id: number;
  monitor_id?: number;
  excluded: boolean;
  enabled: boolean;
  failure_threshold?: number;
  recovery_threshold?: number;
  flap_enabled?: boolean;
  flap_window_minutes?: number;
  flap_pause_threshold?: number;
  flap_recovery_threshold?: number;
  healthy_score_threshold?: number;
  watch_score_threshold?: number;
  quarantine_score_threshold?: number;
  minimum_samples?: number;
  latency_warning_ms?: number;
  latency_critical_ms?: number;
  traffic_pause_below?: number;
  traffic_healthy_at?: number;
  hard_failures_10_threshold?: number;
  persistent_slow_rate?: number;
}

export interface MonitorState {
  monitor_id: number;
  last_checked_at?: string;
  last_status: string;
  healthy_streak: number;
  unhealthy_streak: number;
  phase: string;
}

export type HealthStage = "healthy" | "watch" | "degraded" | "quarantined" | "recovering" | "recovering_25" | "recovering_50" | "recovering_80" | "limited_25" | "limited_50" | "limited_80" | "frozen";

export interface HealthState {
  stage: HealthStage;
  score?: number;
  confidence?: number;
  current_latency_ms?: number;
  baseline_latency_ms?: number;
  availability_15m?: number;
  availability_1h?: number;
  availability_24h?: number;
  sample_count?: number;
  recovery_healthy_count?: number;
  last_two_healthy?: boolean;
  recovery_eligible?: boolean;
  reason_json?: unknown;
  next_recovery_condition?: string;
}

export interface ModelCapability {
  model?: string;
  supported?: boolean;
  status?: string;
  reason?: string;
}

export interface HealthDecision {
  quality_score?: number;
  hard_success_rate_10?: number;
  hard_success_rate_60?: number;
  degraded_rate_10?: number;
  degraded_rate_60?: number;
  traffic_success_rate?: number;
  traffic_sample_count?: number;
  hard_failure_streak?: number;
  hard_failures_10?: number;
  recovery_success_streak?: number;
  suggested_load_percent?: number;
  action?: string;
  disagreement?: boolean;
  response_p90_ms?: number;
  baseline_latency_ms?: number;
  reason_codes?: string[];
  error_category_counts?: Record<string, number>;
  model_capabilities?: Array<ModelCapability | string> | Record<string, boolean | string>;
  checked_at?: string;
}

export interface AccountControl {
  account_id: number;
  monitor_id?: number;
  owns_pause: boolean;
  owner: string;
  expected_schedulable?: boolean;
  manual_override_until?: string;
  last_decision: string;
  flap_active: boolean;
  flap_triggered_at?: string;
  flap_recovery_required: number;
  recent_automatic_pauses: number;
  health_locked: boolean;
  manual_locked: boolean;
  balance_locked: boolean;
  balance_source_id?: number;
  cost_locked: boolean;
  cost_source_id?: number;
  cost_pool?: string;
  original_load_factor?: number;
  owns_load_factor?: boolean;
  expected_load_factor?: number;
  load_stage?: string;
  load_override_until?: string;
  recovery_step?: number;
  recovery_started_at?: string;
}

export interface Binding {
  monitor?: Monitor;
  account: Account;
  policy: Policy;
  source: string;
  state: string;
  reason: string;
  normalized_endpoint: string;
  monitor_state: MonitorState;
  health_state?: HealthState;
  decision?: HealthDecision;
  control: AccountControl;
  failure_threshold: number;
  base_recovery_threshold: number;
  recovery_threshold: number;
  flap_enabled: boolean;
  flap_window_minutes: number;
  flap_pause_threshold: number;
  flap_recovery_threshold: number;
}

export interface Settings {
  dry_run: boolean;
  scheduler_mode: "observe" | "control";
  failover_mode: "disabled" | "observe" | "control";
  group_failover_mutation_budget: number;
  failure_threshold: number;
  recovery_threshold: number;
  manual_hold_minutes: number;
  flap_window_minutes: number;
  flap_pause_threshold: number;
  flap_recovery_threshold: number;
  health_engine_mode?: "legacy" | "observe" | "adaptive";
  healthy_score_threshold?: number;
  watch_score_threshold?: number;
  quarantine_score_threshold?: number;
  degraded_load_percent?: number;
  latency_warning_ms?: number;
  latency_critical_ms?: number;
  traffic_pause_below?: number;
  traffic_healthy_at?: number;
  hard_failures_10_threshold?: number;
  persistent_slow_rate?: number;
  minimum_samples?: number;
  quarantine_minutes?: number;
  recovery_window_size?: number;
  recovery_required_successes?: number;
  recovery_initial_percent?: number;
  recovery_mid_percent?: number;
  recovery_stage_minutes?: number;
  load_manual_hold_minutes?: number;
}

export interface Snapshot {
  bindings: Binding[];
  unmatched_monitors: Monitor[];
  conflicts: string[];
  last_sync_at?: string;
  last_sync_error?: string;
  settings: Settings;
  service_started_at: string;
}

export interface Diagnostics {
  alive: boolean;
  ready: boolean;
  database: "ok" | "error";
  last_sync_at?: string;
  last_sync_error?: string;
  service_started_at: string;
  poll_interval_seconds: number;
  dry_run: boolean;
  balance_poll_interval_seconds: number;
  balance_last_run_at?: string;
}

export interface KeyRate {
  external_id: string;
  name: string;
  key_hint: string;
  group_id: string;
  group_name: string;
  rate_multiplier?: number;
  dynamic: boolean;
  status: string;
}

export interface UpstreamGroup {
  external_id: string;
  name: string;
  rate_multiplier: number;
}

export type FailoverTier = "main" | "backup" | "emergency";

export interface GroupFailoverState {
  source_id: number;
  key_id: string;
  current_tier?: FailoverTier;
  observed_group_id?: string;
  previous_tier?: FailoverTier;
  previous_stable_tier?: FailoverTier;
  previous_group_id?: string;
  frozen: boolean;
  freeze_reason?: string;
  last_error?: string;
  manual_hold_until?: string;
  manual_override_until?: string;
  cooldown_until?: string;
  return_blocked_until?: string;
  recovery_since?: string;
  last_switch_at?: string;
  last_transition_at?: string;
  verification_started_at?: string;
  healthy_since?: string;
  recovery_healthy_count: number;
  last_confirmed_at?: string;
	validation_status?: "unknown" | "stable" | "transitioning" | "awaiting_evidence" | "probing" | "confirmed_healthy" | "confirmed_failed" | "uncertain" | "exhausted";
	validation_mode?: "passive" | "active" | "active_then_passive";
	validation_transition_id?: number;
	validation_from_tier?: FailoverTier;
	validation_target_tier?: FailoverTier;
	validation_from_group_id?: string;
	validation_target_group_id?: string;
	switch_requested_at?: string;
	switch_verified_at?: string;
	validation_not_before?: string;
	evidence_deadline?: string;
	monitor_watermark?: number;
	traffic_watermark?: number;
	monitor_evidence_cursor?: number;
	traffic_evidence_cursor?: number;
	active_probe_attempts?: number;
	successful_evidence_count?: number;
	failed_evidence_count?: number;
	last_evidence_id?: string;
	last_evidence_source?: string;
	last_evidence_reason?: string;
	last_evidence_at?: string;
  updated_at?: string;
}

export interface GroupFailoverPolicy {
  source_id: number;
  enabled: boolean;
  key_id: string;
  key_name: string;
  key_hint: string;
	main_enabled: boolean;
	backup_enabled: boolean;
	emergency_enabled: boolean;
  main_group_id: string;
  backup_group_id: string;
  emergency_group_id: string;
  account_ids: number[];
  pool: string;
  version: number;
  confirmed_version: number;
  confirmed: boolean;
  confirmed_at?: string;
  confirmed_by?: string;
  state: GroupFailoverState;
  created_at?: string;
  updated_at?: string;
}

export interface GroupTierTransition {
  id: number;
  idempotency_key: string;
  source_id: number;
  key_id: string;
  from_tier?: FailoverTier;
  to_tier: FailoverTier;
  from_group_id: string;
  to_group_id: string;
  status: string;
  actor: string;
  producer?: string;
  authority?: string;
  reason: string;
  evidence?: string;
  trigger?: string;
  packet_id?: number;
  run_id?: number;
  error?: string;
  manual: boolean;
  dry_run: boolean;
  attempt_count?: number;
  before_state?: string;
  verified_after_state?: string;
  uncertain?: boolean;
  created_at: string;
  completed_at?: string;
}

export interface UpstreamFailoverPolicyInput {
  enabled: boolean;
  key_id: string;
	main_enabled: boolean;
	backup_enabled: boolean;
	emergency_enabled: boolean;
  main_group_id: string;
  backup_group_id: string;
  emergency_group_id: string;
  account_ids: number[];
  pool: string;
}

export interface AccountRef {
  id: number;
  name: string;
  schedulable: boolean;
}

export interface UpstreamSource {
  id: number;
  name: string;
  provider: "newapi" | "sub2";
  base_url: string;
  normalized_url: string;
  credential_configured: boolean;
  credential_mode?: "password" | "access_key";
  username_hint: string;
  credential_hint: string;
  pause_below: number;
  resume_at: number;
  enabled: boolean;
  balance?: number;
  unit: string;
  low_streak: number;
  recovery_streak: number;
  balance_locked: boolean;
  last_attempt_at?: string;
  last_success_at?: string;
  last_error?: string;
  stale: boolean;
  key_rates: KeyRate[];
  groups: UpstreamGroup[];
  selected_key_id: string;
  routing_enabled: boolean;
  routing_pool: string;
  failover_policies: GroupFailoverPolicy[];
  matched_accounts: AccountRef[];
}

export interface UpstreamInput {
  name: string;
  provider: "newapi" | "sub2";
  base_url: string;
  username: string;
  password: string;
  pause_below: number;
  resume_at: number;
  enabled: boolean;
  selected_key_id: string;
  routing_enabled: boolean;
  routing_pool: string;
}

export interface UpstreamPreview {
  balance: number;
  unit: string;
  username: string;
  key_rates: KeyRate[];
  groups: UpstreamGroup[];
  credential_warning?: string;
  fetched_at: string;
}

export interface EventItem {
  id: number;
  type: string;
  severity: string;
  monitor_id?: number;
  account_id?: number;
  message: string;
  before_state?: string;
  after_state?: string;
  details?: string | Record<string, unknown>;
  actor: string;
  created_at: string;
}

export interface AgentProvider {
  slot: "primary" | "fallback";
  base_url: string;
  model: string;
  api_key_configured: boolean;
  enabled: boolean;
  timeout_seconds: number;
  max_output_tokens: number;
  temperature: number;
  last_validated_at?: string;
  last_error?: string;
  updated_at?: string;
}

export interface AgentProviderInput {
  slot: "primary" | "fallback";
  base_url: string;
  api_key: string;
  model: string;
  enabled: boolean;
  timeout_seconds: number;
  max_output_tokens: number;
  temperature: number;
}

export interface AgentSettings {
  enabled: boolean;
  mode: "observe" | "control";
  optimizer_mode: "disabled" | "observe" | "propose" | "auto";
  operator_mode: "disabled" | "confirm" | "direct";
  daily_policy_change_budget: number;
  analysis_interval_minutes: number;
  emergency_cooldown_minutes: number;
  context_token_budget: number;
  max_anomalies: number;
  max_drilldowns: number;
  retention_days: number;
  observation_started_at?: string;
  successful_observation_runs: number;
  observation_proposed_actions?: number;
  observation_executable_actions?: number;
  observation_violations?: number;
  observation_structure_errors?: number;
  last_scheduled_at?: string;
  last_emergency_at?: string;
  updated_at?: string;
}

export interface AvailabilityAssessment {
  id: number;
  packet_id: number;
  account_id: number;
  state: "available" | "degraded" | "unavailable" | "insufficient_data";
  availability_score: number;
  performance_score: number;
  stability_score: number;
  capacity_score: number;
  cost_score: number;
  confidence: number;
  evidence_conflict: boolean;
  reasons_json: string;
  created_at: string;
}

export interface AgentRun {
  id: number;
  kind: string;
  trigger: string;
  status: string;
  provider_slot?: string;
  model?: string;
  packet_id?: number;
  conversation_id?: number;
  summary?: string;
  conclusion?: string;
  confidence: number;
  actions?: unknown[];
  error?: string;
  started_at: string;
  completed_at?: string;
}

export interface AgentToolCall {
  id: number;
  run_id: number;
  tool: string;
  arguments: Record<string, unknown> | string;
  status: string;
  before_state?: string;
  after_state?: string;
  result?: string;
  created_at: string;
}

export interface ScorePolicyVersion {
  id: number;
  scope_type: string;
  scope_id: string;
  version: number;
  status: string;
  config: Record<string, unknown>;
  patch?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  simulation?: {
    passed: boolean;
    data_sufficient: boolean;
    sample_count: number;
    current_actions?: number;
    proposed_actions?: number;
    summary?: string;
  };
  risk_level?: "low" | "medium" | "high" | "critical";
  affected_account_ids?: number[];
  base_version_id?: number;
  previous_active_version_id?: number;
  approved_by?: string;
  rollback_reason?: string;
  outcome_summary?: string;
  reason: string;
  agent_run_id?: number;
  created_by: string;
  activated_at?: string;
  created_at: string;
}

export interface AnalysisPacket {
  id: number;
  kind: string;
  cutoff_at: string;
  hash: string;
  token_estimate: number;
  no_material_change: boolean;
  system_summary: {
    accounts: number;
    schedulable: number;
    available: number;
    degraded: number;
    unavailable: number;
    insufficient_data: number;
    average_availability: number;
    average_performance: number;
    average_confidence: number;
    critical_anomalies: number;
    data_fresh: boolean;
  };
  changes: string[];
  created_at: string;
}

export interface AgentDailyReport {
  id: number;
  report_date: string;
  status: string;
  summary: string;
  metrics: Record<string, unknown>;
  advice: string[];
  error?: string;
  created_at: string;
}

export interface AgentMessage {
  id: number;
  conversation_id: number;
  role: "user" | "assistant";
  content: string;
  run_id?: number;
  created_at: string;
}

export interface AgentCapability {
  name: string;
  version: string;
  title: string;
  description: string;
  risk_level: string;
  input_schema: Record<string, unknown>;
  scopes: string[];
  auto_executable: boolean;
  approval_required: boolean;
  supports_schedule: boolean;
  supports_compensation: boolean;
}

export interface AgentStep {
  id: number;
  goal_id: number;
  sequence: number;
  depends_on_step_id?: number;
  capability: string;
  arguments: Record<string, unknown> | string;
  preconditions: Record<string, unknown> | string;
  compensation: Record<string, unknown> | string;
  status: string;
  risk_level: string;
  idempotency_key: string;
  scheduled_for?: string;
  expires_at?: string;
  lease_owner?: string;
  lease_until?: string;
  attempt_count: number;
  max_attempts: number;
  before_state?: string;
  after_state?: string;
  result?: string;
  last_error?: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
}

export interface AgentGoal {
  id: number;
  parent_goal_id?: number;
  conversation_id?: number;
  title: string;
  objective: string;
  status: string;
  lane: "interactive" | "background";
  priority: number;
  risk_level: string;
  source: string;
  context: Record<string, unknown> | string;
  plan_hash?: string;
  created_by: string;
  deadline_at?: string;
  last_error?: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
  lease_owner?: string;
  lease_until?: string;
  next_runnable_at?: string;
  steps?: AgentStep[];
}

export interface AgentScheduledTask {
  id: number;
  goal_id?: number;
  step_id?: number;
  capability: string;
  arguments: Record<string, unknown> | string;
  conditions: Record<string, unknown> | string;
  status: string;
  timezone: string;
  execute_at: string;
  expires_at?: string;
  idempotency_key: string;
  lease_owner?: string;
  lease_until?: string;
  attempt_count: number;
  max_attempts: number;
  result?: string;
  last_error?: string;
  created_by: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
}

export interface AgentMemory {
  id: number;
  scope_type: string;
  scope_id: string;
  kind: string;
  key: string;
  summary: string;
  content: string;
  importance: number;
  pinned: boolean;
  expires_at?: string;
  created_at: string;
  updated_at: string;
}

export type AgentFreezeMode = "active" | "agent_paused" | "read_only" | "writes_frozen";

export interface AgentFreezeState {
  scope_type: string;
  scope_id: string;
  mode: AgentFreezeMode;
  reason: string;
  actor: string;
  expires_at?: string;
  created_at?: string;
  updated_at?: string;
}

export interface AgentRuntimeEvent {
  id: number;
  event_key: string;
  goal_id?: number;
  step_id?: number;
  type: string;
  severity: string;
  actor: string;
  payload: Record<string, unknown> | string;
  created_at: string;
}

export interface AgentCommandReceipt {
  conversation_id?: number;
  goal_id?: number;
  run_id?: number;
  status?: string;
  run?: AgentRun;
}

export interface AgentChatIntent {
  intent_type: "query" | "analysis" | "direct_action" | "policy_change" | "scheduled_action" | "ambiguous";
  resource_type: string;
  resource_ids?: string[];
  operation: string;
  duration_seconds?: number;
  expires_at?: string;
  scheduled_at?: string;
  timezone?: string;
  read_only: boolean;
  requires_confirmation: boolean;
  risk_level: string;
  user_facing_summary: string;
  clarification?: string;
}

export interface AgentChatReceipt extends AgentCommandReceipt {
  conversation_id: number;
  intent: AgentChatIntent;
  confirmation_token?: string;
  confirmation_expires_at?: string;
}

export interface AgentGoalList {
  items: AgentGoal[];
  steps: AgentStep[];
}

export interface AgentOverview {
  settings: AgentSettings;
  providers: AgentProvider[];
  runs: AgentRun[];
  assessments: AvailabilityAssessment[];
  daily_reports: AgentDailyReport[];
  policy_versions: ScorePolicyVersion[];
  packets: AnalysisPacket[];
  tool_calls: AgentToolCall[];
  running: boolean;
  next_run_at?: string;
}
