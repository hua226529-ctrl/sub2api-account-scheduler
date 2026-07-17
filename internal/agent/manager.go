package agent

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/telemetry"
)

type ProviderInput struct {
	Slot            string  `json:"slot"`
	BaseURL         string  `json:"base_url"`
	APIKey          string  `json:"api_key"`
	Model           string  `json:"model"`
	Enabled         bool    `json:"enabled"`
	TimeoutSeconds  int     `json:"timeout_seconds"`
	MaxOutputTokens int     `json:"max_output_tokens"`
	Temperature     float64 `json:"temperature"`
}

type Overview struct {
	Settings    model.AgentSettings            `json:"settings"`
	Providers   []model.AgentProvider          `json:"providers"`
	Runs        []model.AgentRun               `json:"runs"`
	Assessments []model.AvailabilityAssessment `json:"assessments"`
	Reports     []model.AgentDailyReport       `json:"daily_reports"`
	Policies    []model.ScorePolicyVersion     `json:"policy_versions"`
	Packets     []model.AnalysisPacket         `json:"packets"`
	ToolCalls   []model.AgentToolCall          `json:"tool_calls"`
	Running     bool                           `json:"running"`
	NextRunAt   *time.Time                     `json:"next_run_at,omitempty"`
}

type Manager struct {
	store     *store.Store
	engine    *reconcile.Engine
	balances  *balance.Manager
	telemetry *telemetry.Manager
	box       *balance.SecretBox
	logger    *slog.Logger
	builder   packetBuilder
	client    completionClient

	interactiveWake      chan struct{}
	backgroundWake       chan struct{}
	interactiveModelSlot chan struct{}
	backgroundModelSlot  chan struct{}
	workerID             string
	onGoalClaimed        func(string, int64)
	onModelSlotWait      func(string)
	randomReader         io.Reader
}

func NewManager(database *store.Store, engine *reconcile.Engine, balances *balance.Manager, box *balance.SecretBox, logger *slog.Logger, telemetryManagers ...*telemetry.Manager) *Manager {
	var telemetryManager *telemetry.Manager
	if len(telemetryManagers) > 0 {
		telemetryManager = telemetryManagers[0]
	}
	manager := &Manager{store: database, engine: engine, balances: balances, telemetry: telemetryManager, box: box, logger: logger,
		interactiveWake: make(chan struct{}, 1), backgroundWake: make(chan struct{}, 1),
		interactiveModelSlot: make(chan struct{}, 1), backgroundModelSlot: make(chan struct{}, 1),
		workerID: fmt.Sprintf("agent-%d", time.Now().UTC().UnixNano()), randomReader: cryptorand.Reader}
	manager.builder = packetBuilder{store: database, engine: engine, balances: balances, telemetry: telemetryManager}
	return manager
}

func (m *Manager) Start(ctx context.Context) {
	if summary, err := m.store.RecoverAgentV2State(ctx, time.Now().UTC()); err != nil {
		m.logger.Error("agent_v2_recovery_failed", "error", err)
	} else if summary.ReconcilingSteps+summary.ReconcilingCommands+summary.ExpiredCommands+summary.FailedCommands > 0 {
		m.logger.Warn("agent_v2_recovered", "summary", summary)
	}
	go m.runtimeWorker(ctx, model.AgentLaneInteractive)
	go m.runtimeWorker(ctx, model.AgentLaneBackground)
	go m.scheduledCommandWorker(ctx)
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		m.tick(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.tick(ctx)
			}
		}
	}()
}

func (m *Manager) tick(ctx context.Context) {
	m.bridgeOperationalEvents(ctx)
	settings, err := m.store.GetAgentSettings(ctx)
	if err != nil {
		return
	}
	if err := m.syncPoolPolicyProjections(ctx); err != nil {
		m.recordEvent(ctx, "pool_policy_sync_failed", "error", 0, err.Error(), 0)
	}
	now := time.Now().UTC()
	if now.Hour() == 18 && now.Minute() < 2 {
		before := now.AddDate(0, 0, -settings.RetentionDays)
		_ = m.store.CleanupAgentData(ctx, before)
		_ = m.store.CleanupAgentV2Data(ctx, before)
	}
	m.evaluatePolicyRollbacks(ctx, now)
	if settings.OptimizerMode == model.AgentOptimizerAuto {
		m.activateEligibleOptimizerProposal(ctx)
	}
	if settings.OptimizerMode == model.AgentOptimizerDisabled {
		return
	}
	interval := time.Duration(settings.AnalysisIntervalMinutes) * time.Minute
	if settings.LastScheduledAt == nil || now.Sub(*settings.LastScheduledAt) >= interval {
		_, _ = m.EnqueueAnalysisGoal(ctx, model.AgentRunScheduled, "固定30分钟全量分析", 55)
	}
	if reason := m.emergencyReason(now); reason != "" &&
		(settings.LastEmergencyAt == nil || now.Sub(*settings.LastEmergencyAt) >= time.Duration(settings.EmergencyCooldownMinutes)*time.Minute) {
		_, _ = m.EnqueueAnalysisGoal(ctx, model.AgentRunEmergency, reason, 95)
	}
	m.maybeDaily(ctx, now)
	m.evaluateOutcomes(ctx, now)
}

func (m *Manager) activateEligibleOptimizerProposal(ctx context.Context) {
	versions, err := m.store.ListPolicyLifecycle(ctx, 100)
	if err != nil {
		return
	}
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.Status != model.PolicyStatusSimulated || version.RiskLevel != model.AgentRiskLow || version.SourceGoalID == nil {
			continue
		}
		goal, err := m.store.GetAgentGoal(ctx, *version.SourceGoalID)
		if err != nil || goal.Lane != model.AgentLaneBackground || goal.Source == "administrator" {
			continue
		}
		if err := m.ActivatePolicyProposal(ctx, version.ID, "", true); err != nil && m.logger != nil {
			m.logger.Warn("optimizer_policy_auto_activation_skipped", "policy_id", version.ID, "error", err)
		}
		return
	}
}

func (m *Manager) syncPoolPolicyProjections(ctx context.Context) error {
	versions, err := m.store.ListActivePolicyVersions(ctx)
	if err != nil {
		return err
	}
	activePools := make(map[string]model.ScorePolicyVersion)
	for _, version := range versions {
		if version.ScopeType == "pool" {
			activePools[version.ScopeID] = version
		}
	}
	sources, err := m.balances.List(ctx)
	if err != nil {
		return err
	}
	poolsByEndpoint := make(map[string]string, len(sources))
	for _, source := range sources {
		name := source.RoutingPool
		if name == "" {
			name = source.Name
		}
		poolsByEndpoint[source.NormalizedURL] = name
	}
	updates := make([]model.Policy, 0)
	for _, binding := range m.engine.Snapshot().Bindings {
		policy := binding.Policy
		if policy.ScorePolicySource == "account" || policy.ScorePolicySource == "account_version" ||
			(policy.ScorePolicySource == "" && hasScoreOverrides(policy)) {
			continue
		}
		version, managed := activePools[poolsByEndpoint[binding.NormalizedEndpoint]]
		if !managed {
			if policy.ScorePolicySource != "pool" {
				continue
			}
			clearScoreOverrides(&policy)
			policy.AccountID = binding.Account.ID
			updates = append(updates, policy)
			continue
		}
		if policy.ScorePolicySource == "pool" && policy.ScorePolicyVersionID != nil && *policy.ScorePolicyVersionID == version.ID {
			continue
		}
		if policy.AccountID == 0 {
			policy = model.Policy{AccountID: binding.Account.ID, Enabled: true}
		}
		if err := validateDispatchPolicyPatch("pool", version.Config); err != nil {
			return fmt.Errorf("上游池策略版本 %d 无效: %w", version.ID, err)
		}
		if err := mergeJSON(version.Config, &policy); err != nil {
			return err
		}
		policy.AccountID = binding.Account.ID
		policy.ScorePolicySource = "pool"
		policy.ScorePolicyVersionID = &version.ID
		updates = append(updates, policy)
	}
	if len(updates) == 0 {
		return nil
	}
	return m.engine.UpdatePolicies(ctx, updates, "agent:policy-sync")
}

func clearScoreOverrides(policy *model.Policy) {
	policy.ScorePolicySource, policy.ScorePolicyVersionID = "", nil
	policy.FailureThreshold, policy.RecoveryThreshold = nil, nil
	policy.FlapEnabled, policy.FlapWindowMinutes, policy.FlapPauseThreshold, policy.FlapRecoveryThreshold = nil, nil, nil, nil
	policy.HealthHealthyScore, policy.HealthWatchScore, policy.HealthQuarantineScore, policy.HealthMinSamples = nil, nil, nil, nil
	policy.HealthLatencyWarningMS, policy.HealthLatencyCriticalMS = nil, nil
	policy.HealthTrafficPauseBelow, policy.HealthTrafficHealthyAt = nil, nil
	policy.HealthHardFailures10, policy.HealthPersistentSlowRate = nil, nil
}

func hasScoreOverrides(policy model.Policy) bool {
	return policy.FailureThreshold != nil || policy.RecoveryThreshold != nil || policy.FlapEnabled != nil ||
		policy.FlapWindowMinutes != nil || policy.FlapPauseThreshold != nil || policy.FlapRecoveryThreshold != nil ||
		policy.HealthHealthyScore != nil || policy.HealthWatchScore != nil || policy.HealthQuarantineScore != nil ||
		policy.HealthMinSamples != nil || policy.HealthLatencyWarningMS != nil || policy.HealthLatencyCriticalMS != nil ||
		policy.HealthTrafficPauseBelow != nil || policy.HealthTrafficHealthyAt != nil || policy.HealthHardFailures10 != nil ||
		policy.HealthPersistentSlowRate != nil
}

func (m *Manager) Overview(ctx context.Context) (Overview, error) {
	var result Overview
	settings, err := m.store.GetAgentSettings(ctx)
	if err != nil {
		return result, err
	}
	providers, err := m.store.ListAgentProviders(ctx)
	if err != nil {
		return result, err
	}
	for i := range providers {
		providers[i].CredentialNonce = nil
		providers[i].CredentialCiphertext = nil
	}
	runs, _ := m.store.ListAgentRuns(ctx, 40)
	assessments, _ := m.store.ListLatestAvailabilityAssessments(ctx)
	reports, _ := m.store.ListDailyReports(ctx, 30)
	policies, _ := m.store.ListPolicyLifecycle(ctx, 100)
	packets, _ := m.store.ListAnalysisPackets(ctx, 20)
	toolCalls := []model.AgentToolCall{}
	if len(runs) > 0 {
		toolCalls, _ = m.store.ListAgentToolCalls(ctx, runs[0].ID)
	}
	runningGoals, _ := m.store.ListAgentGoals(ctx, model.AgentGoalStatusRunning, 1)
	result = Overview{Settings: settings, Providers: providers, Runs: runs, Assessments: assessments,
		Reports: reports, Policies: policies, Packets: packets, ToolCalls: toolCalls, Running: len(runningGoals) > 0}
	if settings.LastScheduledAt != nil {
		next := settings.LastScheduledAt.Add(time.Duration(settings.AnalysisIntervalMinutes) * time.Minute)
		result.NextRunAt = &next
	}
	return result, nil
}

func (m *Manager) UpdateSettings(ctx context.Context, settings model.AgentSettings) error {
	current, err := m.store.GetAgentSettings(ctx)
	if err != nil {
		return err
	}
	settings.SuccessfulObservationRuns = current.SuccessfulObservationRuns
	settings.ObservationProposedActions = current.ObservationProposedActions
	settings.ObservationExecutableActions = current.ObservationExecutableActions
	settings.ObservationViolations = current.ObservationViolations
	settings.ObservationStructureErrors = current.ObservationStructureErrors
	settings.LastScheduledAt, settings.LastEmergencyAt = current.LastScheduledAt, current.LastEmergencyAt
	if settings.OptimizerMode == model.AgentOptimizerObserve &&
		(current.OptimizerMode != model.AgentOptimizerObserve || current.ObservationStartedAt == nil) {
		now := time.Now().UTC()
		settings.ObservationStartedAt = &now
		settings.SuccessfulObservationRuns = 0
		settings.ObservationProposedActions, settings.ObservationExecutableActions = 0, 0
		settings.ObservationViolations, settings.ObservationStructureErrors = 0, 0
	} else if settings.OptimizerMode == model.AgentOptimizerObserve {
		settings.ObservationStartedAt = current.ObservationStartedAt
	} else {
		settings.ObservationStartedAt = current.ObservationStartedAt
	}
	if err := m.store.UpdateAgentSettings(ctx, settings); err != nil {
		return err
	}
	if current.OptimizerMode != settings.OptimizerMode || current.OperatorMode != settings.OperatorMode ||
		current.DailyPolicyChangeBudget != settings.DailyPolicyChangeBudget {
		m.recordEvent(ctx, "agent_operating_modes_updated", "warning", 0,
			fmt.Sprintf("Optimizer %s -> %s; Operator %s -> %s; daily budget %d -> %d",
				current.OptimizerMode, settings.OptimizerMode, current.OperatorMode, settings.OperatorMode,
				current.DailyPolicyChangeBudget, settings.DailyPolicyChangeBudget), 0)
	}
	return nil
}

func (m *Manager) ValidateProvider(ctx context.Context, input ProviderInput) (model.AgentProvider, error) {
	provider, key, err := m.providerFromInput(ctx, input, false)
	if err != nil {
		return provider, err
	}
	validationPrompt := `你是接口能力测试器。只返回 JSON：{"summary":"连接正常","conclusion":"模型支持结构化输出","confidence":1,"no_change":true,"actions":[],"advice":[],"data_limitations":[]}`
	decision, err := m.client.Complete(ctx, provider, key, validationPrompt, "执行一次结构化输出能力测试。")
	if err != nil {
		return provider, err
	}
	if decision.Summary == "" {
		return provider, errors.New("模型没有返回有效结构化结果")
	}
	now := time.Now().UTC()
	provider.LastValidatedAt = &now
	provider.LastError = ""
	provider.APIKeyConfigured = true
	return provider, nil
}

func (m *Manager) SaveProvider(ctx context.Context, input ProviderInput) (model.AgentProvider, error) {
	previous, previousErr := m.store.GetAgentProvider(ctx, strings.TrimSpace(input.Slot))
	validated, err := m.ValidateProvider(ctx, input)
	if err != nil {
		return model.AgentProvider{}, err
	}
	_, key, err := m.providerFromInput(ctx, input, false)
	if err != nil {
		return model.AgentProvider{}, err
	}
	if m.box == nil {
		return model.AgentProvider{}, errors.New("AGENT_CREDENTIAL_KEY 未配置")
	}
	nonce, ciphertext, err := m.box.Encrypt([]byte(key))
	if err != nil {
		return model.AgentProvider{}, err
	}
	validated.CredentialNonce, validated.CredentialCiphertext = nonce, ciphertext
	if err := m.store.UpsertAgentProvider(ctx, validated); err != nil {
		return model.AgentProvider{}, err
	}
	validated.CredentialNonce, validated.CredentialCiphertext = nil, nil
	validated.APIKeyConfigured = true
	if previousErr != nil || previous.BaseURL != validated.BaseURL || previous.Model != validated.Model || strings.TrimSpace(input.APIKey) != "" {
		now := time.Now().UTC()
		_ = m.store.ResetAgentObservation(ctx, &now)
	}
	return validated, nil
}

func (m *Manager) providerFromInput(ctx context.Context, input ProviderInput, allowDisabled bool) (model.AgentProvider, string, error) {
	provider := model.AgentProvider{Slot: strings.TrimSpace(input.Slot), BaseURL: strings.TrimSpace(input.BaseURL),
		Model: strings.TrimSpace(input.Model), Enabled: input.Enabled, TimeoutSeconds: input.TimeoutSeconds,
		MaxOutputTokens: input.MaxOutputTokens, Temperature: input.Temperature}
	if provider.Slot != "primary" && provider.Slot != "fallback" {
		return provider, "", errors.New("模型位置无效")
	}
	if provider.BaseURL == "" || provider.Model == "" {
		return provider, "", errors.New("模型接口和模型名称不能为空")
	}
	if provider.TimeoutSeconds < 10 || provider.TimeoutSeconds > 300 {
		provider.TimeoutSeconds = 90
	}
	if provider.MaxOutputTokens < 512 || provider.MaxOutputTokens > 32768 {
		provider.MaxOutputTokens = 4096
	}
	if provider.Temperature < 0 || provider.Temperature > 1 {
		provider.Temperature = .1
	}
	key := strings.TrimSpace(input.APIKey)
	if key == "" {
		current, err := m.store.GetAgentProvider(ctx, provider.Slot)
		if err != nil {
			return provider, "", errors.New("请输入模型 API 密钥")
		}
		if m.box == nil {
			return provider, "", errors.New("AGENT_CREDENTIAL_KEY 未配置")
		}
		plaintext, err := m.box.Decrypt(current.CredentialNonce, current.CredentialCiphertext)
		if err != nil {
			return provider, "", err
		}
		key = string(plaintext)
		provider.CredentialNonce, provider.CredentialCiphertext = current.CredentialNonce, current.CredentialCiphertext
	}
	_ = allowDisabled
	return provider, key, nil
}

func (m *Manager) Messages(ctx context.Context, conversationID int64) ([]model.AgentMessage, error) {
	return m.store.ListAgentMessages(ctx, conversationID, 80)
}

func (m *Manager) ActivatePolicy(ctx context.Context, id int64, actor string) error {
	if id <= 0 {
		return errors.New("策略版本编号无效")
	}
	if err := m.ActivatePolicyProposal(ctx, id, actor, false); err != nil {
		return err
	}
	m.recordEvent(ctx, "agent_policy_activated", "warning", 0, "已由"+actor+"批准并激活评分策略提案", 0)
	return nil
}

func (m *Manager) RejectPolicy(ctx context.Context, id int64, actor, reason string) error {
	if err := m.store.RejectPolicyProposal(ctx, id, actor, reason); err != nil {
		return err
	}
	m.recordEvent(ctx, "agent_policy_rejected", "info", 0, fmt.Sprintf("策略提案 %d 已被拒绝", id), 0)
	return nil
}

func modelInput(packet model.AnalysisPacket, settings model.AgentSettings) (string, error) {
	budget := settings.ContextTokenBudget
	if budget < 2000 {
		budget = 16000
	}
	working := packet
	working.AccountCompactStates = append([]model.AgentAccountState{}, packet.AccountCompactStates...)
	working.Anomalies = append([]model.AgentAccountState{}, packet.Anomalies...)
	working.Changes = append([]string{}, packet.Changes...)
	working.EvidenceCatalog = append([]string{}, packet.EvidenceCatalog...)
	if packet.NoMaterialChange {
		filtered := make([]model.AgentAccountState, 0)
		for _, item := range working.AccountCompactStates {
			if item.Changed || item.AvailabilityState != "available" || item.RiskScore >= 20 {
				filtered = append(filtered, item)
			}
		}
		working.AccountCompactStates = filtered
		working.Changes = []string{"与上一份同类数据包相比无重要变化"}
	}
	payload, err := json.Marshal(compactPacketForModel(working))
	if err != nil {
		return "", err
	}
	if len(payload)/4 > budget {
		working.EvidenceCatalog = nil
		working.DecisionOutcomes = json.RawMessage("[]")
		if len(working.Changes) > 20 {
			working.Changes = working.Changes[:20]
		}
		if len(working.Anomalies) > 5 {
			working.Anomalies = working.Anomalies[:5]
		}
		prioritizeAccounts(working.AccountCompactStates)
		payload, _ = json.Marshal(compactPacketForModel(working))
	}
	for len(payload)/4 > budget && len(working.AccountCompactStates) > 0 {
		keep := len(working.AccountCompactStates) * 3 / 4
		if keep == len(working.AccountCompactStates) {
			keep--
		}
		working.AccountCompactStates = working.AccountCompactStates[:keep]
		payload, _ = json.Marshal(compactPacketForModel(working))
	}
	if len(payload)/4 > budget {
		return "", fmt.Errorf("压缩后的分析数据包仍超过上下文预算：约 %d/%d 个令牌", len(payload)/4, budget)
	}
	return "以下是本地统计器生成的不可变分析数据包。只能依据数据包作出结论，不得假设未提供的数据：\n" + string(payload), nil
}

func prioritizeAccounts(items []model.AgentAccountState) {
	sort.SliceStable(items, func(i, j int) bool {
		leftPriority := items[i].Changed || items[i].AvailabilityState != "available"
		rightPriority := items[j].Changed || items[j].AvailabilityState != "available"
		if leftPriority != rightPriority {
			return leftPriority
		}
		if items[i].RiskScore != items[j].RiskScore {
			return items[i].RiskScore > items[j].RiskScore
		}
		return items[i].AccountID < items[j].AccountID
	})
}

func (m *Manager) advanceSchedule(ctx context.Context, settings model.AgentSettings, kind string, completed time.Time) {
	_ = settings
	_, _, err := m.store.AdvanceAgentSchedule(ctx, kind, completed)
	if err != nil {
		m.recordEvent(ctx, "agent_schedule_update_failed", "error", 0, err.Error(), 0)
		return
	}
}

func (m *Manager) emergencyReason(now time.Time) string {
	snapshot := m.engine.Snapshot()
	if snapshot.LastSyncError != "" {
		return "Sub2API或调度数据源失联"
	}
	if snapshot.LastSyncAt == nil || now.Sub(snapshot.LastSyncAt.UTC()) > 3*time.Minute {
		return "账号与渠道快照超过三个读取周期未更新"
	}
	if lastBalance := m.balances.LastRunAt(); lastBalance != nil && now.Sub(lastBalance.UTC()) > 30*time.Minute {
		return "余额、倍率和令牌分组数据已失联"
	}
	if m.telemetry != nil {
		lastTelemetry, telemetryError := m.telemetry.Status()
		if telemetryError != "" || lastTelemetry == nil || now.Sub(lastTelemetry.UTC()) > 6*time.Minute {
			return "监控历史与真实请求证据已失联"
		}
	}
	active, hardFailures, credentialFailures, trafficFailures := 0, 0, 0, 0
	for _, binding := range snapshot.Bindings {
		if binding.Account.Status == "active" && binding.Account.Schedulable {
			active++
		}
		if binding.Decision != nil && binding.Decision.HardFailureStreak >= 3 {
			hardFailures++
		}
		if binding.Decision != nil && binding.Decision.ErrorCategoryCounts[model.ErrorClassCredential] > 0 {
			credentialFailures++
		}
		if binding.Decision != nil && binding.Decision.TrafficSampleCount >= 20 && binding.Decision.TrafficSuccessRate < 80 {
			trafficFailures++
		}
	}
	if credentialFailures > 0 {
		return fmt.Sprintf("检测到%d个账号出现凭据拒绝", credentialFailures)
	}
	if len(snapshot.Bindings) > 0 && active == 0 {
		return "全部账号池已无可调度账号"
	}
	if hardFailures >= 2 {
		return fmt.Sprintf("检测到%d个账号连续硬失败", hardFailures)
	}
	if trafficFailures >= 2 {
		return fmt.Sprintf("检测到%d个账号真实请求成功率骤降", trafficFailures)
	}
	return ""
}

func (m *Manager) maybeDaily(ctx context.Context, nowUTC time.Time) {
	location := time.FixedZone("Asia/Shanghai", 8*60*60)
	local := nowUTC.In(location)
	if local.Hour() == 0 && local.Minute() < 10 {
		return
	}
	reportDate := local.AddDate(0, 0, -1).Format("2006-01-02")
	report, err := m.store.GetDailyReport(ctx, reportDate)
	attempts := 0
	if err == nil {
		if report.Status == "completed" || report.Attempts >= 3 {
			return
		}
		attempts = report.Attempts
		if report.Status == "running" && nowUTC.Sub(report.UpdatedAt) < 20*time.Minute {
			return
		}
		delay := 10 * time.Minute
		if attempts >= 2 {
			delay = 20 * time.Minute
		}
		if report.Status == "failed" && nowUTC.Sub(report.UpdatedAt) < delay {
			return
		}
	}
	dayEndLocal := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	cutoff := dayEndLocal.UTC()
	attempts++
	pending := model.AgentDailyReport{ReportDate: reportDate, Status: "running", Attempts: attempts,
		Summary: "日报生成中", MetricsJSON: json.RawMessage("{}"), AdviceJSON: json.RawMessage("[]")}
	_ = m.store.UpsertDailyReport(ctx, &pending)
	if _, queueErr := m.enqueueDailyGoal(ctx, reportDate, cutoff); queueErr != nil {
		failed := model.AgentDailyReport{ReportDate: reportDate, Status: "failed", Attempts: attempts,
			Summary: "日报任务创建失败", Error: queueErr.Error(), MetricsJSON: json.RawMessage("{}"), AdviceJSON: json.RawMessage("[]")}
		_ = m.store.UpsertDailyReport(ctx, &failed)
	}
}

func (m *Manager) saveDailyReport(ctx context.Context, run model.AgentRun, packet model.AnalysisPacket, decision ModelDecision, reportDate string) {
	if reportDate == "" {
		location := time.FixedZone("Asia/Shanghai", 8*60*60)
		reportDate = time.Now().In(location).AddDate(0, 0, -1).Format("2006-01-02")
	}
	attempts := 1
	if current, err := m.store.GetDailyReport(ctx, reportDate); err == nil && current.Attempts > 0 {
		attempts = current.Attempts
	}
	metrics, _ := json.Marshal(packet.SystemSummary)
	advice, _ := json.Marshal(decision.Advice)
	item := model.AgentDailyReport{ReportDate: reportDate, PacketID: &packet.ID, Status: "completed", Attempts: attempts,
		Summary: decision.Summary + "\n\n" + decision.Conclusion, MetricsJSON: metrics, AdviceJSON: advice}
	_ = m.store.UpsertDailyReport(ctx, &item)
}

func (m *Manager) evaluateOutcomes(ctx context.Context, now time.Time) {
	items, err := m.store.ListPendingDecisionOutcomes(ctx, now, 100)
	if err != nil {
		return
	}
	for _, item := range items {
		if item.AccountID == nil {
			continue
		}
		packetID, err := m.store.GetAgentRunPacketID(ctx, item.RunID)
		if err != nil {
			continue
		}
		packet, err := m.store.GetAnalysisPacket(ctx, packetID)
		if err != nil {
			continue
		}
		var baseline model.AgentWindowStats
		for _, account := range packet.AccountCompactStates {
			if account.AccountID == *item.AccountID {
				baseline = account.Windows["30m"]
				break
			}
		}
		current, err := m.store.GetAgentWindowStats(ctx, *item.AccountID, item.CreatedAt, now, "post_action")
		if err != nil {
			continue
		}
		successDelta := current.SuccessRate - baseline.SuccessRate
		latencyDelta := current.P90DurationMS - baseline.P90DurationMS
		item.ActualSuccessRateDelta, item.ActualLatencyDeltaMS = &successDelta, &latencyDelta
		verdict := "mixed"
		if sameDirection(item.PredictedSuccessRateDelta, successDelta) && sameDirection(float64(item.PredictedLatencyDeltaMS), float64(latencyDelta)) {
			verdict = "matched"
		} else if item.PredictedSuccessRateDelta != 0 && !sameDirection(item.PredictedSuccessRateDelta, successDelta) {
			verdict = "missed"
		}
		item.Verdict = verdict
		evaluated := now
		item.EvaluatedAt = &evaluated
		_ = m.store.CompleteDecisionOutcome(ctx, item)
	}
}

func sameDirection(predicted, actual float64) bool {
	if predicted == 0 {
		return true
	}
	return (predicted > 0 && actual >= 0) || (predicted < 0 && actual <= 0)
}

func (m *Manager) recordEvent(ctx context.Context, eventType, severity string, accountID int64, message string, runID int64) {
	details, _ := json.Marshal(map[string]any{"agent_run_id": runID})
	event := model.Event{Type: eventType, Severity: severity, Message: message, Details: string(details), Actor: "agent", CreatedAt: time.Now().UTC()}
	if accountID > 0 {
		event.AccountID = &accountID
	}
	_ = m.store.AddEvent(ctx, event)
}
