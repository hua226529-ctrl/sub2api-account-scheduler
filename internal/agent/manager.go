package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
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

	mu          sync.Mutex
	running     bool
	runtimeMu   sync.Mutex
	runtimeWake chan struct{}
	workerID    string
}

var errAgentAlreadyRunning = errors.New("智能体正在执行另一项目标")

func (m *Manager) beginRun() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return false
	}
	m.running = true
	return true
}

func (m *Manager) endRun() {
	m.mu.Lock()
	m.running = false
	m.mu.Unlock()
}

func NewManager(database *store.Store, engine *reconcile.Engine, balances *balance.Manager, box *balance.SecretBox, logger *slog.Logger, telemetryManagers ...*telemetry.Manager) *Manager {
	var telemetryManager *telemetry.Manager
	if len(telemetryManagers) > 0 {
		telemetryManager = telemetryManagers[0]
	}
	manager := &Manager{store: database, engine: engine, balances: balances, telemetry: telemetryManager, box: box, logger: logger,
		runtimeWake: make(chan struct{}, 1), workerID: fmt.Sprintf("agent-%d", time.Now().UTC().UnixNano())}
	manager.builder = packetBuilder{store: database, engine: engine, balances: balances, telemetry: telemetryManager}
	return manager
}

func (m *Manager) Start(ctx context.Context) {
	if summary, err := m.store.RecoverAgentV2State(ctx, time.Now().UTC()); err != nil {
		m.logger.Error("agent_v2_recovery_failed", "error", err)
	} else if summary.ReconcilingSteps+summary.ReconcilingCommands+summary.ExpiredCommands+summary.FailedCommands > 0 {
		m.logger.Warn("agent_v2_recovered", "summary", summary)
	}
	go m.runtimeWorker(ctx)
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
	if !settings.Enabled {
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
	policies, _ := m.store.ListPolicyVersions(ctx, 100)
	packets, _ := m.store.ListAnalysisPackets(ctx, 20)
	toolCalls := []model.AgentToolCall{}
	if len(runs) > 0 {
		toolCalls, _ = m.store.ListAgentToolCalls(ctx, runs[0].ID)
	}
	m.mu.Lock()
	running := m.running
	m.mu.Unlock()
	result = Overview{Settings: settings, Providers: providers, Runs: runs, Assessments: assessments,
		Reports: reports, Policies: policies, Packets: packets, ToolCalls: toolCalls, Running: running}
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
	if !settings.Enabled {
		settings.Mode = model.AgentModeObserve
		settings.ObservationStartedAt = nil
		settings.SuccessfulObservationRuns = 0
		settings.ObservationProposedActions, settings.ObservationExecutableActions = 0, 0
		settings.ObservationViolations, settings.ObservationStructureErrors = 0, 0
	} else if !current.Enabled || current.ObservationStartedAt == nil {
		now := time.Now().UTC()
		settings.ObservationStartedAt = &now
		settings.SuccessfulObservationRuns = 0
		settings.ObservationProposedActions, settings.ObservationExecutableActions = 0, 0
		settings.ObservationViolations, settings.ObservationStructureErrors = 0, 0
		settings.Mode = model.AgentModeObserve
	} else {
		settings.ObservationStartedAt = current.ObservationStartedAt
		if current.Mode == model.AgentModeControl && settings.Mode == model.AgentModeObserve {
			now := time.Now().UTC()
			settings.ObservationStartedAt = &now
			settings.SuccessfulObservationRuns = 0
			settings.ObservationProposedActions, settings.ObservationExecutableActions = 0, 0
			settings.ObservationViolations, settings.ObservationStructureErrors = 0, 0
		}
		if settings.Mode == model.AgentModeControl && current.Mode != model.AgentModeControl {
			settings.Mode = model.AgentModeObserve
		}
	}
	return m.store.UpdateAgentSettings(ctx, settings)
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

func (m *Manager) Run(ctx context.Context, kind, trigger string, conversationID *int64, userMessage string) (model.AgentRun, error) {
	return m.run(ctx, kind, trigger, conversationID, userMessage, nil, "")
}

func (m *Manager) run(ctx context.Context, kind, trigger string, conversationID *int64, userMessage string, cutoff *time.Time, reportDate string) (model.AgentRun, error) {
	if !m.beginRun() {
		return model.AgentRun{}, errors.New("智能体正在执行另一项分析")
	}
	defer m.endRun()

	settings, err := m.store.GetAgentSettings(ctx)
	if err != nil {
		return model.AgentRun{}, err
	}
	if !settings.Enabled && kind != model.AgentRunManual {
		return model.AgentRun{}, errors.New("智能体未启用")
	}
	var packet model.AnalysisPacket
	if cutoff != nil {
		packet, err = m.builder.BuildAt(ctx, kind, settings, *cutoff)
	} else {
		packet, err = m.builder.Build(ctx, kind, settings)
	}
	if err != nil {
		return model.AgentRun{}, err
	}
	packetID := packet.ID
	run := model.AgentRun{Kind: kind, Trigger: trigger, Status: "running", PacketID: &packetID,
		ConversationID: conversationID, StartedAt: time.Now().UTC(), ActionsJSON: json.RawMessage("[]")}
	if err := m.store.CreateAgentRun(ctx, &run); err != nil {
		return run, err
	}
	if kind == model.AgentRunDaily {
		userMessage = "根据数据包中截至北京时间日界的最近24小时统计生成上一自然日总结，包含可用率、延迟、成本、容量、动作效果、预测准确度和迭代建议；只生成报告，不返回任何执行动作。"
	}
	decision, provider, err := m.callModel(ctx, run.ID, packet, settings, userMessage, conversationID)
	if err != nil {
		completedAt := time.Now().UTC()
		run.CompletedAt = &completedAt
		run.Status, run.Error = "failed", err.Error()
		_ = m.store.UpdateAgentRun(ctx, run)
		m.recordEvent(ctx, "agent_run_failed", "error", 0, err.Error(), run.ID)
		return run, err
	}
	if err := m.validateDecision(packet, decision); err != nil {
		completedAt := time.Now().UTC()
		run.CompletedAt = &completedAt
		run.ProviderSlot, run.Model = provider.Slot, provider.Model
		run.Status, run.Error = "rejected", err.Error()
		_ = m.store.UpdateAgentRun(ctx, run)
		m.recordEvent(ctx, "agent_decision_rejected", "error", 0, err.Error(), run.ID)
		return run, err
	}
	if kind == model.AgentRunDaily && len(decision.Actions) > 0 {
		err := errors.New("日报模型返回了执行动作，已拒绝整次结果")
		completedAt := time.Now().UTC()
		run.CompletedAt, run.ProviderSlot, run.Model = &completedAt, provider.Slot, provider.Model
		run.Status, run.Error = "rejected", err.Error()
		_ = m.store.UpdateAgentRun(ctx, run)
		return run, err
	}
	run.ProviderSlot, run.Model = provider.Slot, provider.Model
	run.Summary, run.Conclusion, run.Confidence = decision.Summary, decision.Conclusion, decision.Confidence
	run.ActionsJSON, _ = json.Marshal(decision.Actions)
	run.Status = "acting"
	if err := m.store.UpdateAgentRun(ctx, run); err != nil {
		return run, err
	}

	if kind != model.AgentRunDaily {
		actionSettings := settings
		if decision.Confidence < .65 {
			actionSettings.Mode = model.AgentModeObserve
			m.recordEvent(ctx, "agent_low_confidence", "warning", 0, "模型置信度不足，所有动作仅记录为提案", run.ID)
		}
		m.executeActions(ctx, run, actionSettings, decision.Actions)
	}
	if conversationID != nil {
		content := decision.Summary
		if decision.Conclusion != "" {
			content += "\n\n" + decision.Conclusion
		}
		_ = m.store.AddAgentMessage(ctx, &model.AgentMessage{ConversationID: *conversationID, Role: "assistant", Content: content, RunID: &run.ID})
	}
	if kind == model.AgentRunDaily {
		m.saveDailyReport(ctx, run, packet, decision, reportDate)
	}
	completedAt := time.Now().UTC()
	run.CompletedAt = &completedAt
	run.Status = "completed"
	if err := m.store.UpdateAgentRun(ctx, run); err != nil {
		return run, err
	}
	m.advanceSchedule(ctx, settings, kind, completedAt)
	m.recordEvent(ctx, "agent_run_completed", "info", 0, decision.Summary, run.ID)
	return run, nil
}

func (m *Manager) Chat(ctx context.Context, conversationID int64, message string) (model.AgentRun, int64, error) {
	message = strings.TrimSpace(message)
	if message == "" || len([]rune(message)) > 4000 {
		return model.AgentRun{}, conversationID, errors.New("对话内容为空或过长")
	}
	message = redactAgentText(message)
	if conversationID <= 0 {
		conversation, err := m.store.CreateConversation(ctx, message)
		if err != nil {
			return model.AgentRun{}, 0, err
		}
		conversationID = conversation.ID
	}
	if err := m.store.AddAgentMessage(ctx, &model.AgentMessage{ConversationID: conversationID, Role: "user", Content: message}); err != nil {
		return model.AgentRun{}, conversationID, err
	}
	run, err := m.Run(ctx, model.AgentRunChat, "管理员对话命令", &conversationID, message)
	return run, conversationID, err
}

func (m *Manager) Messages(ctx context.Context, conversationID int64) ([]model.AgentMessage, error) {
	return m.store.ListAgentMessages(ctx, conversationID, 80)
}

func (m *Manager) ActivatePolicy(ctx context.Context, id int64, actor string) error {
	if id <= 0 {
		return errors.New("策略版本编号无效")
	}
	if err := m.activatePolicyVersion(ctx, id, actor); err != nil {
		return err
	}
	m.recordEvent(ctx, "agent_policy_activated", "warning", 0, "已由"+actor+"激活历史评分策略版本", 0)
	return nil
}

func (m *Manager) callModel(ctx context.Context, runID int64, packet model.AnalysisPacket, settings model.AgentSettings, userMessage string, conversationID *int64) (ModelDecision, model.AgentProvider, error) {
	providers, err := m.store.ListAgentProviders(ctx)
	if err != nil {
		return ModelDecision{}, model.AgentProvider{}, err
	}
	systemPrompt := agentSystemPrompt()
	input, err := modelInput(packet, settings)
	if err != nil {
		return ModelDecision{}, model.AgentProvider{}, err
	}
	if userMessage != "" {
		input += "\n管理员当前命令：" + userMessage
	}
	if conversationID != nil {
		messages, _ := m.store.ListAgentMessages(ctx, *conversationID, 12)
		history, _ := json.Marshal(messages)
		input += "\n最近对话：" + string(history)
	}
	var failures []string
	for _, provider := range providers {
		if !provider.Enabled || provider.BaseURL == "" || provider.Model == "" || len(provider.CredentialCiphertext) == 0 {
			continue
		}
		if m.box == nil {
			failures = append(failures, provider.Slot+": 缺少模型凭据加密密钥")
			continue
		}
		plaintext, err := m.box.Decrypt(provider.CredentialNonce, provider.CredentialCiphertext)
		if err != nil {
			failures = append(failures, provider.Slot+": 凭据解密失败")
			continue
		}
		decision, err := m.client.Complete(ctx, provider, string(plaintext), systemPrompt, input)
		if err == nil {
			decision, err = m.completeEvidenceDrilldowns(ctx, runID, provider, string(plaintext), systemPrompt, input, packet, settings, decision)
		}
		if err == nil {
			now := time.Now().UTC()
			_ = m.store.UpdateAgentProviderStatus(ctx, provider.Slot, "", &now)
			return decision, provider, nil
		}
		failures = append(failures, provider.Slot+": "+err.Error())
		_ = m.store.UpdateAgentProviderStatus(ctx, provider.Slot, err.Error(), nil)
	}
	if len(failures) == 0 {
		return ModelDecision{}, model.AgentProvider{}, errors.New("没有可用的主模型或备用模型")
	}
	return ModelDecision{}, model.AgentProvider{}, errors.New(strings.Join(failures, "; "))
}

func (m *Manager) completeEvidenceDrilldowns(ctx context.Context, runID int64, provider model.AgentProvider, apiKey, systemPrompt,
	baseInput string, packet model.AnalysisPacket, settings model.AgentSettings, decision ModelDecision) (ModelDecision, error) {
	remaining := settings.MaxDrilldowns
	if remaining < 0 {
		remaining = 0
	}
	input := baseInput
	for len(decision.EvidenceRequests) > 0 {
		if remaining == 0 {
			return decision, errors.New("模型证据追查请求超过单轮限制")
		}
		if decision.Confidence >= .80 && !packetHasEvidenceConflict(packet) {
			return decision, errors.New("数据无明显冲突且置信度充足，拒绝扩大证据查询")
		}
		requests := decision.EvidenceRequests
		if len(requests) > remaining {
			requests = requests[:remaining]
		}
		results := make([]map[string]any, 0, len(requests))
		for _, request := range requests {
			result, err := m.collectEvidence(ctx, runID, packet, request)
			if err != nil {
				return decision, err
			}
			results = append(results, result)
			remaining--
		}
		payload, _ := json.Marshal(results)
		input += "\n本地证据工具返回以下脱敏结果。请据此完成最终结论；如证据已足够，evidence_requests 必须为空：\n" + string(payload)
		var err error
		decision, err = m.client.Complete(ctx, provider, apiKey, systemPrompt, input)
		if err != nil {
			return decision, err
		}
	}
	return decision, nil
}

func packetHasEvidenceConflict(packet model.AnalysisPacket) bool {
	for _, item := range packet.AccountCompactStates {
		if item.EvidenceConflict {
			return true
		}
	}
	return false
}

func (m *Manager) collectEvidence(ctx context.Context, runID int64, packet model.AnalysisPacket, request EvidenceRequest) (map[string]any, error) {
	limit := request.Limit
	if limit < 1 || limit > 50 {
		limit = 20
	}
	arguments, _ := json.Marshal(request)
	call := model.AgentToolCall{RunID: runID, Tool: request.Tool, Arguments: arguments, Status: "pending"}
	if err := m.store.AddAgentToolCall(ctx, &call); err != nil {
		return nil, errors.New("证据追查意图无法写入审计")
	}
	result := map[string]any{"tool": request.Tool}
	var err error
	switch request.Tool {
	case "get_account_evidence":
		found := false
		for _, account := range packet.AccountCompactStates {
			if account.AccountID == request.AccountID {
				found = true
				result["account_state"] = account
				break
			}
		}
		if !found {
			err = errors.New("证据账号不在当前数据包")
			break
		}
		result["records"], err = m.store.ListAccountEvidence(ctx, request.AccountID, limit)
	case "get_pool_evidence":
		items := make([]model.AgentAccountState, 0)
		for _, account := range packet.AccountCompactStates {
			if account.Pool == request.Pool && len(items) < limit {
				items = append(items, account)
			}
		}
		if len(items) == 0 {
			err = errors.New("证据池不在当前数据包")
		} else {
			result["accounts"] = items
			failover := make([]model.AgentGroupFailoverToken, 0)
			for _, token := range packet.GroupFailoverTokens {
				if token.Pool == request.Pool {
					failover = append(failover, token)
				}
			}
			result["group_failover_tokens"] = failover
		}
	case "get_policy_history":
		items, listErr := m.store.ListPolicyVersions(ctx, 100)
		if listErr != nil {
			err = listErr
			break
		}
		filtered := make([]model.ScorePolicyVersion, 0)
		for _, item := range items {
			if (request.ScopeType == "" || item.ScopeType == request.ScopeType) && (request.ScopeID == "" || item.ScopeID == request.ScopeID) {
				filtered = append(filtered, item)
				if len(filtered) >= limit {
					break
				}
			}
		}
		result["versions"] = filtered
	case "get_action_outcome":
		items, listErr := m.store.ListRecentDecisionOutcomes(ctx, 100)
		if listErr != nil {
			err = listErr
			break
		}
		filtered := make([]model.DecisionOutcome, 0)
		for _, item := range items {
			if request.RunID == 0 || item.RunID == request.RunID {
				filtered = append(filtered, item)
				if len(filtered) >= limit {
					break
				}
			}
		}
		result["outcomes"] = filtered
	default:
		err = errors.New("不允许的证据追查工具")
	}
	if err != nil {
		call.Status, call.Result = "failed", err.Error()
		_ = m.store.UpdateAgentToolCall(ctx, call)
		return nil, err
	}
	encoded, _ := json.Marshal(result)
	call.Status, call.Result = "completed", fmt.Sprintf("返回 %d 字节脱敏证据", len(encoded))
	_ = m.store.UpdateAgentToolCall(ctx, call)
	return result, nil
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

func agentSystemPrompt() string {
	return `你是 Sub2API 智能调度中心的最高运行决策智能体。目标优先级固定为：可用性、倍率成本、响应速度。
你不得输出密钥，不得要求源码或任意数据库查询。黄色性能下降且真实请求成功时不得仅因延迟判为不可用。
请直接输出一个 JSON 对象，不要使用 Markdown。字段必须是：
summary(中文简明总结), conclusion(中文详细结论), confidence(0到1), no_change(布尔值),
actions(数组), advice(字符串数组), data_limitations(字符串数组), evidence_requests(数组)。
actions 的 type 只能是 pause_account、resume_account、set_load_factor、clear_flap_protection、clear_manual_override、
trigger_reconcile、transition_token_group_tier、update_score_policy、activate_policy_version。
每个动作包含 reason 和 prediction；prediction 包含 success_rate_delta、latency_delta_ms、cost_delta。
transition_token_group_tier 只能包含 source_id、key_id、target_tier(main/backup/emergency)，不得提供真实分组编号；
它只能引用 group_failover_tokens 中已启用、已确认、数据新鲜且未冻结的令牌，一轮最多一个，整体置信度不得低于0.90。
从 main 升到 backup 或从 backup 升到 emergency 前，目标池必须全部不可用且监控与真实流量证据新鲜；
黄色性能下降、数据不足、余额不足、凭据错误、客户端错误和单模型不支持均不能触发分组救灾。
返回 main 只能在数据包显示已满足恢复与冷却约束时提出。智能体不得绕过人工保护、冷却或回退阻断。
update_score_policy 必须包含 scope_type(global/pool/account)、scope_id 和 config 对象。
只有证据冲突或置信度不足时才可请求 evidence_requests；tool 只能是 get_account_evidence、get_pool_evidence、
get_policy_history、get_action_outcome，单次 limit 不得超过50。最终结论必须将 evidence_requests 置为空数组。
没有必要修改时必须设置 no_change=true 且 actions=[]。数据不足时明确说明，不伪造结论。`
}

func (m *Manager) advanceSchedule(ctx context.Context, settings model.AgentSettings, kind string, completed time.Time) {
	_ = settings
	_, activated, err := m.store.AdvanceAgentSchedule(ctx, kind, completed)
	if err != nil {
		m.recordEvent(ctx, "agent_schedule_update_failed", "error", 0, err.Error(), 0)
		return
	}
	if activated {
		m.recordEvent(ctx, "agent_control_activated", "warning", 0,
			"智能体已完成24小时、至少40次有效观察、模拟动作可执行率不低于95%且零越权或结构错误，自动进入完全控制模式", 0)
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
	item := model.AgentDailyReport{ReportDate: reportDate, PacketID: &packet.ID, RunID: &run.ID, Status: "completed", Attempts: attempts,
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
