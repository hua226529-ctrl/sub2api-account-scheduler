package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const (
	runtimeLease               = 8 * time.Minute
	commandLease               = 2 * time.Minute
	maxContractRepairAttempts  = 1
	defaultGoalMaxAttempts     = 5
	maxModelAttemptsPerGoal    = 16
	maxProviderRetryDelay      = 30 * time.Second
	emergencyAggregationWindow = time.Minute
	maxActiveEmergencyGoals    = 20
)

type runtimeGoalContext struct {
	Kind        string              `json:"kind"`
	Trigger     string              `json:"trigger"`
	UserMessage string              `json:"user_message,omitempty"`
	AdminIntent AdministratorIntent `json:"administrator_intent,omitempty"`
	ChatIntent  ChatIntent          `json:"chat_intent,omitempty"`
	// LegacyAdministratorDirect is decoded only so older rows remain readable.
	// It is intentionally ignored: a bare boolean can never grant privilege.
	LegacyAdministratorDirect bool       `json:"administrator_direct,omitempty"`
	Cutoff                    *time.Time `json:"cutoff,omitempty"`
	ReportDate                string     `json:"report_date,omitempty"`
	EventType                 string     `json:"event_type,omitempty"`
	ResourceScope             string     `json:"resource_scope,omitempty"`
	AggregationWindow         string     `json:"aggregation_window,omitempty"`
	AuditEventRefs            []int64    `json:"audit_event_refs,omitempty"`
}

type ChatReceipt struct {
	ConversationID        int64      `json:"conversation_id"`
	GoalID                int64      `json:"goal_id"`
	RunID                 int64      `json:"run_id"`
	Status                string     `json:"status"`
	Intent                ChatIntent `json:"intent"`
	ConfirmationToken     string     `json:"confirmation_token,omitempty"`
	ConfirmationExpiresAt *time.Time `json:"confirmation_expires_at,omitempty"`
}

type runtimeCheckpoint struct {
	Messages     []RuntimeMessage `json:"messages"`
	NextSequence int              `json:"next_sequence"`
	LastFailure  string           `json:"last_failure,omitempty"`
	FailureCount int              `json:"failure_count,omitempty"`
}

// ChatAsync persists the administrator message and returns immediately. The
// cognitive worker owns all subsequent model calls, so HTTP timeouts cannot
// interrupt a goal or lose its execution checkpoint.
func (m *Manager) ChatAsync(ctx context.Context, conversationID int64, message string) (ChatReceipt, error) {
	receipt := ChatReceipt{ConversationID: conversationID}
	message = strings.TrimSpace(message)
	if message == "" || len([]rune(message)) > 4000 {
		return receipt, errors.New("对话内容为空或过长")
	}
	message = redactAgentText(message)
	now := time.Now().UTC()
	chatIntent := m.classifyChatIntent(ctx, message, now)
	if err := chatIntent.Validate(); err != nil {
		return receipt, fmt.Errorf("聊天意图无效: %w", err)
	}
	adminIntent := m.parseAdministratorIntent(ctx, message)
	adminIntent = m.bindChatIntentAdministratorGrants(adminIntent, chatIntent)
	contextPayload, _ := json.Marshal(runtimeGoalContext{Kind: model.AgentRunChat, Trigger: "管理员对话命令",
		UserMessage: message, AdminIntent: adminIntent, ChatIntent: chatIntent})
	status := model.AgentGoalStatusPlanned
	var confirmation *model.ActionConfirmation
	var confirmationToken string
	var confirmationExpiresAt *time.Time
	if chatIntent.RequiresConfirmation {
		status = model.AgentGoalStatusWaiting
		token, err := randomURLToken(m.randomReader, 24)
		if err != nil {
			return receipt, errors.New("无法生成安全确认令牌")
		}
		expiresAt := now.Add(5 * time.Minute)
		resources, _ := json.Marshal(chatIntent.ResourceIDs)
		desired, _ := json.Marshal(chatIntent.DesiredState)
		confirmation = &model.ActionConfirmation{Administrator: "administrator", TokenHash: hashOpaqueToken(token),
			PayloadHash: chatIntentConfirmationHash(chatIntent), Resources: resources, Operation: chatIntent.Operation,
			DesiredState: desired, Status: "pending", ExpiresAt: expiresAt}
		confirmationToken, confirmationExpiresAt = token, &expiresAt
	}
	goal := model.AgentGoal{ConversationID: &conversationID, Title: truncateRunes(message, 80), Objective: message,
		Status: status, Lane: model.AgentLaneInteractive, Priority: 100, RiskLevel: chatIntent.RiskLevel, Source: "administrator",
		Context: contextPayload, CreatedBy: "administrator"}
	committedConversationID, err := m.store.EnqueueChatGoal(ctx, conversationID, message, &goal, confirmation)
	if err != nil {
		return receipt, err
	}
	conversationID = committedConversationID
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "administrator_goal_created", "info", "administrator",
		map[string]any{"conversation_id": conversationID, "lane": goal.Lane, "intent_type": chatIntent.IntentType,
			"read_only": chatIntent.ReadOnly, "requires_confirmation": chatIntent.RequiresConfirmation})
	if goal.Status == model.AgentGoalStatusPlanned {
		m.wakeLane(model.AgentLaneInteractive)
	}
	return ChatReceipt{ConversationID: conversationID, GoalID: goal.ID, Status: goal.Status, Intent: chatIntent,
		ConfirmationToken: confirmationToken, ConfirmationExpiresAt: confirmationExpiresAt}, nil
}

func (m *Manager) ConfirmChatGoal(ctx context.Context, goalID int64, token string) (ChatReceipt, error) {
	goal, err := m.store.GetAgentGoal(ctx, goalID)
	if err != nil {
		return ChatReceipt{}, errors.New("待确认任务不存在")
	}
	var goalContext runtimeGoalContext
	if err := json.Unmarshal(goal.Context, &goalContext); err != nil || !goalContext.ChatIntent.RequiresConfirmation || goalContext.ChatIntent.Confirmed {
		return ChatReceipt{}, errors.New("任务不处于待确认状态")
	}
	goalContext.ChatIntent.Confirmed = true
	confirmedContext, err := json.Marshal(goalContext)
	if err != nil {
		return ChatReceipt{}, err
	}
	confirmed, err := m.store.ConfirmAgentGoal(ctx, goalID, "administrator", hashOpaqueToken(strings.TrimSpace(token)),
		chatIntentConfirmationHash(goalContext.ChatIntent), confirmedContext, time.Now().UTC())
	if err != nil {
		return ChatReceipt{}, err
	}
	m.appendRuntimeEvent(ctx, &goalID, nil, "administrator_confirmation_consumed", "warning", "administrator",
		map[string]any{"intent_type": goalContext.ChatIntent.IntentType, "operation": goalContext.ChatIntent.Operation})
	m.wakeLane(model.AgentLaneInteractive)
	return ChatReceipt{ConversationID: derefInt64(confirmed.ConversationID), GoalID: goalID, Status: confirmed.Status,
		Intent: goalContext.ChatIntent}, nil
}

func randomURLToken(reader io.Reader, size int) (string, error) {
	if reader == nil {
		reader = rand.Reader
	}
	buffer := make([]byte, size)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func hashOpaqueToken(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func (m *Manager) EnqueueAnalysisGoal(ctx context.Context, kind, trigger string, priority int) (model.AgentGoal, error) {
	goalContext := runtimeGoalContext{Kind: kind, Trigger: trigger}
	if kind == model.AgentRunEmergency {
		goalContext.EventType = "computed_emergency"
		goalContext.ResourceScope = "global"
		goalContext.AggregationWindow = time.Now().UTC().Truncate(emergencyAggregationWindow).Format(time.RFC3339)
	}
	return m.enqueueAnalysisGoal(ctx, goalContext, trigger, priority)
}

func (m *Manager) enqueueAnalysisGoal(ctx context.Context, goalContext runtimeGoalContext, objective string, priority int) (model.AgentGoal, error) {
	active := []string{model.AgentGoalStatusPlanned, model.AgentGoalStatusRunning, model.AgentGoalStatusWaiting}
	activeEmergency := 0
	for _, status := range active {
		items, err := m.store.ListAgentGoals(ctx, status, 100)
		if err != nil {
			return model.AgentGoal{}, err
		}
		for _, item := range items {
			var existing runtimeGoalContext
			if json.Unmarshal(item.Context, &existing) != nil {
				continue
			}
			if existing.Kind == model.AgentRunEmergency {
				activeEmergency++
			}
			if existing.Kind == goalContext.Kind && sameRuntimeGoalDedupeKey(existing, goalContext) {
				if goalContext.Kind == model.AgentRunEmergency && len(goalContext.AuditEventRefs) > 0 {
					existing.AuditEventRefs = uniqueInt64s(append(existing.AuditEventRefs, goalContext.AuditEventRefs...))
					item.Context = marshalRaw(existing)
					item.Objective = emergencyObjective(existing.EventType, existing.ResourceScope, existing.AuditEventRefs)
					_ = m.store.UpdateAgentGoal(ctx, item)
				}
				return item, nil
			}
		}
	}
	if goalContext.Kind == model.AgentRunEmergency && activeEmergency >= maxActiveEmergencyGoals {
		m.appendRuntimeEvent(ctx, nil, nil, "emergency_goal_deferred", "warning", "system", map[string]any{
			"event_type": goalContext.EventType, "resource_scope": goalContext.ResourceScope,
			"active_emergency_goals": activeEmergency,
		})
		return model.AgentGoal{}, errors.New("active emergency goal limit reached; event deferred")
	}
	if priority < 1 {
		priority = 50
	}
	contextPayload, _ := json.Marshal(goalContext)
	goal := model.AgentGoal{Title: goalContext.Trigger, Objective: objective, Status: model.AgentGoalStatusPlanned, Priority: priority,
		Lane: model.AgentLaneBackground, RiskLevel: model.AgentRiskMedium, Source: "scheduler", Context: contextPayload, CreatedBy: "scheduler"}
	if err := m.store.CreateAgentGoal(ctx, &goal); err != nil {
		return goal, err
	}
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "analysis_goal_created", "info", "scheduler", map[string]any{
		"kind": goalContext.Kind, "trigger": goalContext.Trigger, "event_type": goalContext.EventType,
		"resource_scope": goalContext.ResourceScope, "audit_event_refs": goalContext.AuditEventRefs,
	})
	m.wakeLane(model.AgentLaneBackground)
	return goal, nil
}

func sameRuntimeGoalDedupeKey(left, right runtimeGoalContext) bool {
	if left.Kind != right.Kind {
		return false
	}
	if left.Kind != model.AgentRunEmergency {
		return left.Trigger == right.Trigger
	}
	return left.EventType == right.EventType && left.ResourceScope == right.ResourceScope &&
		left.AggregationWindow == right.AggregationWindow
}

func uniqueInt64s(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	result := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func emergencyObjective(eventType, scope string, refs []int64) string {
	return fmt.Sprintf("分析严重运行事件 type=%s scope=%s audit_event_ids=%v", eventType, scope, refs)
}

func (m *Manager) enqueueDailyGoal(ctx context.Context, reportDate string, cutoff time.Time) (model.AgentGoal, error) {
	for _, status := range []string{model.AgentGoalStatusPlanned, model.AgentGoalStatusRunning, model.AgentGoalStatusWaiting} {
		items, err := m.store.ListAgentGoals(ctx, status, 100)
		if err != nil {
			return model.AgentGoal{}, err
		}
		for _, item := range items {
			var existing runtimeGoalContext
			if json.Unmarshal(item.Context, &existing) == nil && existing.Kind == model.AgentRunDaily && existing.ReportDate == reportDate {
				return item, nil
			}
		}
	}
	trigger := "生成 " + reportDate + " 每日总结"
	payload, _ := json.Marshal(runtimeGoalContext{Kind: model.AgentRunDaily, Trigger: trigger, ReportDate: reportDate, Cutoff: &cutoff})
	goal := model.AgentGoal{Title: trigger, Objective: "总结上一自然日运行、动作效果、预测准确度并提出迭代意见，只生成报告不执行写入",
		Status: model.AgentGoalStatusPlanned, Lane: model.AgentLaneBackground, Priority: 65, RiskLevel: model.AgentRiskReadOnly, Source: "daily", Context: payload, CreatedBy: "scheduler"}
	if err := m.store.CreateAgentGoal(ctx, &goal); err != nil {
		return goal, err
	}
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "daily_goal_created", "info", "scheduler", map[string]any{"report_date": reportDate})
	m.wakeLane(model.AgentLaneBackground)
	return goal, nil
}

func (m *Manager) Goals(ctx context.Context, status string, limit int) ([]model.AgentGoal, []model.AgentStep, error) {
	goals, err := m.store.ListAgentGoals(ctx, status, limit)
	if err != nil {
		return nil, nil, err
	}
	steps := make([]model.AgentStep, 0)
	for _, goal := range goals {
		items, listErr := m.store.ListAgentSteps(ctx, goal.ID)
		if listErr != nil {
			return nil, nil, listErr
		}
		steps = append(steps, items...)
	}
	return goals, steps, nil
}

func (m *Manager) RuntimeEvents(ctx context.Context, goalID, afterID int64, limit int) ([]model.AgentEvent, error) {
	return m.store.ListAgentEvents(ctx, goalID, afterID, limit)
}

func (m *Manager) ScheduledCommands(ctx context.Context, status string, goalID int64, limit int) ([]model.ScheduledCommand, error) {
	return m.store.ListScheduledCommands(ctx, status, goalID, limit)
}

func (m *Manager) Memories(ctx context.Context, scopeType, scopeID string, limit int) ([]model.AgentMemory, error) {
	return m.store.ListAgentMemories(ctx, scopeType, scopeID, limit)
}

func (m *Manager) wakeLane(lane string) {
	wake := m.backgroundWake
	if lane == model.AgentLaneInteractive {
		wake = m.interactiveWake
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (m *Manager) bridgeOperationalEvents(ctx context.Context) {
	items, err := m.engine.Events(ctx, 200)
	if err != nil {
		return
	}
	for _, item := range items {
		payload := marshalRaw(map[string]any{"audit_event_id": item.ID, "type": item.Type, "severity": item.Severity,
			"monitor_id": item.MonitorID, "account_id": item.AccountID, "message": truncateRunes(item.Message, 1000),
			"before_state": item.BeforeState, "after_state": item.AfterState, "details": truncateRunes(item.Details, 2000),
			"actor": item.Actor, "created_at": item.CreatedAt})
		event := model.AgentEvent{EventKey: fmt.Sprintf("audit:%d", item.ID), Type: "operational_" + item.Type,
			Severity: item.Severity, Actor: "system", Payload: payload, CreatedAt: item.CreatedAt}
		inserted, appendErr := m.store.AppendAgentEvent(ctx, &event)
		if appendErr == nil && inserted && (item.Severity == "critical" || item.Severity == "error") {
			settings, settingsErr := m.store.GetAgentSettings(ctx)
			if settingsErr == nil && settings.Enabled {
				scope := m.operationalEventScope(item)
				window := item.CreatedAt.UTC().Truncate(emergencyAggregationWindow).Format(time.RFC3339)
				goalContext := runtimeGoalContext{Kind: model.AgentRunEmergency, Trigger: "严重运行事件", EventType: item.Type,
					ResourceScope: scope, AggregationWindow: window, AuditEventRefs: []int64{item.ID}}
				_, _ = m.enqueueAnalysisGoal(ctx, goalContext, emergencyObjective(item.Type, scope, []int64{item.ID}), 95)
			} else {
				m.wakeLane(model.AgentLaneBackground)
			}
		}
	}
}

func (m *Manager) operationalEventScope(item model.Event) string {
	if item.AccountID != nil && *item.AccountID > 0 {
		return fmt.Sprintf("account:%d", *item.AccountID)
	}
	if item.MonitorID == nil || *item.MonitorID <= 0 {
		return "global"
	}
	monitorID := *item.MonitorID
	accounts := make(map[int64]struct{})
	if m.engine != nil {
		for _, binding := range m.engine.Snapshot().Bindings {
			if binding.Monitor != nil && binding.Monitor.ID == monitorID && binding.Account.ID > 0 {
				accounts[binding.Account.ID] = struct{}{}
			}
		}
	}
	if len(accounts) == 1 {
		for accountID := range accounts {
			return fmt.Sprintf("account:%d", accountID)
		}
	}
	return fmt.Sprintf("monitor:%d", monitorID)
}

func (m *Manager) runtimeWorker(ctx context.Context, lane string) {
	fallback := 10 * time.Second
	wake := m.backgroundWake
	if lane == model.AgentLaneInteractive {
		fallback = time.Second
		wake = m.interactiveWake
	}
	ticker := time.NewTicker(fallback)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		m.bridgeOperationalEvents(ctx)
		for m.processRuntimeLaneSafely(ctx, lane) {
			if ctx.Err() != nil {
				return
			}
		}
	}
}

func (m *Manager) processRuntimeLaneSafely(ctx context.Context, lane string) (processed bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger := m.logger
			if logger == nil {
				logger = slog.Default()
			}
			logger.Error("agent_lane_worker_panic", "lane", lane, "panic", recovered)
			processed = false
		}
	}()
	return m.processNextRuntimeGoalLane(ctx, lane)
}

func (m *Manager) processNextRuntimeGoal(ctx context.Context) bool {
	return m.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive)
}

func (m *Manager) processNextRuntimeGoalLane(ctx context.Context, lane string) bool {
	freeze, err := m.engine.FreezeState(ctx)
	if err != nil || freeze.Agent || freeze.AllAutomation {
		return false
	}
	worker := m.workerID + ":" + lane
	goal, err := m.store.ClaimAgentGoal(ctx, lane, worker, time.Now().UTC(), runtimeLease)
	if err != nil || goal == nil {
		return false
	}
	if m.onGoalClaimed != nil {
		m.onGoalClaimed(lane, goal.ID)
	}
	err = m.runRuntimeGoalLease(ctx, *goal, lane)
	if err != nil {
		m.releaseRuntimeGoalAfterError(ctx, goal.ID, worker, lane, err)
	}
	return true
}

func (m *Manager) releaseRuntimeGoalAfterError(ctx context.Context, goalID int64, worker, lane string, cause error) {
	status := model.AgentGoalStatusPlanned
	next := time.Now().UTC().Add(time.Second)
	if lane == model.AgentLaneBackground {
		next = time.Now().UTC().Add(5 * time.Second)
	}
	nextRunnable := &next
	if current, err := m.store.GetAgentGoal(ctx, goalID); err == nil {
		switch current.Status {
		case model.AgentGoalStatusCompleted, model.AgentGoalStatusFailed, model.AgentGoalStatusCancelled:
			return
		case model.AgentGoalStatusWaiting:
			status, nextRunnable = model.AgentGoalStatusWaiting, nil
		case model.AgentGoalStatusPlanned:
			if current.NextRunnableAt != nil {
				nextRunnable = current.NextRunnableAt
			}
		}
	}
	_ = m.store.ReleaseAgentGoalLease(ctx, goalID, worker, status, nextRunnable, cause.Error())
	logger := m.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn("agent_runtime_goal_yielded", "lane", lane, "goal_id", goalID, "status", status, "error", cause)
}

func (m *Manager) runRuntimeGoalLease(parent context.Context, goal model.AgentGoal, lane string) error {
	if goal.Lane == "" {
		goal.Lane = lane
	}
	leaseCtx, cancel := context.WithTimeout(parent, runtimeLease)
	defer cancel()
	settings, err := m.store.GetAgentSettings(leaseCtx)
	if err != nil {
		return m.waitGoal(parent, &goal, err)
	}
	if !settings.Enabled && goal.Source != "administrator" {
		return m.waitGoal(parent, &goal, errors.New("智能体未启用"))
	}
	var goalContext runtimeGoalContext
	if err := json.Unmarshal(goal.Context, &goalContext); err != nil {
		return m.failGoal(parent, &goal, errors.New("目标上下文损坏"))
	}
	if goalContext.Kind == "" {
		goalContext.Kind = model.AgentRunChat
	}
	goal.Status, goal.LastError = model.AgentGoalStatusRunning, ""
	if err := m.store.UpdateAgentGoal(leaseCtx, goal); err != nil {
		return err
	}
	m.appendRuntimeEvent(leaseCtx, &goal.ID, nil, "goal_started", "info", "agent", map[string]any{"lane": goal.Lane})

	packet, err := m.buildGoalPacket(leaseCtx, goalContext, settings)
	if err != nil {
		return m.waitGoal(parent, &goal, err)
	}
	packetID := packet.ID
	run := model.AgentRun{Kind: goalContext.Kind, Trigger: goalContext.Trigger, Status: "running", PacketID: &packetID,
		ConversationID: goal.ConversationID, StartedAt: time.Now().UTC(), ActionsJSON: json.RawMessage("[]")}
	if goalContext.ChatIntent.IntentType == ChatIntentAmbiguous {
		decision := ModelDecision{
			Summary:    "需要管理员澄清请求",
			Conclusion: goalContext.ChatIntent.Clarification,
			Confidence: 1,
			NoChange:   true,
			Actions:    []AgentAction{},
		}
		m.appendRuntimeEvent(leaseCtx, &goal.ID, nil, "chat_intent_clarification_required", "info", "system", map[string]any{
			"intent_type":   goalContext.ChatIntent.IntentType,
			"clarification": goalContext.ChatIntent.Clarification,
		})
		return m.completeRuntimeGoal(parent, &goal, &run, packet, settings, goalContext, decision)
	}
	messages, sequence, lastFailure, failureCount, err := m.runtimeMessages(leaseCtx, goal, goalContext, packet, settings)
	if err != nil {
		m.finishRuntimeRun(parent, &run, "waiting", err)
		return m.waitGoal(parent, &goal, err)
	}
	for {
		if leaseCtx.Err() != nil {
			return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
				"checkpointed", model.AgentGoalStatusPlanned, "goal_checkpointed",
				errors.New("执行租约到期，已保存检查点等待续跑"), true)
		}
		if freezeErr := m.runtimeFreezeError(leaseCtx); freezeErr != nil {
			return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
				"waiting", model.AgentGoalStatusWaiting, "goal_paused_by_freeze", freezeErr, false)
		}
		remainingModelAttempts := maxModelAttemptsPerGoal - goal.ModelAttemptCount
		if remainingModelAttempts <= 0 {
			cause := newRuntimeError(runtimeErrorModelNoProgress, errors.New("目标已达到模型调用硬上限"))
			m.finishRuntimeRun(parent, &run, "failed", cause)
			return m.failGoalClass(parent, &goal, runtimeErrorModelNoProgress, cause, true)
		}
		turn, provider, modelAttempts, err := m.completeRuntimeTurn(leaseCtx, messages, lane, remainingModelAttempts)
		goal.ModelAttemptCount += modelAttempts
		if leaseCtx.Err() != nil {
			return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
				"checkpointed", model.AgentGoalStatusPlanned, "goal_checkpointed",
				errors.New("执行租约到期，已保存检查点等待续跑"), true)
		}
		// The freeze may have changed while the model request was in flight.
		// Recheck before accepting even a final conclusion so "freeze agent"
		// stops the entire cognitive loop, not only external writes.
		if freezeErr := m.runtimeFreezeError(leaseCtx); freezeErr != nil {
			return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
				"waiting", model.AgentGoalStatusWaiting, "goal_paused_by_freeze", freezeErr, false)
		}
		if err != nil {
			m.recordObservationModelError(parent, settings, err)
			class := runtimeErrorClassOf(err)
			if class == runtimeErrorModelContractInvalid {
				if m.repairRuntimeContract(parent, &goal, err, &messages) {
					_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
					continue
				}
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
				m.finishRuntimeRun(parent, &run, "failed", err)
				return m.failGoalClass(parent, &goal, class, err, true)
			}
			if runtimeErrorRetryable(class) {
				m.finishRuntimeRun(parent, &run, "waiting", err)
				return m.retryProviderGoal(parent, &goal, class, err)
			}
			_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
			m.finishRuntimeRun(parent, &run, "failed", err)
			return m.failGoalClass(parent, &goal, class, err, true)
		}
		run.ProviderSlot, run.Model = provider.Slot, provider.Model
		if err := m.store.UpdateAgentGoal(parent, goal); err != nil {
			return err
		}
		if turn.Decision != nil {
			if validationErr := ValidateRuntimeDecision(packet, goalContext, *turn.Decision); validationErr != nil {
				contractErr := newRuntimeError(runtimeErrorModelContractInvalid, validationErr)
				if m.repairRuntimeContract(parent, &goal, contractErr, &messages) {
					_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
					continue
				}
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
				m.finishRuntimeRun(parent, &run, "failed", contractErr)
				return m.failGoalClass(parent, &goal, runtimeErrorModelContractInvalid, contractErr, true)
			}
		}
		var fallbackAction *AgentAction
		if len(turn.ToolCalls) == 0 && turn.Decision != nil && len(turn.Decision.Actions) > 0 {
			action := turn.Decision.Actions[0]
			fallbackAction = &action
			call, mapErr := decisionActionToolCall(turn.Decision.Actions[0], turn.Decision.Confidence)
			if mapErr != nil {
				if settings.OptimizerMode == model.AgentOptimizerObserve {
					_ = m.store.RecordAgentObservation(parent, 1, 0, 1, 1)
				}
				messages = append(messages, RuntimeMessage{Role: "user", Content: "动作结构无效：" + mapErr.Error() + "。请重新规划或给出最终结论。"})
				continue
			}
			turn.ToolCalls = []RuntimeToolCall{call}
		}
		if len(turn.ToolCalls) == 0 {
			if turn.Decision == nil {
				m.recordRuntimeObservation(parent, settings, 0, 0, 0, 1)
				messages = append(messages, RuntimeMessage{Role: "user", Content: "没有收到工具调用或最终结构化结论，请继续。"})
				continue
			}
			return m.completeRuntimeGoal(parent, &goal, &run, packet, settings, goalContext, *turn.Decision)
		}
		// A model may emit parallel tool calls, but the runtime deliberately
		// executes one capability and then replans from its readback. Truncating
		// before persisting the assistant message keeps the tool transcript
		// protocol-complete and prevents a batch of stale mutations.
		if len(turn.ToolCalls) > 1 {
			extraMutations := 0
			for _, extra := range turn.ToolCalls[1:] {
				spec, known := capabilitySpec(extra.Function.Name)
				if !known || spec.Mutating {
					extraMutations++
				}
			}
			if extraMutations > 0 {
				m.recordRuntimeObservation(parent, settings, extraMutations, 0, extraMutations, 1)
			} else {
				m.recordRuntimeObservation(parent, settings, 0, 0, 0, 1)
			}
			m.appendRuntimeEvent(parent, &goal.ID, nil, "parallel_tools_replanned", "warning", "agent",
				map[string]any{"requested": len(turn.ToolCalls), "executed": 1, "discarded_mutations": extraMutations})
			turn.ToolCalls = turn.ToolCalls[:1]
		}
		messages = append(messages, RuntimeMessage{Role: "assistant", Content: turn.Content, ToolCalls: turn.ToolCalls})
		for _, call := range turn.ToolCalls {
			if freezeErr := m.runtimeFreezeError(leaseCtx); freezeErr != nil {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: freezeErr.Error()}
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
					"waiting", model.AgentGoalStatusWaiting, "goal_paused_by_freeze", freezeErr, false)
			}
			arguments := json.RawMessage(call.Function.Arguments)
			normalized, normalizeErr := normalizedArguments(arguments)
			if normalizeErr != nil {
				normalized = arguments
			}
			fingerprint := call.Function.Name + ":" + string(normalized)
			if fingerprint == lastFailure && failureCount >= 2 {
				goal.NoProgressCount++
				blocked := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "相同工具在相同状态下连续失败，禁止无意义重试；必须重新规划"}
				payload, _ := json.Marshal(blocked)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, nil, "agent_no_progress", "error", "agent", blocked)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
				cause := newRuntimeError(runtimeErrorModelNoProgress, errors.New(blocked.Message))
				m.finishRuntimeRun(parent, &run, "failed", cause)
				return m.failGoalClass(parent, &goal, runtimeErrorModelNoProgress, cause, true)
			}
			if fingerprint == lastFailure && failureCount == 1 {
				failureCount = 2
				goal.NoProgressCount++
				blocked := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "相同工具参数已失败；这是唯一一次 no-progress 修复提示，请改变计划或给出安全结论"}
				payload, _ := json.Marshal(blocked)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, nil, "agent_no_progress_repair", "warning", "agent", blocked)
				_ = m.store.UpdateAgentGoal(parent, goal)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, nil, messages, sequence, lastFailure, failureCount)
				continue
			}
			sequence++
			spec, known := capabilitySpec(call.Function.Name)
			if !known {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "未授权能力 " + call.Function.Name}
				step := model.AgentStep{GoalID: goal.ID, Sequence: sequence, Capability: call.Function.Name, Arguments: arguments,
					Status: model.AgentStepStatusFailed, RiskLevel: model.AgentRiskCritical,
					IdempotencyKey: runtimeIdempotency(goal.ID, sequence, call.Function.Name, arguments), MaxAttempts: 1,
					Preconditions: json.RawMessage("{}"), Compensation: json.RawMessage("{}"), BeforeState: json.RawMessage("{}"),
					AfterState: json.RawMessage("{}"), Result: marshalRaw(execution), LastError: execution.Message}
				if err := m.store.CreateAgentStep(leaseCtx, &step); err != nil {
					m.finishRuntimeRun(parent, &run, "waiting", err)
					return m.waitGoal(parent, &goal, err)
				}
				m.recordRuntimeObservation(parent, settings, 1, 0, 1, 1)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "unauthorized_capability_blocked", "error", "agent", execution)
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
				continue
			}
			if intentErr := enforceChatIntentCapability(goalContext.ChatIntent, call.Function.Name, arguments, spec.Mutating); intentErr != nil {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "聊天意图约束拒绝执行：" + intentErr.Error()}
				step := model.AgentStep{GoalID: goal.ID, Sequence: sequence, Capability: call.Function.Name, Arguments: arguments,
					Status: model.AgentStepStatusFailed, RiskLevel: spec.RiskLevel,
					IdempotencyKey: runtimeIdempotency(goal.ID, sequence, call.Function.Name, arguments), MaxAttempts: 1,
					Preconditions: json.RawMessage("{}"), Compensation: json.RawMessage("{}"), BeforeState: json.RawMessage("{}"),
					AfterState: json.RawMessage("{}"), Result: marshalRaw(execution), LastError: execution.Message}
				if err := m.store.CreateAgentStep(leaseCtx, &step); err != nil {
					m.finishRuntimeRun(parent, &run, "waiting", err)
					return m.waitGoal(parent, &goal, err)
				}
				attempted := 0
				if spec.Mutating {
					attempted = 1
				}
				m.recordRuntimeObservation(parent, settings, attempted, 0, attempted, 1)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "chat_intent_capability_blocked", "error", "system", execution)
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
				continue
			}
			step := model.AgentStep{GoalID: goal.ID, Sequence: sequence, Capability: call.Function.Name, Arguments: arguments,
				Status: model.AgentStepStatusRunning, RiskLevel: spec.RiskLevel, IdempotencyKey: runtimeIdempotency(goal.ID, sequence, call.Function.Name, arguments),
				MaxAttempts: 1, Preconditions: json.RawMessage("{}"), Compensation: json.RawMessage("{}"), BeforeState: json.RawMessage("{}"),
				AfterState: json.RawMessage("{}"), Result: json.RawMessage("{}")}
			if err := m.store.CreateAgentStep(leaseCtx, &step); err != nil {
				m.finishRuntimeRun(parent, &run, "waiting", err)
				return m.waitGoal(parent, &goal, err)
			}
			if spec.Mutating {
				priorSteps, _ := m.store.ListAgentSteps(leaseCtx, goal.ID)
				blockedByReconciliation := false
				for _, prior := range priorSteps {
					if prior.ID != step.ID && prior.Status == model.AgentStepStatusReconciling && prior.Capability == step.Capability && string(prior.Arguments) == string(step.Arguments) {
						blockedByReconciliation = true
						break
					}
				}
				if blockedByReconciliation {
					execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "相同外部写入仍在核对中，禁止重放"}
					step.Status, step.LastError, step.Result = model.AgentStepStatusSkipped, execution.Message, marshalRaw(execution)
					_ = m.store.UpdateAgentStep(parent, step)
					payload, _ := json.Marshal(execution)
					messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
					m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "reconciling_write_blocked", "error", "agent", execution)
					recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
					_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
					continue
				}
			}
			if goalContext.Kind == model.AgentRunDaily && spec.Mutating {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "日报目标禁止任何写入动作"}
				step.Status, step.LastError, step.Result = model.AgentStepStatusFailed, execution.Message, marshalRaw(execution)
				_ = m.store.UpdateAgentStep(parent, step)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "daily_write_blocked", "error", "agent", execution)
				m.recordRuntimeObservation(parent, settings, 1, 0, 1, 1)
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
				continue
			}
			adminGrant, grantErr := m.administratorGrantForInvocation(goalContext.AdminIntent, call.Function.Name, arguments)
			if grantErr != nil {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: "管理员精确授权校验失败：" + grantErr.Error()}
				step.Status, step.LastError, step.Result = model.AgentStepStatusFailed, execution.Message, marshalRaw(execution)
				_ = m.store.UpdateAgentStep(parent, step)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "administrator_grant_blocked", "error", "system", execution)
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
				continue
			}
			dryRun, modeErr := capabilityExecutionMode(settings, goalContext, call.Function.Name, spec, adminGrant)
			if modeErr != nil {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: modeErr.Error()}
				step.Status, step.LastError, step.Result = model.AgentStepStatusFailed, execution.Message, marshalRaw(execution)
				_ = m.store.UpdateAgentStep(parent, step)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "capability_mode_blocked", "error", "system", execution)
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
				continue
			}
			actor := "agent:v2"
			if adminGrant != nil {
				actor = "administrator:agent"
			}
			expiresAt := step.ExpiresAt
			if goalContext.ChatIntent.ExpiresAt != nil {
				value := goalContext.ChatIntent.ExpiresAt.UTC()
				expiresAt = &value
			}
			invocation := CapabilityInvocation{Name: call.Function.Name, Arguments: arguments,
				RunID: run.ID, GoalID: goal.ID, StepID: step.ID, Actor: actor, IdempotencyKey: step.IdempotencyKey,
				PacketID: packet.ID, PacketHash: packet.Hash,
				AdministratorGrant: adminGrant, DryRun: dryRun, CreatedAt: step.CreatedAt, ExpiresAt: expiresAt,
				SnapshotVersion: fmt.Sprintf("analysis_packet:%d:%s", packet.ID, packet.Hash),
				EvidenceRefs:    []string{fmt.Sprintf("analysis_packet:%d:%s", packet.ID, packet.Hash)}}
			if spec.Mutating && !dryRun {
				// Persist the baseline before entering any external mutation. A
				// process crash after the write can then be resolved by readback.
				step.BeforeState = m.capabilityState(leaseCtx, invocation)
				attemptedAt := time.Now().UTC()
				step.Preconditions = marshalRaw(stepReconciliationEvidence{AttemptedAt: attemptedAt})
				if err := m.store.RecordAgentStepMutationAttempt(leaseCtx, step.ID, step.BeforeState,
					step.Preconditions, attemptedAt); err != nil {
					m.finishRuntimeRun(parent, &run, "waiting", err)
					return m.waitGoal(parent, &goal, fmt.Errorf("写入动作基线保存失败，已拒绝执行: %w", err))
				}
				step.MutationAttemptedAt = &attemptedAt
				step.AttemptCount++
			}
			if freezeErr := m.runtimeFreezeError(leaseCtx); freezeErr != nil {
				execution := CapabilityExecution{Capability: call.Function.Name, Status: "blocked", Message: freezeErr.Error()}
				step.Status, step.LastError, step.Result = model.AgentStepStatusSkipped, execution.Message, marshalRaw(execution)
				_ = m.store.UpdateAgentStep(parent, step)
				payload, _ := json.Marshal(execution)
				messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
				return m.checkpointRuntimeYield(parent, &goal, &run, messages, sequence, lastFailure, failureCount,
					"waiting", model.AgentGoalStatusWaiting, "goal_paused_by_freeze", freezeErr, false)
			}
			execution, execErr := m.ExecuteCapability(leaseCtx, invocation)
			step.BeforeState, step.AfterState = execution.BeforeState, execution.AfterState
			step.Result = marshalRaw(execution)
			if execErr != nil {
				step.Status, step.LastError = model.AgentStepStatusFailed, execErr.Error()
				if execution.Retryable && spec.Mutating {
					uncertainAt := time.Now().UTC()
					var evidence stepReconciliationEvidence
					_ = json.Unmarshal(step.Preconditions, &evidence)
					evidence.UncertainAt = &uncertainAt
					step.Preconditions = marshalRaw(evidence)
					step.Status = model.AgentStepStatusReconciling
					execution.Status = "reconciling"
					execution.Message = "外部写入结果不明确；仅允许回读核对，禁止自动重放"
					step.Result = marshalRaw(execution)
				}
				recordRuntimeFailureFingerprint(fingerprint, &lastFailure, &failureCount)
				if step.Status == model.AgentStepStatusReconciling {
					failureCount = 2
				}
				if strings.Contains(execErr.Error(), "能力参数") {
					goal.LastErrorClass = string(runtimeErrorModelToolArgumentsInvalid)
					now := time.Now().UTC()
					goal.LastErrorAt = &now
				}
			} else {
				step.Status, lastFailure, failureCount = model.AgentStepStatusCompleted, "", 0
				if fallbackAction != nil {
					m.createRuntimeOutcomes(parent, goal.ID, step.ID, packet.ID, packet.Hash, *fallbackAction)
				}
			}
			_ = m.store.UpdateAgentStep(parent, step)
			m.appendRuntimeEvent(parent, &goal.ID, &step.ID, "capability_"+execution.Status, severityForExecution(execution), "agent", execution)
			payload, _ := json.Marshal(execution)
			messages = append(messages, RuntimeMessage{Role: "tool", ToolCallID: call.ID, Content: string(payload)})
			_ = m.saveRuntimeCheckpoint(parent, goal.ID, &step.ID, messages, sequence, lastFailure, failureCount)
			if step.Status == model.AgentStepStatusReconciling {
				cause := errors.New("外部写入结果不明确，目标已暂停并等待只读核对")
				now := time.Now().UTC()
				goal.LastErrorClass, goal.LastErrorAt = string(runtimeErrorExternalMutationUncertain), &now
				m.finishRuntimeRun(parent, &run, "waiting", cause)
				return m.waitGoal(parent, &goal, cause)
			}
		}
		messages = compactRuntimeMessages(messages, settings.ContextTokenBudget, goal.Objective)
	}
}

func capabilityExecutionMode(settings model.AgentSettings, goalContext runtimeGoalContext, capability string,
	spec CapabilitySpec, grant *AdministratorGrant) (bool, error) {
	if !spec.Mutating {
		return false, nil
	}
	if grant != nil {
		if settings.OperatorMode == model.AgentOperatorDisabled {
			return false, errors.New("Agent Operator 已禁用，管理员写命令未执行")
		}
		return false, nil
	}
	switch capability {
	case "propose_dispatch_policy", "update_dispatch_policy":
		switch settings.OptimizerMode {
		case model.AgentOptimizerObserve:
			return true, nil
		case model.AgentOptimizerPropose, model.AgentOptimizerAuto:
			return false, nil
		default:
			return false, errors.New("Agent Optimizer 已禁用，策略提案未创建")
		}
	case "activate_policy_version":
		return false, errors.New("模型不能直接激活策略；仅管理员确认或确定性低风险 auto gate 可以激活")
	default:
		if settings.OperatorMode != model.AgentOperatorDirect {
			return false, errors.New("自主直接动作仅在 agent_operator_mode=direct 时允许")
		}
		return false, nil
	}
}

func (m *Manager) buildGoalPacket(ctx context.Context, goalContext runtimeGoalContext, settings model.AgentSettings) (model.AnalysisPacket, error) {
	if goalContext.Cutoff != nil {
		return m.builder.BuildAt(ctx, goalContext.Kind, settings, goalContext.Cutoff.UTC())
	}
	return m.builder.Build(ctx, goalContext.Kind, settings)
}

func (m *Manager) runtimeMessages(ctx context.Context, goal model.AgentGoal, goalContext runtimeGoalContext,
	packet model.AnalysisPacket, settings model.AgentSettings) ([]RuntimeMessage, int, string, int, error) {
	steps, _ := m.store.ListAgentSteps(ctx, goal.ID)
	maxSequence := 0
	for _, step := range steps {
		if step.Sequence > maxSequence {
			maxSequence = step.Sequence
		}
	}
	if checkpoint, err := m.store.LatestAgentCheckpoint(ctx, goal.ID); err == nil {
		var state runtimeCheckpoint
		if json.Unmarshal(checkpoint.State, &state) == nil && len(state.Messages) > 0 {
			freshInput, inputErr := m.runtimeFreshInput(ctx, goal, packet, settings, steps)
			if inputErr != nil {
				return nil, 0, "", 0, inputErr
			}
			if state.Messages[0].Role == "system" {
				state.Messages[0].Content = runtimeSystemPrompt()
			} else {
				state.Messages = append([]RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, state.Messages...)
			}
			for _, step := range steps {
				if (step.Status == model.AgentStepStatusCompleted || step.Status == model.AgentStepStatusFailed) &&
					!step.UpdatedAt.Before(checkpoint.CreatedAt) {
					state.Messages = append(state.Messages, RuntimeMessage{Role: "user", Content: fmt.Sprintf(
						"只读核对已解析步骤 #%d：状态=%s，说明=%s，当前回读=%s。请基于该结果重新规划，禁止假定原写入仍待执行。",
						step.ID, step.Status, step.LastError, truncateRunes(string(step.AfterState), 2000))})
					state.LastFailure, state.FailureCount = "", 0
				}
			}
			state.Messages = append(state.Messages, RuntimeMessage{Role: "user", Content: "任务从持久检查点续跑。以下是最新数据水位、当前时间和最近运行上下文；它们优先于检查点中的旧快照：\n" + freshInput})
			sequence := state.NextSequence - 1
			if maxSequence > sequence {
				sequence = maxSequence
			}
			return compactRuntimeMessages(state.Messages, settings.ContextTokenBudget, goal.Objective), sequence, state.LastFailure, state.FailureCount, nil
		}
	}
	input, err := m.runtimeFreshInput(ctx, goal, packet, settings, steps)
	if err != nil {
		return nil, 0, "", 0, err
	}
	return []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}, {Role: "user", Content: input}}, maxSequence, "", 0, nil
}

func (m *Manager) runtimeFreshInput(ctx context.Context, goal model.AgentGoal, packet model.AnalysisPacket,
	settings model.AgentSettings, steps []model.AgentStep) (string, error) {
	input, err := modelInput(packet, settings)
	if err != nil {
		return "", err
	}
	recent := m.recentRuntimeContext(ctx)
	now := time.Now().UTC()
	shanghai := time.FixedZone(model.AgentDefaultTimezone, 8*60*60)
	input += "\n当前时间：UTC " + now.Format(time.RFC3339) + "；北京时间 " + now.In(shanghai).Format(time.RFC3339) +
		"\n当前持久目标：" + goal.Objective + "\n目标上下文：" + string(goal.Context) + "\n最近24小时运行上下文：" + string(recent)
	if len(steps) > 0 {
		input += "\n该目标在中断前已持久化的步骤（不得使用旧序号或盲目重放）：" + string(marshalRaw(steps))
	}
	return input, nil
}

func (m *Manager) recentRuntimeContext(ctx context.Context) json.RawMessage {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	runs, _ := m.store.ListAgentRuns(ctx, 200)
	filtered := make([]model.AgentRun, 0, 40)
	for _, run := range runs {
		if run.StartedAt.Before(cutoff) {
			continue
		}
		run.Error = truncateRunes(run.Error, 240)
		filtered = append(filtered, run)
		if len(filtered) >= 40 {
			break
		}
	}
	goals, _ := m.store.ListAgentGoals(ctx, "", 40)
	commands, _ := m.store.ListScheduledCommands(ctx, "", 0, 40)
	memories, _ := m.store.ListAgentMemories(ctx, "", "", 40)
	outcomes, _ := m.store.ListRecentDecisionOutcomes(ctx, 80)
	return marshalRaw(map[string]any{"runs": filtered, "goals": goals, "scheduled_commands": commands,
		"memories": memories, "decision_outcomes": outcomes})
}

func (m *Manager) completeRuntimeTurn(ctx context.Context, messages []RuntimeMessage, lane string, attemptBudget int) (RuntimeTurn, model.AgentProvider, int, error) {
	if attemptBudget <= 0 {
		return RuntimeTurn{}, model.AgentProvider{}, 0,
			newRuntimeError(runtimeErrorModelNoProgress, errors.New("目标已达到模型调用硬上限"))
	}
	release, err := m.acquireModelSlot(ctx, lane)
	if err != nil {
		return RuntimeTurn{}, model.AgentProvider{}, 0, err
	}
	if release == nil {
		return m.completeRuntimeTurnWithoutSlot(ctx, messages, attemptBudget)
	}
	defer release()
	return m.completeRuntimeTurnWithoutSlot(ctx, messages, attemptBudget)
}

func (m *Manager) acquireModelSlot(ctx context.Context, lane string) (func(), error) {
	slot := m.backgroundModelSlot
	if lane == model.AgentLaneInteractive {
		slot = m.interactiveModelSlot
	}
	if slot == nil {
		return nil, nil
	}
	if m.onModelSlotWait != nil {
		m.onModelSlotWait(lane)
	}
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) completeRuntimeTurnWithoutSlot(ctx context.Context, messages []RuntimeMessage, attemptBudget int) (RuntimeTurn, model.AgentProvider, int, error) {
	providers, err := m.store.ListAgentProviders(ctx)
	if err != nil {
		return RuntimeTurn{}, model.AgentProvider{}, 0, err
	}
	failures := make([]string, 0)
	var classifiedFailure error
	modelAttempts := 0
	for _, provider := range providers {
		if !provider.Enabled || provider.BaseURL == "" || provider.Model == "" || len(provider.CredentialCiphertext) == 0 {
			continue
		}
		if m.box == nil {
			failure := newRuntimeError(runtimeErrorProviderAuthFailed, errors.New("缺少模型凭据加密密钥"))
			classifiedFailure = preferredRuntimeFailure(classifiedFailure, failure)
			failures = append(failures, provider.Slot+": "+failure.Error())
			continue
		}
		plaintext, decryptErr := m.box.Decrypt(provider.CredentialNonce, provider.CredentialCiphertext)
		if decryptErr != nil {
			failure := newRuntimeError(runtimeErrorProviderAuthFailed, errors.New("凭据解密失败"))
			classifiedFailure = preferredRuntimeFailure(classifiedFailure, failure)
			failures = append(failures, provider.Slot+": "+failure.Error())
			continue
		}
		if modelAttempts >= attemptBudget {
			break
		}
		modelAttempts++
		turn, completeErr := m.client.CompleteRuntimeNative(ctx, provider, string(plaintext), messages, CapabilitySpecs())
		if errors.Is(completeErr, errNativeToolsUnsupported) {
			if modelAttempts >= attemptBudget {
				return RuntimeTurn{}, model.AgentProvider{}, modelAttempts,
					newRuntimeError(runtimeErrorModelNoProgress, errors.New("目标已达到模型调用硬上限，无法进入兼容模式"))
			}
			modelAttempts++
			turn, completeErr = m.completeRuntimeFallback(ctx, provider, string(plaintext), messages)
		}
		if completeErr == nil {
			now := time.Now().UTC()
			_ = m.store.RecordAgentProviderSuccess(ctx, provider.Slot, now)
			return turn, provider, modelAttempts, nil
		}
		classifiedFailure = preferredRuntimeFailure(classifiedFailure, completeErr)
		_ = m.store.RecordAgentProviderFailure(ctx, provider.Slot, string(runtimeErrorClassOf(completeErr)),
			completeErr.Error(), time.Now().UTC())
		failures = append(failures, provider.Slot+": "+completeErr.Error())
	}
	if modelAttempts >= attemptBudget {
		return RuntimeTurn{}, model.AgentProvider{}, modelAttempts,
			newRuntimeError(runtimeErrorModelNoProgress, errors.New("目标已达到模型调用硬上限"))
	}
	if len(failures) == 0 {
		return RuntimeTurn{}, model.AgentProvider{}, modelAttempts, newRuntimeError(runtimeErrorProviderAuthFailed,
			errors.New("没有可用的主模型或备用模型"))
	}
	class := runtimeErrorClassOf(classifiedFailure)
	return RuntimeTurn{}, model.AgentProvider{}, modelAttempts, newRuntimeError(class, errors.New(strings.Join(failures, "; ")))
}

func (m *Manager) completeRuntimeFallback(ctx context.Context, provider model.AgentProvider, apiKey string, messages []RuntimeMessage) (RuntimeTurn, error) {
	transcript, err := json.Marshal(redactRuntimeMessages(messages))
	if err != nil {
		return RuntimeTurn{}, fmt.Errorf("构造兼容模式上下文失败: %w", err)
	}
	system := runtimeSystemPrompt() + "\n当前模型不支持原生工具。若需要工具，只返回一个 actions 元素：" +
		`{"type":"能力名","arguments":{...},"reason":"原因"}；一次只请求一个能力。可用能力：` + string(capabilityCatalogJSON())
	decision, err := m.client.Complete(ctx, provider, apiKey, system, string(transcript))
	if err != nil {
		return RuntimeTurn{}, err
	}
	return RuntimeTurn{Decision: &decision}, nil
}

func decisionActionToolCall(action AgentAction, confidence float64) (RuntimeToolCall, error) {
	if _, ok := capabilitySpec(action.Type); !ok {
		return RuntimeToolCall{}, fmt.Errorf("未授权能力 %s", action.Type)
	}
	arguments := action.Arguments
	if len(arguments) == 0 {
		legacy := map[string]any{"reason": action.Reason}
		if action.AccountID > 0 {
			legacy["account_id"] = action.AccountID
		}
		if action.SourceID > 0 {
			legacy["source_id"] = action.SourceID
		}
		if action.KeyID != "" {
			legacy["key_id"] = action.KeyID
		}
		if action.TargetTier != "" {
			legacy["target_tier"] = action.TargetTier
			legacy["confidence"] = confidence
		}
		if action.LoadFactor != nil {
			legacy["load_factor"] = action.LoadFactor
		}
		if action.ScopeType != "" {
			legacy["scope_type"] = action.ScopeType
		}
		if action.ScopeID != "" {
			legacy["scope_id"] = action.ScopeID
		}
		if len(action.Config) > 0 {
			legacy["config"] = action.Config
		}
		if action.PolicyID > 0 {
			legacy["policy_id"] = action.PolicyID
		}
		arguments, _ = json.Marshal(legacy)
	}
	if _, err := normalizedArguments(arguments); err != nil {
		return RuntimeToolCall{}, err
	}
	hash := sha256.Sum256(append([]byte(action.Type+":"), arguments...))
	return RuntimeToolCall{ID: "fallback-" + hex.EncodeToString(hash[:8]), Type: "function",
		Function: RuntimeFunctionCall{Name: action.Type, Arguments: string(arguments)}}, nil
}

func (m *Manager) completeRuntimeGoal(ctx context.Context, goal *model.AgentGoal, run *model.AgentRun,
	packet model.AnalysisPacket, settings model.AgentSettings, goalContext runtimeGoalContext, decision ModelDecision) error {
	now := time.Now().UTC()
	run.Summary, run.Conclusion, run.Confidence = decision.Summary, decision.Conclusion, decision.Confidence
	run.ActionsJSON, _ = json.Marshal(decision.Actions)
	run.Status, run.CompletedAt = "completed", &now
	goal.Status, goal.CompletedAt, goal.LastError = model.AgentGoalStatusCompleted, &now, ""
	goal.CompletedWithWarnings = goal.AttemptCount > 0 || goal.ContractFailureCount > 0 || goal.NoProgressCount > 0
	if err := m.store.UpdateAgentGoal(ctx, *goal); err != nil {
		return err
	}
	if goal.ConversationID != nil {
		content := strings.TrimSpace(decision.Summary + "\n\n" + decision.Conclusion)
		_ = m.store.AddAgentMessage(ctx, &model.AgentMessage{ConversationID: *goal.ConversationID, Role: "assistant", Content: content})
	}
	if goalContext.Kind == model.AgentRunDaily {
		m.saveDailyReport(ctx, *run, packet, decision, goalContext.ReportDate)
	}
	m.advanceSchedule(ctx, settings, goalContext.Kind, now)
	memory := model.AgentMemory{ScopeType: "goal", ScopeID: fmt.Sprint(goal.ID), Kind: model.AgentMemoryEpisodic,
		Key: "outcome", Summary: truncateRunes(decision.Summary, 300), Content: marshalRaw(map[string]any{"conclusion": decision.Conclusion,
			"confidence": decision.Confidence, "goal_id": goal.ID}), Importance: .6}
	_ = m.store.UpsertAgentMemory(ctx, &memory)
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "goal_completed", "info", "agent", map[string]any{"summary": decision.Summary})
	return nil
}

func (m *Manager) waitGoal(ctx context.Context, goal *model.AgentGoal, cause error) error {
	next := time.Now().UTC().Add(5 * time.Second)
	if goal.Lane == model.AgentLaneInteractive {
		next = time.Now().UTC().Add(time.Second)
	}
	goal.Status, goal.LastError, goal.NextRunnableAt = model.AgentGoalStatusPlanned, truncateRunes(cause.Error(), 1000), &next
	_ = m.store.UpdateAgentGoal(ctx, *goal)
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "goal_waiting", "warning", "agent", map[string]any{"reason": goal.LastError})
	return cause
}

func (m *Manager) runtimeFreezeError(ctx context.Context) error {
	if m.engine == nil {
		return errors.New("无法确认智能体冻结状态")
	}
	freeze, err := m.engine.FreezeState(ctx)
	if err != nil {
		return fmt.Errorf("无法确认智能体冻结状态，已安全暂停: %w", err)
	}
	if freeze.AllAutomation {
		return errors.New("全部自动化已被冻结，智能体目标已保存并暂停")
	}
	if freeze.Agent {
		return errors.New("智能体已被冻结，目标已保存并暂停")
	}
	return nil
}

func (m *Manager) finishRuntimeRun(ctx context.Context, run *model.AgentRun, status string, cause error) {
	if run == nil {
		return
	}
	now := time.Now().UTC()
	run.Status, run.CompletedAt = status, &now
	if cause != nil {
		run.Error = truncateRunes(cause.Error(), 1000)
	}
}

func (m *Manager) checkpointRuntimeYield(ctx context.Context, goal *model.AgentGoal, run *model.AgentRun,
	messages []RuntimeMessage, sequence int, lastFailure string, failureCount int, runStatus, goalStatus, eventType string,
	cause error, wake bool) error {
	if cause == nil {
		cause = errors.New("智能体运行已暂停")
	}
	reason := truncateRunes(cause.Error(), 1000)
	if err := m.saveRuntimeCheckpoint(ctx, goal.ID, nil, messages, sequence, lastFailure, failureCount); err != nil {
		reason = truncateRunes(reason+"；检查点保存失败："+err.Error(), 1000)
		cause = errors.New(reason)
	}
	m.finishRuntimeRun(ctx, run, runStatus, cause)
	goal.Status, goal.LastError, goal.CompletedAt = goalStatus, reason, nil
	_ = m.store.UpdateAgentGoal(ctx, *goal)
	m.appendRuntimeEvent(ctx, &goal.ID, nil, eventType, "warning", "agent", map[string]any{"reason": reason})
	if wake {
		m.wakeLane(goal.Lane)
	}
	return cause
}

func (m *Manager) recordRuntimeObservation(ctx context.Context, settings model.AgentSettings,
	proposed, executable, violations, structureErrors int) {
	if settings.OptimizerMode != model.AgentOptimizerObserve {
		return
	}
	_ = m.store.RecordAgentObservation(ctx, proposed, executable, violations, structureErrors)
}

func (m *Manager) failGoal(ctx context.Context, goal *model.AgentGoal, cause error) error {
	return m.failGoalClass(ctx, goal, runtimeErrorRuntimeInternal, cause, false)
}

func (m *Manager) repairRuntimeContract(ctx context.Context, goal *model.AgentGoal, cause error, messages *[]RuntimeMessage) bool {
	now := time.Now().UTC()
	goal.ContractFailureCount++
	goal.LastErrorClass = string(runtimeErrorModelContractInvalid)
	goal.LastErrorAt = &now
	goal.LastError = truncateRunes(cause.Error(), 1000)
	_ = m.store.UpdateAgentGoal(ctx, *goal)
	if goal.ContractFailureCount > maxContractRepairAttempts {
		return false
	}
	*messages = append(*messages, RuntimeMessage{Role: "user", Content: "最终决策违反机器契约：" +
		truncateRunes(cause.Error(), 600) + "。请严格按系统消息中的完整 JSON Schema 重新生成一次；不得输出 Markdown，不得在最终结论保留 evidence_requests。"})
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "model_contract_repair_requested", "warning", "agent",
		map[string]any{"error_class": runtimeErrorModelContractInvalid, "attempt": goal.ContractFailureCount})
	return true
}

func (m *Manager) retryProviderGoal(ctx context.Context, goal *model.AgentGoal, class runtimeErrorClass, cause error) error {
	if goal.MaxAttempts <= 0 {
		goal.MaxAttempts = defaultGoalMaxAttempts
	}
	goal.AttemptCount++
	now := time.Now().UTC()
	goal.LastErrorClass, goal.LastErrorAt, goal.LastError = string(class), &now, truncateRunes(cause.Error(), 1000)
	if goal.AttemptCount >= goal.MaxAttempts {
		return m.failGoalClass(ctx, goal, class, cause, true)
	}
	delay := providerRetryDelay(goal.ID, goal.AttemptCount)
	next := now.Add(delay)
	goal.Status, goal.NextRunnableAt = model.AgentGoalStatusPlanned, &next
	_ = m.store.UpdateAgentGoal(ctx, *goal)
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "provider_retry_scheduled", "warning", "agent",
		map[string]any{"error_class": class, "attempt": goal.AttemptCount, "max_attempts": goal.MaxAttempts, "delay_ms": delay.Milliseconds()})
	return cause
}

func providerRetryDelay(goalID int64, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second << min(attempt-1, 5)
	if delay > maxProviderRetryDelay {
		delay = maxProviderRetryDelay
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", goalID, attempt)))
	jitter := time.Duration(int(hash[0])<<8|int(hash[1])) % (500 * time.Millisecond)
	if delay+jitter > maxProviderRetryDelay {
		return maxProviderRetryDelay
	}
	return delay + jitter
}

func recordRuntimeFailureFingerprint(fingerprint string, lastFailure *string, failureCount *int) {
	if lastFailure == nil || failureCount == nil {
		return
	}
	if fingerprint == *lastFailure {
		*failureCount++
		return
	}
	*lastFailure, *failureCount = fingerprint, 1
}

func (m *Manager) failGoalClass(ctx context.Context, goal *model.AgentGoal, class runtimeErrorClass, cause error, deadLetter bool) error {
	if cause == nil {
		cause = errors.New(string(class))
	}
	now := time.Now().UTC()
	goal.Status, goal.LastError = model.AgentGoalStatusFailed, truncateRunes(cause.Error(), 1000)
	goal.LastErrorClass, goal.LastErrorAt, goal.NextRunnableAt = string(class), &now, nil
	if deadLetter {
		goal.DeadLetteredAt = &now
	}
	_ = m.store.UpdateAgentGoal(ctx, *goal)
	m.appendRuntimeEvent(ctx, &goal.ID, nil, "goal_failed", "error", "agent",
		map[string]any{"reason": goal.LastError, "error_class": class, "dead_lettered": deadLetter})
	return cause
}

func (m *Manager) saveRuntimeCheckpoint(ctx context.Context, goalID int64, stepID *int64, messages []RuntimeMessage,
	sequence int, lastFailure string, failureCount int) error {
	state := runtimeCheckpoint{Messages: messages, NextSequence: sequence + 1, LastFailure: lastFailure, FailureCount: failureCount}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(payload)
	return m.store.SaveAgentCheckpoint(ctx, &model.AgentCheckpoint{GoalID: goalID, StepID: stepID, Kind: "runtime",
		State: payload, StateHash: hex.EncodeToString(hash[:])})
}

func compactRuntimeMessages(messages []RuntimeMessage, budget int, objective string) []RuntimeMessage {
	if budget < 2000 {
		budget = 16000
	}
	payload, _ := json.Marshal(messages)
	if len(payload)/4 <= budget || len(messages) <= 8 {
		return messages
	}
	keep := 6
	if keep > len(messages)-1 {
		keep = len(messages) - 1
	}
	result := []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}, {Role: "user", Content: "继续持久目标：" + objective + "。较早工具结果已在数据库检查点和审计中，仅保留最近结果。"}}
	result = append(result, messages[len(messages)-keep:]...)
	return result
}

func (m *Manager) appendRuntimeEvent(ctx context.Context, goalID, stepID *int64, eventType, severity, actor string, payload any) {
	raw := marshalRaw(payload)
	seed := fmt.Sprintf("%s:%d:%d:%d:%s", eventType, derefInt64(goalID), derefInt64(stepID), time.Now().UTC().UnixNano(), raw)
	hash := sha256.Sum256([]byte(seed))
	_, _ = m.store.AppendAgentEvent(ctx, &model.AgentEvent{EventKey: eventType + ":" + hex.EncodeToString(hash[:12]),
		GoalID: goalID, StepID: stepID, Type: eventType, Severity: severity, Actor: actor, Payload: raw})
}

func (m *Manager) scheduledCommandWorker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		// Reconciliation is read-only with respect to managed systems and must
		// continue while automation writes are frozen.
		m.reconcileUncertainWork(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		freeze, err := m.engine.FreezeState(ctx)
		if err != nil || freeze.Agent || freeze.AllAutomation {
			continue
		}
		commands, err := m.store.ClaimDueScheduledCommands(ctx, m.workerID, time.Now().UTC(), commandLease, 10)
		if err != nil {
			m.logger.Warn("agent_command_claim_failed", "error", err)
			continue
		}
		for _, command := range commands {
			m.executeScheduledCommand(ctx, command)
		}
	}
}

func (m *Manager) executeScheduledCommand(ctx context.Context, command model.ScheduledCommand) {
	var conditions struct {
		AdministratorGrant *AdministratorGrant `json:"administrator_grant,omitempty"`
		SnapshotVersion    string              `json:"snapshot_version,omitempty"`
		EvidenceRefs       []string            `json:"evidence_refs,omitempty"`
		// Legacy fields are deliberately not consumed. Existing rows containing
		// only administrator_direct therefore execute with ordinary agent rules.
		LegacyAdministratorDirect bool `json:"administrator_direct,omitempty"`
	}
	_ = json.Unmarshal(command.Conditions, &conditions)
	if conditions.LegacyAdministratorDirect && conditions.AdministratorGrant == nil {
		reason := "旧版管理员布尔授权已失效，定时命令必须由管理员重新确认"
		_, _ = m.store.FailScheduledCommand(ctx, command.ID, m.workerID, reason, nil)
		m.appendRuntimeEvent(ctx, command.GoalID, command.StepID, "legacy_administrator_command_rejected", "error", "system",
			map[string]any{"command_id": command.ID, "reason": reason})
		return
	}
	freeze, freezeErr := m.engine.FreezeState(ctx)
	if freezeErr != nil || freeze.Agent || freeze.AllAutomation {
		reason := "定时命令领取后检测到自动化冻结，未进入能力执行"
		if freezeErr != nil {
			reason = "定时命令领取后无法确认冻结状态，未进入能力执行: " + freezeErr.Error()
		}
		if err := m.store.DeferLeasedScheduledCommand(ctx, command.ID, m.workerID, reason,
			time.Now().UTC().Add(30*time.Second)); err != nil {
			m.logger.Warn("agent_command_freeze_deferral_failed", "command_id", command.ID, "error", err)
		}
		return
	}
	run := model.AgentRun{Kind: model.AgentRunManual, Trigger: fmt.Sprintf("持久定时命令 #%d", command.ID), Status: "acting",
		StartedAt: time.Now().UTC(), ActionsJSON: json.RawMessage("[]")}
	invocation := CapabilityInvocation{Name: command.Capability, Arguments: command.Arguments,
		RunID: run.ID, GoalID: derefInt64(command.GoalID), StepID: derefInt64(command.StepID), Actor: command.CreatedBy,
		IdempotencyKey: command.IdempotencyKey, AdministratorGrant: conditions.AdministratorGrant,
		CreatedAt: command.CreatedAt, ExpiresAt: command.ExpiresAt, SnapshotVersion: conditions.SnapshotVersion,
		EvidenceRefs: append([]string(nil), conditions.EvidenceRefs...)}
	attemptedAt := time.Now().UTC()
	evidence := scheduledReconciliationEvidence{AttemptedAt: attemptedAt, BeforeState: m.capabilityState(ctx, invocation)}
	if err := m.store.RecordScheduledCommandAttemptState(ctx, command.ID, m.workerID, marshalRaw(evidence), attemptedAt); err != nil {
		_, _ = m.store.FailScheduledCommand(ctx, command.ID, m.workerID, "写入动作基线保存失败，已拒绝执行: "+err.Error(), nil)
		now := time.Now().UTC()
		run.Status, run.Error, run.CompletedAt = "failed", err.Error(), &now
		return
	}
	execution, execErr := m.ExecuteCapability(ctx, invocation)
	now := time.Now().UTC()
	if execErr == nil {
		_ = m.store.CompleteScheduledCommand(ctx, command.ID, m.workerID, marshalRaw(execution), now)
		run.Status, run.Summary, run.CompletedAt = "completed", execution.Message, &now
		m.appendRuntimeEvent(ctx, command.GoalID, command.StepID, "scheduled_command_completed", "info", "agent", execution)
	} else {
		if isAutomationFreezeError(execErr) {
			_ = m.store.DeferLeasedScheduledCommand(ctx, command.ID, m.workerID,
				"能力执行前检测到自动化冻结，未发生外部写入", now.Add(30*time.Second))
			run.Status, run.Error, run.CompletedAt = "waiting", execErr.Error(), &now
			m.appendRuntimeEvent(ctx, command.GoalID, command.StepID, "scheduled_command_deferred_by_freeze", "warning", "system", execution)
			return
		} else if execution.Retryable {
			uncertainAt := now
			evidence.UncertainAt = &uncertainAt
			evidence.Execution = &execution
			_ = m.store.MarkScheduledCommandReconcilingWithResult(ctx, command.ID, m.workerID,
				"外部写入结果不明确，等待只读核对: "+execErr.Error(), marshalRaw(evidence))
		} else {
			_, _ = m.store.FailScheduledCommand(ctx, command.ID, m.workerID, execErr.Error(), nil)
		}
		run.Status, run.Error, run.CompletedAt = "failed", execErr.Error(), &now
		m.appendRuntimeEvent(ctx, command.GoalID, command.StepID, "scheduled_command_failed", "error", "agent", execution)
	}
}

func isAutomationFreezeError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "自动化已被冻结") || strings.Contains(message, "智能体已被冻结") ||
		strings.Contains(message, "冻结状态")
}

func runtimeIdempotency(goalID int64, sequence int, capability string, arguments json.RawMessage) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s:%s", goalID, sequence, capability, arguments)))
	return "goal-" + fmt.Sprint(goalID) + "-" + hex.EncodeToString(hash[:12])
}

func explicitAdministratorCommand(message string) bool {
	lower := strings.ToLower(strings.TrimSpace(message))
	if lower == "" {
		return false
	}
	for _, marker := range []string{"为什么", "为何", "怎么", "如何", "是否", "能否", "可不可以", "可以吗", "行吗", "吗？", "吗?", "?", "？", "解释", "分析", "查询", "查看", "看看", "告诉我", "汇报"} {
		if strings.Contains(lower, marker) {
			return false
		}
	}
	for _, marker := range []string{"暂停", "恢复", "设置", "设为", "固定", "保持到", "切换", "开启", "关闭", "解除", "回退", "执行", "立即", "定时", "schedule", "resume", "pause", "set ", "switch"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func administratorCommandHash(message string) string {
	hash := sha256.Sum256([]byte(strings.TrimSpace(message)))
	return hex.EncodeToString(hash[:])
}

func severityForExecution(execution CapabilityExecution) string {
	if execution.Status == "failed" || execution.Status == "blocked" {
		return "error"
	}
	if execution.Status == "proposed" {
		return "warning"
	}
	return "info"
}

func (m *Manager) recordObservationModelError(ctx context.Context, settings model.AgentSettings, err error) {
	if settings.OptimizerMode != model.AgentOptimizerObserve || err == nil {
		return
	}
	message := strings.ToLower(err.Error())
	violations, structureErrors := 0, 0
	if strings.Contains(message, "未授权能力") || strings.Contains(message, "invalid tool") || strings.Contains(message, "工具调用") {
		structureErrors = 1
	}
	if strings.Contains(message, "未授权能力") || strings.Contains(message, "shell") || strings.Contains(message, "sql") || strings.Contains(message, "secret") {
		violations = 1
	}
	if violations+structureErrors > 0 {
		_ = m.store.RecordAgentObservation(ctx, 0, 0, violations, structureErrors)
	}
}

func truncateRunes(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit])
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
