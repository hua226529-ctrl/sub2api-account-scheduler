package agent

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

const reconciliationInterval = 30 * time.Second

type reconciliationVerdict string

const (
	reconciliationApplied      reconciliationVerdict = "applied"
	reconciliationNotApplied   reconciliationVerdict = "not_applied"
	reconciliationInconclusive reconciliationVerdict = "inconclusive"
)

type reconciliationAssessment struct {
	Verdict   reconciliationVerdict `json:"verdict"`
	Reason    string                `json:"reason"`
	Fresh     bool                  `json:"fresh"`
	Readback  json.RawMessage       `json:"readback"`
	CheckedAt time.Time             `json:"checked_at"`
}

type scheduledReconciliationEvidence struct {
	AttemptedAt time.Time                 `json:"attempted_at"`
	UncertainAt *time.Time                `json:"uncertain_at,omitempty"`
	BeforeState json.RawMessage           `json:"before_state"`
	Execution   *CapabilityExecution      `json:"execution,omitempty"`
	Assessment  *reconciliationAssessment `json:"assessment,omitempty"`
}

type stepReconciliationEvidence struct {
	AttemptedAt time.Time  `json:"attempted_at"`
	UncertainAt *time.Time `json:"uncertain_at,omitempty"`
}

type accountReconciliationState struct {
	AccountID        int64                `json:"account_id"`
	Schedulable      bool                 `json:"schedulable"`
	LoadFactor       *int                 `json:"load_factor"`
	Control          model.AccountControl `json:"control"`
	Policy           model.Policy         `json:"policy"`
	SnapshotSyncedAt *time.Time           `json:"snapshot_synced_at"`
}

type sourceReconciliationState struct {
	ID               int64      `json:"id"`
	Enabled          bool       `json:"enabled"`
	PauseBelow       float64    `json:"pause_below"`
	ResumeAt         float64    `json:"resume_at"`
	SelectedKeyID    string     `json:"selected_key_id"`
	RoutingEnabled   bool       `json:"routing_enabled"`
	RoutingPool      string     `json:"routing_pool"`
	LastSuccessAt    *time.Time `json:"last_success_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	FailoverPolicies []struct {
		KeyID           string    `json:"key_id"`
		CurrentTier     string    `json:"current_tier"`
		ObservedGroupID string    `json:"observed_group_id"`
		StateUpdatedAt  time.Time `json:"state_updated_at"`
	} `json:"failover_policies"`
}

func (m *Manager) reconcileUncertainWork(ctx context.Context) {
	now := time.Now().UTC()
	commands, err := m.store.ListScheduledCommands(ctx, model.AgentCommandStatusReconciling, 0, 100)
	if err != nil {
		m.logger.Warn("agent_command_reconciliation_list_failed", "error", err)
	} else {
		for _, command := range commands {
			if now.Sub(command.UpdatedAt) < reconciliationInterval {
				continue
			}
			m.reconcileScheduledCommand(ctx, command, now)
		}
	}
	steps, err := m.store.ListAgentStepsByStatus(ctx, model.AgentStepStatusReconciling, 100)
	if err != nil {
		m.logger.Warn("agent_step_reconciliation_list_failed", "error", err)
		return
	}
	for _, step := range steps {
		if now.Sub(step.UpdatedAt) < reconciliationInterval {
			continue
		}
		m.reconcileAgentStep(ctx, step, now)
	}
}

func (m *Manager) reconcileScheduledCommand(ctx context.Context, command model.ScheduledCommand, now time.Time) {
	if !store.CapabilitySupportsRestartReadback(command.Capability) {
		reason := "该能力没有可证明外部效果的只读状态；旧命令已失败关闭，必须基于最新状态重新规划"
		if err := m.store.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusFailed,
			command.Result, reason, nil); err != nil {
			m.logger.Warn("agent_command_unobservable_reconciliation_failed", "command_id", command.ID, "error", err)
			return
		}
		assessment := reconciliationAssessment{Verdict: reconciliationInconclusive, Reason: reason,
			Readback: json.RawMessage("{}"), CheckedAt: now}
		m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciled_replan", "warning",
			fmt.Sprintf("command:%d", command.ID), assessment)
		if command.GoalID != nil {
			m.resumeGoalAfterReconciliation(ctx, *command.GoalID, reason)
		}
		return
	}
	var evidence scheduledReconciliationEvidence
	if err := json.Unmarshal(command.Result, &evidence); err != nil || evidence.AttemptedAt.IsZero() || len(evidence.BeforeState) == 0 {
		assessment := reconciliationAssessment{Verdict: reconciliationInconclusive,
			Reason: "缺少写入前持久化基线，无法证明动作已发生或未发生", Readback: json.RawMessage("{}"), CheckedAt: now}
		m.keepScheduledCommandReconciling(ctx, command, evidence, assessment)
		return
	}
	invocation := CapabilityInvocation{Name: command.Capability, Arguments: command.Arguments,
		GoalID: derefInt64(command.GoalID), StepID: derefInt64(command.StepID), IdempotencyKey: command.IdempotencyKey}
	freshAfter := evidence.AttemptedAt.Add(time.Second)
	if evidence.UncertainAt != nil {
		freshAfter = evidence.UncertainAt.UTC()
	}
	assessment := m.assessCapabilityReconciliation(ctx, invocation, evidence.BeforeState, freshAfter, now)
	evidence.Assessment = &assessment
	result := marshalRaw(evidence)
	switch assessment.Verdict {
	case reconciliationApplied:
		if err := m.store.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusCompleted,
			result, assessment.Reason, nil); err != nil {
			m.logger.Warn("agent_command_reconciliation_complete_failed", "command_id", command.ID, "error", err)
			return
		}
		m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciled_applied", "info",
			fmt.Sprintf("command:%d", command.ID), assessment)
	case reconciliationNotApplied:
		if command.Capability == "transition_token_group_tier" {
			// A group transition reserves its upstream idempotency key before
			// writing. Once that transition is terminal, replaying the same key
			// would only return the old failed row, not perform a new switch.
			if err := m.store.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusFailed,
				result, "已确认切组未生效；原幂等流水不可重放，必须基于新快照重新规划", nil); err != nil {
				m.logger.Warn("agent_command_group_reconciliation_fail_failed", "command_id", command.ID, "error", err)
				return
			}
			m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciled_replan", "warning",
				fmt.Sprintf("command:%d", command.ID), assessment)
			return
		}
		if command.AttemptCount >= command.MaxAttempts || (command.ExpiresAt != nil && !command.ExpiresAt.After(now)) {
			if err := m.store.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusFailed,
				result, "已确认原动作未生效，但任务已过期或重试次数耗尽", nil); err != nil {
				m.logger.Warn("agent_command_reconciliation_fail_failed", "command_id", command.ID, "error", err)
				return
			}
			m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciled_failed", "error",
				fmt.Sprintf("command:%d", command.ID), assessment)
			return
		}
		retryAt := now.Add(10 * time.Second)
		if err := m.store.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusPending,
			result, "新鲜回读确认原动作未发生，已允许使用原幂等编号安全重试", &retryAt); err != nil {
			m.logger.Warn("agent_command_reconciliation_retry_failed", "command_id", command.ID, "error", err)
			return
		}
		m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciled_retry", "warning",
			fmt.Sprintf("command:%d", command.ID), assessment)
	default:
		m.keepScheduledCommandReconciling(ctx, command, evidence, assessment)
	}
}

func (m *Manager) keepScheduledCommandReconciling(ctx context.Context, command model.ScheduledCommand,
	evidence scheduledReconciliationEvidence, assessment reconciliationAssessment) {
	evidence.Assessment = &assessment
	if err := m.store.TouchScheduledCommandReconciliation(ctx, command.ID, marshalRaw(evidence), assessment.Reason); err != nil {
		m.logger.Warn("agent_command_reconciliation_touch_failed", "command_id", command.ID, "error", err)
		return
	}
	m.appendReconciliationEvent(ctx, command.GoalID, command.StepID, "scheduled_command_reconciliation_inconclusive", "warning",
		fmt.Sprintf("command:%d", command.ID), assessment)
}

func (m *Manager) reconcileAgentStep(ctx context.Context, step model.AgentStep, now time.Time) {
	spec, known := capabilitySpec(step.Capability)
	if !known || !spec.Mutating || !store.CapabilitySupportsRestartReadback(step.Capability) {
		step.Status = model.AgentStepStatusFailed
		if known && spec.Mutating {
			step.LastError = "该能力没有可证明外部效果的只读状态；旧步骤已失败关闭，必须基于最新状态重新规划"
		} else {
			step.LastError = "服务中断前的只读或未知步骤没有可核对的外部写入，可由智能体重新查询"
		}
		step.AfterState = json.RawMessage("{}")
		if err := m.store.UpdateAgentStep(ctx, step); err == nil {
			assessment := reconciliationAssessment{Verdict: reconciliationInconclusive, Reason: step.LastError,
				Readback: step.AfterState, CheckedAt: now}
			m.appendReconciliationEvent(ctx, &step.GoalID, &step.ID, "agent_step_reconciled_replan", "warning",
				fmt.Sprintf("step:%d", step.ID), assessment)
			m.resumeGoalAfterReconciliation(ctx, step.GoalID, step.LastError)
		}
		return
	}
	invocation := CapabilityInvocation{Name: step.Capability, Arguments: step.Arguments, GoalID: step.GoalID,
		StepID: step.ID, IdempotencyKey: step.IdempotencyKey}
	var evidence stepReconciliationEvidence
	_ = json.Unmarshal(step.Preconditions, &evidence)
	freshAfter := evidence.AttemptedAt.Add(time.Second)
	if evidence.AttemptedAt.IsZero() {
		freshAfter = step.CreatedAt.Add(time.Second)
	}
	if evidence.UncertainAt != nil {
		freshAfter = evidence.UncertainAt.UTC()
	}
	assessment := m.assessCapabilityReconciliation(ctx, invocation, step.BeforeState, freshAfter, now)
	step.AfterState = assessment.Readback
	switch assessment.Verdict {
	case reconciliationApplied:
		step.Status, step.LastError = model.AgentStepStatusCompleted, ""
	case reconciliationNotApplied:
		step.Status, step.LastError = model.AgentStepStatusFailed, "新鲜回读确认原动作未发生，允许智能体基于当前状态重新规划"
	default:
		step.LastError = assessment.Reason
	}
	if err := m.store.UpdateAgentStep(ctx, step); err != nil {
		m.logger.Warn("agent_step_reconciliation_update_failed", "step_id", step.ID, "error", err)
		return
	}
	eventType, severity := "agent_step_reconciliation_inconclusive", "warning"
	if assessment.Verdict == reconciliationApplied {
		eventType, severity = "agent_step_reconciled_applied", "info"
	} else if assessment.Verdict == reconciliationNotApplied {
		eventType = "agent_step_reconciled_not_applied"
	}
	m.appendReconciliationEvent(ctx, &step.GoalID, &step.ID, eventType, severity, fmt.Sprintf("step:%d", step.ID), assessment)
	if assessment.Verdict != reconciliationInconclusive {
		m.resumeGoalAfterReconciliation(ctx, step.GoalID, assessment.Reason)
	}
}

func (m *Manager) resumeGoalAfterReconciliation(ctx context.Context, goalID int64, reason string) {
	steps, err := m.store.ListAgentSteps(ctx, goalID)
	if err != nil {
		return
	}
	for _, step := range steps {
		if step.Status == model.AgentStepStatusReconciling {
			return
		}
	}
	goal, err := m.store.GetAgentGoal(ctx, goalID)
	if err != nil || goal.Status == model.AgentGoalStatusCompleted || goal.Status == model.AgentGoalStatusCancelled ||
		goal.Status == model.AgentGoalStatusFailed {
		return
	}
	goal.Status = model.AgentGoalStatusPlanned
	goal.NextRunnableAt = nil
	goal.LastError = truncateRunes("不明确动作已完成只读核对："+reason, 1000)
	if m.store.UpdateAgentGoal(ctx, goal) == nil {
		m.wakeLane(goal.Lane)
	}
}

func (m *Manager) assessCapabilityReconciliation(ctx context.Context, invocation CapabilityInvocation,
	before json.RawMessage, attemptedAt, now time.Time) reconciliationAssessment {
	current := m.capabilityState(ctx, invocation)
	fresh := reconciliationReadbackFresh(invocation.Name, invocation.Arguments, current, attemptedAt)
	if invocation.Name == "transition_token_group_tier" {
		if transition, err := m.store.GetGroupTierTransitionByKey(ctx, invocation.IdempotencyKey); err == nil {
			if groupTransitionWasApplied(transition.Status) {
				return reconciliationAssessment{Verdict: reconciliationApplied, Reason: "幂等切组流水已完成并确认目标分组",
					Fresh: true, Readback: current, CheckedAt: now}
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return reconciliationAssessment{Verdict: reconciliationInconclusive, Reason: "读取切组幂等流水失败: " + err.Error(),
				Fresh: false, Readback: current, CheckedAt: now}
		}
	}
	verdict, reason := evaluateCapabilityReconciliation(invocation.Name, invocation.Arguments, before, current, fresh)
	return reconciliationAssessment{Verdict: verdict, Reason: reason, Fresh: fresh, Readback: current, CheckedAt: now}
}

func groupTransitionWasApplied(status string) bool {
	return status == model.GroupTransitionApplied || status == model.GroupTransitionCompleted
}

func evaluateCapabilityReconciliation(capability string, arguments, before, current json.RawMessage,
	fresh bool) (reconciliationVerdict, string) {
	applied, observable := capabilityTargetApplied(capability, arguments, current)
	if !observable {
		return reconciliationInconclusive, "该能力没有足够的可观察状态，保持隔离并禁止重放"
	}
	if !fresh {
		return reconciliationInconclusive, "当前回读数据早于动作尝试，等待下一份新鲜快照"
	}
	if applied {
		return reconciliationApplied, "新鲜只读回读确认目标状态已经生效"
	}
	same, comparable := capabilityStateUnchanged(capability, arguments, before, current)
	if comparable && same {
		return reconciliationNotApplied, "新鲜只读回读与写入前基线一致，确认目标状态未生效"
	}
	return reconciliationInconclusive, "当前状态既未达到目标，也不同于写入前基线，可能存在并发人工或自动变更"
}

func capabilityTargetApplied(capability string, arguments, current json.RawMessage) (bool, bool) {
	var account accountReconciliationState
	var source sourceReconciliationState
	switch capability {
	case "pause_account", "resume_account":
		if json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return account.Schedulable == (capability == "resume_account"), true
	case "set_load_factor":
		desired, ok := loadFactorArgument(arguments)
		if !ok || json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return sameOptionalInt(account.LoadFactor, desired), true
	case "pin_load_until":
		var args struct {
			LoadFactor int       `json:"load_factor"`
			Until      time.Time `json:"until"`
		}
		if json.Unmarshal(arguments, &args) != nil || json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return account.LoadFactor != nil && *account.LoadFactor == args.LoadFactor &&
			account.Control.LoadPinValue != nil && *account.Control.LoadPinValue == args.LoadFactor &&
			account.Control.LoadPinUntil != nil && account.Control.LoadPinUntil.Equal(args.Until), true
	case "clear_load_pin":
		if json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return account.Control.LoadPinValue == nil && account.Control.LoadPinUntil == nil, true
	case "clear_flap_protection":
		if json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return !account.Control.FlapActive, true
	case "clear_manual_override":
		if json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return account.Control.ManualOverrideUntil == nil, true
	case "update_binding":
		if json.Unmarshal(current, &account) != nil || account.AccountID <= 0 {
			return false, false
		}
		return bindingArgumentsMatch(arguments, account.Policy), true
	case "update_upstream_control":
		if json.Unmarshal(current, &source) != nil || source.ID <= 0 {
			return false, false
		}
		return upstreamArgumentsMatch(arguments, source), true
	case "transition_token_group_tier":
		var args struct {
			KeyID      string `json:"key_id"`
			TargetTier string `json:"target_tier"`
		}
		if json.Unmarshal(arguments, &args) != nil || json.Unmarshal(current, &source) != nil || source.ID <= 0 {
			return false, false
		}
		for _, policy := range source.FailoverPolicies {
			if policy.KeyID == args.KeyID {
				return policy.CurrentTier == args.TargetTier, true
			}
		}
		return false, false
	default:
		return false, false
	}
}

func capabilityStateUnchanged(capability string, arguments, before, current json.RawMessage) (bool, bool) {
	var oldAccount, newAccount accountReconciliationState
	var oldSource, newSource sourceReconciliationState
	switch capability {
	case "pause_account", "resume_account":
		if json.Unmarshal(before, &oldAccount) != nil || json.Unmarshal(current, &newAccount) != nil || oldAccount.AccountID <= 0 {
			return false, false
		}
		return oldAccount.Schedulable == newAccount.Schedulable && oldAccount.Control.OwnsPause == newAccount.Control.OwnsPause &&
			oldAccount.Control.Owner == newAccount.Control.Owner && oldAccount.Control.ManualLocked == newAccount.Control.ManualLocked &&
			sameOptionalBool(oldAccount.Control.ExpectedSchedulable, newAccount.Control.ExpectedSchedulable) &&
			sameOptionalTime(oldAccount.Control.ManualOverrideUntil, newAccount.Control.ManualOverrideUntil), true
	case "set_load_factor", "pin_load_until", "clear_load_pin":
		if json.Unmarshal(before, &oldAccount) != nil || json.Unmarshal(current, &newAccount) != nil || oldAccount.AccountID <= 0 {
			return false, false
		}
		return sameOptionalInt(oldAccount.LoadFactor, newAccount.LoadFactor) &&
			oldAccount.Control.OwnsLoadFactor == newAccount.Control.OwnsLoadFactor &&
			sameOptionalInt(oldAccount.Control.ExpectedLoadFactor, newAccount.Control.ExpectedLoadFactor) &&
			sameOptionalInt(oldAccount.Control.LoadPinValue, newAccount.Control.LoadPinValue) &&
			sameOptionalTime(oldAccount.Control.LoadPinUntil, newAccount.Control.LoadPinUntil), true
	case "clear_flap_protection":
		if json.Unmarshal(before, &oldAccount) != nil || json.Unmarshal(current, &newAccount) != nil || oldAccount.AccountID <= 0 {
			return false, false
		}
		return oldAccount.Control.FlapActive == newAccount.Control.FlapActive &&
			sameOptionalTime(oldAccount.Control.FlapTriggeredAt, newAccount.Control.FlapTriggeredAt), true
	case "clear_manual_override":
		if json.Unmarshal(before, &oldAccount) != nil || json.Unmarshal(current, &newAccount) != nil || oldAccount.AccountID <= 0 {
			return false, false
		}
		return sameOptionalTime(oldAccount.Control.ManualOverrideUntil, newAccount.Control.ManualOverrideUntil), true
	case "update_binding":
		if json.Unmarshal(before, &oldAccount) != nil || json.Unmarshal(current, &newAccount) != nil || oldAccount.AccountID <= 0 {
			return false, false
		}
		return bindingArgumentsEquivalent(arguments, oldAccount.Policy, newAccount.Policy), true
	case "update_upstream_control":
		if json.Unmarshal(before, &oldSource) != nil || json.Unmarshal(current, &newSource) != nil || oldSource.ID <= 0 {
			return false, false
		}
		return upstreamArgumentsEquivalent(arguments, oldSource, newSource), true
	case "transition_token_group_tier":
		if json.Unmarshal(before, &oldSource) != nil || json.Unmarshal(current, &newSource) != nil || oldSource.ID <= 0 {
			return false, false
		}
		var args struct {
			KeyID string `json:"key_id"`
		}
		if json.Unmarshal(arguments, &args) != nil {
			return false, false
		}
		oldTier, oldOK := tokenTier(oldSource, args.KeyID)
		newTier, newOK := tokenTier(newSource, args.KeyID)
		return oldOK && newOK && oldTier == newTier, oldOK && newOK
	default:
		return false, false
	}
}

func reconciliationReadbackFresh(capability string, arguments, current json.RawMessage, attemptedAt time.Time) bool {
	if attemptedAt.IsZero() {
		return false
	}
	var account accountReconciliationState
	if capabilityAccountID(arguments) > 0 {
		return json.Unmarshal(current, &account) == nil && account.SnapshotSyncedAt != nil &&
			!account.SnapshotSyncedAt.UTC().Before(attemptedAt.UTC())
	}
	var source sourceReconciliationState
	if json.Unmarshal(current, &source) != nil || source.ID <= 0 {
		return false
	}
	if capability == "transition_token_group_tier" {
		var args struct {
			KeyID string `json:"key_id"`
		}
		_ = json.Unmarshal(arguments, &args)
		for _, policy := range source.FailoverPolicies {
			if policy.KeyID == args.KeyID && !policy.StateUpdatedAt.IsZero() && !policy.StateUpdatedAt.UTC().Before(attemptedAt.UTC()) {
				return true
			}
		}
		return source.LastSuccessAt != nil && !source.LastSuccessAt.UTC().Before(attemptedAt.UTC())
	}
	return !source.UpdatedAt.IsZero() && !source.UpdatedAt.UTC().Before(attemptedAt.UTC())
}

func (m *Manager) appendReconciliationEvent(ctx context.Context, goalID, stepID *int64, eventType, severity,
	objectKey string, assessment reconciliationAssessment) {
	hash := sha256.Sum256(append([]byte(objectKey+":"+eventType+":"), assessment.Readback...))
	_, _ = m.store.AppendAgentEvent(ctx, &model.AgentEvent{EventKey: "reconciliation:" + hex.EncodeToString(hash[:16]),
		GoalID: goalID, StepID: stepID, Type: eventType, Severity: severity, Actor: "system", Payload: marshalRaw(assessment)})
}

func loadFactorArgument(arguments json.RawMessage) (*int, bool) {
	var values map[string]json.RawMessage
	if json.Unmarshal(arguments, &values) != nil {
		return nil, false
	}
	raw, ok := values["load_factor"]
	if !ok {
		return nil, false
	}
	var value *int
	if json.Unmarshal(raw, &value) != nil {
		return nil, false
	}
	return value, true
}

func bindingArgumentsMatch(arguments json.RawMessage, policy model.Policy) bool {
	return bindingArgumentsEquivalent(arguments, policy, policy) && bindingDesiredValuesMatch(arguments, policy)
}

func bindingDesiredValuesMatch(arguments json.RawMessage, policy model.Policy) bool {
	var args struct {
		MonitorID    *int64 `json:"monitor_id"`
		ClearMonitor bool   `json:"clear_monitor"`
		Excluded     *bool  `json:"excluded"`
		Enabled      *bool  `json:"enabled"`
	}
	if json.Unmarshal(arguments, &args) != nil {
		return false
	}
	if args.ClearMonitor && policy.MonitorID != nil {
		return false
	}
	if args.MonitorID != nil && (policy.MonitorID == nil || *policy.MonitorID != *args.MonitorID) {
		return false
	}
	if args.Excluded != nil && policy.Excluded != *args.Excluded {
		return false
	}
	return args.Enabled == nil || policy.Enabled == *args.Enabled
}

func bindingArgumentsEquivalent(arguments json.RawMessage, left, right model.Policy) bool {
	var values map[string]json.RawMessage
	if json.Unmarshal(arguments, &values) != nil {
		return false
	}
	if _, ok := values["monitor_id"]; ok || rawJSONBool(values["clear_monitor"]) {
		if !sameOptionalInt64(left.MonitorID, right.MonitorID) {
			return false
		}
	}
	if _, ok := values["excluded"]; ok && left.Excluded != right.Excluded {
		return false
	}
	if _, ok := values["enabled"]; ok && left.Enabled != right.Enabled {
		return false
	}
	return true
}

func upstreamArgumentsMatch(arguments json.RawMessage, source sourceReconciliationState) bool {
	var args struct {
		Enabled        *bool    `json:"enabled"`
		PauseBelow     *float64 `json:"pause_below"`
		ResumeAt       *float64 `json:"resume_at"`
		RoutingEnabled *bool    `json:"routing_enabled"`
		RoutingPool    *string  `json:"routing_pool"`
		SelectedKeyID  *string  `json:"selected_key_id"`
	}
	if json.Unmarshal(arguments, &args) != nil {
		return false
	}
	return (args.Enabled == nil || source.Enabled == *args.Enabled) &&
		(args.PauseBelow == nil || source.PauseBelow == *args.PauseBelow) &&
		(args.ResumeAt == nil || source.ResumeAt == *args.ResumeAt) &&
		(args.RoutingEnabled == nil || source.RoutingEnabled == *args.RoutingEnabled) &&
		(args.RoutingPool == nil || source.RoutingPool == strings.TrimSpace(*args.RoutingPool)) &&
		(args.SelectedKeyID == nil || source.SelectedKeyID == strings.TrimSpace(*args.SelectedKeyID))
}

func upstreamArgumentsEquivalent(arguments json.RawMessage, left, right sourceReconciliationState) bool {
	var values map[string]json.RawMessage
	if json.Unmarshal(arguments, &values) != nil {
		return false
	}
	checks := map[string]bool{
		"enabled": left.Enabled == right.Enabled, "pause_below": left.PauseBelow == right.PauseBelow,
		"resume_at": left.ResumeAt == right.ResumeAt, "routing_enabled": left.RoutingEnabled == right.RoutingEnabled,
		"routing_pool": left.RoutingPool == right.RoutingPool, "selected_key_id": left.SelectedKeyID == right.SelectedKeyID,
	}
	for field, equal := range checks {
		if _, ok := values[field]; ok && !equal {
			return false
		}
	}
	return true
}

func tokenTier(source sourceReconciliationState, keyID string) (string, bool) {
	for _, policy := range source.FailoverPolicies {
		if policy.KeyID == keyID {
			return policy.CurrentTier, true
		}
	}
	return "", false
}

func sameOptionalInt(left, right *int) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func sameOptionalInt64(left, right *int64) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func sameOptionalBool(left, right *bool) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func sameOptionalTime(left, right *time.Time) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Equal(*right))
}

func rawJSONBool(raw json.RawMessage) bool {
	var value bool
	return len(raw) > 0 && json.Unmarshal(raw, &value) == nil && value
}
