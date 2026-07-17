package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

// DispatchPolicyPatch is the intentionally bounded policy surface available
// to Optimizer and chat. Legacy versions remain readable, but new model output
// cannot smuggle identity, enablement, credentials, or arbitrary settings.
type DispatchPolicyPatch struct {
	FailureThreshold         *int  `json:"failure_threshold,omitempty"`
	RecoveryThreshold        *int  `json:"recovery_threshold,omitempty"`
	FlapEnabled              *bool `json:"flap_enabled,omitempty"`
	FlapWindowMinutes        *int  `json:"flap_window_minutes,omitempty"`
	FlapPauseThreshold       *int  `json:"flap_pause_threshold,omitempty"`
	FlapRecoveryThreshold    *int  `json:"flap_recovery_threshold,omitempty"`
	HealthyScoreThreshold    *int  `json:"healthy_score_threshold,omitempty"`
	WatchScoreThreshold      *int  `json:"watch_score_threshold,omitempty"`
	QuarantineScoreThreshold *int  `json:"quarantine_score_threshold,omitempty"`
	MinimumSamples           *int  `json:"minimum_samples,omitempty"`
	LatencyWarningMS         *int  `json:"latency_warning_ms,omitempty"`
	LatencyCriticalMS        *int  `json:"latency_critical_ms,omitempty"`
	TrafficPauseBelow        *int  `json:"traffic_pause_below,omitempty"`
	TrafficHealthyAt         *int  `json:"traffic_healthy_at,omitempty"`
	HardFailures10Threshold  *int  `json:"hard_failures_10_threshold,omitempty"`
	PersistentSlowRate       *int  `json:"persistent_slow_rate,omitempty"`
}

type PolicyProposalInput struct {
	ScopeType      string
	ScopeID        string
	Patch          json.RawMessage
	Reason         string
	Actor          string
	RunID          int64
	GoalID         int64
	IdempotencyKey string
}

func decodeDispatchPolicyPatch(scopeType string, raw json.RawMessage) (DispatchPolicyPatch, json.RawMessage, error) {
	if scopeType != "global" && scopeType != "pool" && scopeType != "account" {
		return DispatchPolicyPatch{}, nil, errors.New("策略作用域必须是 global、pool 或 account")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var patch DispatchPolicyPatch
	if err := decoder.Decode(&patch); err != nil {
		return patch, nil, fmt.Errorf("策略补丁必须符合类型化 schema: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return patch, nil, errors.New("策略补丁包含多余 JSON")
	}
	normalized, err := json.Marshal(patch)
	if err != nil {
		return patch, nil, err
	}
	if string(normalized) == "{}" {
		return patch, nil, errors.New("策略补丁不能为空")
	}
	for name, value := range map[string]*int{
		"failure_threshold": patch.FailureThreshold, "recovery_threshold": patch.RecoveryThreshold,
		"flap_window_minutes": patch.FlapWindowMinutes, "flap_pause_threshold": patch.FlapPauseThreshold,
		"flap_recovery_threshold": patch.FlapRecoveryThreshold, "minimum_samples": patch.MinimumSamples,
		"latency_warning_ms": patch.LatencyWarningMS, "latency_critical_ms": patch.LatencyCriticalMS,
		"hard_failures_10_threshold": patch.HardFailures10Threshold,
	} {
		if value != nil && (*value < 1 || *value > 100000) {
			return patch, nil, fmt.Errorf("策略字段 %s 超出安全范围", name)
		}
	}
	for name, value := range map[string]*int{
		"healthy_score_threshold": patch.HealthyScoreThreshold, "watch_score_threshold": patch.WatchScoreThreshold,
		"quarantine_score_threshold": patch.QuarantineScoreThreshold, "traffic_pause_below": patch.TrafficPauseBelow,
		"traffic_healthy_at": patch.TrafficHealthyAt, "persistent_slow_rate": patch.PersistentSlowRate,
	} {
		if value != nil && (*value < 0 || *value > 100) {
			return patch, nil, fmt.Errorf("策略字段 %s 必须在 0 到 100", name)
		}
	}
	return patch, normalized, nil
}

func (m *Manager) ProposeDispatchPolicy(ctx context.Context, input PolicyProposalInput) (model.ScorePolicyVersion, error) {
	input.ScopeType, input.ScopeID = strings.TrimSpace(input.ScopeType), strings.TrimSpace(input.ScopeID)
	if (input.ScopeType == "pool" || input.ScopeType == "account") && input.ScopeID == "" {
		return model.ScorePolicyVersion{}, errors.New("策略作用域编号不能为空")
	}
	patch, normalized, err := decodeDispatchPolicyPatch(input.ScopeType, input.Patch)
	if err != nil {
		return model.ScorePolicyVersion{}, err
	}
	base, err := m.store.FindActivePolicyVersion(ctx, input.ScopeType, input.ScopeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.ScorePolicyVersion{}, err
	}
	var baseID *int64
	if err == nil {
		baseID = &base.ID
	}
	version := model.ScorePolicyVersion{ScopeType: input.ScopeType, ScopeID: input.ScopeID, Config: normalized, Patch: normalized,
		Reason: strings.TrimSpace(input.Reason), AgentRunID: optionalPositiveInt64(input.RunID), SourceGoalID: optionalPositiveInt64(input.GoalID),
		BaseVersionID: baseID, CreatedBy: strings.TrimSpace(input.Actor), IdempotencyKey: strings.TrimSpace(input.IdempotencyKey)}
	if version.CreatedBy == "" {
		version.CreatedBy = "agent:optimizer"
	}
	if version.IdempotencyKey == "" {
		return version, errors.New("策略提案缺少稳定幂等键")
	}
	settings, policies, err := m.materializePolicyVersion(ctx, version)
	if err != nil {
		return version, err
	}
	version.AffectedAccountIDs = affectedPolicyAccounts(m.engine.Snapshot().Bindings, input.ScopeType, input.ScopeID, policies)
	version.Diff, err = m.policyDiff(ctx, version, settings, policies)
	if err != nil {
		return version, err
	}
	version.Simulation = m.simulatePolicy(ctx, patch, version.AffectedAccountIDs)
	version.RiskLevel = assessPolicyRisk(patch, input.ScopeType, len(version.AffectedAccountIDs), len(m.engine.Snapshot().Bindings), version.Simulation)
	version.Status = model.PolicyStatusPendingApproval
	if version.RiskLevel == model.AgentRiskLow && version.Simulation.Passed {
		version.Status = model.PolicyStatusSimulated
	}
	version.SemanticHash = policyProposalSemanticHash(version)
	if err := m.store.CreatePolicyProposal(ctx, &version); err != nil {
		return version, err
	}
	m.recordEvent(ctx, "dispatch_policy_proposed", "info", 0,
		fmt.Sprintf("策略提案 %d 已完成类型校验、模拟和风险评级", version.ID), input.RunID)
	return version, nil
}

func affectedPolicyAccounts(bindings []model.ResolvedBinding, scopeType, scopeID string, policies []model.Policy) []int64 {
	set := make(map[int64]bool)
	if scopeType == "global" {
		for _, binding := range bindings {
			set[binding.Account.ID] = true
		}
	} else {
		for _, policy := range policies {
			set[policy.AccountID] = true
		}
	}
	result := make([]int64, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (m *Manager) policyDiff(ctx context.Context, version model.ScorePolicyVersion, settings *model.Settings, policies []model.Policy) (json.RawMessage, error) {
	var patch map[string]any
	if err := json.Unmarshal(version.Patch, &patch); err != nil {
		return nil, err
	}
	var current any
	switch version.ScopeType {
	case "global":
		value, err := m.engine.Settings(ctx)
		if err != nil {
			return nil, err
		}
		current = value
	default:
		values := make([]model.Policy, 0, len(policies))
		for _, next := range policies {
			for _, binding := range m.engine.Snapshot().Bindings {
				if binding.Account.ID == next.AccountID {
					values = append(values, binding.Policy)
					break
				}
			}
		}
		current = values
	}
	currentJSON, _ := json.Marshal(current)
	var currentValue any
	_ = json.Unmarshal(currentJSON, &currentValue)
	diff := make(map[string]map[string]any, len(patch))
	for field, after := range patch {
		diff[field] = map[string]any{"before": policyFieldValue(currentValue, field), "after": after}
	}
	return json.Marshal(diff)
}

func policyFieldValue(value any, field string) any {
	if object, ok := value.(map[string]any); ok {
		return object[field]
	}
	items, _ := value.([]any)
	var first any
	for index, item := range items {
		object, _ := item.(map[string]any)
		candidate := object[field]
		if index == 0 {
			first = candidate
		} else if fmt.Sprint(candidate) != fmt.Sprint(first) {
			return "varies"
		}
	}
	return first
}

func (m *Manager) simulatePolicy(ctx context.Context, patch DispatchPolicyPatch, accountIDs []int64) model.PolicySimulation {
	result := model.PolicySimulation{Window: "24h", DataSufficient: true, Passed: true}
	totalSuccess := 0
	bindings := make(map[int64]model.ResolvedBinding, len(m.engine.Snapshot().Bindings))
	for _, binding := range m.engine.Snapshot().Bindings {
		bindings[binding.Account.ID] = binding
	}
	now := time.Now().UTC()
	for _, accountID := range accountIDs {
		binding, ok := bindings[accountID]
		if !ok {
			continue
		}
		stats, err := m.store.GetAgentWindowStats(ctx, accountID, now.Add(-24*time.Hour), now, "24h")
		if err == nil {
			result.SampleCount += stats.SampleCount
			totalSuccess += stats.SuccessCount
		}
		if !binding.Account.Schedulable {
			result.CurrentActionCount++
			result.ProposedActionCount++
		}
		failureThreshold := binding.FailureThreshold
		if patch.FailureThreshold != nil {
			failureThreshold = *patch.FailureThreshold
		}
		if binding.Account.Schedulable && binding.MonitorState.UnhealthyStreak >= failureThreshold {
			result.ProposedActionCount++
			result.PauseDelta++
		}
		recoveryThreshold := binding.RecoveryThreshold
		if patch.RecoveryThreshold != nil {
			recoveryThreshold = *patch.RecoveryThreshold
		}
		if !binding.Account.Schedulable && binding.MonitorState.HealthyStreak >= recoveryThreshold &&
			!binding.Control.BalanceLocked && !binding.Control.CostLocked && !binding.Control.HealthLocked {
			result.ProposedActionCount--
			result.ResumeDelta++
		}
	}
	if result.SampleCount < 20 {
		result.DataSufficient, result.Passed = false, false
	}
	if result.SampleCount > 0 {
		result.BaselineSuccessRate = float64(totalSuccess) * 100 / float64(result.SampleCount)
	}
	fields, _ := json.Marshal(patch)
	var values map[string]any
	_ = json.Unmarshal(fields, &values)
	for field := range values {
		if field != "failure_threshold" && field != "recovery_threshold" {
			result.UnsimmulatableFields = append(result.UnsimmulatableFields, field)
		}
	}
	sort.Strings(result.UnsimmulatableFields)
	return result
}

func assessPolicyRisk(patch DispatchPolicyPatch, scopeType string, affected, total int, simulation model.PolicySimulation) string {
	if scopeType == "global" || !simulation.DataSufficient || (affected > 3 && total > 0 && affected*2 > total) {
		return model.AgentRiskCritical
	}
	criticalChanges := 0
	if patch.FailureThreshold != nil || patch.RecoveryThreshold != nil {
		criticalChanges++
	}
	if patch.TrafficPauseBelow != nil || patch.TrafficHealthyAt != nil || patch.HardFailures10Threshold != nil {
		criticalChanges++
	}
	if criticalChanges > 1 || len(simulation.UnsimmulatableFields) > 1 {
		return model.AgentRiskHigh
	}
	if affected > 3 || len(simulation.UnsimmulatableFields) == 1 {
		return model.AgentRiskMedium
	}
	return model.AgentRiskLow
}

func policyProposalSemanticHash(version model.ScorePolicyVersion) string {
	payload, _ := json.Marshal(struct {
		ScopeType string          `json:"scope_type"`
		ScopeID   string          `json:"scope_id"`
		BaseID    *int64          `json:"base_version_id,omitempty"`
		Patch     json.RawMessage `json:"patch"`
	}{ScopeType: version.ScopeType, ScopeID: version.ScopeID, BaseID: version.BaseVersionID, Patch: version.Patch})
	hash := sha256.Sum256(append([]byte("sub2api-policy-proposal/v1\x00"), payload...))
	return hex.EncodeToString(hash[:])
}

func (m *Manager) ActivatePolicyProposal(ctx context.Context, id int64, actor string, automatic bool) error {
	version, err := m.store.GetPolicyLifecycle(ctx, id)
	if err != nil {
		return err
	}
	settings, err := m.store.GetAgentSettings(ctx)
	if err != nil {
		return err
	}
	if automatic {
		if settings.OptimizerMode != model.AgentOptimizerAuto || version.RiskLevel != model.AgentRiskLow ||
			!version.Simulation.Passed || !version.Simulation.DataSufficient {
			return errors.New("策略提案不满足低风险自动发布条件")
		}
		count, err := m.store.CountPolicyActivationsSince(ctx, time.Now().UTC().Truncate(24*time.Hour))
		if err != nil {
			return err
		}
		if count >= settings.DailyPolicyChangeBudget {
			return errors.New("策略自动发布已达到每日变更预算")
		}
		actor = "agent:optimizer:auto"
	} else if strings.TrimSpace(actor) == "" {
		return errors.New("管理员策略激活缺少明确 actor")
	}
	projectionSettings, policies, err := m.materializePolicyVersion(ctx, version)
	if err != nil {
		return err
	}
	if err := m.store.PublishPolicyProposal(ctx, id, actor, projectionSettings, policies); err != nil {
		return err
	}
	m.recordEvent(ctx, "dispatch_policy_activated", "warning", 0, fmt.Sprintf("策略版本 %d 已受控激活", id), 0)
	if version.ScopeType == "account" {
		accountID, parseErr := strconv.ParseInt(version.ScopeID, 10, 64)
		if parseErr == nil && accountID > 0 {
			m.engine.RequestAccountsFrom("policy_activation", accountID)
			return nil
		}
	}
	m.engine.RequestFullFrom("policy_activation")
	return nil
}

func (m *Manager) RollbackPolicy(ctx context.Context, activeID int64, actor, reason string, automatic bool) error {
	active, err := m.store.GetPolicyLifecycle(ctx, activeID)
	if err != nil {
		return err
	}
	if active.Status != model.PolicyStatusActive || active.PreviousActiveVersionID == nil || active.AutoRollbackCount > 0 {
		return errors.New("策略版本不可回滚或已经自动回滚过")
	}
	previous, err := m.store.GetPolicyLifecycle(ctx, *active.PreviousActiveVersionID)
	if err != nil {
		return err
	}
	projectionSettings, policies, err := m.materializePolicyVersion(ctx, previous)
	if err != nil {
		return err
	}
	if automatic {
		actor = "system:policy-rollback"
	}
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("策略回滚需要 actor 和原因")
	}
	if err := m.store.RollbackPolicyProposal(ctx, active.ID, previous.ID, actor, reason, projectionSettings, policies); err != nil {
		return err
	}
	m.recordEvent(ctx, "dispatch_policy_rolled_back", "error", 0,
		fmt.Sprintf("策略版本 %d 已确定性回滚到 %d：%s", active.ID, previous.ID, reason), 0)
	if active.ScopeType == "account" {
		if accountID, parseErr := strconv.ParseInt(active.ScopeID, 10, 64); parseErr == nil && accountID > 0 {
			m.engine.RequestAccountsFrom("policy_rollback", accountID)
			return nil
		}
	}
	m.engine.RequestFullFrom("policy_rollback")
	return nil
}

func (m *Manager) evaluatePolicyRollbacks(ctx context.Context, now time.Time) {
	versions, err := m.store.ListPolicyLifecycle(ctx, 100)
	if err != nil {
		return
	}
	bindings := make(map[int64]model.ResolvedBinding, len(m.engine.Snapshot().Bindings))
	for _, binding := range m.engine.Snapshot().Bindings {
		bindings[binding.Account.ID] = binding
	}
	for _, version := range versions {
		if version.Status != model.PolicyStatusActive || version.PreviousActiveVersionID == nil ||
			version.AutoRollbackCount > 0 || version.ActivatedAt == nil || now.Sub(*version.ActivatedAt) < 10*time.Minute {
			continue
		}
		samples, successes, paused := 0, 0, 0
		for _, accountID := range version.AffectedAccountIDs {
			stats, statsErr := m.store.GetAgentWindowStats(ctx, accountID, now.Add(-time.Hour), now, "1h")
			if statsErr == nil {
				samples += stats.SampleCount
				successes += stats.SuccessCount
			}
			if binding, ok := bindings[accountID]; ok && !binding.Account.Schedulable {
				paused++
			}
		}
		if samples < 20 {
			continue
		}
		currentRate := float64(successes) * 100 / float64(samples)
		degraded := version.Simulation.BaselineSuccessRate > 0 && currentRate+10 < version.Simulation.BaselineSuccessRate
		excessivePause := len(version.AffectedAccountIDs) > 0 && paused*10 >= len(version.AffectedAccountIDs)*7
		if degraded || excessivePause {
			reason := fmt.Sprintf("激活后确定性保护触发：success_rate=%.2f baseline=%.2f paused=%d/%d",
				currentRate, version.Simulation.BaselineSuccessRate, paused, len(version.AffectedAccountIDs))
			if err := m.RollbackPolicy(ctx, version.ID, "system:policy-rollback", reason, true); err != nil && m.logger != nil {
				m.logger.Error("policy_auto_rollback_failed", "policy_id", version.ID, "error", err)
			}
		}
	}
}
