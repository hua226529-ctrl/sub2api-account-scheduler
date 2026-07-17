package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var allowedActions = map[string]bool{
	"pause_account": true, "resume_account": true, "set_load_factor": true,
	"clear_flap_protection": true, "clear_manual_override": true, "trigger_reconcile": true,
	"transition_token_group_tier": true, "update_score_policy": true, "activate_policy_version": true,
}

func (m *Manager) validateDecision(packet model.AnalysisPacket, decision ModelDecision) error {
	if len(decision.EvidenceRequests) > 0 {
		return errors.New("模型在最终结论中仍保留证据追查请求")
	}
	if decision.NoChange && len(decision.Actions) > 0 {
		return errors.New("模型声明维持现状却同时返回了执行动作")
	}
	if len(decision.Actions) > 12 {
		return errors.New("单次分析动作超过安全上限 12 个")
	}
	accountIDs := make(map[int64]bool, len(packet.AccountCompactStates))
	for _, account := range packet.AccountCompactStates {
		accountIDs[account.AccountID] = true
	}
	accountAction := make(map[int64]string)
	groupTransitions := 0
	for _, action := range decision.Actions {
		if !allowedActions[action.Type] {
			return fmt.Errorf("模型返回未授权动作 %s", action.Type)
		}
		if len([]rune(strings.TrimSpace(action.Reason))) < 4 {
			return fmt.Errorf("动作 %s 缺少可审计原因", action.Type)
		}
		if action.AccountID > 0 {
			if !accountIDs[action.AccountID] {
				return fmt.Errorf("账号 %d 不在本次不可变数据包中", action.AccountID)
			}
			if previous := accountAction[action.AccountID]; previous != "" {
				if previous != action.Type || action.Type == "pause_account" || action.Type == "resume_account" || action.Type == "set_load_factor" {
					return fmt.Errorf("账号 %d 在同一轮包含冲突或重复动作", action.AccountID)
				}
			}
			accountAction[action.AccountID] = action.Type
		}
		if action.Type == "set_load_factor" && action.LoadFactor != nil && (*action.LoadFactor < 1 || *action.LoadFactor > 100) {
			return errors.New("模型给出的负载系数超出 1 到 100")
		}
		if (action.Type == "resume_account" || action.Type == "clear_flap_protection" || action.Type == "clear_manual_override") && decision.Confidence < .85 {
			return fmt.Errorf("高风险动作 %s 的置信度必须不低于 0.85", action.Type)
		}
		if action.Type == "transition_token_group_tier" {
			groupTransitions++
			if groupTransitions > 1 {
				return errors.New("单次分析最多允许一个令牌分组层级动作")
			}
			if action.SourceID <= 0 || strings.TrimSpace(action.KeyID) == "" {
				return errors.New("令牌分组层级动作缺少上游或令牌编号")
			}
			switch action.TargetTier {
			case model.GroupTierMain, model.GroupTierBackup, model.GroupTierEmergency:
			default:
				return errors.New("目标分组层级必须是 main、backup 或 emergency")
			}
			if !packetContainsGroupTarget(packet, action.SourceID, action.KeyID, action.TargetTier) {
				return errors.New("令牌分组层级动作不在本次不可变数据包的已确认策略中")
			}
			if decision.Confidence < .90 {
				return errors.New("令牌分组层级动作的置信度必须不低于 0.90")
			}
		}
		if (action.Type == "update_score_policy" || action.Type == "activate_policy_version") && decision.Confidence < .80 {
			return fmt.Errorf("配置动作 %s 的置信度必须不低于 0.80", action.Type)
		}
		if len(action.Config) > 16*1024 {
			return errors.New("评分策略配置超过 16KB 安全限制")
		}
	}
	return nil
}

func packetContainsGroupTarget(packet model.AnalysisPacket, sourceID int64, keyID, targetTier string) bool {
	now := packet.CutoffAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, token := range packet.GroupFailoverTokens {
		if token.SourceID != sourceID || token.KeyID != keyID {
			continue
		}
		if !token.Enabled || !token.Confirmed || !token.DataFresh || token.Frozen || token.CurrentTier == targetTier ||
			!packet.DataHealth.MonitorFresh || !packet.DataHealth.TrafficFresh {
			return false
		}
		if token.ManualHoldUntil != nil && token.ManualHoldUntil.After(now) {
			return false
		}
		if token.ManualOverrideUntil != nil && token.ManualOverrideUntil.After(now) {
			return false
		}
		if token.CooldownUntil != nil && token.CooldownUntil.After(now) &&
			!(token.CurrentTier == model.GroupTierBackup && targetTier == model.GroupTierEmergency) {
			return false
		}
		if targetTier == model.GroupTierMain || nextAgentPacketTier(token) != targetTier {
			return false
		}
		if !poolCompletelyUnavailable(packet, token.Pool) || !failoverEscalationEvidence(packet, token) {
			return false
		}
		return true
	}
	return false
}

func nextAgentPacketTier(token model.AgentGroupFailoverToken) string {
	tiers := []model.AgentGroupTierSummary{token.Main, token.Backup, token.Emergency}
	current := -1
	for index, tier := range tiers {
		if tier.Tier == token.CurrentTier {
			current = index
			break
		}
	}
	if current < 0 {
		return ""
	}
	for _, tier := range tiers[current+1:] {
		if tier.Enabled && tier.Configured {
			return tier.Tier
		}
	}
	return ""
}

func failoverEscalationEvidence(packet model.AnalysisPacket, token model.AgentGroupFailoverToken) bool {
	if len(token.AccountIDs) == 0 {
		return false
	}
	states := make(map[int64]model.AgentAccountState, len(packet.AccountCompactStates))
	for _, state := range packet.AccountCompactStates {
		states[state.AccountID] = state
	}
	eligible, successes, allowedHardErrors := 0, 0, 0
	allFiveMonitorFailures := true
	for _, accountID := range token.AccountIDs {
		state, ok := states[accountID]
		if !ok || state.Schedulable || state.AvailabilityState != "unavailable" || state.HardFailureStreak < 3 {
			return false
		}
		if state.HardFailureStreak < 5 {
			allFiveMonitorFailures = false
		}
		if state.ErrorCategoryCounts[model.ErrorClassCredential] > 0 {
			return false
		}
		window := state.Windows["5m"]
		allowed := window.ErrorCategoryCounts[model.ErrorClassInfrastructure] + window.ErrorCategoryCounts[model.ErrorClassCapacity]
		nonActionable := window.ErrorCategoryCounts[model.ErrorClassCredential] + window.ErrorCategoryCounts[model.ErrorClassClient] +
			window.ErrorCategoryCounts[model.ErrorClassModelCapability] + window.ErrorCategoryCounts[model.ErrorClassSemantic] +
			window.ErrorCategoryCounts[model.ErrorClassUnknown]
		if nonActionable > 0 {
			return false
		}
		allowedHardErrors += allowed
		eligible += window.EligibleCount
		successes += window.SuccessCount
	}
	if eligible >= 10 {
		failed := eligible - successes
		return failed > 0 && allowedHardErrors >= failed && float64(successes)*100/float64(eligible) < 20
	}
	return eligible == 0 && allFiveMonitorFailures
}

func poolCompletelyUnavailable(packet model.AnalysisPacket, poolName string) bool {
	for _, pool := range packet.PoolSummaries {
		if pool.Name == poolName {
			return pool.Accounts > 0 && pool.Unavailable == pool.Accounts
		}
	}
	return false
}

func (m *Manager) materializePolicyVersion(ctx context.Context, version model.ScorePolicyVersion) (*model.Settings, []model.Policy, error) {
	if err := validateDispatchPolicyPatch(version.ScopeType, version.Config); err != nil {
		return nil, nil, fmt.Errorf("策略版本 %d 无效: %w", version.ID, err)
	}
	switch version.ScopeType {
	case "global":
		settings, err := m.engine.Settings(ctx)
		if err != nil {
			return nil, nil, err
		}
		if err := mergeJSON(version.Config, &settings); err != nil {
			return nil, nil, err
		}
		return &settings, nil, nil
	case "account":
		accountID, err := strconv.ParseInt(version.ScopeID, 10, 64)
		if err != nil || accountID <= 0 {
			return nil, nil, errors.New("账号策略作用域编号无效")
		}
		var policy model.Policy
		found := false
		for _, binding := range m.engine.Snapshot().Bindings {
			if binding.Account.ID == accountID {
				policy, found = binding.Policy, true
				break
			}
		}
		if !found {
			policy = model.Policy{AccountID: accountID, Enabled: true}
		}
		if err := mergeJSON(version.Config, &policy); err != nil {
			return nil, nil, err
		}
		policy.AccountID = accountID
		policy.ScorePolicySource = "account_version"
		policy.ScorePolicyVersionID = &version.ID
		return nil, []model.Policy{policy}, nil
	case "pool":
		pool := strings.TrimSpace(version.ScopeID)
		if pool == "" {
			return nil, nil, errors.New("上游池策略缺少池名称")
		}
		sources, err := m.balances.List(ctx)
		if err != nil {
			return nil, nil, err
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
			if poolsByEndpoint[binding.NormalizedEndpoint] != pool {
				continue
			}
			policy := binding.Policy
			if policy.ScorePolicySource == "account" || policy.ScorePolicySource == "account_version" ||
				(policy.ScorePolicySource == "" && hasScoreOverrides(policy)) {
				continue
			}
			if policy.AccountID == 0 {
				policy = model.Policy{AccountID: binding.Account.ID, Enabled: true}
			}
			if err := mergeJSON(version.Config, &policy); err != nil {
				return nil, nil, err
			}
			policy.AccountID = binding.Account.ID
			policy.ScorePolicySource = "pool"
			policy.ScorePolicyVersionID = &version.ID
			updates = append(updates, policy)
		}
		if len(updates) == 0 {
			return nil, nil, errors.New("上游池没有可应用的账号")
		}
		return nil, updates, nil
	}
	return nil, nil, errors.New("不支持的评分策略作用域")
}

func mergeJSON(patch json.RawMessage, target any) error {
	base, err := json.Marshal(target)
	if err != nil {
		return err
	}
	var baseMap, patchMap map[string]any
	if err := json.Unmarshal(base, &baseMap); err != nil {
		return err
	}
	if err := json.Unmarshal(patch, &patchMap); err != nil {
		return err
	}
	for key, value := range patchMap {
		baseMap[key] = value
	}
	merged, err := json.Marshal(baseMap)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(merged))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("策略参数无效: %w", err)
	}
	return nil
}

func (m *Manager) actionState(action AgentAction) string {
	if action.AccountID > 0 {
		for _, binding := range m.engine.Snapshot().Bindings {
			if binding.Account.ID == action.AccountID {
				payload, _ := json.Marshal(map[string]any{
					"schedulable": binding.Account.Schedulable, "load_factor": binding.Account.LoadFactor,
					"owner": binding.Control.Owner, "health_locked": binding.Control.HealthLocked,
					"balance_locked": binding.Control.BalanceLocked, "cost_locked": binding.Control.CostLocked,
				})
				return string(payload)
			}
		}
	}
	if action.Type == "transition_token_group_tier" && action.SourceID > 0 && action.KeyID != "" {
		if sources, err := m.balances.List(context.Background()); err == nil {
			for _, source := range sources {
				if source.ID != action.SourceID {
					continue
				}
				for _, policy := range source.FailoverPolicies {
					if policy.KeyID == action.KeyID {
						payload, _ := json.Marshal(map[string]any{
							"source_id": source.ID, "key_id": policy.KeyID, "current_tier": policy.State.CurrentTier,
							"confirmed": policy.Confirmed && policy.ConfirmedVersion == policy.Version,
							"frozen":    policy.State.Frozen, "cooldown_until": policy.State.CooldownUntil,
							"manual_hold_until": policy.State.ManualHoldUntil,
						})
						return string(payload)
					}
				}
			}
		}
	}
	return "{}"
}

func (m *Manager) createOutcomes(ctx context.Context, runID, toolCallID int64, action AgentAction) {
	if action.AccountID <= 0 {
		return
	}
	for _, delay := range []time.Duration{10 * time.Minute, 30 * time.Minute, 2 * time.Hour} {
		item := model.DecisionOutcome{RunID: runID, ToolCallID: &toolCallID, AccountID: &action.AccountID,
			PredictedSuccessRateDelta: action.Prediction.SuccessRateDelta, PredictedLatencyDeltaMS: action.Prediction.LatencyDeltaMS,
			PredictedCostDelta: action.Prediction.CostDelta, EvaluateAt: time.Now().UTC().Add(delay), CreatedAt: time.Now().UTC()}
		_ = m.store.AddDecisionOutcome(ctx, &item)
	}
}
