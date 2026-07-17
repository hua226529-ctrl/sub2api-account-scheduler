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

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var allowedActions = map[string]bool{
	"pause_account": true, "resume_account": true, "set_load_factor": true,
	"clear_flap_protection": true, "clear_manual_override": true, "trigger_reconcile": true,
	"transition_token_group_tier": true, "update_score_policy": true, "activate_policy_version": true,
}

func (m *Manager) executeActions(ctx context.Context, run model.AgentRun, settings model.AgentSettings, actions []AgentAction) {
	for _, action := range actions {
		arguments, _ := json.Marshal(action)
		call := model.AgentToolCall{RunID: run.ID, Tool: action.Type, Arguments: arguments, Status: "pending", BeforeState: m.actionState(action)}
		if err := m.store.AddAgentToolCall(ctx, &call); err != nil {
			m.recordEvent(ctx, "agent_action_audit_failed", "error", action.AccountID, "动作意图无法写入审计，已拒绝执行", run.ID)
			continue
		}
		if !allowedActions[action.Type] {
			call.Status, call.Result = "rejected", "未知或未授权的动作类型"
			_ = m.store.UpdateAgentToolCall(ctx, call)
			continue
		}
		if settings.Mode != model.AgentModeControl {
			call.Status, call.Result = "proposed", "观察模式，仅记录拟执行动作"
			call.AfterState = call.BeforeState
			_ = m.store.UpdateAgentToolCall(ctx, call)
			m.recordEvent(ctx, "agent_would_execute", "warning", action.AccountID, "智能体观察模式拟执行 "+action.Type, run.ID)
			continue
		}
		err := m.executeAction(ctx, run, call.ID, action)
		if err != nil {
			call.Status, call.Result = "failed", err.Error()
			call.AfterState = m.actionState(action)
			_ = m.store.UpdateAgentToolCall(ctx, call)
			m.recordEvent(ctx, "agent_action_failed", "error", action.AccountID, action.Type+" 执行失败: "+err.Error(), run.ID)
			continue
		}
		call.Status, call.Result = "completed", "已执行并确认"
		call.AfterState = m.actionState(action)
		_ = m.store.UpdateAgentToolCall(ctx, call)
		m.recordEvent(ctx, "agent_action_completed", "info", action.AccountID, "智能体已执行 "+action.Type, run.ID)
		m.createOutcomes(ctx, run.ID, call.ID, action)
	}
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
		if targetTier == model.GroupTierMain {
			if token.ReturnBlockedUntil != nil && token.ReturnBlockedUntil.After(now) {
				return false
			}
			return token.Main.Name != "" && (token.CurrentTier == model.GroupTierBackup || token.CurrentTier == model.GroupTierEmergency) &&
				failoverRecoveryEvidence(packet, token, now)
		}
		if !poolCompletelyUnavailable(packet, token.Pool) || !failoverEscalationEvidence(packet, token) {
			return false
		}
		if targetTier == model.GroupTierBackup {
			return token.Backup.Name != "" && token.CurrentTier == model.GroupTierMain
		}
		return targetTier == model.GroupTierEmergency && token.Emergency.Name != "" && token.CurrentTier == model.GroupTierBackup
	}
	return false
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

func failoverRecoveryEvidence(packet model.AnalysisPacket, token model.AgentGroupFailoverToken, now time.Time) bool {
	if len(token.AccountIDs) == 0 || token.HealthySince == nil || now.Sub(token.HealthySince.UTC()) < 30*time.Minute ||
		token.RecoveryHealthyCount < 10 {
		return false
	}
	states := make(map[int64]model.AgentAccountState, len(packet.AccountCompactStates))
	for _, state := range packet.AccountCompactStates {
		states[state.AccountID] = state
	}
	eligible, successes := 0, 0
	for _, accountID := range token.AccountIDs {
		state, ok := states[accountID]
		if !ok || state.AvailabilityState != "available" {
			return false
		}
		window := state.Windows["30m"]
		eligible += window.EligibleCount
		successes += window.SuccessCount
	}
	return eligible >= 20 && float64(successes)*100/float64(eligible) >= 98
}

func poolCompletelyUnavailable(packet model.AnalysisPacket, poolName string) bool {
	for _, pool := range packet.PoolSummaries {
		if pool.Name == poolName {
			return pool.Accounts > 0 && pool.Unavailable == pool.Accounts
		}
	}
	return false
}

func (m *Manager) executeAction(ctx context.Context, run model.AgentRun, toolCallID int64, action AgentAction) error {
	actor := "agent:" + strconv.FormatInt(run.ID, 10)
	reason := strings.TrimSpace(action.Reason)
	if reason == "" {
		reason = "智能体根据分析数据包执行"
	}
	if action.Type == "pause_account" || action.Type == "resume_account" || action.Type == "set_load_factor" {
		if run.PacketID == nil || *run.PacketID <= 0 || toolCallID <= 0 {
			return errors.New("智能体账号动作缺少持久分析数据包或动作编号")
		}
		created := run.StartedAt.UTC()
		expires := created.Add(accountcontrol.DefaultAutonomousTTL)
		evidence := "analysis_packet:" + strconv.FormatInt(*run.PacketID, 10)
		ctx = accountcontrol.WithCommandContext(ctx, accountcontrol.CommandContext{
			CommandID: fmt.Sprintf("agent-v1:run:%d:tool:%d", run.ID, toolCallID), CreatedAt: created, ExpiresAt: &expires,
			SnapshotVersion: evidence, EvidenceRefs: []string{evidence}, RunID: run.ID,
		})
	}
	switch action.Type {
	case "pause_account":
		if action.AccountID <= 0 {
			return errors.New("缺少账号编号")
		}
		return m.engine.AgentPause(ctx, action.AccountID, actor, reason)
	case "resume_account":
		if action.AccountID <= 0 {
			return errors.New("缺少账号编号")
		}
		return m.engine.AgentResume(ctx, action.AccountID, actor, reason)
	case "set_load_factor":
		if action.AccountID <= 0 {
			return errors.New("缺少账号编号")
		}
		return m.engine.AgentSetLoadFactor(ctx, action.AccountID, action.LoadFactor, actor, reason)
	case "clear_flap_protection":
		return m.engine.ClearFlapProtection(ctx, action.AccountID, actor)
	case "clear_manual_override":
		return m.engine.ClearOverride(ctx, action.AccountID, actor)
	case "trigger_reconcile":
		m.engine.Trigger()
		return nil
	case "transition_token_group_tier":
		if action.SourceID <= 0 || strings.TrimSpace(action.KeyID) == "" || strings.TrimSpace(action.TargetTier) == "" {
			return errors.New("切换令牌分组层级参数不完整")
		}
		if run.PacketID == nil || *run.PacketID <= 0 {
			return errors.New("令牌分组层级动作缺少不可变分析数据包")
		}
		originalPacket, err := m.store.GetAnalysisPacket(ctx, *run.PacketID)
		if err != nil {
			return fmt.Errorf("读取动作分析数据包失败: %w", err)
		}
		originalToken, ok := findGroupFailoverToken(originalPacket, action.SourceID, action.KeyID)
		if !ok {
			return errors.New("受控令牌不在动作分析数据包中")
		}
		settings, err := m.store.GetAgentSettings(ctx)
		if err != nil {
			return fmt.Errorf("读取智能体设置失败: %w", err)
		}
		currentPacket, err := m.builder.Build(ctx, "execution_guard", settings)
		if err != nil {
			return fmt.Errorf("刷新切组安全证据失败: %w", err)
		}
		if !packetContainsGroupTarget(currentPacket, action.SourceID, action.KeyID, action.TargetTier) {
			return errors.New("当前数据已不再满足令牌分组切换条件")
		}
		currentToken, ok := findGroupFailoverToken(currentPacket, action.SourceID, action.KeyID)
		if !ok || currentToken.Pool != originalToken.Pool {
			return errors.New("受控令牌的调度池策略已经变化")
		}
		evidence := fmt.Sprintf(`{"agent_run_id":%d}`, run.ID)
		packetID := *run.PacketID
		evidence = fmt.Sprintf(`{"agent_run_id":%d,"analysis_packet_id":%d}`, run.ID, *run.PacketID)
		cutoff := originalPacket.CutoffAt
		if cutoff.IsZero() {
			cutoff = originalPacket.CreatedAt
		}
		if cutoff.IsZero() {
			return errors.New("动作分析数据包缺少有效统计截止时间")
		}
		_, err = m.balances.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
			SourceID: action.SourceID, KeyID: action.KeyID, TargetTier: action.TargetTier,
			IdempotencyKey: fmt.Sprintf("agent-%d-%d-%s-%s", run.ID, action.SourceID, action.KeyID, action.TargetTier),
			Actor:          actor, Reason: reason, Evidence: evidence, Trigger: "agent",
			PacketID: packetID, RunID: run.ID, Manual: false, ExpectedPool: currentToken.Pool,
			ExpectedFromTier: currentToken.CurrentTier, EvidenceCutoffAt: &cutoff,
		})
		return err
	case "update_score_policy":
		return m.applyPolicyAction(ctx, run, action, actor, reason)
	case "activate_policy_version":
		if action.PolicyID <= 0 {
			return errors.New("策略版本编号无效")
		}
		return m.activatePolicyVersion(ctx, action.PolicyID, actor)
	default:
		return errors.New("不支持的动作")
	}
}

func findGroupFailoverToken(packet model.AnalysisPacket, sourceID int64, keyID string) (model.AgentGroupFailoverToken, bool) {
	for _, token := range packet.GroupFailoverTokens {
		if token.SourceID == sourceID && token.KeyID == keyID {
			return token, true
		}
	}
	return model.AgentGroupFailoverToken{}, false
}

func (m *Manager) applyPolicyAction(ctx context.Context, run model.AgentRun, action AgentAction, actor, reason string) error {
	scope := strings.TrimSpace(action.ScopeType)
	if err := validateDispatchPolicyPatch(scope, action.Config); err != nil {
		return err
	}
	runID := run.ID
	version := model.ScorePolicyVersion{ScopeType: scope, ScopeID: strings.TrimSpace(action.ScopeID), Config: action.Config,
		Reason: reason, AgentRunID: &runID, CreatedBy: actor}
	if err := m.store.CreatePolicyVersion(ctx, &version, false); err != nil {
		return err
	}
	return m.publishPolicyVersion(ctx, version, actor)
}

func (m *Manager) activatePolicyVersion(ctx context.Context, id int64, actor string) error {
	version, err := m.store.GetPolicyVersion(ctx, id)
	if err != nil {
		return err
	}
	if err := m.publishPolicyVersion(ctx, version, actor); err != nil {
		return err
	}
	return nil
}

func (m *Manager) publishPolicyVersion(ctx context.Context, version model.ScorePolicyVersion, actor string) error {
	if err := m.engine.RunExclusive(ctx, func() error {
		settings, policies, err := m.materializePolicyVersion(ctx, version)
		if err != nil {
			return err
		}
		return m.store.PublishPolicyVersion(ctx, version.ID, settings, policies)
	}); err != nil {
		return err
	}
	m.recordEvent(ctx, "dispatch_policy_published", "warning", 0,
		fmt.Sprintf("策略版本 %d 已由 %s 原子发布", version.ID, actor), 0)
	m.engine.Trigger()
	return nil
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
