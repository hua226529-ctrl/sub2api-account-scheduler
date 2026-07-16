package balance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
)

func (m *Manager) ListGroupFailoverPolicies(ctx context.Context, sourceID int64) ([]model.GroupFailoverPolicy, error) {
	return m.store.ListGroupFailoverPolicies(ctx, sourceID)
}

func (m *Manager) SaveGroupFailoverPolicy(ctx context.Context, policy model.GroupFailoverPolicy, actor string) (model.GroupFailoverPolicy, error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	policy.KeyID = strings.TrimSpace(policy.KeyID)
	policy.Pool = strings.TrimSpace(policy.Pool)
	if policy.SourceID <= 0 || policy.KeyID == "" {
		return model.GroupFailoverPolicy{}, errors.New("上游和受控令牌不能为空")
	}
	if policy.Pool == "" {
		return model.GroupFailoverPolicy{}, errors.New("受控令牌必须指定调度池")
	}
	if len(policy.AccountIDs) == 0 {
		return model.GroupFailoverPolicy{}, errors.New("至少绑定一个 Sub2API 账号")
	}
	source, key, observedTier, err := m.validateGroupFailoverPolicy(ctx, policy)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	policy.KeyName = key.Name
	policy.KeyHint = key.KeyHint
	saved, err := m.store.SaveGroupFailoverPolicy(ctx, policy)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	state := saved.State
	state.SourceID = saved.SourceID
	state.KeyID = saved.KeyID
	state.ObservedGroupID = key.GroupID
	state.CurrentTier = observedTier
	state.LastConfirmedAt = source.LastSuccessAt
	state.Frozen = observedTier == ""
	if state.Frozen {
		state.FreezeReason = "当前令牌分组不在已配置的主、备用、紧急分组中"
	} else {
		state.FreezeReason = ""
		state.LastError = ""
	}
	if err := m.store.SaveGroupFailoverState(ctx, state); err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	saved, err = m.store.GetGroupFailoverPolicy(ctx, saved.SourceID, saved.KeyID)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	m.record(ctx, model.Event{Type: "group_failover_policy_saved", Severity: "info", Message: source.Name + " 的三级分组策略已保存，等待确认", Actor: fallbackActor(actor), Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q,"version":%d,"pool":%q}`, saved.SourceID, saved.KeyID, saved.Version, saved.Pool)})
	m.trigger.Trigger()
	return saved, nil
}

func (m *Manager) ConfirmGroupFailoverPolicy(ctx context.Context, sourceID int64, keyID string, version int64, actor string) (model.GroupFailoverPolicy, error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	policy, err := m.store.GetGroupFailoverPolicy(ctx, sourceID, keyID)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	if policy.Version != version {
		return model.GroupFailoverPolicy{}, errors.New("策略版本已变化，请重新检查后确认")
	}
	_, _, tier, err := m.validateGroupFailoverPolicy(ctx, policy)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	if tier == "" {
		return model.GroupFailoverPolicy{}, errors.New("当前令牌分组不属于主、备用或紧急分组，不能确认自动救灾")
	}
	confirmed, err := m.store.ConfirmGroupFailoverPolicy(ctx, sourceID, keyID, version, fallbackActor(actor))
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	m.record(ctx, model.Event{Type: "group_failover_policy_confirmed", Severity: "warning", Message: "三级分组自动救灾策略已人工确认", Actor: fallbackActor(actor), Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q,"version":%d}`, sourceID, keyID, version)})
	m.trigger.Trigger()
	return confirmed, nil
}

func (m *Manager) DeleteGroupFailoverPolicy(ctx context.Context, sourceID int64, keyID, actor string) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	if _, err := m.store.GetGroupFailoverPolicy(ctx, sourceID, keyID); err != nil {
		return err
	}
	if err := m.store.DeleteGroupFailoverPolicy(ctx, sourceID, keyID); err != nil {
		return err
	}
	m.record(ctx, model.Event{Type: "group_failover_policy_deleted", Severity: "warning", Message: "已删除三级分组自动救灾策略", Actor: fallbackActor(actor), Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q}`, sourceID, keyID)})
	m.trigger.Trigger()
	return nil
}

// TransitionGroupTier is the only automated group-write entry point. The
// caller selects a configured tier, never an arbitrary upstream group ID.
func (m *Manager) TransitionGroupTier(ctx context.Context, request model.GroupTierTransitionRequest) (model.GroupTierTransition, error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	request.KeyID = strings.TrimSpace(request.KeyID)
	request.TargetTier = strings.ToLower(strings.TrimSpace(request.TargetTier))
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = fallbackActor(request.Actor)
	if !validGroupTier(request.TargetTier) {
		return model.GroupTierTransition{}, errors.New("目标层级只能是 main、backup 或 emergency")
	}
	if request.IdempotencyKey == "" {
		return model.GroupTierTransition{}, errors.New("切换幂等编号不能为空")
	}
	if existing, err := m.store.GetGroupTierTransitionByKey(ctx, request.IdempotencyKey); err == nil {
		return existing, nil
	}
	settings, err := m.store.GetSettings(ctx)
	if err != nil {
		return model.GroupTierTransition{}, fmt.Errorf("读取救灾策略: %w", err)
	}

	source, err := m.store.GetUpstreamSource(ctx, request.SourceID)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	policy, err := m.store.GetGroupFailoverPolicy(ctx, request.SourceID, request.KeyID)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	if !request.Manual && (!source.Enabled || source.MigrationRequired) {
		return model.GroupTierTransition{}, errors.New("上游已停用或账号密码尚未重新配置")
	}
	credentials, err := m.decrypt(source)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	if !request.Manual && (credentials.AuthMode == "access_key" || credentials.AccessKey != "" || credentials.Username == "" || credentials.Password == "") {
		_ = m.store.MarkUpstreamCredentialMigrationRequired(ctx, source.ID)
		m.trigger.Trigger()
		return model.GroupTierTransition{}, errors.New("自动切组仅允许使用已验证的账号密码")
	}
	if (!request.Manual && !policy.Enabled) || !policy.Confirmed || policy.ConfirmedVersion != policy.Version {
		return model.GroupTierTransition{}, errors.New("三级分组策略未启用或尚未确认当前版本")
	}
	if expectedPool := strings.TrimSpace(request.ExpectedPool); expectedPool != "" && expectedPool != strings.TrimSpace(policy.Pool) {
		return model.GroupTierTransition{}, errors.New("调度池策略已变化，拒绝执行过期的切组动作")
	}
	now := time.Now().UTC()
	if !request.Manual && (source.LastSuccessAt == nil || now.Sub(*source.LastSuccessAt) > time.Duration(settings.FailoverGroupFreshMinutes)*time.Minute) {
		return model.GroupTierTransition{}, fmt.Errorf("令牌和分组数据超过 %d 分钟未更新，禁止切换", settings.FailoverGroupFreshMinutes)
	}
	if !request.Manual && (source.Balance == nil || source.BalanceLocked || *source.Balance < source.PauseBelow) {
		return model.GroupTierTransition{}, errors.New("上游余额不足或余额状态不可信，禁止切换")
	}
	targetGroupID := groupIDForTier(policy, request.TargetTier)
	groups, err := m.store.ListUpstreamGroups(ctx, source.ID)
	if err != nil || !containsGroup(groups, targetGroupID) {
		return model.GroupTierTransition{}, errors.New("目标分组不存在或分组快照不可用")
	}
	rates, err := m.store.ListUpstreamKeyRates(ctx, source.ID)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	key, ok := findKeyRate(rates, request.KeyID)
	if !request.Manual && (!ok || !keyRateActive(key.Status)) {
		return model.GroupTierTransition{}, errors.New("受控令牌不存在或已停用")
	}
	if request.Manual && !ok {
		key = model.KeyRate{ExternalID: policy.KeyID, GroupID: policy.State.ObservedGroupID}
	}
	currentTier := tierForGroup(policy, key.GroupID)
	if currentTier == "" && !request.Manual {
		state := policy.State
		state.SourceID, state.KeyID = source.ID, policy.KeyID
		state.ObservedGroupID = key.GroupID
		state.Frozen = true
		state.FreezeReason = "检测到未知的上游令牌分组"
		state.LastError = state.FreezeReason
		_ = m.store.SaveGroupFailoverState(ctx, state)
		return model.GroupTierTransition{}, errors.New("当前令牌分组不在已确认策略中，已冻结自动切换")
	}
	state := policy.State
	state.SourceID, state.KeyID = source.ID, policy.KeyID
	liveResult, liveErr := m.fetcher.Fetch(ctx, source.Provider, source.BaseURL, credentials)
	if liveErr != nil {
		return model.GroupTierTransition{}, fmt.Errorf("切组前无法回读上游令牌实际状态: %w", liveErr)
	}
	liveKey, liveFound := findKeyRate(liveResult.KeyRates, request.KeyID)
	if !liveFound || (!request.Manual && !keyRateActive(liveKey.Status)) {
		state.Frozen = true
		state.FreezeReason = "切组前回读发现受控令牌不存在或已停用"
		state.LastError = state.FreezeReason
		_ = m.store.SaveGroupFailoverState(ctx, state)
		return model.GroupTierTransition{}, errors.New(state.FreezeReason)
	}
	if liveKey.GroupID != key.GroupID && !request.Manual {
		liveTier := tierForGroup(policy, liveKey.GroupID)
		state.PreviousTier = state.CurrentTier
		state.PreviousGroupID = state.ObservedGroupID
		state.ObservedGroupID = liveKey.GroupID
		state.LastConfirmedAt = &now
		if liveTier == "" {
			state.Frozen = true
			state.FreezeReason = "切组前检测到上游后台已将令牌改到未知分组"
			state.LastError = state.FreezeReason
		} else {
			state.CurrentTier = liveTier
			state.Frozen = false
			state.FreezeReason = ""
			state.LastError = ""
			state.ManualHoldUntil = timePointer(now.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
			state.ManualOverrideUntil = timePointer(now.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
		}
		_ = m.store.SaveGroupFailoverState(ctx, state)
		m.record(ctx, model.Event{Type: "group_manual_override_detected", Severity: "warning", Message: "切组前回读发现上游后台已人工修改令牌分组，自动动作已取消", Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q,"tier":%q}`, policy.SourceID, policy.KeyID, liveTier)})
		return model.GroupTierTransition{}, errors.New("上游令牌实际分组已变化，已进入人工保护或冻结状态")
	}
	key = liveKey
	currentTier = tierForGroup(policy, liveKey.GroupID)
	if currentTier == "" {
		if !request.Manual {
			return model.GroupTierTransition{}, errors.New("上游令牌实际分组不在已确认策略中")
		}
		currentTier = "unknown"
	}
	if expectedTier := strings.TrimSpace(request.ExpectedFromTier); expectedTier != "" && expectedTier != currentTier {
		return model.GroupTierTransition{}, errors.New("令牌当前层级已经变化，拒绝执行过期的切组动作")
	}
	if request.EvidenceCutoffAt != nil {
		changed, changedErr := m.store.HasAutomaticGroupTransitionInPoolSince(ctx, policy.Pool, request.EvidenceCutoffAt.UTC())
		if changedErr != nil {
			return model.GroupTierTransition{}, changedErr
		}
		if changed {
			return model.GroupTierTransition{}, errors.New("分析数据生成后该调度池已有救灾动作，必须重新分析")
		}
	}
	if state.Frozen && !request.Manual {
		return model.GroupTierTransition{}, errors.New("该令牌的自动切换已冻结")
	}
	if !request.Manual {
		if activeUntil(state.ManualOverrideUntil, state.ManualHoldUntil, now) {
			return model.GroupTierTransition{}, errors.New("人工保护期内禁止自动切换")
		}
		mainTrialRollback := request.Trigger == "main_trial_rollback"
		if mainTrialRollback {
			validRollback := currentTier == model.GroupTierMain && state.PreviousStableTier != "" &&
				request.TargetTier == state.PreviousStableTier && state.LastTransitionAt != nil &&
				now.Sub(state.LastTransitionAt.UTC()) <= time.Duration(settings.FailoverMainVerifyMinutes)*time.Minute &&
				(state.PreviousGroupID == "" || state.PreviousGroupID == targetGroupID)
			if !validRollback {
				return model.GroupTierTransition{}, errors.New("主分组试运行回滚条件已失效")
			}
		} else {
			emergencyEscalation := currentTier == model.GroupTierBackup && request.TargetTier == model.GroupTierEmergency
			if state.CooldownUntil != nil && now.Before(*state.CooldownUntil) && !emergencyEscalation {
				return model.GroupTierTransition{}, fmt.Errorf("令牌仍在 %d 分钟切换冷却期", settings.FailoverSwitchCooldownMinutes)
			}
			count30, countErr := m.store.CountCompletedGroupTierTransitions(ctx, source.ID, policy.KeyID, now.Add(-time.Duration(settings.FailoverShortLimitWindowMinutes)*time.Minute))
			if countErr != nil {
				return model.GroupTierTransition{}, countErr
			}
			count6h, countErr := m.store.CountCompletedGroupTierTransitions(ctx, source.ID, policy.KeyID, now.Add(-time.Duration(settings.FailoverLongLimitWindowMinutes)*time.Minute))
			if countErr != nil {
				return model.GroupTierTransition{}, countErr
			}
			if count30 >= settings.FailoverShortLimitCount || count6h >= settings.FailoverLongLimitCount {
				state.Frozen = true
				state.FreezeReason = "自动切换次数达到安全上限"
				state.LastError = state.FreezeReason
				_ = m.store.SaveGroupFailoverState(ctx, state)
				return model.GroupTierTransition{}, errors.New("自动切换次数达到上限，已冻结该令牌")
			}
		}
	}

	transition := model.GroupTierTransition{
		IdempotencyKey: request.IdempotencyKey, SourceID: source.ID, KeyID: policy.KeyID,
		FromTier: currentTier, ToTier: request.TargetTier, FromGroupID: key.GroupID, ToGroupID: targetGroupID,
		Actor: request.Actor, Reason: strings.TrimSpace(request.Reason), Evidence: strings.TrimSpace(request.Evidence),
		Trigger: request.Trigger, PacketID: request.PacketID, RunID: request.RunID, Manual: request.Manual, DryRun: request.DryRun, CreatedAt: now,
	}
	if err := m.store.AssertNoPendingGroupTransition(ctx, source.ID, policy.KeyID); err != nil {
		return model.GroupTierTransition{}, err
	}
	transition, existed, err := m.store.BeginGroupTierTransition(ctx, transition)
	if err != nil || existed {
		return transition, err
	}
	if request.DryRun {
		if err := m.store.SimulateGroupTierTransition(ctx, transition.ID, now); err != nil {
			return transition, err
		}
		transition.Status = model.GroupTransitionSimulated
		transition.CompletedAt = &now
		return transition, nil
	}
	if currentTier == request.TargetTier && key.GroupID == targetGroupID {
		state.CurrentTier = currentTier
		state.ObservedGroupID = key.GroupID
		state.LastConfirmedAt = &now
		if err := m.store.CompleteGroupTierTransition(ctx, transition.ID, state, now); err != nil {
			return transition, err
		}
		transition.Status = model.GroupTransitionCompleted
		transition.CompletedAt = &now
		return transition, nil
	}

	result, switchErr := m.switchAutomatedGroup(ctx, request.Manual, request.AutomationLeaseHeld, source, credentials, policy.KeyID, targetGroupID)
	if switchErr != nil {
		if mutation.IsUncertain(switchErr) {
			_ = m.store.MarkGroupTierTransitionUncertain(ctx, transition.ID, switchErr.Error())
			transition.Error = switchErr.Error()
			m.record(ctx, model.Event{Type: "group_failover_transition_reconciling", Severity: "critical", Message: source.Name + " 三级分组切换结果不明确，已保留幂等流水并只允许回读: " + switchErr.Error(), Actor: request.Actor, Details: transitionDetails(transition)})
			return transition, switchErr
		}
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, switchErr.Error(), now)
		m.record(ctx, model.Event{Type: "group_failover_transition_failed", Severity: "error", Message: source.Name + " 三级分组切换失败: " + switchErr.Error(), Actor: request.Actor, Details: transitionDetails(transition)})
		return transition, switchErr
	}
	confirmedKey, confirmed := findKeyRate(result.KeyRates, policy.KeyID)
	if !confirmed || confirmedKey.GroupID != targetGroupID {
		switchErr = errors.New("上游写后确认结果与目标分组不一致")
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, switchErr.Error(), time.Now().UTC())
		return transition, switchErr
	}
	completedAt := time.Now().UTC()
	state.PreviousTier = currentTier
	state.PreviousStableTier = currentTier
	state.PreviousGroupID = key.GroupID
	state.CurrentTier = request.TargetTier
	state.ObservedGroupID = targetGroupID
	state.Frozen = false
	state.FreezeReason = ""
	state.LastError = ""
	state.LastSwitchAt = &completedAt
	state.LastTransitionAt = &completedAt
	state.VerificationStartedAt = &completedAt
	state.LastConfirmedAt = &completedAt
	state.CooldownUntil = timePointer(completedAt.Add(time.Duration(settings.FailoverSwitchCooldownMinutes) * time.Minute))
	state.HealthySince = nil
	state.RecoveryHealthyCount = 0
	if request.Trigger == "main_trial_rollback" && !request.Manual {
		state.ReturnBlockedUntil = timePointer(completedAt.Add(time.Duration(settings.FailoverReturnRetryMinutes) * time.Minute))
		state.PreviousTier = ""
		state.PreviousStableTier = ""
		state.PreviousGroupID = ""
	}
	if request.Manual {
		state.ManualHoldUntil = timePointer(completedAt.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
		state.ManualOverrideUntil = timePointer(completedAt.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
	}
	if err := m.store.CompleteGroupTierTransition(ctx, transition.ID, state, completedAt); err != nil {
		return transition, err
	}
	// The group write was already confirmed. A later balance-lock reconciliation
	// failure must not turn a successful upstream mutation into an unknown state.
	if err := m.applySuccess(ctx, &source, result); err != nil {
		m.logger.Warn("group_transition_post_refresh_failed", "source_id", source.ID, "key_id", policy.KeyID, "error", err)
	}
	transition.Status = model.GroupTransitionCompleted
	transition.CompletedAt = &completedAt
	m.record(ctx, model.Event{Type: "group_failover_transition_completed", Severity: "warning", Message: fmt.Sprintf("%s 令牌已从 %s 切换到 %s 并完成写后确认", source.Name, currentTier, request.TargetTier), BeforeState: currentTier, AfterState: request.TargetTier, Actor: request.Actor, Details: transitionDetails(transition)})
	m.trigger.Trigger()
	return transition, nil
}

func (m *Manager) switchAutomatedGroup(ctx context.Context, manual, automationLeaseHeld bool, source model.UpstreamSource, credentials model.UpstreamCredentials, keyID, targetGroupID string) (model.UpstreamResult, error) {
	if manual {
		return m.fetcher.SwitchGroup(ctx, source.Provider, source.BaseURL, credentials, keyID, targetGroupID)
	}
	if m.barrier == nil || m.freeze == nil {
		return model.UpstreamResult{}, errors.New("自动切组缺少全局冻结屏障，已拒绝外部写入")
	}
	if !automationLeaseHeld {
		release := m.barrier.EnterMutation()
		defer release()
	}
	freeze, err := m.freeze.FreezeState(ctx)
	if err != nil {
		return model.UpstreamResult{}, fmt.Errorf("切组前读取自动化冻结状态失败: %w", err)
	}
	if freeze.AllAutomation {
		return model.UpstreamResult{}, errors.New("全部自动化已冻结，自动切组被拒绝")
	}
	return m.fetcher.SwitchGroup(ctx, source.Provider, source.BaseURL, credentials, keyID, targetGroupID)
}

// ReconcileGroupFailoverStates detects manual changes made directly in the
// upstream console. Known groups receive the configured manual hold; unknown
// groups freeze automation until an operator updates and confirms the policy.
func (m *Manager) ReconcileGroupFailoverStates(ctx context.Context) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	return m.reconcileGroupFailoverStatesForSourcesLocked(ctx, nil)
}

func (m *Manager) reconcileGroupFailoverStatesLocked(ctx context.Context) error {
	return m.reconcileGroupFailoverStatesForSourcesLocked(ctx, nil)
}

func (m *Manager) reconcileGroupFailoverStatesForSourcesLocked(ctx context.Context, refreshed map[int64]bool) error {
	settings, err := m.store.GetSettings(ctx)
	if err != nil {
		return fmt.Errorf("读取救灾策略: %w", err)
	}
	policies, err := m.store.ListGroupFailoverPolicies(ctx, 0)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, policy := range policies {
		if refreshed != nil && !refreshed[policy.SourceID] {
			continue
		}
		rates, rateErr := m.store.ListUpstreamKeyRates(ctx, policy.SourceID)
		if rateErr != nil {
			return rateErr
		}
		key, found := findKeyRate(rates, policy.KeyID)
		if !found {
			policy.State.Frozen = true
			policy.State.FreezeReason = "受控令牌已不存在"
			policy.State.LastError = policy.State.FreezeReason
			if err := m.store.SaveGroupFailoverState(ctx, policy.State); err != nil {
				return err
			}
			continue
		}
		tier := tierForGroup(policy, key.GroupID)
		state := policy.State
		pending, pendingErr := m.store.ListGroupTierTransitions(ctx, policy.SourceID, policy.KeyID, 100)
		if pendingErr != nil {
			return pendingErr
		}
		for _, transition := range pending {
			if transition.Status != model.GroupTransitionPending {
				continue
			}
			if key.GroupID == transition.ToGroupID {
				state.PreviousTier = transition.FromTier
				state.PreviousStableTier = transition.FromTier
				state.PreviousGroupID = transition.FromGroupID
				state.CurrentTier = transition.ToTier
				state.ObservedGroupID = transition.ToGroupID
				state.LastTransitionAt = &now
				state.LastConfirmedAt = &now
				state.VerificationStartedAt = &now
				state.CooldownUntil = timePointer(now.Add(time.Duration(settings.FailoverSwitchCooldownMinutes) * time.Minute))
				if err := m.store.CompleteGroupTierTransition(ctx, transition.ID, state, now); err != nil {
					return err
				}
				m.record(ctx, model.Event{Type: "group_failover_transition_recovered", Severity: "warning", Message: "服务重启后已通过上游实际分组确认未完成的切换流水", Actor: "system", Details: transitionDetails(transition)})
			} else {
				if err := m.store.FailGroupTierTransition(ctx, transition.ID, "服务重启后实际分组未达到目标，未重放写操作", now); err != nil {
					return err
				}
			}
		}
		if tier == "" {
			state.Frozen = true
			state.FreezeReason = "检测到未知的上游令牌分组"
			state.LastError = state.FreezeReason
		} else if state.ObservedGroupID != "" && state.ObservedGroupID != key.GroupID {
			state.PreviousTier = state.CurrentTier
			state.PreviousGroupID = state.ObservedGroupID
			state.CurrentTier = tier
			state.ManualHoldUntil = timePointer(now.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
			state.ManualOverrideUntil = timePointer(now.Add(time.Duration(settings.FailoverManualProtectionMinutes) * time.Minute))
			state.LastTransitionAt = &now
			state.Frozen = false
			state.FreezeReason = ""
			state.LastError = ""
			m.record(ctx, model.Event{Type: "group_manual_override_detected", Severity: "warning", Message: fmt.Sprintf("检测到上游后台人工切换令牌分组，已进入 %d 分钟保护", settings.FailoverManualProtectionMinutes), Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q,"tier":%q}`, policy.SourceID, policy.KeyID, tier)})
		} else {
			state.CurrentTier = tier
			state.Frozen = false
			state.FreezeReason = ""
		}
		state.ObservedGroupID = key.GroupID
		state.LastConfirmedAt = &now
		if err := m.store.SaveGroupFailoverState(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) validateGroupFailoverPolicy(ctx context.Context, policy model.GroupFailoverPolicy) (model.UpstreamSource, model.KeyRate, string, error) {
	groups := []string{strings.TrimSpace(policy.MainGroupID), strings.TrimSpace(policy.BackupGroupID), strings.TrimSpace(policy.EmergencyGroupID)}
	if groups[0] == "" || groups[1] == "" || groups[2] == "" || groups[0] == groups[1] || groups[0] == groups[2] || groups[1] == groups[2] {
		return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("主、备用、紧急分组必须存在且互不相同")
	}
	source, err := m.store.GetUpstreamSource(ctx, policy.SourceID)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", err
	}
	if !source.Enabled || source.MigrationRequired {
		return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("上游已停用或账号密码尚未重新配置")
	}
	settings, err := m.store.GetSettings(ctx)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", fmt.Errorf("读取救灾策略: %w", err)
	}
	credentials, err := m.decrypt(source)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", err
	}
	if credentials.AuthMode == "access_key" || credentials.AccessKey != "" || credentials.Username == "" || credentials.Password == "" {
		_ = m.store.MarkUpstreamCredentialMigrationRequired(ctx, source.ID)
		m.trigger.Trigger()
		return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("三级分组策略仅允许使用已验证的账号密码")
	}
	if source.LastSuccessAt == nil || time.Since(*source.LastSuccessAt) > time.Duration(settings.FailoverGroupFreshMinutes)*time.Minute {
		return model.UpstreamSource{}, model.KeyRate{}, "", fmt.Errorf("令牌和分组数据超过 %d 分钟未更新", settings.FailoverGroupFreshMinutes)
	}
	rates, err := m.store.ListUpstreamKeyRates(ctx, source.ID)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", err
	}
	key, found := findKeyRate(rates, policy.KeyID)
	if !found || !keyRateActive(key.Status) {
		return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("受控令牌不存在或已停用")
	}
	availableGroups, err := m.store.ListUpstreamGroups(ctx, source.ID)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", err
	}
	for _, groupID := range groups {
		if !containsGroup(availableGroups, groupID) {
			return model.UpstreamSource{}, model.KeyRate{}, "", fmt.Errorf("分组 %s 不存在", groupID)
		}
	}
	accounts, err := m.api.ListAccounts(ctx)
	if err != nil {
		return model.UpstreamSource{}, model.KeyRate{}, "", fmt.Errorf("读取 Sub2API 账号失败: %w", err)
	}
	matched := map[int64]bool{}
	for _, account := range accounts {
		normalized, normalizeErr := NormalizeURL(account.BaseURL(), true)
		if normalizeErr == nil && normalized == source.NormalizedURL {
			matched[account.ID] = true
		}
	}
	for _, accountID := range policy.AccountIDs {
		if !matched[accountID] {
			return model.UpstreamSource{}, model.KeyRate{}, "", fmt.Errorf("账号 %d 与该上游地址不匹配", accountID)
		}
	}
	return source, key, tierForGroup(policy, key.GroupID), nil
}

func groupIDForTier(policy model.GroupFailoverPolicy, tier string) string {
	switch tier {
	case model.GroupTierMain:
		return policy.MainGroupID
	case model.GroupTierBackup:
		return policy.BackupGroupID
	case model.GroupTierEmergency:
		return policy.EmergencyGroupID
	default:
		return ""
	}
}

func tierForGroup(policy model.GroupFailoverPolicy, groupID string) string {
	groupID = strings.TrimSpace(groupID)
	switch groupID {
	case strings.TrimSpace(policy.MainGroupID):
		return model.GroupTierMain
	case strings.TrimSpace(policy.BackupGroupID):
		return model.GroupTierBackup
	case strings.TrimSpace(policy.EmergencyGroupID):
		return model.GroupTierEmergency
	default:
		return ""
	}
}

func validGroupTier(tier string) bool {
	return tier == model.GroupTierMain || tier == model.GroupTierBackup || tier == model.GroupTierEmergency
}

func containsGroup(groups []model.UpstreamGroup, groupID string) bool {
	for _, group := range groups {
		if group.ExternalID == groupID {
			return true
		}
	}
	return false
}

func findKeyRate(rates []model.KeyRate, keyID string) (model.KeyRate, bool) {
	for _, key := range rates {
		if key.ExternalID == keyID {
			return key, true
		}
	}
	return model.KeyRate{}, false
}

func activeUntil(first, second *time.Time, now time.Time) bool {
	return (first != nil && now.Before(*first)) || (second != nil && now.Before(*second))
}

func timePointer(value time.Time) *time.Time { return &value }

func fallbackActor(actor string) string {
	if actor = strings.TrimSpace(actor); actor != "" {
		return actor
	}
	return "system"
}

func transitionDetails(item model.GroupTierTransition) string {
	return fmt.Sprintf(`{"transition_id":%d,"idempotency_key":%q,"source_id":%d,"key_id":%q,"from_tier":%q,"to_tier":%q,"packet_id":%d,"run_id":%d}`, item.ID, item.IdempotencyKey, item.SourceID, item.KeyID, item.FromTier, item.ToTier, item.PacketID, item.RunID)
}
