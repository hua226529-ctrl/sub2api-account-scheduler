package balance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
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
	if err := m.validateGroupFailoverEvidenceSource(policy); err != nil {
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
	state.LastConfirmedAt = nil
	if state.ValidationStatus == "" {
		state.ValidationStatus = model.GroupValidationUnknown
	}
	if state.ValidationMode == "" {
		state.ValidationMode = model.GroupValidationModePassive
	}
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
	if err := m.validateGroupFailoverEvidenceSource(policy); err != nil {
		return model.GroupFailoverPolicy{}, err
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
	request.KeyID = strings.TrimSpace(request.KeyID)
	request.TargetTier = strings.ToLower(strings.TrimSpace(request.TargetTier))
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = fallbackActor(request.Actor)
	request.Producer, request.Authority = groupRequestIdentity(request)
	if !validGroupTier(request.TargetTier) {
		return model.GroupTierTransition{}, errors.New("目标层级只能是 main、backup 或 emergency")
	}
	if request.IdempotencyKey == "" {
		return model.GroupTierTransition{}, errors.New("切换幂等编号不能为空")
	}
	if existing, err := m.store.GetGroupTierTransitionByKey(ctx, request.IdempotencyKey); err == nil {
		if !groupRequestMatchesExisting(request, existing) {
			return model.GroupTierTransition{}, store.ErrGroupTransitionIdempotencyConflict
		}
		return existing, nil
	}
	release, err := m.groupLocks.acquire(ctx, request.SourceID, request.KeyID)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	defer release()
	if existing, err := m.store.GetGroupTierTransitionByKey(ctx, request.IdempotencyKey); err == nil {
		if !groupRequestMatchesExisting(request, existing) {
			return model.GroupTierTransition{}, store.ErrGroupTransitionIdempotencyConflict
		}
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
	if !request.Manual {
		if err := m.validateGroupFailoverEvidenceSource(policy); err != nil {
			return model.GroupTierTransition{}, err
		}
	}
	if !tierEnabled(policy, request.TargetTier) || strings.TrimSpace(groupIDForTier(policy, request.TargetTier)) == "" {
		return model.GroupTierTransition{}, errors.New("目标固定层级未配置或已停用")
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
		state.LastConfirmedAt = nil
		state.ValidationStatus = model.GroupValidationUncertain
		state.LastError = "manual_switch_requires_new_evidence_boundary"
		if liveTier == "" {
			state.Frozen = true
			state.FreezeReason = "切组前检测到上游后台已将令牌改到未知分组"
			state.LastError = state.FreezeReason
		} else {
			state.CurrentTier = liveTier
			state.Frozen = false
			state.FreezeReason = ""
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

	transition := model.GroupTierTransition{
		IdempotencyKey: request.IdempotencyKey, SourceID: source.ID, KeyID: policy.KeyID,
		FromTier: currentTier, ToTier: request.TargetTier, FromGroupID: key.GroupID, ToGroupID: targetGroupID,
		Actor: request.Actor, Producer: request.Producer, Authority: request.Authority, Reason: strings.TrimSpace(request.Reason), Evidence: strings.TrimSpace(request.Evidence), SnapshotVersion: strings.TrimSpace(request.SnapshotVersion),
		Trigger: request.Trigger, PacketID: request.PacketID, PacketHash: request.PacketHash, RunID: request.RunID, GoalID: request.GoalID, StepID: request.StepID,
		Manual: request.Manual, DryRun: request.DryRun, CreatedAt: now,
		BeforeState: key.GroupID,
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
	watermarks, err := m.store.GetFailoverEvidenceWatermarks(ctx)
	if err != nil {
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, err.Error(), now)
		return transition, fmt.Errorf("读取切换证据水位: %w", err)
	}
	previousValidationStatus := state.ValidationStatus
	state.ValidationStatus = model.GroupValidationTransitioning
	state.ValidationMode = model.GroupValidationModePassive
	state.ValidationTransitionID = transition.ID
	state.ValidationFromTier = currentTier
	state.ValidationTargetTier = request.TargetTier
	state.ValidationFromGroupID = key.GroupID
	state.ValidationTargetGroupID = targetGroupID
	state.SwitchRequestedAt = &now
	state.SwitchVerifiedAt = nil
	state.ValidationNotBefore = nil
	state.EvidenceDeadline = nil
	state.MonitorWatermark = watermarks.Monitor
	state.TrafficWatermark = watermarks.Traffic
	state.MonitorEvidenceCursor = watermarks.Monitor
	state.TrafficEvidenceCursor = watermarks.Traffic
	state.ActiveProbeAttempts = 0
	state.SuccessfulEvidenceCount = 0
	state.FailedEvidenceCount = 0
	state.LastEvidenceID = ""
	state.LastEvidenceSource = ""
	state.LastEvidenceReason = ""
	state.LastEvidenceAt = nil
	if err := m.store.SaveGroupFailoverState(ctx, state); err != nil {
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, err.Error(), now)
		return transition, err
	}
	if currentTier == request.TargetTier && key.GroupID == targetGroupID {
		state.CurrentTier = currentTier
		state.ObservedGroupID = key.GroupID
		state.ValidationStatus = previousValidationStatus
		if err := m.store.CompleteGroupTierTransition(ctx, transition.ID, state, now); err != nil {
			return transition, err
		}
		transition.Status = model.GroupTransitionApplied
		transition.VerifiedAfter = key.GroupID
		transition.CompletedAt = &now
		return transition, nil
	}

	result, switchErr := m.executeGroupTransport(ctx, request.Manual, request.AutomationLeaseHeld, source, credentials, policy.KeyID, targetGroupID)
	if switchErr != nil {
		if mutation.IsUncertain(switchErr) {
			_ = m.store.MarkGroupTierTransitionUncertain(ctx, transition.ID, switchErr.Error())
			state.ValidationStatus = model.GroupValidationUncertain
			state.LastError = "switch_result_uncertain"
			_ = m.store.SaveGroupFailoverState(ctx, state)
			transition.Error = switchErr.Error()
			transition.Uncertain = true
			m.record(ctx, transitionEvent(transition, model.Event{Type: "group_failover_transition_reconciling", Severity: "critical", Message: source.Name + " 三级分组切换结果不明确，已保留幂等流水并只允许回读: " + switchErr.Error(), Actor: request.Actor, Details: transitionDetails(transition)}))
			return transition, switchErr
		}
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, switchErr.Error(), now)
		state.ValidationStatus = previousValidationStatus
		state.LastError = switchErr.Error()
		_ = m.store.SaveGroupFailoverState(ctx, state)
		m.record(ctx, transitionEvent(transition, model.Event{Type: "group_failover_transition_failed", Severity: "error", Message: source.Name + " 三级分组切换失败: " + switchErr.Error(), Actor: request.Actor, Details: transitionDetails(transition)}))
		return transition, switchErr
	}
	confirmedKey, confirmed := findKeyRate(result.KeyRates, policy.KeyID)
	if !confirmed || confirmedKey.GroupID != targetGroupID {
		switchErr = errors.New("上游写后确认结果与目标分组不一致")
		_ = m.store.FailGroupTierTransition(ctx, transition.ID, switchErr.Error(), time.Now().UTC())
		state.ValidationStatus = model.GroupValidationUncertain
		state.LastError = "post_write_readback_mismatch"
		_ = m.store.SaveGroupFailoverState(ctx, state)
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
	state.LastConfirmedAt = nil
	state.ValidationStatus = model.GroupValidationAwaitingEvidence
	state.ValidationMode = model.GroupValidationModePassive
	state.SwitchVerifiedAt = &completedAt
	validationNotBefore := completedAt.Add(model.GroupValidationPropagationDelay)
	evidenceDeadline := completedAt.Add(model.GroupValidationEvidenceTimeout)
	state.ValidationNotBefore = &validationNotBefore
	state.EvidenceDeadline = &evidenceDeadline
	state.CooldownUntil = timePointer(completedAt.Add(time.Duration(settings.FailoverSwitchCooldownMinutes) * time.Minute))
	state.HealthySince = nil
	state.RecoveryHealthyCount = 0
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
		m.logger.Warn("group_transition_post_refresh_failed", "source_id", source.ID, "error", err)
	}
	transition.Status = model.GroupTransitionApplied
	transition.VerifiedAfter = targetGroupID
	transition.CompletedAt = &completedAt
	m.record(ctx, transitionEvent(transition, model.Event{Type: "group_failover_transition_applied", Severity: "warning", Message: fmt.Sprintf("%s 令牌已从 %s 切换到 %s 并完成写后确认，等待切换后证据", source.Name, currentTier, request.TargetTier), BeforeState: currentTier, AfterState: request.TargetTier, Actor: request.Actor, Details: transitionDetails(transition)}))
	m.trigger.Trigger()
	return transition, nil
}

func (m *Manager) executeGroupTransport(ctx context.Context, manual, automationLeaseHeld bool, source model.UpstreamSource, credentials model.UpstreamCredentials, keyID, targetGroupID string) (model.UpstreamResult, error) {
	if !manual {
		if m.barrier == nil || m.freeze == nil {
			return model.UpstreamResult{}, errors.New("自动切组缺少全局冻结屏障，已拒绝外部写入")
		}
		if !automationLeaseHeld {
			release, err := m.barrier.EnterMutation(ctx)
			if err != nil {
				return model.UpstreamResult{}, fmt.Errorf("等待自动切组冻结屏障: %w", err)
			}
			defer release()
		}
		freeze, err := m.freeze.FreezeState(ctx)
		if err != nil {
			return model.UpstreamResult{}, fmt.Errorf("切组前读取自动化冻结状态失败: %w", err)
		}
		if freeze.AllAutomation {
			return model.UpstreamResult{}, errors.New("全部自动化已冻结，自动切组被拒绝")
		}
	}
	return m.fetcher.SwitchGroup(ctx, source.Provider, source.BaseURL, credentials, keyID, targetGroupID)
}

// RecoverGroupTransitions resolves pending journal rows by readback only. It
// never replays a group write because the pre-crash transport outcome may be
// unknown.
func (m *Manager) RecoverGroupTransitions(ctx context.Context) error {
	items, err := m.store.ListPendingGroupTierTransitions(ctx)
	if err != nil {
		return err
	}
	var recoveryErrors []error
	for _, item := range items {
		if err := m.recoverGroupTransition(ctx, item); err != nil {
			recoveryErrors = append(recoveryErrors, fmt.Errorf("recover transition %d: %w", item.ID, err))
		}
	}
	return errors.Join(recoveryErrors...)
}

func (m *Manager) recoverGroupTransition(ctx context.Context, item model.GroupTierTransition) error {
	release, err := m.groupLocks.acquire(ctx, item.SourceID, item.KeyID)
	if err != nil {
		return err
	}
	defer release()
	current, err := m.store.GetGroupTierTransitionByKey(ctx, item.IdempotencyKey)
	if err != nil || current.Status != model.GroupTransitionPending {
		return err
	}
	source, err := m.store.GetUpstreamSource(ctx, current.SourceID)
	if err != nil {
		return err
	}
	credentials, err := m.decrypt(source)
	if err != nil {
		return err
	}
	result, err := m.fetcher.Fetch(ctx, source.Provider, source.BaseURL, credentials)
	if err != nil {
		return err
	}
	key, ok := findKeyRate(result.KeyRates, current.KeyID)
	if !ok {
		return errors.New("readback did not contain the controlled key")
	}
	now := time.Now().UTC()
	if key.GroupID == current.FromGroupID {
		return m.store.FailGroupTierTransition(ctx, current.ID, "recovery readback confirmed the write was not applied", now)
	}
	if key.GroupID != current.ToGroupID {
		return fmt.Errorf("readback group does not match journal before or target state")
	}
	policy, err := m.store.GetGroupFailoverPolicy(ctx, current.SourceID, current.KeyID)
	if err != nil {
		return err
	}
	state := policy.State
	state.SourceID = current.SourceID
	state.KeyID = current.KeyID
	state.PreviousTier = current.FromTier
	state.PreviousStableTier = current.FromTier
	state.PreviousGroupID = current.FromGroupID
	state.CurrentTier = current.ToTier
	state.ObservedGroupID = current.ToGroupID
	state.Frozen = false
	state.FreezeReason = ""
	state.LastError = ""
	state.LastSwitchAt = &now
	state.LastTransitionAt = &now
	state.VerificationStartedAt = &now
	state.LastConfirmedAt = nil
	if state.ValidationTransitionID == current.ID && state.ValidationStatus == model.GroupValidationTransitioning {
		state.ValidationStatus = model.GroupValidationAwaitingEvidence
		state.SwitchVerifiedAt = &now
		validationNotBefore := now.Add(model.GroupValidationPropagationDelay)
		evidenceDeadline := now.Add(model.GroupValidationEvidenceTimeout)
		state.ValidationNotBefore = &validationNotBefore
		state.EvidenceDeadline = &evidenceDeadline
	} else {
		// Historical pending journals have no trustworthy pre-switch watermark.
		state.ValidationStatus = model.GroupValidationUncertain
		state.LastError = "recovered_without_evidence_watermark"
	}
	if err := m.store.CompleteGroupTierTransition(ctx, current.ID, state, now); err != nil {
		return err
	}
	if err := m.applySuccess(ctx, &source, result); err != nil {
		m.logger.Warn("group_transition_recovery_refresh_failed", "transition_id", current.ID, "error", err)
	}
	m.record(ctx, transitionEvent(current, model.Event{Type: "group_transition_recovered", Severity: "warning", Message: "分组切换流水已通过只读回读恢复", Actor: "system:recovery", Details: transitionDetails(current)}))
	m.trigger.Trigger()
	return nil
}

func groupRequestIdentity(request model.GroupTierTransitionRequest) (string, string) {
	producer := strings.TrimSpace(request.Producer)
	authority := strings.TrimSpace(request.Authority)
	if producer == "" {
		switch {
		case request.Manual:
			producer = "human_operator"
		case strings.Contains(strings.ToLower(request.Actor), "agent"):
			producer = "agent_operator"
		default:
			producer = "deterministic_failover"
		}
	}
	if authority == "" {
		if request.Manual {
			authority = "administrator_command"
		} else if producer == "agent_operator" {
			authority = "autonomous_agent"
		} else {
			authority = "deterministic_safety"
		}
	}
	return producer, authority
}

func groupRequestMatchesExisting(request model.GroupTierTransitionRequest, existing model.GroupTierTransition) bool {
	producer, authority := groupRequestIdentity(request)
	return request.SourceID == existing.SourceID && strings.TrimSpace(request.KeyID) == existing.KeyID &&
		strings.ToLower(strings.TrimSpace(request.TargetTier)) == existing.ToTier && fallbackActor(request.Actor) == existing.Actor &&
		producer == existing.Producer && authority == existing.Authority && strings.TrimSpace(request.Reason) == existing.Reason &&
		strings.TrimSpace(request.Evidence) == existing.Evidence && strings.TrimSpace(request.SnapshotVersion) == existing.SnapshotVersion &&
		request.Trigger == existing.Trigger && request.PacketID == existing.PacketID && request.RunID == existing.RunID &&
		request.GoalID == existing.GoalID && request.StepID == existing.StepID &&
		request.Manual == existing.Manual && request.DryRun == existing.DryRun
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
				state.LastConfirmedAt = nil
				state.VerificationStartedAt = &now
				if state.ValidationTransitionID == transition.ID && state.ValidationStatus == model.GroupValidationTransitioning {
					state.ValidationStatus = model.GroupValidationAwaitingEvidence
					state.SwitchVerifiedAt = &now
					validationNotBefore := now.Add(model.GroupValidationPropagationDelay)
					evidenceDeadline := now.Add(model.GroupValidationEvidenceTimeout)
					state.ValidationNotBefore = &validationNotBefore
					state.EvidenceDeadline = &evidenceDeadline
				} else {
					state.ValidationStatus = model.GroupValidationUncertain
					state.LastError = "recovered_without_evidence_watermark"
				}
				state.CooldownUntil = timePointer(now.Add(time.Duration(settings.FailoverSwitchCooldownMinutes) * time.Minute))
				if err := m.store.CompleteGroupTierTransition(ctx, transition.ID, state, now); err != nil {
					return err
				}
				m.record(ctx, transitionEvent(transition, model.Event{Type: "group_failover_transition_recovered", Severity: "warning", Message: "服务重启后已通过上游实际分组确认未完成的切换流水", Actor: "system", Details: transitionDetails(transition)}))
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
			state.ValidationStatus = model.GroupValidationUncertain
			state.LastConfirmedAt = nil
			state.LastError = "manual_switch_requires_new_evidence_boundary"
			state.Frozen = false
			state.FreezeReason = ""
			m.record(ctx, model.Event{Type: "group_manual_override_detected", Severity: "warning", Message: fmt.Sprintf("检测到上游后台人工切换令牌分组，已进入 %d 分钟保护", settings.FailoverManualProtectionMinutes), Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"key_id":%q,"tier":%q}`, policy.SourceID, policy.KeyID, tier)})
		} else {
			state.CurrentTier = tier
			state.Frozen = false
			state.FreezeReason = ""
		}
		state.ObservedGroupID = key.GroupID
		if err := m.store.SaveGroupFailoverState(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) validateGroupFailoverPolicy(ctx context.Context, policy model.GroupFailoverPolicy) (model.UpstreamSource, model.KeyRate, string, error) {
	if !policy.MainEnabled && !policy.BackupEnabled && !policy.EmergencyEnabled {
		policy.MainEnabled, policy.BackupEnabled, policy.EmergencyEnabled = true, true, true
	}
	groups := make([]string, 0, 3)
	for _, tier := range []string{model.GroupTierMain, model.GroupTierBackup, model.GroupTierEmergency} {
		if !tierEnabled(policy, tier) {
			continue
		}
		groupID := strings.TrimSpace(groupIDForTier(policy, tier))
		if groupID == "" {
			return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("启用的固定层级必须配置分组")
		}
		for _, existing := range groups {
			if existing == groupID {
				return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("启用的固定层级必须使用不同分组")
			}
		}
		groups = append(groups, groupID)
	}
	if len(groups) == 0 {
		return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("固定三级策略至少需要一个启用层级")
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
			return model.UpstreamSource{}, model.KeyRate{}, "", errors.New("目标分组不存在")
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

func (m *Manager) validateGroupFailoverEvidenceSource(policy model.GroupFailoverPolicy) error {
	if !policy.Enabled {
		return nil
	}
	snapshots, ok := m.trigger.(validationSnapshotProvider)
	if !ok {
		return errors.New("validation_evidence_source_unavailable: 无法确认三级救灾的被动证据来源")
	}
	accountIDs := make(map[int64]bool, len(policy.AccountIDs))
	for _, accountID := range policy.AccountIDs {
		accountIDs[accountID] = true
	}
	for _, binding := range snapshots.Snapshot().Bindings {
		monitor := binding.Monitor
		if !accountIDs[binding.Account.ID] || monitor == nil || !monitor.Enabled || monitor.IntervalSeconds <= 0 {
			continue
		}
		maximumEvidenceDelay := model.GroupValidationPropagationDelay +
			time.Duration(monitor.IntervalSeconds)*time.Second + model.GroupValidationMonitorRequestTimeout
		if maximumEvidenceDelay < model.GroupValidationEvidenceTimeout {
			return nil
		}
	}
	return errors.New("validation_evidence_source_unavailable: 没有关联 monitor 能在证据截止时间前完成一次切换后检查")
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

func tierEnabled(policy model.GroupFailoverPolicy, tier string) bool {
	switch tier {
	case model.GroupTierMain:
		return policy.MainEnabled
	case model.GroupTierBackup:
		return policy.BackupEnabled
	case model.GroupTierEmergency:
		return policy.EmergencyEnabled
	default:
		return false
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
	return fmt.Sprintf(`{"transition_id":%d,"source_id":%d,"from_tier":%q,"to_tier":%q,"producer":%q,"authority":%q,"status":%q,"verified":%t,"uncertain":%t,"packet_id":%d,"run_id":%d,"goal_id":%d,"step_id":%d}`,
		item.ID, item.SourceID, item.FromTier, item.ToTier, item.Producer, item.Authority, item.Status,
		item.VerifiedAfter != "", item.Uncertain, item.PacketID, item.RunID, item.GoalID, item.StepID)
}

func transitionEvent(item model.GroupTierTransition, event model.Event) model.Event {
	event.GoalID, event.StepID = item.GoalID, item.StepID
	return event
}
