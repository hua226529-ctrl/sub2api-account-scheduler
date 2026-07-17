package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const (
	agentGroupAccountMaxAge = 3 * time.Minute
	agentGroupTrafficMaxAge = 6 * time.Minute
	agentGroupDataMaxAge    = 30 * time.Minute
)

// validateAutonomousGroupTransition repeats the disaster conditions outside
// the model. Confidence is advisory; only fresh, independently observed pool
// evidence can authorize an autonomous group write.
func (m *Manager) validateAutonomousGroupTransition(ctx context.Context, sourceID int64, keyID, targetTier string) error {
	if m.engine == nil || m.balances == nil || m.telemetry == nil || m.store == nil {
		return errors.New("自动切组所需的数据源尚未就绪")
	}
	now := time.Now().UTC()
	snapshot := m.engine.Snapshot()
	if snapshot.LastSyncAt == nil || snapshot.LastSyncError != "" || now.Sub(snapshot.LastSyncAt.UTC()) > agentGroupAccountMaxAge {
		return errors.New("账号与监控快照不新鲜，禁止自动切组")
	}
	trafficAt, trafficErr := m.telemetry.Status()
	if trafficAt == nil || trafficErr != "" || now.Sub(trafficAt.UTC()) > agentGroupTrafficMaxAge {
		return errors.New("真实流量数据不新鲜，禁止自动切组")
	}
	sources, err := m.balances.List(ctx)
	if err != nil {
		return fmt.Errorf("读取上游状态失败: %w", err)
	}
	var source model.UpstreamSource
	found := false
	for _, item := range sources {
		if item.ID == sourceID {
			source, found = item, true
			break
		}
	}
	if !found {
		return errors.New("上游不存在")
	}
	if source.LastSuccessAt == nil || now.Sub(source.LastSuccessAt.UTC()) > agentGroupDataMaxAge || source.Stale {
		return errors.New("令牌分组数据不新鲜，禁止自动切组")
	}
	var policy model.GroupFailoverPolicy
	found = false
	for _, item := range source.FailoverPolicies {
		if item.KeyID == strings.TrimSpace(keyID) {
			policy, found = item, true
			break
		}
	}
	if !found || !policy.Enabled || !policy.Confirmed || policy.ConfirmedVersion != policy.Version {
		return errors.New("令牌三级分组策略不存在、未启用或未确认")
	}
	currentTier := strings.TrimSpace(policy.State.CurrentTier)
	if currentTier == targetTier {
		return errors.New("令牌已经处于目标分组层级")
	}
	if targetTier == model.GroupTierMain {
		return errors.New("自动返回主组的产品规则尚未定义，只允许管理员显式切回")
	}
	if expected := nextConfiguredAgentTier(policy, currentTier); expected == "" || expected != targetTier {
		return errors.New("自主切组只能进入固定链的下一个已配置启用层级")
	}
	bindings := bindingsForAgentPool(snapshot.Bindings, sources, source, policy)
	if len(bindings) == 0 {
		return errors.New("调度池没有可核验的账号")
	}
	if targetTier != model.GroupTierBackup && targetTier != model.GroupTierEmergency {
		return errors.New("自动切组目标层级无效")
	}
	if policy.State.ValidationStatus != model.GroupValidationConfirmedFailed && !agentPoolOutageProven(ctx, m.store, bindings, now) {
		return errors.New("执行器未能独立证明整个调度池完全不可用，拒绝自动切组")
	}
	return nil
}

func nextConfiguredAgentTier(policy model.GroupFailoverPolicy, current string) string {
	type level struct {
		tier    string
		groupID string
		enabled bool
	}
	legacyEnablement := !policy.MainEnabled && !policy.BackupEnabled && !policy.EmergencyEnabled
	levels := []level{
		{tier: model.GroupTierMain, groupID: policy.MainGroupID, enabled: policy.MainEnabled},
		{tier: model.GroupTierBackup, groupID: policy.BackupGroupID, enabled: policy.BackupEnabled},
		{tier: model.GroupTierEmergency, groupID: policy.EmergencyGroupID, enabled: policy.EmergencyEnabled},
	}
	currentIndex := -1
	for index, level := range levels {
		if level.tier == current {
			currentIndex = index
			break
		}
	}
	if currentIndex < 0 {
		return ""
	}
	for _, level := range levels[currentIndex+1:] {
		if strings.TrimSpace(level.groupID) != "" && (level.enabled || legacyEnablement) {
			return level.tier
		}
	}
	return ""
}

type agentWindowStore interface {
	GetAgentWindowStats(context.Context, int64, time.Time, time.Time, string) (model.AgentWindowStats, error)
}

func agentPoolOutageProven(ctx context.Context, store agentWindowStore, bindings []model.ResolvedBinding, now time.Time) bool {
	allStopped, hardThree, hardFive := true, true, true
	samples, eligible, successes, hardErrors := 0, 0, 0, 0
	for _, binding := range bindings {
		if binding.Account.Status == "active" && binding.Account.Schedulable {
			allStopped = false
		}
		hardThree = hardThree && agentMonitorHardStreak(binding, now, 3)
		hardFive = hardFive && agentMonitorHardStreak(binding, now, 5)
		window, err := store.GetAgentWindowStats(ctx, binding.Account.ID, now.Add(-5*time.Minute), now.Add(time.Nanosecond), "agent_group_guard")
		if err != nil {
			return false
		}
		samples += window.SampleCount
		eligible += window.EligibleCount
		successes += window.SuccessCount
		hardErrors += window.ErrorCategoryCounts[model.ErrorClassInfrastructure] + window.ErrorCategoryCounts[model.ErrorClassCapacity]
	}
	if !allStopped || !hardThree {
		return false
	}
	failed := eligible - successes
	rateHard := eligible >= 10 && failed > 0 && float64(successes)*100/float64(eligible) < 20 && hardErrors >= failed
	return rateHard || (samples == 0 && hardFive)
}

func agentMonitorHardStreak(binding model.ResolvedBinding, now time.Time, threshold int) bool {
	if binding.Monitor == nil || !binding.Monitor.Enabled || binding.Monitor.LastCheckedAt == nil || binding.MonitorState.LastCheckedAt == nil {
		return false
	}
	checkedAt := binding.Monitor.LastCheckedAt.UTC()
	stateAt := binding.MonitorState.LastCheckedAt.UTC()
	if checkedAt.After(now) || now.Sub(checkedAt) > agentGroupAccountMaxAge || !checkedAt.Equal(stateAt) {
		return false
	}
	monitorHard := strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusFailed) || strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusError)
	stateHard := strings.EqualFold(binding.MonitorState.LastStatus, model.StatusFailed) || strings.EqualFold(binding.MonitorState.LastStatus, model.StatusError)
	return monitorHard && stateHard && binding.MonitorState.UnhealthyStreak >= threshold
}

func bindingsForAgentPool(all []model.ResolvedBinding, sources []model.UpstreamSource, selected model.UpstreamSource, policy model.GroupFailoverPolicy) []model.ResolvedBinding {
	pool := strings.TrimSpace(policy.Pool)
	if pool == "" {
		pool = strings.TrimSpace(selected.RoutingPool)
	}
	if pool == "" {
		pool = selected.Name
	}
	endpoints := make(map[string]bool)
	for _, source := range sources {
		sourcePool := strings.TrimSpace(source.RoutingPool)
		if sourcePool == "" {
			sourcePool = source.Name
		}
		if sourcePool == pool {
			endpoints[source.NormalizedURL] = true
		}
	}
	accountIDs := make(map[int64]bool, len(policy.AccountIDs))
	for _, id := range policy.AccountIDs {
		accountIDs[id] = true
	}
	result := make([]model.ResolvedBinding, 0)
	for _, binding := range all {
		if endpoints[binding.NormalizedEndpoint] || accountIDs[binding.Account.ID] {
			result = append(result, binding)
		}
	}
	return result
}

func (m *Manager) autonomousGroupTransitionFence(ctx context.Context, sourceID int64, keyID string) (string, string, *time.Time) {
	if m.engine == nil || m.balances == nil {
		return "", "", nil
	}
	snapshot := m.engine.Snapshot()
	sources, err := m.balances.List(ctx)
	if err != nil {
		return "", "", snapshot.LastSyncAt
	}
	for _, source := range sources {
		if source.ID != sourceID {
			continue
		}
		for _, policy := range source.FailoverPolicies {
			if policy.KeyID == strings.TrimSpace(keyID) {
				pool := strings.TrimSpace(policy.Pool)
				if pool == "" {
					pool = strings.TrimSpace(source.RoutingPool)
				}
				if pool == "" {
					pool = source.Name
				}
				return pool, policy.State.CurrentTier, snapshot.LastSyncAt
			}
		}
	}
	return "", "", snapshot.LastSyncAt
}
