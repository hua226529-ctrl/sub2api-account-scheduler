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
	agentGroupRecoveryAge   = 30 * time.Minute
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
	bindings := bindingsForAgentPool(snapshot.Bindings, sources, source, policy)
	if len(bindings) == 0 {
		return errors.New("调度池没有可核验的账号")
	}
	if targetTier == model.GroupTierMain {
		return m.validateAutonomousReturnMain(ctx, policy, bindings, now)
	}
	if targetTier != model.GroupTierBackup && targetTier != model.GroupTierEmergency {
		return errors.New("自动切组目标层级无效")
	}
	if !agentPoolOutageProven(ctx, m.store, bindings, now) {
		return errors.New("执行器未能独立证明整个调度池完全不可用，拒绝自动切组")
	}
	return nil
}

func (m *Manager) validateAutonomousReturnMain(ctx context.Context, policy model.GroupFailoverPolicy, bindings []model.ResolvedBinding, now time.Time) error {
	state := policy.State
	if state.CurrentTier != model.GroupTierBackup && state.CurrentTier != model.GroupTierEmergency {
		return errors.New("只有备用或紧急分组可以自动试回主组")
	}
	if state.ReturnBlockedUntil != nil && now.Before(state.ReturnBlockedUntil.UTC()) {
		return errors.New("试回主组仍在失败保护期")
	}
	if state.HealthySince == nil || now.Sub(state.HealthySince.UTC()) < agentGroupRecoveryAge || state.RecoveryHealthyCount < 10 {
		return errors.New("尚未连续稳定30分钟并获得10次正常监控结果")
	}
	accountIDs := make(map[int64]bool, len(policy.AccountIDs))
	for _, id := range policy.AccountIDs {
		accountIDs[id] = true
	}
	eligible, successes := 0, 0
	for _, binding := range bindings {
		if !accountIDs[binding.Account.ID] {
			continue
		}
		if binding.Monitor == nil || !binding.Monitor.Enabled || binding.Monitor.LastCheckedAt == nil ||
			now.Sub(binding.Monitor.LastCheckedAt.UTC()) > agentGroupAccountMaxAge ||
			!strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusOperational) || binding.MonitorState.HealthyStreak < 10 {
			return errors.New("关联账号尚未获得10次新鲜正常监控结果")
		}
		window, err := m.store.GetAgentWindowStats(ctx, binding.Account.ID, now.Add(-30*time.Minute), now.Add(time.Nanosecond), "agent_return_main")
		if err != nil {
			return fmt.Errorf("读取恢复流量证据失败: %w", err)
		}
		eligible += window.EligibleCount
		successes += window.SuccessCount
	}
	if eligible < 20 || float64(successes)*100/float64(eligible) < 98 {
		return errors.New("最近30分钟真实请求尚未达到20个样本和98%成功率")
	}
	return nil
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
		candidate := strings.TrimSpace(source.RoutingPool)
		if candidate == "" {
			candidate = source.Name
		}
		if candidate == pool {
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
