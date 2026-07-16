package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/health"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/telemetry"
)

type packetBuilder struct {
	store     *store.Store
	engine    *reconcile.Engine
	balances  *balance.Manager
	telemetry *telemetry.Manager
}

func (b packetBuilder) Build(ctx context.Context, kind string, settings model.AgentSettings) (model.AnalysisPacket, error) {
	return b.BuildAt(ctx, kind, settings, time.Now().UTC())
}

func (b packetBuilder) BuildAt(ctx context.Context, kind string, settings model.AgentSettings, cutoff time.Time) (model.AnalysisPacket, error) {
	cutoff = cutoff.UTC()
	buildStarted := time.Now().UTC()
	snapshot := b.engine.Snapshot()
	monitorFresh := snapshot.LastSyncAt != nil && snapshot.LastSyncError == "" && buildStarted.Sub(snapshot.LastSyncAt.UTC()) <= 3*time.Minute
	balanceLastRun := b.balances.LastRunAt()
	var telemetryLastRun *time.Time
	var telemetryError string
	trafficFresh := b.telemetry == nil
	if b.telemetry != nil {
		telemetryLastRun, telemetryError = b.telemetry.Status()
		trafficFresh = telemetryLastRun != nil && telemetryError == "" && buildStarted.Sub(telemetryLastRun.UTC()) <= 6*time.Minute
	}
	packet := model.AnalysisPacket{
		Kind: kind, CutoffAt: cutoff, CreatedAt: cutoff,
		DataHealth: model.AgentDataHealth{
			SchedulerLastSyncAt: snapshot.LastSyncAt, SchedulerError: snapshot.LastSyncError,
			BalanceLastSyncAt: balanceLastRun, MonitorFresh: monitorFresh,
			TelemetryLastSyncAt: telemetryLastRun, TelemetryError: telemetryError, TrafficFresh: trafficFresh,
			MissingSources: []string{},
		},
		PoolSummaries: []model.AgentPoolSummary{}, GroupFailoverTokens: []model.AgentGroupFailoverToken{},
		AccountCompactStates: []model.AgentAccountState{},
		Anomalies:            []model.AgentAccountState{}, Changes: []string{}, EvidenceCatalog: []string{},
	}
	if snapshot.LastSyncAt == nil {
		packet.DataHealth.MissingSources = append(packet.DataHealth.MissingSources, "账号与监控快照")
	} else if !monitorFresh {
		packet.DataHealth.MissingSources = append(packet.DataHealth.MissingSources, "账号与监控快照已过期")
	}
	if packet.DataHealth.BalanceLastSyncAt == nil {
		packet.DataHealth.MissingSources = append(packet.DataHealth.MissingSources, "余额与倍率快照")
	} else if buildStarted.Sub(packet.DataHealth.BalanceLastSyncAt.UTC()) > 30*time.Minute {
		packet.DataHealth.MissingSources = append(packet.DataHealth.MissingSources, "余额与倍率快照已过期")
	}
	if b.telemetry != nil && !trafficFresh {
		packet.DataHealth.MissingSources = append(packet.DataHealth.MissingSources, "监控历史与真实请求证据已过期")
	}

	upstreams, _ := b.balances.List(ctx)
	upstreamByURL := make(map[string]model.UpstreamSource)
	for _, source := range upstreams {
		upstreamByURL[source.NormalizedURL] = source
	}

	var previous model.AnalysisPacket
	if item, err := b.store.LatestAnalysisPacket(ctx, kind); err == nil {
		previous = item
		packet.PreviousPacketID = &item.ID
	}
	previousAccounts := make(map[int64]model.AgentAccountState)
	for _, account := range previous.AccountCompactStates {
		previousAccounts[account.AccountID] = account
	}

	poolStates := make(map[string]*model.AgentPoolSummary)
	poolRateTotals := make(map[string]float64)
	poolRateCounts := make(map[string]int)
	for _, binding := range snapshot.Bindings {
		state, err := b.buildAccountState(ctx, binding, upstreamByURL, snapshot.Settings, cutoff)
		if err != nil {
			return packet, err
		}
		if previousState, ok := previousAccounts[state.AccountID]; ok {
			state.Changed = state.AvailabilityState != previousState.AvailabilityState ||
				math.Abs(state.AvailabilityScore-previousState.AvailabilityScore) >= 5 ||
				state.Schedulable != previousState.Schedulable || loadFactorValue(state) != loadFactorValue(previousState)
			if state.Changed {
				packet.Changes = append(packet.Changes, changeDescription(previousState, state))
			}
		} else {
			state.Changed = true
			packet.Changes = append(packet.Changes, "发现新账号 "+state.Name)
		}
		packet.AccountCompactStates = append(packet.AccountCompactStates, state)
		packet.EvidenceCatalog = append(packet.EvidenceCatalog, "account:"+formatID(state.AccountID))
		poolName := state.Pool
		if poolName == "" {
			poolName = "未分池"
		}
		pool := poolStates[poolName]
		if pool == nil {
			pool = &model.AgentPoolSummary{Name: poolName}
			poolStates[poolName] = pool
		}
		accumulatePool(pool, state)
	}
	for accountID, previousState := range previousAccounts {
		found := false
		for _, current := range packet.AccountCompactStates {
			if current.AccountID == accountID {
				found = true
				break
			}
		}
		if !found {
			packet.Changes = append(packet.Changes, "账号已移除 "+previousState.Name)
		}
	}
	transitions, _ := b.store.ListGroupTierTransitions(ctx, 0, "", 500)
	packet.GroupFailoverTokens = buildGroupFailoverTokens(upstreams, transitions)

	for _, source := range upstreams {
		name := source.RoutingPool
		if name == "" {
			name = source.Name
		}
		pool := poolStates[name]
		if pool == nil {
			pool = &model.AgentPoolSummary{Name: name}
			poolStates[name] = pool
		}
		if source.Stale {
			pool.StaleSources++
		}
		if source.Balance != nil && (pool.MinimumBalance == nil || *source.Balance < *pool.MinimumBalance) {
			value := *source.Balance
			pool.MinimumBalance = &value
		}
		for _, rate := range source.KeyRates {
			if rate.RateMultiplier != nil {
				poolRateTotals[name] += *rate.RateMultiplier
				poolRateCounts[name]++
			}
		}
	}
	poolNames := make([]string, 0, len(poolStates))
	for name := range poolStates {
		poolNames = append(poolNames, name)
	}
	sort.Strings(poolNames)
	for _, name := range poolNames {
		if poolRateCounts[name] > 0 {
			poolStates[name].AverageMultiplier = round(poolRateTotals[name]/float64(poolRateCounts[name]), 3)
		}
		packet.PoolSummaries = append(packet.PoolSummaries, *poolStates[name])
		packet.EvidenceCatalog = append(packet.EvidenceCatalog, "pool:"+name)
	}

	sort.Slice(packet.AccountCompactStates, func(i, j int) bool {
		return packet.AccountCompactStates[i].AccountID < packet.AccountCompactStates[j].AccountID
	})
	anomalies := append([]model.AgentAccountState{}, packet.AccountCompactStates...)
	sort.Slice(anomalies, func(i, j int) bool {
		if anomalies[i].RiskScore == anomalies[j].RiskScore {
			return anomalies[i].AccountID < anomalies[j].AccountID
		}
		return anomalies[i].RiskScore > anomalies[j].RiskScore
	})
	limit := settings.MaxAnomalies
	if limit < 1 {
		limit = 20
	}
	for _, item := range anomalies {
		if len(packet.Anomalies) >= limit {
			break
		}
		if item.RiskScore >= 20 || item.Changed {
			packet.Anomalies = append(packet.Anomalies, item)
		}
	}
	packet.SystemSummary = summarizeSystem(packet.AccountCompactStates, packet.DataHealth)
	if stats, err := b.store.GetAgentWindowStats(ctx, 0, cutoff.Add(-30*time.Minute), cutoff, "30m"); err == nil {
		packet.DataHealth.TrafficSamples30M = stats.SampleCount
	}
	activeVersions, _ := b.store.ListActivePolicyVersions(ctx)
	policyPayload, _ := json.Marshal(map[string]any{"global": snapshot.Settings, "versions": activeVersions})
	packet.ActivePolicies = policyPayload
	outcomes, _ := b.store.ListRecentDecisionOutcomes(ctx, 30)
	packet.DecisionOutcomes, _ = json.Marshal(outcomes)

	digestInput := struct {
		Summary  model.AgentSystemSummary
		Pools    []model.AgentPoolSummary
		Failover []model.AgentGroupFailoverToken
		Accounts []model.AgentAccountState
		Changes  []string
	}{packet.SystemSummary, packet.PoolSummaries, packet.GroupFailoverTokens, packet.AccountCompactStates, packet.Changes}
	payload, _ := json.Marshal(digestInput)
	digest := sha256.Sum256(payload)
	packet.Hash = hex.EncodeToString(digest[:])
	packet.NoMaterialChange = previous.Hash != "" && previous.Hash == packet.Hash
	contextPayload, _ := json.Marshal(compactPacketForModel(packet))
	packet.TokenEstimate = (len(contextPayload) + 3) / 4
	if err := b.store.SaveAnalysisPacketWithAssessments(ctx, &packet, packet.AccountCompactStates); err != nil {
		return packet, err
	}
	return packet, nil
}

func buildGroupFailoverTokens(sources []model.UpstreamSource, transitions []model.GroupTierTransition) []model.AgentGroupFailoverToken {
	items := make([]model.AgentGroupFailoverToken, 0)
	history := make(map[string][]model.AgentGroupTransitionResult)
	for _, transition := range transitions {
		key := strconv.FormatInt(transition.SourceID, 10) + ":" + transition.KeyID
		if len(history[key]) >= 3 {
			continue
		}
		history[key] = append(history[key], model.AgentGroupTransitionResult{
			FromTier: transition.FromTier, ToTier: transition.ToTier, Status: transition.Status,
			Manual: transition.Manual, CreatedAt: transition.CreatedAt, CompletedAt: transition.CompletedAt,
		})
	}
	for _, source := range sources {
		groups := make(map[string]model.UpstreamGroup, len(source.Groups))
		for _, group := range source.Groups {
			groups[group.ExternalID] = group
		}
		keys := make(map[string]model.KeyRate, len(source.KeyRates))
		for _, key := range source.KeyRates {
			keys[key.ExternalID] = key
		}
		accountNames := make(map[int64]string, len(source.MatchedAccounts))
		for _, account := range source.MatchedAccounts {
			accountNames[account.ID] = account.Name
		}
		pool := source.RoutingPool
		if pool == "" {
			pool = source.Name
		}
		for _, policy := range source.FailoverPolicies {
			historyKey := strconv.FormatInt(source.ID, 10) + ":" + policy.KeyID
			keyName, keyHint := policy.KeyName, policy.KeyHint
			if key, ok := keys[policy.KeyID]; ok {
				if keyName == "" {
					keyName = key.Name
				}
				if keyHint == "" {
					keyHint = key.KeyHint
				}
			}
			names := make([]string, 0, len(policy.AccountIDs))
			for _, accountID := range policy.AccountIDs {
				if name := accountNames[accountID]; name != "" {
					names = append(names, name)
				}
			}
			items = append(items, model.AgentGroupFailoverToken{
				SourceID: source.ID, SourceName: source.Name, Provider: source.Provider, Pool: pool,
				KeyID: policy.KeyID, KeyName: keyName, KeyHint: keyHint, Enabled: policy.Enabled,
				Confirmed: policy.Confirmed && policy.ConfirmedVersion == policy.Version, PolicyVersion: policy.Version,
				CurrentTier: policy.State.CurrentTier, PreviousTier: policy.State.PreviousTier,
				PreviousStableTier: policy.State.PreviousStableTier,
				Main:               groupTierSummary(model.GroupTierMain, policy.MainGroupID, groups),
				Backup:             groupTierSummary(model.GroupTierBackup, policy.BackupGroupID, groups),
				Emergency:          groupTierSummary(model.GroupTierEmergency, policy.EmergencyGroupID, groups),
				AccountIDs:         append([]int64(nil), policy.AccountIDs...), AccountNames: names,
				Balance: source.Balance, Unit: source.Unit, DataFresh: !source.Stale && source.LastSuccessAt != nil,
				Frozen: policy.State.Frozen, FreezeReason: policy.State.FreezeReason,
				ManualHoldUntil: policy.State.ManualHoldUntil, CooldownUntil: policy.State.CooldownUntil,
				ManualOverrideUntil: policy.State.ManualOverrideUntil,
				ReturnBlockedUntil:  policy.State.ReturnBlockedUntil, LastSwitchAt: policy.State.LastSwitchAt,
				LastTransitionAt: policy.State.LastTransitionAt, VerificationStartedAt: policy.State.VerificationStartedAt,
				HealthySince: policy.State.HealthySince, RecoveryHealthyCount: policy.State.RecoveryHealthyCount,
				LastConfirmedAt:   policy.State.LastConfirmedAt,
				RecentTransitions: append([]model.AgentGroupTransitionResult(nil), history[historyKey]...),
			})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].SourceID != items[j].SourceID {
			return items[i].SourceID < items[j].SourceID
		}
		return items[i].KeyID < items[j].KeyID
	})
	return items
}

func groupTierSummary(tier, groupID string, groups map[string]model.UpstreamGroup) model.AgentGroupTierSummary {
	group := groups[groupID]
	return model.AgentGroupTierSummary{Tier: tier, Name: group.Name, RateMultiplier: group.RateMultiplier}
}

func (b packetBuilder) buildAccountState(ctx context.Context, binding model.ResolvedBinding, upstreams map[string]model.UpstreamSource, settings model.Settings, cutoff time.Time) (model.AgentAccountState, error) {
	state := model.AgentAccountState{
		AccountID: binding.Account.ID, Name: binding.Account.Name, Schedulable: binding.Account.Schedulable,
		AccountStatus: binding.Account.Status, LoadFactor: binding.Account.LoadFactor, Concurrency: binding.Account.Concurrency,
		BalanceLocked: binding.Control.BalanceLocked, CostLocked: binding.Control.CostLocked,
		HealthLocked: binding.Control.HealthLocked, FlapActive: binding.Control.FlapActive,
		AvailabilityScore: 50, PerformanceScore: 50, StabilityScore: 100,
		CapacityScore: 100, CostScore: 50, Confidence: 0, Reasons: []string{}, Windows: map[string]model.AgentWindowStats{},
		ErrorCategoryCounts: map[string]int{},
	}
	if binding.Monitor != nil {
		state.MonitorID = &binding.Monitor.ID
		state.MonitorStatus = binding.Monitor.PrimaryStatus
		state.MonitorCheckedAt = binding.Monitor.LastCheckedAt
	}
	if binding.Decision != nil {
		state.HardFailureStreak = binding.Decision.HardFailureStreak
		state.HardFailures10 = binding.Decision.HardFailures10
		state.TrafficSampleCount = binding.Decision.TrafficSampleCount
		state.TrafficSuccessRate = binding.Decision.TrafficSuccessRate
		state.ErrorCategoryCounts = make(map[string]int, len(binding.Decision.ErrorCategoryCounts))
		for category, count := range binding.Decision.ErrorCategoryCounts {
			state.ErrorCategoryCounts[category] = count
		}
	}
	if source, ok := upstreams[binding.NormalizedEndpoint]; ok {
		state.Pool = source.RoutingPool
		if state.Pool == "" {
			state.Pool = source.Name
		}
		state.CostScore = sourceCostScore(source)
	}
	windows := []struct {
		name     string
		duration time.Duration
	}{{"5m", 5 * time.Minute}, {"30m", 30 * time.Minute}, {"6h", 6 * time.Hour}, {"24h", 24 * time.Hour}, {"7d", 7 * 24 * time.Hour}}
	for _, window := range windows {
		stats, err := b.store.GetAgentWindowStats(ctx, binding.Account.ID, cutoff.Add(-window.duration), cutoff, window.name)
		if err != nil {
			return state, err
		}
		state.Windows[window.name] = stats
	}
	deriveAvailability(&state, binding, health.ResolveSettings(settings, binding.Policy), cutoff)
	return state, nil
}

func deriveAvailability(state *model.AgentAccountState, binding model.ResolvedBinding, settings model.Settings, now time.Time) {
	monitorKnown, monitorOK, monitorDegraded := false, false, false
	if binding.Monitor != nil && binding.Monitor.Enabled && binding.Monitor.LastCheckedAt != nil {
		staleAfter := time.Duration(binding.Monitor.IntervalSeconds*3) * time.Second
		if staleAfter < 3*time.Minute {
			staleAfter = 3 * time.Minute
		}
		checkedAt := binding.Monitor.LastCheckedAt.UTC()
		if !checkedAt.After(now) && now.Sub(checkedAt) <= staleAfter {
			monitorKnown = true
			monitorOK = strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusOperational)
			monitorDegraded = strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusDegraded)
		}
	}
	traffic := state.Windows["30m"]
	trafficKnown := traffic.EligibleCount >= maxIntValue(3, settings.HealthMinSamples)
	trafficOK := trafficKnown && traffic.SuccessRate >= float64(settings.HealthTrafficHealthyAt)
	trafficBad := trafficKnown && traffic.SuccessRate < float64(settings.HealthTrafficPauseBelow)

	switch {
	case monitorKnown && !monitorOK && !monitorDegraded && trafficBad:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence = "unavailable", math.Min(traffic.SuccessRate, 25), .95
		state.Reasons = append(state.Reasons, "监控和真实流量同时异常")
	case monitorKnown && !monitorOK && !monitorDegraded && trafficOK:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence, state.EvidenceConflict = "degraded", traffic.SuccessRate, .72, true
		state.Reasons = append(state.Reasons, "监控异常但真实流量正常，疑似监控误报")
	case (monitorOK || monitorDegraded) && trafficBad:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence, state.EvidenceConflict = "degraded", traffic.SuccessRate, .78, true
		state.Reasons = append(state.Reasons, "监控正常但真实流量异常")
	case monitorOK && (!trafficKnown || trafficOK):
		state.AvailabilityState, state.AvailabilityScore = "available", 100
		state.Confidence = .55
		if trafficKnown {
			state.AvailabilityScore, state.Confidence = traffic.SuccessRate, .92
		}
		state.Reasons = append(state.Reasons, "监控正常"+trafficSuffix(trafficKnown))
	case monitorDegraded && trafficOK:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence = "available", traffic.SuccessRate, .88
		state.Reasons = append(state.Reasons, "真实请求成功，监控仅为性能下降")
	case monitorDegraded:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence = "degraded", 90, .58
		state.Reasons = append(state.Reasons, "监控显示性能下降，真实流量样本不足")
	case trafficKnown:
		state.AvailabilityScore = traffic.SuccessRate
		state.Confidence = .65
		if trafficBad {
			state.AvailabilityState = "unavailable"
			state.Reasons = append(state.Reasons, "只有真实流量证据且成功率偏低")
		} else {
			state.AvailabilityState = "available"
			state.Reasons = append(state.Reasons, "真实流量可用，监控数据不足")
		}
	default:
		state.AvailabilityState, state.AvailabilityScore, state.Confidence = "insufficient_data", 50, .15
		state.Reasons = append(state.Reasons, "监控失联且真实流量样本不足")
	}

	quality := 100.0
	if binding.Decision != nil && binding.Decision.QualityScore > 0 {
		quality = binding.Decision.QualityScore
	} else if traffic.P90DurationMS > 0 {
		quality = latencyQuality(traffic.P90DurationMS)
	}
	state.PerformanceScore = round(quality, 2)
	longWindow := state.Windows["24h"]
	state.StabilityScore = clamp(100-float64(longWindow.StateChanges*3)-float64(longWindow.AutomaticPauseCount*12), 0, 100)
	if !state.Schedulable || state.AccountStatus != "active" {
		state.CapacityScore = 0
	} else if binding.Account.Concurrency > 0 {
		state.CapacityScore = clamp(60+math.Min(40, float64(binding.Account.Concurrency)), 0, 100)
	}
	state.RiskScore = round(clamp((100-state.AvailabilityScore)*.55+(100-state.PerformanceScore)*.15+
		(100-state.StabilityScore)*.15+(100-state.CapacityScore)*.10+(100-state.Confidence*100)*.05, 0, 100), 2)
}

func compactPacketForModel(packet model.AnalysisPacket) any {
	type compact struct {
		ID           int64          `json:"id"`
		Name         string         `json:"name"`
		Pool         string         `json:"pool,omitempty"`
		State        string         `json:"state"`
		Availability float64        `json:"availability"`
		Performance  float64        `json:"performance"`
		Stability    float64        `json:"stability"`
		Cost         float64        `json:"cost"`
		Confidence   float64        `json:"confidence"`
		Concurrency  int            `json:"concurrency"`
		Schedulable  bool           `json:"schedulable"`
		Changed      bool           `json:"changed"`
		HardFailures int            `json:"hard_failure_streak"`
		TrafficCount int            `json:"traffic_sample_count"`
		TrafficRate  float64        `json:"traffic_success_rate"`
		ErrorCounts  map[string]int `json:"error_category_counts,omitempty"`
	}
	accounts := make([]compact, 0, len(packet.AccountCompactStates))
	for _, item := range packet.AccountCompactStates {
		accounts = append(accounts, compact{
			ID: item.AccountID, Name: item.Name, Pool: item.Pool, State: item.AvailabilityState,
			Availability: item.AvailabilityScore, Performance: item.PerformanceScore, Stability: item.StabilityScore,
			Cost: item.CostScore, Confidence: item.Confidence, Concurrency: item.Concurrency,
			Schedulable: item.Schedulable, Changed: item.Changed, HardFailures: item.HardFailureStreak,
			TrafficCount: item.TrafficSampleCount, TrafficRate: item.TrafficSuccessRate,
			ErrorCounts: item.ErrorCategoryCounts,
		})
	}
	omitted := packet.SystemSummary.Accounts - len(accounts)
	if omitted < 0 {
		omitted = 0
	}
	return map[string]any{
		"metadata": map[string]any{"packet_id": packet.ID, "kind": packet.Kind, "cutoff_at": packet.CutoffAt,
			"hash": packet.Hash, "no_material_change": packet.NoMaterialChange},
		"data_health": packet.DataHealth, "system_summary": packet.SystemSummary, "pool_summaries": packet.PoolSummaries,
		"group_failover_tokens":  packet.GroupFailoverTokens,
		"account_compact_states": accounts, "anomalies": packet.Anomalies, "changes": packet.Changes,
		"active_policies": packet.ActivePolicies, "decision_outcomes": packet.DecisionOutcomes,
		"evidence_catalog":    packet.EvidenceCatalog,
		"context_compression": map[string]int{"included_accounts": len(accounts), "omitted_accounts": omitted},
	}
}

func summarizeSystem(states []model.AgentAccountState, health model.AgentDataHealth) model.AgentSystemSummary {
	result := model.AgentSystemSummary{Accounts: len(states), DataFresh: health.MonitorFresh && health.TrafficFresh && health.SchedulerError == "" && health.TelemetryError == ""}
	var availability, performance, confidence float64
	for _, state := range states {
		if state.Schedulable {
			result.Schedulable++
		}
		switch state.AvailabilityState {
		case "available":
			result.Available++
		case "degraded":
			result.Degraded++
		case "unavailable":
			result.Unavailable++
		default:
			result.InsufficientData++
		}
		if state.RiskScore >= 60 {
			result.CriticalAnomalies++
		}
		availability += state.AvailabilityScore
		performance += state.PerformanceScore
		confidence += state.Confidence * 100
	}
	if len(states) > 0 {
		result.AverageAvailability = round(availability/float64(len(states)), 2)
		result.AveragePerformance = round(performance/float64(len(states)), 2)
		result.AverageConfidence = round(confidence/float64(len(states)), 2)
	}
	return result
}

func accumulatePool(pool *model.AgentPoolSummary, state model.AgentAccountState) {
	pool.Accounts++
	if state.Schedulable {
		pool.Schedulable++
		if state.AccountStatus == "active" {
			pool.Capacity += state.Concurrency
		}
	}
	switch state.AvailabilityState {
	case "available":
		pool.Available++
	case "degraded":
		pool.Degraded++
	case "unavailable":
		pool.Unavailable++
	default:
		pool.InsufficientData++
	}
}

func sourceCostScore(source model.UpstreamSource) float64 {
	best := 0.0
	for _, rate := range source.KeyRates {
		if rate.RateMultiplier != nil && (best == 0 || *rate.RateMultiplier < best) {
			best = *rate.RateMultiplier
		}
	}
	if best == 0 {
		return 50
	}
	return round(clamp(110-best*50, 0, 100), 2)
}

func latencyQuality(milliseconds int64) float64 {
	switch {
	case milliseconds <= 3000:
		return 100
	case milliseconds <= 8000:
		return 90 - float64(milliseconds-3000)/500
	case milliseconds <= 15000:
		return 80 - float64(milliseconds-8000)/350
	default:
		return clamp(60-float64(milliseconds-15000)/1000, 20, 60)
	}
}

func trafficSuffix(known bool) string {
	if known {
		return "且真实请求成功"
	}
	return "，真实流量样本不足"
}

func changeDescription(before, after model.AgentAccountState) string {
	if before.AvailabilityState != after.AvailabilityState {
		return after.Name + " 可用性由 " + before.AvailabilityState + " 变为 " + after.AvailabilityState
	}
	if before.Schedulable != after.Schedulable {
		return after.Name + " 调度状态发生变化"
	}
	return after.Name + " 综合分数发生明显变化"
}

func loadFactorValue(state model.AgentAccountState) int {
	if state.LoadFactor == nil {
		return 0
	}
	return *state.LoadFactor
}

func formatID(id int64) string {
	return strconv.FormatInt(id, 10)
}

func round(value float64, digits int) float64 {
	factor := math.Pow10(digits)
	return math.Round(value*factor) / factor
}

func clamp(value, minimum, maximum float64) float64 {
	return math.Max(minimum, math.Min(maximum, value))
}

func maxIntValue(left, right int) int {
	if left > right {
		return left
	}
	return right
}
