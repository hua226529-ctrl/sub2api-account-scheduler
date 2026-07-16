package failover

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type Store interface {
	ListGroupFailoverPolicies(context.Context, int64) ([]model.GroupFailoverPolicy, error)
	SaveGroupFailoverState(context.Context, model.GroupFailoverState) error
	CountCompletedGroupTierTransitions(context.Context, int64, string, time.Time) (int, error)
	GetAgentWindowStats(context.Context, int64, time.Time, time.Time, string) (model.AgentWindowStats, error)
	GetAgentSettings(context.Context) (model.AgentSettings, error)
	GetSettings(context.Context) (model.Settings, error)
	AddEvent(context.Context, model.Event) error
}

type SnapshotProvider interface {
	Snapshot() model.Snapshot
}

type UpstreamManager interface {
	List(context.Context) ([]model.UpstreamSource, error)
	TransitionGroupTier(context.Context, model.GroupTierTransitionRequest) (model.GroupTierTransition, error)
}

type TelemetryStatus interface {
	Status() (*time.Time, string)
}

type Controller struct {
	store     Store
	snapshots SnapshotProvider
	upstreams UpstreamManager
	telemetry TelemetryStatus
	interval  time.Duration
	logger    *slog.Logger

	mu           sync.Mutex
	lastRunAt    time.Time
	lastError    string
	outageSince  map[string]time.Time
	verification map[string]*verificationTracker
	now          func() time.Time
}

// verificationTracker deliberately records monitor results observed after a
// group switch. The snapshot only exposes the latest monitor result, so a
// persisted streak cannot prove that two results happened after the switch.
// After a restart the tracker starts again, which is conservative: automation
// waits for two more results instead of escalating on evidence it cannot prove.
type verificationTracker struct {
	startedAt      time.Time
	monitorTimes   map[int64]time.Time
	monitorResults int
}

func NewController(store Store, snapshots SnapshotProvider, upstreams UpstreamManager, telemetry TelemetryStatus, interval time.Duration, logger *slog.Logger) *Controller {
	if interval < 10*time.Second {
		interval = 50 * time.Second
	}
	return &Controller{
		store: store, snapshots: snapshots, upstreams: upstreams, telemetry: telemetry,
		interval: interval, logger: logger, outageSince: make(map[string]time.Time), verification: make(map[string]*verificationTracker), now: func() time.Time { return time.Now().UTC() },
	}
}

func (c *Controller) Start(ctx context.Context) {
	go func() {
		c.runLogged(ctx)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.runLogged(ctx)
			}
		}
	}()
}

func (c *Controller) Status() (*time.Time, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var last *time.Time
	if !c.lastRunAt.IsZero() {
		value := c.lastRunAt
		last = &value
	}
	return last, c.lastError
}

func (c *Controller) RunOnce(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now().UTC()
	snapshot := c.snapshots.Snapshot()
	if snapshot.Freeze.AllAutomation {
		c.lastRunAt = now
		c.lastError = ""
		return nil
	}
	settings, err := c.store.GetSettings(ctx)
	if err != nil {
		return fmt.Errorf("读取救灾策略: %w", err)
	}
	accountSnapshotMaxAge := time.Duration(settings.FailoverAccountFreshMinutes) * time.Minute
	telemetryMaxAge := time.Duration(settings.FailoverTelemetryFreshMinutes) * time.Minute
	if snapshot.LastSyncAt == nil || now.Sub(snapshot.LastSyncAt.UTC()) > accountSnapshotMaxAge || snapshot.LastSyncError != "" {
		return fmt.Errorf("账号与监控快照不新鲜")
	}
	telemetryAt, telemetryError := c.telemetry.Status()
	if telemetryAt == nil || now.Sub(telemetryAt.UTC()) > telemetryMaxAge || telemetryError != "" {
		return fmt.Errorf("真实流量数据不新鲜")
	}

	sources, err := c.upstreams.List(ctx)
	if err != nil {
		return fmt.Errorf("读取上游状态: %w", err)
	}
	policies, err := c.store.ListGroupFailoverPolicies(ctx, 0)
	if err != nil {
		return fmt.Errorf("读取三级分组策略: %w", err)
	}
	if len(policies) == 0 {
		c.lastRunAt = now
		return nil
	}
	agentSettings, err := c.store.GetAgentSettings(ctx)
	if err != nil {
		return fmt.Errorf("读取智能体控制模式: %w", err)
	}
	dryRun := agentSettings.Mode != model.AgentModeControl

	sourceByID := make(map[int64]model.UpstreamSource, len(sources))
	sourcePoolByURL := make(map[string]string, len(sources))
	for _, source := range sources {
		sourceByID[source.ID] = source
		sourcePoolByURL[source.NormalizedURL] = normalizedPool(source.RoutingPool)
	}
	poolPolicies := make(map[string][]model.GroupFailoverPolicy)
	for _, policy := range policies {
		source, ok := sourceByID[policy.SourceID]
		if !ok {
			continue
		}
		pool := normalizedPool(firstNonEmpty(policy.Pool, source.RoutingPool))
		policy.Pool = pool
		poolPolicies[pool] = append(poolPolicies[pool], policy)
	}

	pools := make([]string, 0, len(poolPolicies))
	for pool := range poolPolicies {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	for _, pool := range pools {
		bindings := bindingsForPool(snapshot.Bindings, pool, poolPolicies[pool], sourcePoolByURL)
		assessment, assessErr := c.assessPool(ctx, bindings, now, settings)
		if assessErr != nil {
			return fmt.Errorf("评估倍率池 %s: %w", pool, assessErr)
		}
		if assessment.Outage {
			if c.outageSince[pool].IsZero() {
				c.outageSince[pool] = now
				c.record(ctx, model.Event{Type: "group_failover_outage_confirmed", Severity: "critical", Message: "调度池 " + pool + " 已确认没有可用渠道", Actor: "system", Details: assessment.Evidence})
				continue
			}
			// Give the minute-level emergency agent enough time to start and
			// finish one bounded model call. A late agent action is also rejected
			// by the pool transition lease in the deterministic executor.
			if now.Sub(c.outageSince[pool]) < maxDuration(2*c.interval, time.Duration(settings.FailoverAgentGraceSeconds)*time.Second) {
				continue
			}
			acted, handleErr := c.handleOutage(ctx, pool, poolPolicies[pool], sourceByID, bindings, assessment, now, dryRun, settings)
			if handleErr != nil {
				return handleErr
			}
			if acted {
				break
			}
			continue
		}
		delete(c.outageSince, pool)
		acted, recoverErr := c.handleRecovery(ctx, poolPolicies[pool], sourceByID, bindings, now, dryRun, settings)
		if recoverErr != nil {
			return recoverErr
		}
		if acted {
			break
		}
	}
	c.lastRunAt = now
	return nil
}

type poolAssessment struct {
	Outage     bool
	Evidence   string
	Samples    int
	Eligible   int
	Success    int
	HardErrors int
}

func (c *Controller) assessPool(ctx context.Context, bindings []model.ResolvedBinding, now time.Time, settings model.Settings) (poolAssessment, error) {
	result := poolAssessment{}
	if len(bindings) == 0 {
		return result, nil
	}
	hardThree, hardFive, allStopped := true, true, true
	for _, binding := range bindings {
		if binding.Account.Schedulable && binding.Account.Status == "active" {
			allStopped = false
		}
		hardThree = hardThree && hasDistinctMonitorHardStreak(binding, now, settings.FailoverMonitorFailures, time.Duration(settings.FailoverAccountFreshMinutes)*time.Minute)
		hardFive = hardFive && hasDistinctMonitorHardStreak(binding, now, settings.FailoverNoTrafficFailures, time.Duration(settings.FailoverAccountFreshMinutes)*time.Minute)

		windowLabel := fmt.Sprintf("%dm", settings.FailoverTrafficWindowMinutes)
		window, err := c.store.GetAgentWindowStats(ctx, binding.Account.ID, now.Add(-time.Duration(settings.FailoverTrafficWindowMinutes)*time.Minute), now.Add(time.Nanosecond), windowLabel)
		if err != nil {
			return result, err
		}
		result.Samples += window.SampleCount
		result.Eligible += window.EligibleCount
		result.Success += window.SuccessCount
		result.HardErrors += allowedHardErrors(binding, window)
	}
	if !hardThree {
		return result, nil
	}
	successRate := 0.0
	if result.Eligible > 0 {
		successRate = float64(result.Success) * 100 / float64(result.Eligible)
	}
	// A low success rate is actionable only when every failed eligible request
	// is an allowed hard error. Credential, unknown, client and generic model
	// capability errors must never turn a yellow/ambiguous pool into a failover.
	failedEligible := result.Eligible - result.Success
	rateHard := result.Eligible >= settings.FailoverTrafficMinSamples && successRate < float64(settings.FailoverTrafficSuccessBelow) && failedEligible > 0 && result.HardErrors >= failedEligible
	consecutiveHard, err := c.hasConsecutiveHardTraffic(ctx, bindings, now, settings)
	if err != nil {
		return result, err
	}
	trafficHard := allStopped && (rateHard || consecutiveHard)
	noTrafficFallback := result.Samples == 0 && hardFive && allStopped
	result.Outage = trafficHard || noTrafficFallback
	if result.Outage {
		evidence, _ := json.Marshal(map[string]any{
			"bindings": len(bindings), "traffic_window_minutes": settings.FailoverTrafficWindowMinutes,
			"traffic_samples": result.Samples, "eligible_requests": result.Eligible,
			"successful_requests": result.Success, "success_rate": successRate, "hard_errors": result.HardErrors,
			"consecutive_hard_traffic": consecutiveHard, "all_accounts_unschedulable": allStopped, "no_traffic_fallback": noTrafficFallback,
		})
		result.Evidence = string(evidence)
	}
	return result, nil
}

func hasDistinctMonitorHardStreak(binding model.ResolvedBinding, now time.Time, threshold int, maxAge time.Duration) bool {
	if binding.Monitor == nil || !binding.Monitor.Enabled || binding.Monitor.LastCheckedAt == nil || binding.MonitorState.LastCheckedAt == nil {
		return false
	}
	checkedAt := binding.Monitor.LastCheckedAt.UTC()
	stateCheckedAt := binding.MonitorState.LastCheckedAt.UTC()
	if checkedAt.After(now) || now.Sub(checkedAt) > maxAge || !checkedAt.Equal(stateCheckedAt) {
		return false
	}
	monitorHard := strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusFailed) || strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusError)
	stateHard := strings.EqualFold(binding.MonitorState.LastStatus, model.StatusFailed) || strings.EqualFold(binding.MonitorState.LastStatus, model.StatusError)
	return monitorHard && stateHard && binding.MonitorState.UnhealthyStreak >= threshold
}

func allowedHardErrors(binding model.ResolvedBinding, window model.AgentWindowStats) int {
	count := window.ErrorCategoryCounts[model.ErrorClassInfrastructure] + window.ErrorCategoryCounts[model.ErrorClassCapacity]
	// The telemetry adapter currently groups unsupported-model and no-channel
	// responses together. Count that class only when a future/explicit reason
	// code proves it is a no-channel failure; "model_unsupported" is purposely
	// not accepted.
	if hasExplicitNoChannelReason(binding.Decision) {
		count += window.ErrorCategoryCounts[model.ErrorClassModelCapability]
	}
	return count
}

func hasExplicitNoChannelReason(decision *model.HealthDecision) bool {
	if decision == nil {
		return false
	}
	for _, reason := range decision.ReasonCodes {
		normalized := strings.ToLower(strings.TrimSpace(reason))
		switch normalized {
		case "no_available_channel", "no_channel_available", "no_schedulable_channel", "无可用渠道", "没有可用渠道":
			return true
		}
	}
	return false
}

// hasConsecutiveHardTraffic proves that the configured number of newest
// traffic records are all allowed hard errors. GetAgentWindowStats exposes
// aggregates rather than rows, so a bounded binary search narrows the suffix
// to the latest group containing at least the configured record count. Timestamp ties are
// treated conservatively: any non-hard record in that suffix rejects it.
func (c *Controller) hasConsecutiveHardTraffic(ctx context.Context, bindings []model.ResolvedBinding, now time.Time, settings model.Settings) (bool, error) {
	required := settings.FailoverConsecutiveHardErrors
	start := now.Add(-time.Duration(settings.FailoverTrafficWindowMinutes) * time.Minute)
	best, err := c.aggregateTraffic(ctx, bindings, start, now.Add(time.Nanosecond), "hard_tail")
	if err != nil || best.samples < required || best.hard < required {
		return false, err
	}
	low, high := start, now
	for range 12 {
		mid := low.Add(high.Sub(low) / 2)
		candidate, queryErr := c.aggregateTraffic(ctx, bindings, mid, now.Add(time.Nanosecond), "hard_tail")
		if queryErr != nil {
			return false, queryErr
		}
		if candidate.samples >= required {
			low = mid
			best = candidate
		} else {
			high = mid
		}
	}
	return best.samples >= required && best.samples == best.hard, nil
}

type trafficAggregate struct {
	samples int
	hard    int
}

func (c *Controller) aggregateTraffic(ctx context.Context, bindings []model.ResolvedBinding, since, until time.Time, label string) (trafficAggregate, error) {
	result := trafficAggregate{}
	for _, binding := range bindings {
		window, err := c.store.GetAgentWindowStats(ctx, binding.Account.ID, since, until, label)
		if err != nil {
			return result, err
		}
		result.samples += window.SampleCount
		result.hard += allowedHardErrors(binding, window)
	}
	return result, nil
}

type transitionCandidate struct {
	policy   model.GroupFailoverPolicy
	source   model.UpstreamSource
	target   string
	rate     float64
	rollback bool
}

func (c *Controller) handleOutage(ctx context.Context, pool string, policies []model.GroupFailoverPolicy, sources map[int64]model.UpstreamSource, bindings []model.ResolvedBinding, assessment poolAssessment, now time.Time, dryRun bool, settings model.Settings) (bool, error) {
	candidates := make([]transitionCandidate, 0)
	activePolicy := activeOutagePolicy(policies, sources, now, time.Duration(settings.FailoverMainVerifyMinutes)*time.Minute)
	for _, policy := range policies {
		if activePolicy != "" && failoverPolicyKey(policy) != activePolicy {
			continue
		}
		source, ok := sources[policy.SourceID]
		if !ok || !policy.Enabled || !policy.Confirmed || policy.ConfirmedVersion != policy.Version || len(policy.AccountIDs) == 0 {
			continue
		}
		if !source.Enabled || source.CredentialMode != "password" || source.MigrationRequired || source.BalanceLocked || source.Balance == nil || *source.Balance < source.PauseBelow || source.LastSuccessAt == nil || now.Sub(source.LastSuccessAt.UTC()) > time.Duration(settings.FailoverGroupFreshMinutes)*time.Minute {
			continue
		}
		state := policy.State
		if state.Frozen || before(state.ManualHoldUntil, now) || before(state.ManualOverrideUntil, now) {
			continue
		}
		current := effectiveTier(policy, source)
		target, rollback := "", false
		if current == model.GroupTierMain && state.PreviousStableTier != "" && state.LastTransitionAt != nil && now.Sub(state.LastTransitionAt.UTC()) <= time.Duration(settings.FailoverMainVerifyMinutes)*time.Minute {
			target, rollback = state.PreviousStableTier, true
		} else {
			switch current {
			case model.GroupTierMain:
				target = model.GroupTierBackup
			case model.GroupTierBackup:
				started := firstTime(state.VerificationStartedAt, state.LastTransitionAt, state.LastSwitchAt)
				ready, verifyErr := c.postSwitchEvidenceReady(ctx, policy, bindings, started, now, settings)
				if verifyErr != nil {
					return false, verifyErr
				}
				if !ready {
					continue
				}
				target = model.GroupTierEmergency
			case model.GroupTierEmergency:
				started := firstTime(state.VerificationStartedAt, state.LastTransitionAt, state.LastSwitchAt)
				ready, verifyErr := c.postSwitchEvidenceReady(ctx, policy, bindings, started, now, settings)
				if verifyErr != nil {
					return false, verifyErr
				}
				if ready {
					state.Frozen = true
					state.FreezeReason = "主、备用和紧急分组均未恢复可用渠道"
					state.LastError = state.FreezeReason
					if err := c.store.SaveGroupFailoverState(ctx, state); err != nil {
						return false, err
					}
					delete(c.verification, failoverPolicyKey(policy))
					c.record(ctx, model.Event{Type: "group_failover_exhausted", Severity: "critical", Message: source.Name + " 的三级分组均未恢复服务，已冻结自动切换", Actor: "system", Details: assessment.Evidence})
				}
				continue
			default:
				state.Frozen = true
				state.FreezeReason = "当前令牌分组不属于已确认的三级策略"
				_ = c.store.SaveGroupFailoverState(ctx, state)
				continue
			}
		}
		if !rollback && before(state.CooldownUntil, now) && !(current == model.GroupTierBackup && target == model.GroupTierEmergency) {
			continue
		}
		count30, err := c.store.CountCompletedGroupTierTransitions(ctx, policy.SourceID, policy.KeyID, now.Add(-time.Duration(settings.FailoverShortLimitWindowMinutes)*time.Minute))
		if err != nil {
			return false, err
		}
		count6h, err := c.store.CountCompletedGroupTierTransitions(ctx, policy.SourceID, policy.KeyID, now.Add(-time.Duration(settings.FailoverLongLimitWindowMinutes)*time.Minute))
		if err != nil {
			return false, err
		}
		if !rollback && (count30 >= settings.FailoverShortLimitCount || count6h >= settings.FailoverLongLimitCount) {
			state.Frozen = true
			state.FreezeReason = "自动切换次数超过安全上限"
			_ = c.store.SaveGroupFailoverState(ctx, state)
			continue
		}
		groupID := groupForTier(policy, target)
		if groupID == "" || !sourceHasGroup(source, groupID) || !sourceHasKey(source, policy.KeyID) {
			continue
		}
		candidates = append(candidates, transitionCandidate{policy: policy, source: source, target: target, rate: groupRate(source, groupID), rollback: rollback})
	}
	if len(candidates) == 0 {
		return false, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].rollback != candidates[j].rollback {
			return candidates[i].rollback
		}
		if candidates[i].policy.State.CurrentTier != candidates[j].policy.State.CurrentTier {
			return candidates[i].policy.State.CurrentTier == model.GroupTierBackup
		}
		if candidates[i].rate != candidates[j].rate {
			return candidates[i].rate < candidates[j].rate
		}
		return candidates[i].source.ID < candidates[j].source.ID
	})
	candidate := candidates[0]
	reason := "全局断流，切换到" + tierLabel(candidate.target)
	trigger := "global_outage"
	if candidate.rollback {
		reason, trigger = "回主组试运行失败，恢复上一个稳定分组", "main_trial_rollback"
	}
	_, err := c.upstreams.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: candidate.policy.SourceID, KeyID: candidate.policy.KeyID, TargetTier: candidate.target,
		IdempotencyKey: fmt.Sprintf("fallback-%s-%d-%s-%s-%d", safeID(pool), candidate.policy.SourceID, safeID(candidate.policy.KeyID), candidate.target, now.Unix()/int64(c.interval.Seconds())),
		Actor:          "system:failover", Reason: reason, Evidence: assessment.Evidence, Trigger: trigger, DryRun: dryRun,
	})
	return true, err
}

// activeOutagePolicy locks one pool to its current rescue chain. A token in a
// backup/emergency verification stage (or a main trial awaiting rollback) must
// finish or freeze before another token can start at main -> backup.
func activeOutagePolicy(policies []model.GroupFailoverPolicy, sources map[int64]model.UpstreamSource, now time.Time, mainVerifyWindow time.Duration) string {
	type active struct {
		key string
		at  time.Time
	}
	items := make([]active, 0)
	for _, policy := range policies {
		if policy.State.Frozen || !policy.Enabled || !policy.Confirmed || policy.ConfirmedVersion != policy.Version {
			continue
		}
		tier := policy.State.CurrentTier
		if source, ok := sources[policy.SourceID]; ok {
			if observedTier := effectiveTier(policy, source); observedTier != "" {
				tier = observedTier
			}
		}
		isActive := tier == model.GroupTierBackup || tier == model.GroupTierEmergency
		if tier == model.GroupTierMain && policy.State.PreviousStableTier != "" && policy.State.LastTransitionAt != nil && now.Sub(policy.State.LastTransitionAt.UTC()) <= mainVerifyWindow {
			isActive = true
		}
		if !isActive {
			continue
		}
		at := time.Time{}
		if value := firstTime(policy.State.LastTransitionAt, policy.State.VerificationStartedAt, policy.State.LastSwitchAt); value != nil {
			at = value.UTC()
		}
		items = append(items, active{key: failoverPolicyKey(policy), at: at})
	}
	if len(items) == 0 {
		return ""
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.After(items[j].at)
		}
		return items[i].key < items[j].key
	})
	return items[0].key
}

func (c *Controller) postSwitchEvidenceReady(ctx context.Context, policy model.GroupFailoverPolicy, bindings []model.ResolvedBinding, started *time.Time, now time.Time, settings model.Settings) (bool, error) {
	if started == nil || started.After(now) {
		return false, nil
	}
	key := failoverPolicyKey(policy)
	tracker := c.verification[key]
	if tracker == nil || !tracker.startedAt.Equal(started.UTC()) {
		tracker = &verificationTracker{startedAt: started.UTC(), monitorTimes: make(map[int64]time.Time)}
		c.verification[key] = tracker
	}
	accountIDs := make(map[int64]bool, len(policy.AccountIDs))
	for _, accountID := range policy.AccountIDs {
		accountIDs[accountID] = true
	}
	trafficSamples := 0
	for _, binding := range bindings {
		if !accountIDs[binding.Account.ID] {
			continue
		}
		checkedAt := binding.MonitorState.LastCheckedAt
		if checkedAt == nil && binding.Monitor != nil {
			checkedAt = binding.Monitor.LastCheckedAt
		}
		if binding.Monitor != nil && checkedAt != nil {
			value := checkedAt.UTC()
			previous := tracker.monitorTimes[binding.Monitor.ID]
			if value.After(tracker.startedAt) && (previous.IsZero() || value.After(previous)) {
				tracker.monitorTimes[binding.Monitor.ID] = value
				tracker.monitorResults++
			}
		}
		window, err := c.store.GetAgentWindowStats(ctx, binding.Account.ID, tracker.startedAt, now.Add(time.Nanosecond), "post_group_switch")
		if err != nil {
			return false, err
		}
		trafficSamples += window.SampleCount
	}
	return now.Sub(tracker.startedAt) >= time.Duration(settings.FailoverBackupVerifyMinutes)*time.Minute &&
		tracker.monitorResults >= settings.FailoverPostSwitchMonitors && trafficSamples >= settings.FailoverPostSwitchRequests, nil
}

func failoverPolicyKey(policy model.GroupFailoverPolicy) string {
	return fmt.Sprintf("%d:%s", policy.SourceID, strings.TrimSpace(policy.KeyID))
}

func (c *Controller) handleRecovery(ctx context.Context, policies []model.GroupFailoverPolicy, sources map[int64]model.UpstreamSource, bindings []model.ResolvedBinding, now time.Time, dryRun bool, settings model.Settings) (bool, error) {
	bindingsByAccount := make(map[int64]model.ResolvedBinding, len(bindings))
	for _, binding := range bindings {
		bindingsByAccount[binding.Account.ID] = binding
	}
	for _, policy := range policies {
		source, ok := sources[policy.SourceID]
		if !ok || !policy.Enabled || !policy.Confirmed || policy.State.Frozen {
			continue
		}
		state := policy.State
		current := effectiveTier(policy, source)
		if current == model.GroupTierMain {
			delete(c.verification, failoverPolicyKey(policy))
			if state.PreviousStableTier != "" && state.LastTransitionAt != nil && now.Sub(state.LastTransitionAt.UTC()) > time.Duration(settings.FailoverMainVerifyMinutes)*time.Minute {
				state.PreviousStableTier = ""
				state.PreviousTier = ""
				state.PreviousGroupID = ""
				state.VerificationStartedAt = nil
				state.LastError = ""
				_ = c.store.SaveGroupFailoverState(ctx, state)
			}
			continue
		}
		if current != model.GroupTierBackup && current != model.GroupTierEmergency {
			continue
		}
		if before(state.ManualHoldUntil, now) || before(state.ManualOverrideUntil, now) || before(state.ReturnBlockedUntil, now) {
			continue
		}
		healthy, minStreak := true, int(^uint(0)>>1)
		eligible, success := 0, 0
		for _, accountID := range policy.AccountIDs {
			binding, exists := bindingsByAccount[accountID]
			if !exists || binding.Monitor == nil || binding.Monitor.LastCheckedAt == nil || now.Sub(binding.Monitor.LastCheckedAt.UTC()) > time.Duration(settings.FailoverAccountFreshMinutes)*time.Minute ||
				!strings.EqualFold(binding.Monitor.PrimaryStatus, model.StatusOperational) || binding.MonitorState.HealthyStreak < settings.FailoverRecoveryMonitorSuccesses {
				healthy = false
				break
			}
			if binding.MonitorState.HealthyStreak < minStreak {
				minStreak = binding.MonitorState.HealthyStreak
			}
			windowLabel := fmt.Sprintf("%dm", settings.FailoverRecoveryWindowMinutes)
			window, err := c.store.GetAgentWindowStats(ctx, accountID, now.Add(-time.Duration(settings.FailoverRecoveryWindowMinutes)*time.Minute), now.Add(time.Nanosecond), windowLabel)
			if err != nil {
				return false, err
			}
			eligible += window.EligibleCount
			success += window.SuccessCount
		}
		successRate := 0.0
		if eligible > 0 {
			successRate = float64(success) * 100 / float64(eligible)
		}
		healthy = healthy && eligible >= settings.FailoverRecoveryMinSamples && successRate >= float64(settings.FailoverRecoverySuccessAt)
		if !healthy {
			if state.HealthySince != nil || state.RecoveryHealthyCount != 0 {
				state.HealthySince = nil
				state.RecoveryHealthyCount = 0
				_ = c.store.SaveGroupFailoverState(ctx, state)
			}
			continue
		}
		if state.HealthySince == nil {
			state.HealthySince = timePtr(now)
			state.RecoveryHealthyCount = minStreak
			if err := c.store.SaveGroupFailoverState(ctx, state); err != nil {
				return false, err
			}
			continue
		}
		state.RecoveryHealthyCount = minStreak
		if now.Sub(state.HealthySince.UTC()) < time.Duration(settings.FailoverRecoveryStableMinutes)*time.Minute {
			_ = c.store.SaveGroupFailoverState(ctx, state)
			continue
		}
		evidence, _ := json.Marshal(map[string]any{"recovery_window_minutes": settings.FailoverRecoveryWindowMinutes, "eligible_requests": eligible, "success_rate": successRate, "healthy_monitor_streak": minStreak})
		_, err := c.upstreams.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
			SourceID: policy.SourceID, KeyID: policy.KeyID, TargetTier: model.GroupTierMain,
			IdempotencyKey: fmt.Sprintf("recover-%d-%s-%d", policy.SourceID, safeID(policy.KeyID), now.Unix()/int64(c.interval.Seconds())),
			Actor:          "system:failover", Reason: fmt.Sprintf("系统稳定%d分钟，直接试回主分组", settings.FailoverRecoveryStableMinutes), Evidence: string(evidence), Trigger: "stable_return_main", DryRun: dryRun,
		})
		if err != nil {
			state.ReturnBlockedUntil = timePtr(now.Add(time.Duration(settings.FailoverReturnRetryMinutes) * time.Minute))
			state.LastError = err.Error()
			_ = c.store.SaveGroupFailoverState(ctx, state)
		}
		return true, err
	}
	return false, nil
}

func bindingsForPool(bindings []model.ResolvedBinding, pool string, policies []model.GroupFailoverPolicy, sourcePoolByURL map[string]string) []model.ResolvedBinding {
	accountIDs := make(map[int64]bool)
	for _, policy := range policies {
		for _, accountID := range policy.AccountIDs {
			accountIDs[accountID] = true
		}
	}
	result := make([]model.ResolvedBinding, 0)
	for _, binding := range bindings {
		bindingPool := normalizedPool(sourcePoolByURL[binding.NormalizedEndpoint])
		if accountIDs[binding.Account.ID] || bindingPool == pool {
			result = append(result, binding)
		}
	}
	return result
}

func effectiveTier(policy model.GroupFailoverPolicy, source model.UpstreamSource) string {
	groupID := policy.State.ObservedGroupID
	for _, rate := range source.KeyRates {
		if rate.ExternalID == policy.KeyID {
			groupID = rate.GroupID
			break
		}
	}
	switch groupID {
	case policy.MainGroupID:
		return model.GroupTierMain
	case policy.BackupGroupID:
		return model.GroupTierBackup
	case policy.EmergencyGroupID:
		return model.GroupTierEmergency
	default:
		return ""
	}
}

func groupForTier(policy model.GroupFailoverPolicy, tier string) string {
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

func sourceHasKey(source model.UpstreamSource, keyID string) bool {
	for _, rate := range source.KeyRates {
		if rate.ExternalID == keyID && keyRateActive(rate.Status) {
			return true
		}
	}
	return false
}

func sourceHasGroup(source model.UpstreamSource, groupID string) bool {
	for _, group := range source.Groups {
		if group.ExternalID == groupID {
			return true
		}
	}
	return false
}

func groupRate(source model.UpstreamSource, groupID string) float64 {
	for _, group := range source.Groups {
		if group.ExternalID == groupID {
			return group.RateMultiplier
		}
	}
	return 1e9
}

func keyRateActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "1", "active", "enabled", "normal":
		return true
	default:
		return false
	}
}

func normalizedPool(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "默认调度池"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func before(value *time.Time, now time.Time) bool {
	return value != nil && now.Before(value.UTC())
}

func timePtr(value time.Time) *time.Time { return &value }

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func safeID(value string) string {
	value = strings.TrimSpace(value)
	value = strings.NewReplacer("/", "_", "\\", "_", " ", "_").Replace(value)
	if len(value) > 48 {
		value = value[:48]
	}
	return value
}

func tierLabel(tier string) string {
	switch tier {
	case model.GroupTierMain:
		return "主分组"
	case model.GroupTierBackup:
		return "备用分组"
	case model.GroupTierEmergency:
		return "紧急分组"
	default:
		return tier
	}
}

func (c *Controller) record(ctx context.Context, event model.Event) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = c.now().UTC()
	}
	if err := c.store.AddEvent(ctx, event); err != nil {
		c.logger.Error("group_failover_event_failed", "type", event.Type, "error", err)
	}
}

func (c *Controller) runLogged(ctx context.Context) {
	err := c.RunOnce(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		message := err.Error()
		changed := message != c.lastError
		c.lastError = message
		if changed {
			c.logger.Error("group_failover_cycle_failed", "error", err)
			_ = c.store.AddEvent(context.Background(), model.Event{Type: "group_failover_cycle_failed", Severity: "error", Message: message, Actor: "system", CreatedAt: c.now().UTC()})
		}
		return
	}
	if c.lastError != "" {
		c.lastError = ""
		_ = c.store.AddEvent(context.Background(), model.Event{Type: "group_failover_cycle_recovered", Severity: "info", Message: "三级分组救灾判定已恢复", Actor: "system", CreatedAt: c.now().UTC()})
	}
}
