package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type Store interface {
	ListGroupFailoverPolicies(context.Context, int64) ([]model.GroupFailoverPolicy, error)
	SaveGroupFailoverState(context.Context, model.GroupFailoverState) error
	CompareAndSaveGroupFailoverState(context.Context, model.GroupFailoverState, model.GroupFailoverState) (bool, error)
	CountCompletedGroupTierTransitions(context.Context, int64, string, time.Time) (int, error)
	GetAgentWindowStats(context.Context, int64, time.Time, time.Time, string) (model.AgentWindowStats, error)
	GetSettings(context.Context) (model.Settings, error)
	AddEvent(context.Context, model.Event) error
}

type evidenceStore interface {
	ListGroupValidationEvidence(context.Context, []int64, []int64, int64, int64) ([]model.GroupValidationEvidence, error)
}

type PostSwitchProbe interface {
	Probe(context.Context, PostSwitchProbeRequest) (PostSwitchProbeResult, error)
}

type PostSwitchProbeRequest struct {
	Policy           model.GroupFailoverPolicy
	TransitionID     int64
	SourceID         int64
	KeyID            string
	TargetTier       string
	TargetGroupID    string
	RequestStartedAt time.Time
}

type PostSwitchProbeResult struct {
	TransitionID     int64
	SourceID         int64
	KeyID            string
	TargetTier       string
	TargetGroupID    string
	RequestStartedAt time.Time
	Status           string
	EvidenceID       string
	ObservedAt       time.Time
	ReasonCode       string
}

type Option func(*Controller)

func WithPostSwitchProbe(probe PostSwitchProbe) Option {
	return func(controller *Controller) { controller.probe = probe }
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
	probe     PostSwitchProbe
	wake      chan struct{}

	mu          sync.Mutex
	running     atomic.Bool
	evidenceRun atomic.Bool
	lastRunAt   time.Time
	lastError   string
	outageSince map[string]time.Time
	now         func() time.Time
}

func NewController(store Store, snapshots SnapshotProvider, upstreams UpstreamManager, telemetry TelemetryStatus, interval time.Duration, logger *slog.Logger, options ...Option) *Controller {
	if interval < 10*time.Second {
		interval = 50 * time.Second
	}
	controller := &Controller{
		store: store, snapshots: snapshots, upstreams: upstreams, telemetry: telemetry,
		interval: interval, logger: logger, outageSince: make(map[string]time.Time), wake: make(chan struct{}, 1), now: func() time.Time { return time.Now().UTC() },
	}
	for _, option := range options {
		if option != nil {
			option(controller)
		}
	}
	return controller
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
			case <-c.wake:
				c.runLogged(ctx)
			case <-ticker.C:
				c.runLogged(ctx)
			}
		}
	}()
}

func (c *Controller) Wake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
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
	if !c.running.CompareAndSwap(false, true) {
		return nil
	}
	defer c.running.Store(false)

	if _, ok := c.store.(evidenceStore); ok {
		if err := c.ProcessFailoverEvidence(ctx); err != nil {
			return fmt.Errorf("处理切换后证据: %w", err)
		}
	}
	now := c.now().UTC()
	snapshot := c.snapshots.Snapshot()
	if snapshot.Freeze.AllAutomation {
		c.setLastRunAt(now)
		return nil
	}
	settings, err := c.store.GetSettings(ctx)
	if err != nil {
		return fmt.Errorf("读取救灾策略: %w", err)
	}
	if settings.FailoverMode == model.FailoverModeDisabled {
		c.setLastRunAt(now)
		return nil
	}
	accountSnapshotMaxAge := time.Duration(settings.FailoverAccountFreshMinutes) * time.Minute
	telemetryMaxAge := time.Duration(settings.FailoverTelemetryFreshMinutes) * time.Minute
	if snapshot.LastSyncAt == nil || now.Sub(snapshot.LastSyncAt.UTC()) > accountSnapshotMaxAge || snapshot.LastSyncError != "" {
		return fmt.Errorf("data_stale: 账号与监控快照不新鲜")
	}
	telemetryAt, telemetryError := c.telemetry.Status()
	if telemetryAt == nil || now.Sub(telemetryAt.UTC()) > telemetryMaxAge || telemetryError != "" {
		return fmt.Errorf("data_stale: 真实流量数据不新鲜")
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
		c.setLastRunAt(now)
		return nil
	}
	dryRun := settings.FailoverMode != model.FailoverModeControl
	mutationBudget := settings.FailoverMutationBudget
	if mutationBudget < 1 {
		mutationBudget = 1
	}
	mutations := 0
	var cycleErrors []error

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
			cycleErrors = append(cycleErrors, fmt.Errorf("评估倍率池 %s: %w", pool, assessErr))
			c.record(ctx, model.Event{Type: "group_failover_pool_assessment_failed", Severity: "error", Message: "调度池救灾评估失败，已继续处理其他池", Actor: "system", Details: fmt.Sprintf(`{"pool":%q,"reason_code":"pool_assessment_failed"}`, pool)})
			continue
		}
		confirmedFailure := hasConfirmedGroupFailure(poolPolicies[pool])
		if assessment.Outage || confirmedFailure {
			if confirmedFailure {
				delete(c.outageSince, pool)
			} else if c.outageSince[pool].IsZero() {
				c.outageSince[pool] = now
				c.record(ctx, model.Event{Type: "group_failover_outage_confirmed", Severity: "critical", Message: "调度池 " + pool + " 已确认没有可用渠道", Actor: "system", Details: assessment.Evidence})
				continue
			}
			// Give the minute-level emergency agent enough time to start and
			// finish one bounded model call. A late agent action is also rejected
			// by the pool transition lease in the deterministic executor.
			if !confirmedFailure && now.Sub(c.outageSince[pool]) < maxDuration(2*c.interval, time.Duration(settings.FailoverAgentGraceSeconds)*time.Second) {
				continue
			}
			budgetAvailable := mutations < mutationBudget
			acted, handleErr := c.handleOutage(ctx, pool, poolPolicies[pool], sourceByID, assessment, now, dryRun || !budgetAvailable, settings)
			if handleErr != nil {
				cycleErrors = append(cycleErrors, fmt.Errorf("处理倍率池 %s 断流: %w", pool, handleErr))
				continue
			}
			if acted {
				if budgetAvailable && !dryRun {
					mutations++
				} else if !dryRun {
					c.record(ctx, model.Event{Type: "group_failover_mutation_deferred", Severity: "warning", Message: "本轮分组写入预算已用尽，候选动作已延后", Actor: "system", Details: fmt.Sprintf(`{"pool":%q,"reason_code":"mutation_budget_exhausted","budget":%d}`, pool, mutationBudget)})
				}
			}
			continue
		}
		delete(c.outageSince, pool)
	}
	c.setLastRunAt(now)
	return errors.Join(cycleErrors...)
}

func hasConfirmedGroupFailure(policies []model.GroupFailoverPolicy) bool {
	for _, policy := range policies {
		if policy.State.ValidationStatus == model.GroupValidationConfirmedFailed {
			return true
		}
	}
	return false
}

func (c *Controller) setLastRunAt(value time.Time) {
	c.mu.Lock()
	c.lastRunAt = value
	c.mu.Unlock()
}

type poolAssessment struct {
	Outage     bool
	ReasonCode string
	Evidence   string
	Samples    int
	Eligible   int
	Success    int
	HardErrors int
}

func (c *Controller) assessPool(ctx context.Context, bindings []model.ResolvedBinding, now time.Time, settings model.Settings) (poolAssessment, error) {
	result := poolAssessment{}
	if len(bindings) == 0 {
		result.ReasonCode = "group_empty"
		return result, nil
	}
	hardThree, hardFive, allStopped, allDisabled := true, true, true, true
	for _, binding := range bindings {
		channelActive := strings.EqualFold(strings.TrimSpace(binding.Account.Status), "active")
		if binding.Account.Schedulable && channelActive {
			allStopped = false
		}
		if channelActive {
			allDisabled = false
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
	if allDisabled {
		result.Outage = true
		result.ReasonCode = "all_channels_disabled"
		return result, nil
	}
	if !hardThree {
		if allStopped {
			result.Outage = true
			result.ReasonCode = "no_schedulable_channels"
		} else {
			result.ReasonCode = "evidence_insufficient"
		}
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
	result.Outage = trafficHard || allStopped
	if trafficHard {
		result.ReasonCode = "all_channels_failed"
	} else if allStopped {
		result.ReasonCode = "no_schedulable_channels"
	} else {
		result.ReasonCode = "evidence_insufficient"
	}
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
		tail, queryErr := c.aggregateTraffic(ctx, bindings, mid, now.Add(time.Nanosecond), "hard_tail")
		if queryErr != nil {
			return false, queryErr
		}
		if tail.samples >= required {
			low = mid
			best = tail
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

func (c *Controller) handleOutage(ctx context.Context, pool string, policies []model.GroupFailoverPolicy, sources map[int64]model.UpstreamSource, assessment poolAssessment, now time.Time, dryRun bool, settings model.Settings) (bool, error) {
	activePolicy := activeFixedPolicy(policies, sources)
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
		switch state.ValidationStatus {
		case model.GroupValidationTransitioning, model.GroupValidationAwaitingEvidence, model.GroupValidationProbing, model.GroupValidationUncertain, model.GroupValidationExhausted:
			continue
		}
		if state.ValidationStatus != model.GroupValidationConfirmedFailed {
			if !assessment.Outage {
				continue
			}
			state.ValidationStatus = model.GroupValidationConfirmedFailed
			state.LastError = firstNonEmpty(assessment.ReasonCode, "current_level_confirmed_failed")
			if err := c.store.SaveGroupFailoverState(ctx, state); err != nil {
				return false, err
			}
		}
		target, skipped := nextEnabledTier(policy, current)
		if target == "" {
			state.ValidationStatus = model.GroupValidationExhausted
			state.Frozen = true
			state.FreezeReason = "fixed_failover_levels_exhausted"
			state.LastError = state.FreezeReason
			if err := c.store.SaveGroupFailoverState(ctx, state); err != nil {
				return false, err
			}
			details, _ := json.Marshal(map[string]any{"source_id": policy.SourceID, "key_id": policy.KeyID, "current_level": current, "reason_code": "fixed_failover_levels_exhausted", "skipped_levels": skipped})
			c.record(ctx, model.Event{Type: "group_failover_exhausted", Severity: "critical", Message: source.Name + " 的已配置固定救灾层级均已确认失败", Actor: "system", Details: string(details)})
			return false, nil
		}
		if before(state.CooldownUntil, now) && !(current == model.GroupTierBackup && target == model.GroupTierEmergency) {
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
		if count30 >= settings.FailoverShortLimitCount || count6h >= settings.FailoverLongLimitCount {
			state.Frozen = true
			state.FreezeReason = "自动切换次数超过安全上限"
			_ = c.store.SaveGroupFailoverState(ctx, state)
			continue
		}
		groupID := groupForTier(policy, target)
		if groupID == "" || !sourceHasGroup(source, groupID) || !sourceHasKey(source, policy.KeyID) {
			state.ValidationStatus = model.GroupValidationUncertain
			state.LastError = "configuration_invalid"
			_ = c.store.SaveGroupFailoverState(ctx, state)
			return false, errors.New("固定下一级分组配置无效")
		}
		details, _ := json.Marshal(map[string]any{"assessment": assessment.Evidence, "reason_code": "fixed_next_level", "skipped_levels": skipped})
		_, err = c.upstreams.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
			SourceID: policy.SourceID, KeyID: policy.KeyID, TargetTier: target,
			IdempotencyKey: fmt.Sprintf("fallback-%s-%d-%s-%s-%d", safeID(pool), policy.SourceID, safeID(policy.KeyID), target, now.Unix()/int64(c.interval.Seconds())),
			Actor:          "system:failover", Producer: "deterministic_failover", Authority: "deterministic_safety", Reason: "当前固定层级已确认失败，切换到" + tierLabel(target), Evidence: string(details), Trigger: "fixed_next_level", DryRun: dryRun,
		})
		return true, err
	}
	return false, nil
}

func activeFixedPolicy(policies []model.GroupFailoverPolicy, sources map[int64]model.UpstreamSource) string {
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
		isActive := tier == model.GroupTierBackup || tier == model.GroupTierEmergency || policy.State.ValidationStatus == model.GroupValidationConfirmedFailed || policy.State.ValidationStatus == model.GroupValidationAwaitingEvidence || policy.State.ValidationStatus == model.GroupValidationTransitioning || policy.State.ValidationStatus == model.GroupValidationProbing || policy.State.ValidationStatus == model.GroupValidationUncertain
		if !isActive {
			continue
		}
		return failoverPolicyKey(policy)
	}
	return ""
}

func nextEnabledTier(policy model.GroupFailoverPolicy, current string) (string, []string) {
	tiers := []string{model.GroupTierMain, model.GroupTierBackup, model.GroupTierEmergency}
	currentIndex := -1
	for index, tier := range tiers {
		if tier == current {
			currentIndex = index
			break
		}
	}
	if currentIndex < 0 {
		return "", nil
	}
	skipped := make([]string, 0, 2)
	for _, tier := range tiers[currentIndex+1:] {
		if groupTierEnabled(policy, tier) && strings.TrimSpace(groupForTier(policy, tier)) != "" {
			return tier, skipped
		}
		skipped = append(skipped, tier)
	}
	return "", skipped
}

func groupTierEnabled(policy model.GroupFailoverPolicy, tier string) bool {
	if !policy.MainEnabled && !policy.BackupEnabled && !policy.EmergencyEnabled {
		return strings.TrimSpace(groupForTier(policy, tier)) != ""
	}
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

// ProcessFailoverEvidence consumes only committed telemetry rows beyond the
// transition's persisted watermarks. It never performs a group mutation.
func (c *Controller) ProcessFailoverEvidence(ctx context.Context) error {
	if !c.evidenceRun.CompareAndSwap(false, true) {
		return nil
	}
	defer c.evidenceRun.Store(false)

	evidenceReader, ok := c.store.(evidenceStore)
	if !ok {
		return errors.New("failover evidence store is unavailable")
	}
	settings, err := c.store.GetSettings(ctx)
	if err != nil {
		return err
	}
	policies, err := c.store.ListGroupFailoverPolicies(ctx, 0)
	if err != nil {
		return err
	}
	snapshot := c.snapshots.Snapshot()
	now := c.now().UTC()
	var processErrors []error
	for _, policy := range policies {
		expected := policy.State
		state := expected
		if state.SwitchVerifiedAt == nil || state.ValidationNotBefore == nil {
			continue
		}
		switch state.ValidationStatus {
		case model.GroupValidationAwaitingEvidence, model.GroupValidationProbing, model.GroupValidationUncertain:
		default:
			continue
		}
		monitorIDs := monitorIDsForAccounts(snapshot.Bindings, policy.AccountIDs)
		items, readErr := evidenceReader.ListGroupValidationEvidence(ctx, monitorIDs, policy.AccountIDs, state.MonitorEvidenceCursor, state.TrafficEvidenceCursor)
		if readErr != nil {
			processErrors = append(processErrors, fmt.Errorf("读取 %s 切换后证据: %w", failoverPolicyKey(policy), readErr))
			continue
		}
		changed := false
		for _, item := range items {
			if item.Source == "monitor" && item.ID > state.MonitorEvidenceCursor {
				state.MonitorEvidenceCursor = item.ID
				changed = true
			}
			if item.Source == "traffic" && item.ID > state.TrafficEvidenceCursor {
				state.TrafficEvidenceCursor = item.ID
				changed = true
			}
			if !evidenceMatchesValidation(item, state) || item.ObservedAt.IsZero() || item.ObservedAt.After(now) || now.Sub(item.ObservedAt) > time.Duration(settings.FailoverAccountFreshMinutes)*time.Minute {
				continue
			}
			occurrenceAt, attributable := evidenceOccurrenceAt(item, state)
			if !attributable || occurrenceAt.Before(state.ValidationNotBefore.UTC()) || occurrenceAt.After(now) {
				continue
			}
			result := classifyPostSwitchEvidence(item)
			if result == "" {
				continue
			}
			state.LastEvidenceID = fmt.Sprintf("%s:%d", item.Source, item.ID)
			state.LastEvidenceSource = item.Source
			state.LastEvidenceReason = firstNonEmpty(item.ReasonCode, item.ErrorClass, item.Status)
			observedAt := item.ObservedAt.UTC()
			state.LastEvidenceAt = &observedAt
			changed = true
			if result == "success" {
				state.SuccessfulEvidenceCount++
				if state.SuccessfulEvidenceCount >= model.GroupValidationMinimumSuccesses {
					state.ValidationStatus = model.GroupValidationConfirmedHealthy
					state.LastConfirmedAt = &observedAt
					state.LastError = ""
				}
				continue
			}
			state.FailedEvidenceCount++
			if state.FailedEvidenceCount >= settings.FailoverMonitorFailures {
				state.ValidationStatus = model.GroupValidationConfirmedFailed
				state.LastError = "post_switch_failure_threshold"
			}
		}
		if state.ValidationStatus != model.GroupValidationConfirmedHealthy && state.ValidationStatus != model.GroupValidationConfirmedFailed && state.EvidenceDeadline != nil && !now.Before(state.EvidenceDeadline.UTC()) {
			state.ValidationStatus = model.GroupValidationUncertain
			state.LastError = "evidence_timeout"
			changed = true
		}
		if !changed {
			continue
		}
		saved, saveErr := c.store.CompareAndSaveGroupFailoverState(ctx, expected, state)
		if saveErr != nil {
			processErrors = append(processErrors, fmt.Errorf("保存 %s 验证状态: %w", failoverPolicyKey(policy), saveErr))
			continue
		}
		if !saved {
			continue
		}
		switch state.ValidationStatus {
		case model.GroupValidationConfirmedHealthy:
			c.record(ctx, model.Event{Type: "group_failover_validation_healthy", Severity: "info", Message: "固定层级已由切换后新证据确认可用", Actor: "system:telemetry", Details: validationDetails(state)})
		case model.GroupValidationConfirmedFailed:
			c.record(ctx, model.Event{Type: "group_failover_validation_failed", Severity: "critical", Message: "固定层级已由切换后新证据确认失败", Actor: "system:telemetry", Details: validationDetails(state)})
			c.Wake()
		case model.GroupValidationUncertain:
			c.record(ctx, model.Event{Type: "group_failover_validation_uncertain", Severity: "warning", Message: "切换后证据超时或归属不明确，未自动推进", Actor: "system:telemetry", Details: validationDetails(state)})
		}
	}
	return errors.Join(processErrors...)
}

func evidenceMatchesValidation(item model.GroupValidationEvidence, state model.GroupFailoverState) bool {
	if item.TransitionID > 0 && item.TransitionID != state.ValidationTransitionID {
		return false
	}
	if item.SourceID > 0 && item.SourceID != state.SourceID {
		return false
	}
	if keyID := strings.TrimSpace(item.KeyID); keyID != "" && keyID != strings.TrimSpace(state.KeyID) {
		return false
	}
	if tier := strings.TrimSpace(item.TargetTier); tier != "" && tier != state.ValidationTargetTier {
		return false
	}
	if groupID := strings.TrimSpace(item.TargetGroupID); groupID != "" && groupID != state.ValidationTargetGroupID {
		return false
	}
	return true
}

func evidenceOccurrenceAt(item model.GroupValidationEvidence, state model.GroupFailoverState) (time.Time, bool) {
	if item.RequestStartedAt != nil && !item.RequestStartedAt.IsZero() {
		return item.RequestStartedAt.UTC(), true
	}
	switch item.TimeBasis {
	case model.EvidenceTimeBasisMonitorRequestStart, model.EvidenceTimeBasisRequestStart:
		if item.ObservedAt.IsZero() {
			return time.Time{}, false
		}
		return item.ObservedAt.UTC(), true
	case model.EvidenceTimeBasisCompletion:
		if item.Source != "monitor" || state.SwitchVerifiedAt == nil || item.ObservedAt.IsZero() {
			return time.Time{}, false
		}
		safeBoundary := state.SwitchVerifiedAt.UTC().Add(model.GroupValidationPropagationDelay + model.GroupValidationMonitorRequestTimeout)
		if item.ObservedAt.Before(safeBoundary) {
			return time.Time{}, false
		}
		return item.ObservedAt.UTC(), true
	default:
		return time.Time{}, false
	}
}

func monitorIDsForAccounts(bindings []model.ResolvedBinding, accountIDs []int64) []int64 {
	accounts := make(map[int64]bool, len(accountIDs))
	for _, accountID := range accountIDs {
		accounts[accountID] = true
	}
	seen := make(map[int64]bool)
	result := make([]int64, 0)
	for _, binding := range bindings {
		if !accounts[binding.Account.ID] || binding.Monitor == nil || binding.Monitor.ID <= 0 || seen[binding.Monitor.ID] {
			continue
		}
		seen[binding.Monitor.ID] = true
		result = append(result, binding.Monitor.ID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func classifyPostSwitchEvidence(item model.GroupValidationEvidence) string {
	if item.Source == "monitor" {
		switch strings.ToLower(strings.TrimSpace(item.Status)) {
		case model.StatusOperational:
			return "success"
		case model.StatusFailed, model.StatusError:
			return "failed"
		default:
			return ""
		}
	}
	if strings.EqualFold(item.Status, "success") {
		return "success"
	}
	if !strings.EqualFold(item.Status, "error") {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(item.ErrorClass)) {
	case model.ErrorClassInfrastructure, model.ErrorClassCapacity:
		return "failed"
	default:
		return ""
	}
}

func validationDetails(state model.GroupFailoverState) string {
	details, _ := json.Marshal(map[string]any{
		"transition_id": state.ValidationTransitionID, "source_id": state.SourceID, "key_id": state.KeyID,
		"target_level": state.ValidationTargetTier, "target_group_id": state.ValidationTargetGroupID,
		"validation_status": state.ValidationStatus, "successful_evidence": state.SuccessfulEvidenceCount,
		"failed_evidence": state.FailedEvidenceCount, "last_evidence_id": state.LastEvidenceID,
		"last_evidence_source": state.LastEvidenceSource, "reason_code": state.LastError,
	})
	return string(details)
}

// RunPostSwitchProbe runs an optional, bounded probe only after readback has
// confirmed the target group. Production defaults to passive validation.
func (c *Controller) RunPostSwitchProbe(ctx context.Context, sourceID int64, keyID string) error {
	if c.probe == nil {
		return errors.New("post-switch active probe is not configured")
	}
	policies, err := c.store.ListGroupFailoverPolicies(ctx, sourceID)
	if err != nil {
		return err
	}
	settings, err := c.store.GetSettings(ctx)
	if err != nil {
		return err
	}
	failureThreshold := settings.FailoverMonitorFailures
	if failureThreshold < 2 {
		failureThreshold = 2
	}
	for _, policy := range policies {
		if policy.KeyID != strings.TrimSpace(keyID) {
			continue
		}
		expected := policy.State
		state := expected
		if state.SwitchVerifiedAt == nil || state.ValidationStatus != model.GroupValidationAwaitingEvidence {
			return errors.New("active probe requires an applied transition awaiting evidence")
		}
		requestStartedAt := c.now().UTC()
		if !requestStartedAt.After(state.SwitchVerifiedAt.UTC()) || state.ValidationNotBefore == nil || requestStartedAt.Before(state.ValidationNotBefore.UTC()) {
			return errors.New("active probe cannot start before the post-switch validation boundary")
		}
		state.ValidationStatus = model.GroupValidationProbing
		state.ValidationMode = model.GroupValidationModeActive
		state.ActiveProbeAttempts++
		saved, err := c.store.CompareAndSaveGroupFailoverState(ctx, expected, state)
		if err != nil {
			return err
		}
		if !saved {
			return errors.New("active probe transition was superseded before start")
		}
		request := PostSwitchProbeRequest{
			Policy: policy, TransitionID: state.ValidationTransitionID, SourceID: state.SourceID, KeyID: strings.TrimSpace(state.KeyID),
			TargetTier: state.ValidationTargetTier, TargetGroupID: state.ValidationTargetGroupID, RequestStartedAt: requestStartedAt,
		}
		result, probeErr := c.probe.Probe(ctx, request)
		now := c.now().UTC()
		if result.ObservedAt.IsZero() {
			result.ObservedAt = now
		}
		if probeErr == nil && !postSwitchProbeResultMatches(request, result) {
			return c.rejectActiveProbeResult(ctx, state, "active_probe_binding_mismatch")
		}
		if result.ObservedAt.Before(requestStartedAt) {
			return c.rejectActiveProbeResult(ctx, state, "active_probe_time_invalid")
		}
		expected = state
		state.LastEvidenceID = fmt.Sprintf("active_probe:%d:%s", request.TransitionID, strings.TrimSpace(result.EvidenceID))
		state.LastEvidenceSource = "active_probe"
		state.LastEvidenceReason = result.ReasonCode
		state.LastEvidenceAt = &result.ObservedAt
		if probeErr != nil || strings.EqualFold(result.Status, model.GroupValidationUncertain) {
			state.ValidationStatus = model.GroupValidationUncertain
			state.LastError = "active_probe_uncertain"
		} else if strings.EqualFold(result.Status, "success") || strings.EqualFold(result.Status, model.StatusOperational) {
			state.SuccessfulEvidenceCount++
			state.ValidationStatus = model.GroupValidationConfirmedHealthy
			state.LastConfirmedAt = &result.ObservedAt
			state.LastError = ""
		} else {
			state.FailedEvidenceCount++
			if state.FailedEvidenceCount >= failureThreshold {
				state.ValidationStatus = model.GroupValidationConfirmedFailed
				c.Wake()
			} else {
				state.ValidationStatus = model.GroupValidationAwaitingEvidence
			}
			state.LastError = "active_probe_failed"
		}
		saved, err = c.store.CompareAndSaveGroupFailoverState(ctx, expected, state)
		if err != nil {
			return err
		}
		if !saved {
			return errors.New("active probe result belongs to a superseded transition")
		}
		return nil
	}
	return errors.New("fixed failover policy not found")
}

func (c *Controller) rejectActiveProbeResult(ctx context.Context, probing model.GroupFailoverState, reason string) error {
	next := probing
	next.ValidationStatus = model.GroupValidationUncertain
	next.LastError = reason
	saved, err := c.store.CompareAndSaveGroupFailoverState(ctx, probing, next)
	if err != nil {
		return err
	}
	if !saved {
		return errors.New("active probe result belongs to a superseded transition")
	}
	return errors.New(reason)
}

func postSwitchProbeResultMatches(request PostSwitchProbeRequest, result PostSwitchProbeResult) bool {
	return result.TransitionID == request.TransitionID && result.SourceID == request.SourceID &&
		strings.TrimSpace(result.KeyID) == request.KeyID && result.TargetTier == request.TargetTier &&
		result.TargetGroupID == request.TargetGroupID && !result.RequestStartedAt.IsZero() &&
		result.RequestStartedAt.UTC().Equal(request.RequestStartedAt)
}

func failoverPolicyKey(policy model.GroupFailoverPolicy) string {
	return fmt.Sprintf("%d:%s", policy.SourceID, strings.TrimSpace(policy.KeyID))
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
	if err != nil {
		message := err.Error()
		changed := message != c.lastError
		c.lastError = message
		c.mu.Unlock()
		if changed {
			c.logger.Error("group_failover_cycle_failed", "error", err)
			_ = c.store.AddEvent(context.Background(), model.Event{Type: "group_failover_cycle_failed", Severity: "error", Message: message, Actor: "system", CreatedAt: c.now().UTC()})
		}
		return
	}
	recovered := c.lastError != ""
	if recovered {
		c.lastError = ""
	}
	c.mu.Unlock()
	if recovered {
		_ = c.store.AddEvent(context.Background(), model.Event{Type: "group_failover_cycle_recovered", Severity: "info", Message: "三级分组救灾判定已恢复", Actor: "system", CreatedAt: c.now().UTC()})
	}
}
