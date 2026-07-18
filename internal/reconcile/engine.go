package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/automation"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/health"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const maxLaggingV3RecoveryAge = 3 * time.Minute

type Sub2API interface {
	ListMonitors(context.Context) ([]model.Monitor, error)
	ListAccounts(context.Context) ([]model.Account, error)
	SetSchedulable(context.Context, int64, bool) (model.Account, error)
	UpdateLoadFactor(context.Context, int64, *int) (model.Account, error)
}

type EngineOption func(*Engine)

type Repository interface {
	accountcontrol.Repository
	GetSettings(context.Context) (model.Settings, error)
	UpdateSettings(context.Context, model.Settings) error
	GetAgentFreezeState(context.Context, string, string) (model.AgentFreezeState, error)
	SetAgentFreezeState(context.Context, *model.AgentFreezeState) error
	ListPolicies(context.Context) (map[int64]model.Policy, error)
	UpsertPolicy(context.Context, model.Policy) error
	GetMonitorState(context.Context, int64) (model.MonitorState, error)
	UpsertMonitorState(context.Context, model.MonitorState) error
	InsertMonitorObservation(context.Context, model.MonitorObservation) (bool, error)
	ListMonitorObservations(context.Context, int64, time.Time, int) ([]model.MonitorObservation, error)
	ListMonitorHistory(context.Context, int64, string, int) ([]model.MonitorHistoryRecord, error)
	GetTrafficWindow(context.Context, int64, time.Time, time.Time) (model.TrafficWindow, error)
	ListAccountModelCapabilities(context.Context, int64) ([]model.AccountModelCapability, error)
	CommitDecisionSnapshot(context.Context, model.DecisionSnapshot, *model.Event) (bool, error)
	GetMonitorHealthState(context.Context, int64) (model.MonitorHealthState, error)
	UpsertMonitorHealthState(context.Context, model.MonitorHealthState) error
	DeleteMonitorObservationsBefore(context.Context, time.Time) error
	GetControl(context.Context, int64) (model.AccountControl, error)
	GetActiveBalanceLock(context.Context, int64) (*model.BalanceLock, error)
	GetActiveCostLock(context.Context, int64) (*model.CostLock, error)
	UpsertControl(context.Context, model.AccountControl) error
	AddEvent(context.Context, model.Event) error
	CountAutomaticPauses(context.Context, int64, time.Time, time.Time) (int, error)
	CommitAutomaticPause(context.Context, model.AccountControl, model.Event, model.FlapPolicy) (model.AccountControl, int, bool, error)
	CommitControlEvents(context.Context, model.AccountControl, ...model.Event) error
	ListEvents(context.Context, int) ([]model.Event, error)
}

type Engine struct {
	api            Sub2API
	store          Repository
	pollInterval   time.Duration
	logger         *slog.Logger
	startedAt      time.Time
	accountControl *accountcontrol.Service

	runMu        sync.Mutex
	barrier      *automation.Barrier
	snapshotMu   sync.RWMutex
	snapshot     model.Snapshot
	conflicts    map[string]bool
	coordinator  *Coordinator
	expiryWorker *OverrideExpiryWorker
}

func NewEngine(api Sub2API, store Repository, pollInterval time.Duration, logger *slog.Logger, options ...EngineOption) *Engine {
	started := time.Now().UTC()
	engine := &Engine{
		api: api, store: store, pollInterval: pollInterval, logger: logger, startedAt: started,
		barrier: automation.NewBarrier(), snapshot: model.Snapshot{ServiceStarted: started}, conflicts: make(map[string]bool),
	}
	engine.accountControl = accountcontrol.New(store, api)
	engine.coordinator = NewCoordinator(engine, logger, WithReconcileInterval(pollInterval))
	if expiryRepository, ok := store.(overrideExpiryRepository); ok {
		engine.expiryWorker = NewOverrideExpiryWorker(expiryRepository, engine, logger)
	}
	for _, option := range options {
		if option != nil {
			option(engine)
		}
	}
	return engine
}

// AutomationBarrier is shared with out-of-engine automation such as upstream
// group failover, so a global freeze has a single process-wide write boundary.
func (e *Engine) AutomationBarrier() *automation.Barrier {
	return e.barrier
}

func (e *Engine) Start(ctx context.Context) {
	pending, pendingErr := e.store.ListPendingAccountMutations(ctx, 100)
	if pendingErr != nil {
		e.logger.Warn("account_mutation_pending_read_failed", "error", pendingErr)
	}
	release, barrierErr := e.barrier.EnterMutation(ctx)
	if barrierErr != nil {
		e.logger.Error("account_mutation_startup_recovery_blocked", "error", barrierErr)
	} else {
		err := e.accountControl.ReconcilePendingAccountMutations(ctx)
		release()
		if err != nil {
			e.logger.Error("account_mutation_startup_recovery_failed", "error", err)
		}
	}
	seenPending := make(map[int64]struct{}, len(pending))
	for _, mutation := range pending {
		if mutation.AccountID > 0 {
			seenPending[mutation.AccountID] = struct{}{}
		}
	}
	go e.coordinator.Run(ctx)
	if len(seenPending) > 0 {
		ids := make([]int64, 0, len(seenPending))
		for accountID := range seenPending {
			ids = append(ids, accountID)
		}
		e.RequestAccountsFrom("startup_recovery", ids...)
	}
	e.RequestFullFrom("startup")
	if e.expiryWorker != nil {
		go e.expiryWorker.Run(ctx)
	}
}

func (e *Engine) Trigger() {
	e.RequestFullFrom("legacy_trigger")
}

func (e *Engine) RequestAccounts(accountIDs ...int64) {
	e.RequestAccountsFrom("external", accountIDs...)
}

func (e *Engine) RequestAccountsFrom(source string, accountIDs ...int64) {
	if e.coordinator != nil {
		e.coordinator.RequestAccountsFrom(source, accountIDs...)
		return
	}
	e.RequestFullFrom(source)
}

func (e *Engine) RequestFull() {
	e.RequestFullFrom("external")
}

func (e *Engine) RequestFullFrom(source string) {
	if e.coordinator != nil {
		e.coordinator.RequestFullFrom(source)
	}
}

func (e *Engine) notifyOverrideChanged() {
	if e.expiryWorker != nil {
		e.expiryWorker.Wake()
	}
}

func (e *Engine) Snapshot() model.Snapshot {
	e.snapshotMu.RLock()
	defer e.snapshotMu.RUnlock()
	copy := e.snapshot
	copy.Bindings = append([]model.ResolvedBinding{}, e.snapshot.Bindings...)
	copy.Unmatched = append([]model.Monitor{}, e.snapshot.Unmatched...)
	copy.Conflicts = append([]string{}, e.snapshot.Conflicts...)
	return copy
}

// AccountIDsForMonitors resolves the monitor/account association already held
// in the latest scheduler snapshot. Telemetry uses it after a successful
// history commit and therefore does not issue an extra database query.
func (e *Engine) AccountIDsForMonitors(monitorIDs ...int64) []int64 {
	requested := make(map[int64]struct{}, len(monitorIDs))
	for _, monitorID := range monitorIDs {
		if monitorID > 0 {
			requested[monitorID] = struct{}{}
		}
	}
	seen := make(map[int64]struct{})
	e.snapshotMu.RLock()
	for _, binding := range e.snapshot.Bindings {
		if binding.State == "bound" && binding.Monitor != nil {
			if _, ok := requested[binding.Monitor.ID]; ok {
				seen[binding.Account.ID] = struct{}{}
			}
		}
	}
	e.snapshotMu.RUnlock()
	ids := make([]int64, 0, len(seen))
	for accountID := range seen {
		ids = append(ids, accountID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// FilterReconcileAccountIDs uses only the current in-memory binding snapshot.
// It keeps Telemetry from waking targeted passes for OAuth or incomplete
// accounts that matcher has already classified as unbound.
func (e *Engine) FilterReconcileAccountIDs(accountIDs ...int64) (accepted []int64, ignored []int64) {
	requested := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID > 0 {
			requested[accountID] = struct{}{}
		}
	}
	bound := make(map[int64]struct{}, len(requested))
	e.snapshotMu.RLock()
	for _, binding := range e.snapshot.Bindings {
		if binding.Account.ID > 0 && binding.State == "bound" && binding.Monitor != nil &&
			!strings.Contains(strings.ToLower(strings.TrimSpace(binding.Account.Type)), "oauth") {
			bound[binding.Account.ID] = struct{}{}
		}
	}
	e.snapshotMu.RUnlock()
	for accountID := range requested {
		if _, ok := bound[accountID]; ok {
			accepted = append(accepted, accountID)
		} else {
			ignored = append(ignored, accountID)
		}
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i] < accepted[j] })
	sort.Slice(ignored, func(i, j int) bool { return ignored[i] < ignored[j] })
	return accepted, ignored
}

func (e *Engine) Events(ctx context.Context, limit int) ([]model.Event, error) {
	return e.store.ListEvents(ctx, limit)
}

func (e *Engine) Settings(ctx context.Context) (model.Settings, error) {
	return e.store.GetSettings(ctx)
}

func (e *Engine) FreezeState(ctx context.Context) (model.FreezeState, error) {
	state, err := e.store.GetAgentFreezeState(ctx, "global", "")
	if err != nil {
		return model.FreezeState{}, err
	}
	return effectiveFreezeState(state, time.Now().UTC()), nil
}

func (e *Engine) UpdateFreezeState(ctx context.Context, state model.FreezeState, actor string) error {
	releaseBarrier, err := e.barrier.EnterFreeze(ctx)
	if err != nil {
		return fmt.Errorf("等待自动化冻结屏障: %w", err)
	}
	defer releaseBarrier()
	e.runMu.Lock()
	defer e.runMu.Unlock()
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return errors.New("冻结状态缺少操作者")
	}
	mode := strings.TrimSpace(state.Mode)
	if mode == "" {
		switch {
		case state.AllAutomation:
			mode = model.AgentFreezeModeWritesFrozen
		case state.Agent:
			mode = model.AgentFreezeModeAgentPaused
		default:
			mode = model.AgentFreezeModeActive
		}
	}
	persisted := model.AgentFreezeState{ScopeType: "global", Mode: mode, Reason: strings.TrimSpace(state.Reason),
		Actor: actor, ExpiresAt: state.ExpiresAt}
	if err := e.store.SetAgentFreezeState(ctx, &persisted); err != nil {
		return err
	}
	current, err := e.FreezeState(ctx)
	if err != nil {
		return err
	}
	e.snapshotMu.Lock()
	e.snapshot.Freeze = current
	e.snapshotMu.Unlock()
	e.record(ctx, model.Event{Type: "automation_freeze_updated", Severity: "warning", Message: "自动化冻结状态已更新", Actor: actor,
		Details: mustJSON(current)})
	e.RequestFullFrom("freeze_update")
	return nil
}

func (e *Engine) UpdateSettings(ctx context.Context, settings model.Settings, actor string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	if err := e.store.UpdateSettings(ctx, settings); err != nil {
		return err
	}
	e.record(ctx, model.Event{Type: "settings_updated", Severity: "info", Message: "调度策略已更新", Actor: actor, Details: mustJSON(settings)})
	e.RequestFullFrom("settings_update")
	return nil
}

func (e *Engine) UpdatePolicy(ctx context.Context, policy model.Policy, actor string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	if actor == "web" {
		policy.ScorePolicySource = "account"
		policy.ScorePolicyVersionID = nil
	}
	if err := e.store.UpsertPolicy(ctx, policy); err != nil {
		return err
	}
	accountID := policy.AccountID
	e.record(ctx, model.Event{Type: "binding_updated", Severity: "info", AccountID: &accountID, Message: "账号绑定规则已更新", Actor: actor, Details: mustJSON(policy)})
	e.RequestAccountsFrom("policy_update", accountID)
	return nil
}

// UpdatePolicies publishes a pool projection atomically. The concrete store
// owns the transaction; refusing a repository without batch support is safer
// than leaving a partially updated pool behind.
func (e *Engine) UpdatePolicies(ctx context.Context, policies []model.Policy, actor string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	if len(policies) == 0 {
		return errors.New("批量策略不能为空")
	}
	if actor == "web" {
		for index := range policies {
			policies[index].ScorePolicySource = "account"
			policies[index].ScorePolicyVersionID = nil
		}
	}
	batch, ok := e.store.(interface {
		UpsertPolicies(context.Context, []model.Policy) error
	})
	if !ok {
		return errors.New("策略存储不支持原子批量发布")
	}
	if err := batch.UpsertPolicies(ctx, policies); err != nil {
		return err
	}
	accountIDs := make([]int64, 0, len(policies))
	for _, policy := range policies {
		accountID := policy.AccountID
		accountIDs = append(accountIDs, accountID)
		e.record(ctx, model.Event{Type: "binding_updated", Severity: "info", AccountID: &accountID,
			Message: "账号池策略已原子更新", Actor: actor, Details: mustJSON(policy)})
	}
	e.RequestAccountsFrom("policy_update", accountIDs...)
	return nil
}

// RunExclusive serializes local policy publications and other short control
// plane store updates. The callback must only use store operations; calling an
// Engine mutation method from it would attempt to acquire runMu again.
func (e *Engine) RunExclusive(ctx context.Context, action func() error) error {
	if action == nil {
		return errors.New("独占协调动作不能为空")
	}
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return action()
}

func (e *Engine) ensureActorAutomationAllowed(ctx context.Context, actor string) error {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(actor)), "agent:") {
		return nil
	}
	state, err := e.FreezeState(ctx)
	if err != nil {
		return fmt.Errorf("读取自动化冻结状态失败: %w", err)
	}
	if state.AllAutomation {
		return errors.New("全部自动化已冻结，智能体写操作被拒绝")
	}
	if state.Agent {
		return errors.New("智能体已冻结，写操作被拒绝")
	}
	return nil
}

func effectiveFreezeState(state model.AgentFreezeState, now time.Time) model.FreezeState {
	mode := state.Mode
	if state.ExpiresAt != nil && !state.ExpiresAt.UTC().After(now.UTC()) {
		mode = model.AgentFreezeModeActive
	}
	view := model.FreezeState{Mode: mode, Reason: state.Reason, Actor: state.Actor,
		ExpiresAt: state.ExpiresAt, UpdatedAt: state.UpdatedAt}
	switch mode {
	case model.AgentFreezeModeAgentPaused:
		view.Agent = true
	case model.AgentFreezeModeReadOnly, model.AgentFreezeModeWritesFrozen:
		view.Agent = true
		view.AllAutomation = true
	}
	return view
}

func (e *Engine) ManualPause(ctx context.Context, accountID int64, actor string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ManualPauseCommand(ctx, accountID, actor, commandID)
	return legacyMutationResult(result, err)
}

func (e *Engine) AgentPause(ctx context.Context, accountID int64, actor, reason string) error {
	result, err := e.agentSchedulable(ctx, accountID, false, actor, reason)
	return legacyMutationResult(result, err)
}

func (e *Engine) AgentResume(ctx context.Context, accountID int64, actor, reason string) error {
	result, err := e.agentSchedulable(ctx, accountID, true, actor, reason)
	return legacyMutationResult(result, err)
}

func (e *Engine) AgentSetLoadFactor(ctx context.Context, accountID int64, value *int, actor, reason string) error {
	result, err := e.agentLoad(ctx, accountID, value, actor, reason)
	return legacyMutationResult(result, err)
}

func (e *Engine) ForceSetLoadFactor(ctx context.Context, accountID int64, value *int, actor, reason string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ForceSetLoadFactorCommand(ctx, accountID, value, actor, reason, commandID, accountcontrol.DefaultAdministratorTTL)
	return legacyMutationResult(result, err)
}

func (e *Engine) PinLoad(ctx context.Context, accountID int64, value int, until time.Time, actor, reason string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	var deadline *time.Time
	permanent := until.IsZero()
	if !permanent {
		until = until.UTC()
		deadline = &until
	}
	result, err := e.PinLoadCommand(ctx, accountID, value, deadline, permanent, actor, reason, commandID)
	return legacyMutationResult(result, err)
}

func (e *Engine) ClearLoadPin(ctx context.Context, accountID int64, actor, reason string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ClearLoadPinCommand(ctx, accountID, actor, reason, commandID)
	return legacyMutationResult(result, err)
}

func (e *Engine) ManualResume(ctx context.Context, accountID int64, actor string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ManualResumeCommand(ctx, accountID, actor, commandID, accountcontrol.DefaultAdministratorTTL)
	return legacyMutationResult(result, err)
}

func (e *Engine) ForceResume(ctx context.Context, accountID int64, actor, reason string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ForceResumeCommand(ctx, accountID, actor, reason, commandID, accountcontrol.DefaultAdministratorTTL)
	return legacyMutationResult(result, err)
}

func (e *Engine) ClearFlapProtection(ctx context.Context, accountID int64, actor string) error {
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	if !control.FlapActive {
		return errors.New("该账号未处于抖动保护")
	}
	now := time.Now().UTC()
	clearFlapState(&control)
	event := flapClearedEvent(&accountID, control.MonitorID, actor, "管理端解除本次抖动保护", now)
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		return err
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

func (e *Engine) ClearOverride(ctx context.Context, accountID int64, actor string) error {
	commandID, err := accountcontrol.NewCommandID()
	if err != nil {
		return err
	}
	result, err := e.ReleaseManualHoldCommand(ctx, accountID, actor, commandID)
	return legacyMutationResult(result, err)
}

func (e *Engine) Reconcile(ctx context.Context) error {
	return e.ReconcileFull(ctx)
}

func (e *Engine) ReconcileFull(ctx context.Context) error {
	return e.reconcilePass(ctx, nil)
}

func (e *Engine) ReconcileAccounts(ctx context.Context, accountIDs []int64) error {
	targets := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID > 0 {
			targets[accountID] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	return e.reconcilePass(ctx, targets)
}

func (e *Engine) reconcilePass(ctx context.Context, targets map[int64]struct{}) error {
	if targets == nil {
		releaseRecovery, err := e.barrier.EnterMutation(ctx)
		if err != nil {
			return e.syncFailed(fmt.Errorf("等待账号恢复屏障: %w", err))
		}
		recoveryErr := e.accountControl.ReconcilePendingAccountMutations(ctx)
		releaseRecovery()
		if recoveryErr != nil {
			e.logger.Warn("account_mutation_runtime_recovery_incomplete", "error", recoveryErr)
		}
	}

	settings, err := e.store.GetSettings(ctx)
	if err != nil {
		return e.syncFailed(err)
	}
	persistedFreeze, err := e.store.GetAgentFreezeState(ctx, "global", "")
	if err != nil {
		return e.syncFailed(fmt.Errorf("读取自动化冻结状态失败: %w", err))
	}
	freeze := effectiveFreezeState(persistedFreeze, time.Now().UTC())
	monitors, err := e.api.ListMonitors(ctx)
	if err != nil {
		return e.syncFailed(fmt.Errorf("读取监控失败: %w", err))
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return e.syncFailed(fmt.Errorf("读取账号失败: %w", err))
	}
	policies, err := e.store.ListPolicies(ctx)
	if err != nil {
		return e.syncFailed(err)
	}

	states := make(map[int64]model.MonitorState, len(monitors))
	healthStates := make(map[int64]model.MonitorHealthState, len(monitors))
	now := time.Now().UTC()
	for _, monitor := range monitors {
		state, err := e.advanceMonitorState(ctx, monitor, now)
		if err != nil {
			return e.syncFailed(err)
		}
		states[monitor.ID] = state
		healthState, err := e.advanceHealthState(ctx, monitor, settings, now)
		if err != nil {
			return e.syncFailed(err)
		}
		healthStates[monitor.ID] = healthState
	}
	if err := e.store.DeleteMonitorObservationsBefore(ctx, now.Add(-14*24*time.Hour)); err != nil {
		return e.syncFailed(err)
	}

	bindings, unmatched, conflicts := ResolveBindings(monitors, accounts, policies)
	e.auditConflicts(ctx, conflicts)
	accountIssues := make([]AccountReconcileIssue, 0)
	seenTargets := make(map[int64]struct{}, len(targets))
	for i := range bindings {
		binding := &bindings[i]
		_, targeted := targets[binding.Account.ID]
		if targets == nil {
			targeted = true
		}
		if targeted {
			seenTargets[binding.Account.ID] = struct{}{}
		}
		accountErr := func() error {
			binding.FailureThreshold = settings.FailureThreshold
			binding.BaseRecoveryThreshold = settings.RecoveryThreshold
			if binding.Policy.FailureThreshold != nil {
				binding.FailureThreshold = *binding.Policy.FailureThreshold
			}
			if binding.Policy.RecoveryThreshold != nil {
				binding.BaseRecoveryThreshold = *binding.Policy.RecoveryThreshold
			}
			if settings.HealthMode == model.HealthModeAdaptive {
				binding.BaseRecoveryThreshold = maxInt(binding.BaseRecoveryThreshold, settings.HealthRecoverySuccesses)
			}
			flap := resolveFlapPolicy(settings, binding.Policy)
			binding.FlapEnabled = flap.Enabled
			binding.FlapWindowMinutes = flap.WindowMinutes
			binding.FlapPauseThreshold = flap.PauseThreshold
			binding.FlapRecoveryThreshold = flap.RecoveryThreshold
			if binding.Monitor != nil {
				binding.MonitorState = states[binding.Monitor.ID]
				binding.HealthState = healthStates[binding.Monitor.ID]
				decision, decisionErr := e.evaluateV3Decision(ctx, *binding, now, settings)
				if decisionErr != nil {
					return decisionErr
				}
				binding.Decision = decision
			}
			control, err := e.store.GetControl(ctx, binding.Account.ID)
			if err != nil {
				return err
			}
			recentPauses, err := e.store.CountAutomaticPauses(ctx, binding.Account.ID, now.Add(-time.Duration(flap.WindowMinutes)*time.Minute), now)
			if err != nil {
				return err
			}
			control.RecentAutomaticPauses = recentPauses
			balanceLock, err := e.store.GetActiveBalanceLock(ctx, binding.Account.ID)
			if err != nil {
				return err
			}
			control.BalanceLocked = balanceLock != nil
			if balanceLock != nil {
				control.BalanceSourceID = &balanceLock.SourceID
			} else {
				control.BalanceSourceID = nil
			}
			costLock, err := e.store.GetActiveCostLock(ctx, binding.Account.ID)
			if err != nil {
				return err
			}
			control.CostLocked = costLock != nil
			if costLock != nil {
				control.CostSourceID = &costLock.SourceID
				control.CostPool = costLock.Pool
			} else {
				control.CostSourceID = nil
				control.CostPool = ""
			}
			if targeted && control.FlapActive && !flap.Enabled {
				clearFlapState(&control)
				event := flapClearedEvent(&binding.Account.ID, control.MonitorID, "system", "账号策略已关闭抖动保护", now)
				if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
					return err
				}
				e.logEvent(event)
			}
			if targeted && flap.Enabled && !control.FlapActive && control.HealthLocked && recentPauses >= flap.PauseThreshold {
				triggeredAt := now
				control.FlapActive = true
				control.FlapTriggeredAt = &triggeredAt
				control.FlapRecoveryRequired = maxInt(binding.BaseRecoveryThreshold, flap.RecoveryThreshold)
				event := model.Event{
					Type: "flap_protection_activated", Severity: "warning", MonitorID: control.MonitorID, AccountID: &binding.Account.ID,
					Message: "升级后检测到账号在滚动窗口内反复暂停，已补充抖动保护", BeforeState: "normal_recovery", AfterState: "flap_protected",
					Details: mustJSON(map[string]any{"window_minutes": flap.WindowMinutes, "recent_pause_count": recentPauses, "pause_threshold": flap.PauseThreshold, "recovery_threshold": control.FlapRecoveryRequired}), Actor: "system", CreatedAt: now,
				}
				if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
					return err
				}
				e.logEvent(event)
			}
			binding.RecoveryThreshold = effectiveRecoveryThreshold(binding.BaseRecoveryThreshold, control)
			binding.Control = control
			if targeted {
				if err := e.reconcileAccount(ctx, binding, settings, now); err != nil {
					return err
				}
				control, err = e.store.GetControl(ctx, binding.Account.ID)
				if err != nil {
					return err
				}
				recentPauses, err = e.store.CountAutomaticPauses(ctx, binding.Account.ID, now.Add(-time.Duration(flap.WindowMinutes)*time.Minute), now)
				if err != nil {
					return err
				}
				control.RecentAutomaticPauses = recentPauses
				balanceLock, err = e.store.GetActiveBalanceLock(ctx, binding.Account.ID)
				if err != nil {
					return err
				}
				control.BalanceLocked = balanceLock != nil
				if balanceLock != nil {
					control.BalanceSourceID = &balanceLock.SourceID
				} else {
					control.BalanceSourceID = nil
				}
				costLock, err = e.store.GetActiveCostLock(ctx, binding.Account.ID)
				if err != nil {
					return err
				}
				control.CostLocked = costLock != nil
				if costLock != nil {
					control.CostSourceID = &costLock.SourceID
					control.CostPool = costLock.Pool
				} else {
					control.CostSourceID = nil
					control.CostPool = ""
				}
				binding.Control = control
				binding.RecoveryThreshold = effectiveRecoveryThreshold(binding.BaseRecoveryThreshold, control)
			}
			return nil
		}()
		if accountErr != nil {
			issue := accountReconcileIssue(binding.Account.ID, accountErr)
			accountIssues = append(accountIssues, issue)
			details := mustJSON(map[string]any{"status": issue.Status, "error_code": issue.Code,
				"account_context": json.RawMessage(actionDetails(binding, &binding.Control, "账号协调失败"))})
			e.record(ctx, model.Event{Type: "account_action_failed", Severity: "error", MonitorID: bindingMonitorID(binding),
				AccountID: &binding.Account.ID, Message: accountErr.Error(), Details: details, Actor: "system"})
			e.logger.Error("account_reconcile_failed", "account_id", binding.Account.ID, "status", issue.Status,
				"code", issue.Code, "error", accountErr)
		}
	}
	if targets != nil {
		for accountID := range targets {
			if _, ok := seenTargets[accountID]; !ok {
				e.logger.Info("reconcile_target_account_not_found", "account_id", accountID)
			}
		}
	}

	syncedAt := time.Now().UTC()
	var aggregate *AccountReconcileErrors
	lastSyncError := ""
	if len(accountIssues) > 0 {
		aggregate = &AccountReconcileErrors{Issues: accountIssues}
		lastSyncError = aggregate.Error()
	}
	e.snapshotMu.Lock()
	e.snapshot = model.Snapshot{Bindings: bindings, Unmatched: unmatched, Conflicts: conflicts, LastSyncAt: &syncedAt,
		LastSyncError: lastSyncError, Settings: settings, Freeze: freeze, ServiceStarted: e.startedAt}
	e.snapshotMu.Unlock()
	e.logger.Info("reconcile_complete", "monitors", len(monitors), "accounts", len(accounts), "bindings", len(bindings),
		"account_errors", len(accountIssues), "dry_run", settings.DryRun)
	if aggregate != nil {
		return aggregate
	}
	return nil
}

func (e *Engine) syncFailed(err error) error {
	e.snapshotMu.Lock()
	e.snapshot.LastSyncError = err.Error()
	e.snapshotMu.Unlock()
	e.record(context.Background(), model.Event{Type: "sync_failed", Severity: "error", Message: err.Error(), Actor: "system"})
	return err
}

func (e *Engine) auditConflicts(ctx context.Context, conflicts []string) {
	current := make(map[string]bool, len(conflicts))
	for _, conflict := range conflicts {
		current[conflict] = true
		if !e.conflicts[conflict] {
			e.record(ctx, model.Event{Type: "binding_conflict", Severity: "warning", Message: conflict, Actor: "system"})
		}
	}
	for conflict := range e.conflicts {
		if !current[conflict] {
			e.record(ctx, model.Event{Type: "binding_conflict_cleared", Severity: "info", Message: conflict, Actor: "system"})
		}
	}
	e.conflicts = current
}

func (e *Engine) advanceMonitorState(ctx context.Context, monitor model.Monitor, now time.Time) (model.MonitorState, error) {
	state, err := e.store.GetMonitorState(ctx, monitor.ID)
	if err != nil {
		return state, err
	}
	if !monitor.Enabled || monitor.LastCheckedAt == nil {
		state.Phase = model.PhaseFrozen
		state.HealthyStreak = 0
		state.UnhealthyStreak = 0
		return state, e.store.UpsertMonitorState(ctx, state)
	}
	staleAfter := time.Duration(monitor.IntervalSeconds*3) * time.Second
	if staleAfter < 3*time.Minute {
		staleAfter = 3 * time.Minute
	}
	if now.Sub(monitor.LastCheckedAt.UTC()) > staleAfter {
		state.Phase = model.PhaseFrozen
		state.HealthyStreak = 0
		state.UnhealthyStreak = 0
		return state, e.store.UpsertMonitorState(ctx, state)
	}
	if state.LastCheckedAt != nil && state.LastCheckedAt.Equal(monitor.LastCheckedAt.UTC()) {
		return state, nil
	}
	checked := monitor.LastCheckedAt.UTC()
	state.LastCheckedAt = &checked
	state.LastStatus = monitor.PrimaryStatus
	switch monitor.PrimaryStatus {
	case model.StatusOperational:
		state.HealthyStreak++
		state.UnhealthyStreak = 0
		state.Phase = model.PhaseHealthy
	case model.StatusFailed, model.StatusError:
		state.UnhealthyStreak++
		state.HealthyStreak = 0
		state.Phase = model.PhaseUnhealthy
	case model.StatusDegraded:
		state.HealthyStreak = 0
		state.UnhealthyStreak = 0
		state.Phase = model.PhaseDegraded
	default:
		state.HealthyStreak = 0
		state.UnhealthyStreak = 0
		state.Phase = model.PhaseUnknown
	}
	return state, e.store.UpsertMonitorState(ctx, state)
}

func (e *Engine) advanceHealthState(ctx context.Context, monitor model.Monitor, settings model.Settings, now time.Time) (model.MonitorHealthState, error) {
	previous, err := e.store.GetMonitorHealthState(ctx, monitor.ID)
	if err != nil {
		return previous, err
	}
	observations, err := e.store.ListMonitorObservations(ctx, monitor.ID, now.Add(-24*time.Hour), 2500)
	if err != nil {
		return previous, err
	}
	state, observation := health.Evaluate(monitor, observations, previous, settings, now)
	inserted := false
	if !observation.CheckedAt.IsZero() {
		inserted, err = e.store.InsertMonitorObservation(ctx, observation)
		if err != nil {
			return previous, err
		}
	}
	if err := e.store.UpsertMonitorHealthState(ctx, state); err != nil {
		return previous, err
	}
	if previous.Stage != "" && previous.Stage != state.Stage {
		event := model.Event{
			Type: "health_stage_changed", Severity: healthStageSeverity(state.Stage), MonitorID: &monitor.ID,
			Message:     fmt.Sprintf("渠道健康阶段由%s变为%s", healthStageLabel(previous.Stage), healthStageLabel(state.Stage)),
			BeforeState: previous.Stage, AfterState: state.Stage, Details: state.ReasonJSON, Actor: "system", CreatedAt: now,
		}
		e.record(ctx, event)
	}
	if inserted && settings.HealthMode == model.HealthModeObserve {
		switch state.Stage {
		case model.HealthStageDegraded:
			e.record(ctx, model.Event{Type: "would_reduce_load", Severity: "warning", MonitorID: &monitor.ID, Message: "智能判定建议降低渠道负载", Details: state.ReasonJSON, Actor: "system", CreatedAt: now})
		case model.HealthStageQuarantined:
			e.record(ctx, model.Event{Type: "would_quarantine", Severity: "warning", MonitorID: &monitor.ID, Message: "智能判定建议隔离渠道", Details: state.ReasonJSON, Actor: "system", CreatedAt: now})
		}
	}
	return state, nil
}

func (e *Engine) evaluateV3Decision(ctx context.Context, binding model.ResolvedBinding, now time.Time, settings model.Settings) (*model.HealthDecision, error) {
	monitor := binding.Monitor
	if monitor == nil || binding.State != "bound" || binding.MonitorState.Phase == model.PhaseFrozen || binding.MonitorState.Phase == model.PhaseUnknown {
		return nil, nil
	}
	history, err := e.store.ListMonitorHistory(ctx, monitor.ID, monitor.PrimaryModel, 60)
	if err != nil {
		return nil, fmt.Errorf("读取监控 %d 第三版证据失败: %w", monitor.ID, err)
	}
	if len(history) == 0 {
		return nil, nil
	}
	// The history endpoint is newest first; the evaluator consumes chronological input.
	checks := make([]health.Check, 0, len(history))
	resolvedSettings := health.ResolveSettings(settings, binding.Policy)
	for i := len(history) - 1; i >= 0; i-- {
		check := v3CheckFromHistory(history[i])
		check.SlowThresholdMS = resolvedSettings.HealthLatencyWarningMS
		checks = append(checks, check)
	}
	latest := history[0].CheckedAt.UTC()
	if monitor.LastCheckedAt == nil {
		return nil, nil
	}
	traffic, err := e.store.GetTrafficWindow(ctx, binding.Account.ID, now.Add(-10*time.Minute), now.Add(time.Nanosecond))
	if err != nil {
		return nil, fmt.Errorf("读取账号 %d 真实流量失败: %w", binding.Account.ID, err)
	}
	traffic60, err := e.store.GetTrafficWindow(ctx, binding.Account.ID, now.Add(-time.Hour), now.Add(time.Nanosecond))
	if err != nil {
		return nil, fmt.Errorf("读取账号 %d 一小时真实流量失败: %w", binding.Account.ID, err)
	}
	baseline := v3LatencyBaseline(checks)
	longTerm := normalizePercentPointer(monitor.Availability7D)
	result := health.EvaluateV3(health.V3Input{
		Checks: checks, BaselineLatencyMS: baseline, LongTermSuccessRate: longTerm,
		RealTraffic:           health.RealTraffic{SampleCount: traffic.EligibleCount, Successes: traffic.SuccessCount},
		RealTrafficMinSamples: maxInt(20, resolvedSettings.HealthMinSamples),
		HardFailureStreak:     maxInt(1, binding.FailureThreshold),
		HardFailuresInWindow:  maxInt(resolvedSettings.HealthHardFailures10, binding.FailureThreshold),
		TrafficPauseBelow:     float64(resolvedSettings.HealthTrafficPauseBelow),
		TrafficHealthyAt:      float64(resolvedSettings.HealthTrafficHealthyAt),
		LatencyWarningMS:      resolvedSettings.HealthLatencyWarningMS,
		LatencyCriticalMS:     resolvedSettings.HealthLatencyCriticalMS,
		QualityFullAt:         float64(resolvedSettings.HealthHealthyScore),
		QualityReduce80At:     float64(resolvedSettings.HealthWatchScore),
		QualityReduce50At:     float64(resolvedSettings.HealthQuarantineScore),
		PersistentSlowRate:    float64(resolvedSettings.HealthPersistentSlowRate),
	})
	capabilities, err := e.store.ListAccountModelCapabilities(ctx, binding.Account.ID)
	if err != nil {
		return nil, fmt.Errorf("读取账号 %d 模型能力失败: %w", binding.Account.ID, err)
	}
	decision := healthDecisionFromResult(result, capabilities)
	decision.RecoverySuccessStreak = trailingV3Successes(result.Checks)
	if decision.CheckedAt.IsZero() {
		decision.CheckedAt = latest
	}

	monitorID, accountID := monitor.ID, binding.Account.ID
	actionResult := "informational"
	if settings.HealthMode == model.HealthModeObserve {
		actionResult = "proposed"
	} else if settings.HealthMode == model.HealthModeAdaptive {
		actionResult = "eligible"
	}
	reasonCode := "healthy"
	if len(decision.ReasonCodes) > 0 {
		reasonCode = decision.ReasonCodes[0]
	}
	availability := model.PhaseHealthy
	if result.Pause {
		availability = model.PhaseUnhealthy
	} else if result.RecommendedLoad < 100 {
		availability = model.PhaseDegraded
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("v3:%d:%d:%s", monitorID, accountID, decision.CheckedAt.UTC().Format(time.RFC3339Nano))))
	snapshot := model.DecisionSnapshot{
		DecisionID: fmt.Sprintf("%x", digest[:]), MonitorID: &monitorID, AccountID: &accountID,
		CheckedAt: decision.CheckedAt, AvailabilityState: availability,
		LoadStage: loadStageForPercent(result.RecommendedLoad), TargetLoadPercent: result.RecommendedLoad,
		Action: result.Action, ActionResult: actionResult, ReasonCode: reasonCode,
		Evidence: model.DecisionEvidence{
			HardFailureStreak: result.HardFailureStreak, HardFailures10: result.HardFailures10,
			HardSuccessRate10: result.HardSuccessRate10, HardSuccessRate60: result.HardSuccessRate60,
			DegradedCount10: countRate(result.DegradedRate10, minInt(len(result.Checks), 10)),
			DegradedCount60: countRate(result.DegradedRate60, minInt(len(result.Checks), 60)),
			DegradedStreak:  trailingSignalCount(result.Checks, health.SignalPerformanceSlow),
			LatencyP90MS:    result.ResponseP90MS, BaselineLatencyMS: int64(math.Round(result.BaselineLatencyMS)),
			TrafficSuccessCount10M: traffic.SuccessCount,
			TrafficFailureCount10M: maxInt(0, traffic.EligibleCount-traffic.SuccessCount),
			TrafficSuccessRate10M:  traffic.SuccessRate, QualityScore: result.QualityScore,
			TrafficSuccessRate60M: traffic60.SuccessRate,
			Confidence:            v3Confidence(len(result.Checks), traffic.EligibleCount),
		},
		CreatedAt: now,
	}
	event := model.Event{
		Type: "health_decision_snapshot", Severity: decisionSeverity(result), MonitorID: &monitorID, AccountID: &accountID,
		Message: "已生成第三版渠道决策", BeforeState: formatLoadFactor(binding.Account.LoadFactor),
		AfterState: fmt.Sprintf("%s:%d%%", result.Action, result.RecommendedLoad), Details: mustJSON(decision), Actor: "system", CreatedAt: now,
	}
	inserted, err := e.store.CommitDecisionSnapshot(ctx, snapshot, &event)
	if err != nil {
		return nil, fmt.Errorf("保存账号 %d 第三版决策失败: %w", accountID, err)
	}
	if inserted {
		e.logEvent(event)
	}
	return &decision, nil
}

func v3CheckFromHistory(item model.MonitorHistoryRecord) health.Check {
	check := health.Check{CheckedAt: item.CheckedAt, Status: item.Status, HTTPStatus: item.StatusCode, LatencyMS: item.LatencyMS, SlowThresholdMS: 6000}
	switch item.ErrorClass {
	case model.ErrorClassCredential:
		check.HTTPStatus = 401
		check.Message = "credential rejected"
	case model.ErrorClassInfrastructure:
		if check.HTTPStatus == 0 {
			check.HTTPStatus = 502
		}
		check.Message = "upstream timeout"
	case model.ErrorClassCapacity:
		check.HTTPStatus = 429
		check.Message = "rate limit"
	case model.ErrorClassSemantic:
		check.Message = "challenge mismatch"
	case model.ErrorClassClient:
		check.Message = "invalid parameter"
	case model.ErrorClassModelCapability:
		check.Message = "model not found"
	}
	return check
}

func healthDecisionFromResult(result health.V3Result, capabilities []model.AccountModelCapability) model.HealthDecision {
	counts := make(map[string]int, len(result.ErrorCategoryCounts))
	for class, count := range result.ErrorCategoryCounts {
		counts[string(class)] = count
	}
	return model.HealthDecision{
		QualityScore: result.QualityScore, HardSuccessRate10: result.HardSuccessRate10,
		HardSuccessRate60: result.HardSuccessRate60, DegradedRate10: result.DegradedRate10,
		DegradedRate60: result.DegradedRate60, TrafficSuccessRate: result.TrafficSuccessRate,
		TrafficSampleCount: result.TrafficSampleCount, HardFailureStreak: result.HardFailureStreak,
		HardFailures10: result.HardFailures10, SuggestedLoadPercent: result.SuggestedLoadPercent,
		Action: result.Action, Disagreement: result.Disagreement, ResponseP90MS: result.ResponseP90MS,
		BaselineLatencyMS: result.BaselineLatencyMS, ReasonCodes: append([]string(nil), result.ReasonCodes...),
		ErrorCategoryCounts: counts, ModelCapabilities: capabilities, CheckedAt: result.CheckedAt,
	}
}

func v3LatencyBaseline(checks []health.Check) float64 {
	values := make([]int64, 0, len(checks))
	for _, check := range checks {
		classified := health.ClassifyCheck(check)
		if classified.AvailabilitySuccess && check.LatencyMS > 0 {
			values = append(values, check.LatencyMS)
		}
	}
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return float64(values[middle])
	}
	return float64(values[middle-1]+values[middle]) / 2
}

func normalizePercentPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	normalized := *value
	if normalized >= 0 && normalized <= 1 {
		normalized *= 100
	}
	return &normalized
}

func trailingV3Successes(checks []health.ClassifiedCheck) int {
	count := 0
	for i := len(checks) - 1; i >= 0; i-- {
		if !checks[i].CountedInAvailability || !checks[i].AvailabilitySuccess {
			break
		}
		count++
	}
	return count
}

func trailingSignalCount(checks []health.ClassifiedCheck, class health.SignalClass) int {
	count := 0
	for i := len(checks) - 1; i >= 0 && checks[i].Class == class; i-- {
		count++
	}
	return count
}

func countRate(rate float64, samples int) int {
	return int(math.Round(rate * float64(samples) / 100))
}

func v3Confidence(monitorSamples, trafficSamples int) float64 {
	monitorPart := math.Min(1, float64(monitorSamples)/60) * 0.7
	trafficPart := math.Min(1, float64(trafficSamples)/20) * 0.3
	return math.Round((monitorPart+trafficPart)*1000) / 1000
}

func decisionSeverity(result health.V3Result) string {
	if result.Pause {
		return "error"
	}
	if result.RecommendedLoad < 100 {
		return "warning"
	}
	return "info"
}

func loadStageForPercent(percent int) string {
	switch {
	case percent <= 25:
		return model.HealthStageLimited25
	case percent <= 50:
		return model.HealthStageLimited50
	case percent <= 80:
		return model.HealthStageLimited80
	default:
		return model.HealthStageHealthy
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func (e *Engine) reconcileAccount(ctx context.Context, binding *model.ResolvedBinding, settings model.Settings, now time.Time) error {
	control := binding.Control
	control.AccountID = binding.Account.ID
	monitorID := bindingMonitorID(binding)
	if monitorID != nil {
		control.MonitorID = monitorID
	}
	control.LastObserved = boolPtr(binding.Account.Schedulable)
	effectiveDryRun := settings.DryRun || settings.HealthMode == model.HealthModeObserve

	adaptive := settings.HealthMode == model.HealthModeAdaptive
	if adaptive {
		e.advanceAdaptiveHealthControl(ctx, binding, &control, settings, now)
	} else if settings.HealthMode == model.HealthModeLegacy {
		healthUsable := binding.State == "bound" && binding.Monitor != nil &&
			binding.MonitorState.Phase != model.PhaseFrozen && binding.MonitorState.Phase != model.PhaseUnknown
		if healthUsable && binding.MonitorState.Phase == model.PhaseUnhealthy &&
			binding.MonitorState.UnhealthyStreak >= binding.FailureThreshold && accountCanBeManaged(binding.Account, now) {
			control.HealthLocked = true
		}
		if healthUsable && binding.MonitorState.Phase == model.PhaseHealthy &&
			binding.MonitorState.HealthyStreak >= binding.RecoveryThreshold && accountCanBeManaged(binding.Account, now) {
			control.HealthLocked = false
		}
	}
	if err := e.store.UpsertControl(ctx, control); err != nil {
		return err
	}
	binding.Control = control

	if adaptive {
		if err := e.reconcileAdaptiveLoad(ctx, binding, &control, settings, now); err != nil {
			return err
		}
	} else if err := e.reconcileExpiredLoadControl(ctx, binding, &control, settings, now); err != nil {
		return err
	}
	locked := control.HealthLocked || control.BalanceLocked || control.CostLocked
	if locked {
		reason := "账号存在有效控制锁"
		switch {
		case control.CostLocked && (control.BalanceLocked || control.HealthLocked):
			reason = "账号同时受到健康、余额或倍率策略限制"
		case control.BalanceLocked && control.HealthLocked:
			reason = "余额不足且渠道连续异常"
		case control.BalanceLocked:
			reason = "上游余额连续低于停用阈值"
		case control.HealthLocked:
			reason = "上游连续检测异常"
		case control.CostLocked:
			reason = "倍率池已有更低成本的可用上游，该账号转入待命"
		}
		return e.applyPause(ctx, binding, &control, effectiveDryRun, reason, now)
	}
	return e.applyResume(ctx, binding, &control, effectiveDryRun, "全部控制锁均已解除", now)
}

func (e *Engine) advanceAdaptiveHealthControl(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, settings model.Settings, now time.Time) {
	if binding.State != "bound" || binding.Monitor == nil {
		return
	}
	// An account paused outside this scheduler remains entirely under operator
	// control. Preserve any existing load ownership so it can be reconciled after
	// the operator enables the account again, but do not advance it while paused.
	if !binding.Account.Schedulable && !control.OwnsPause {
		return
	}
	if binding.Decision != nil {
		e.advanceV3HealthControl(ctx, binding, control, settings, now)
		return
	}
	// Adaptive V3 never changes an account until detailed evidence has caught up.
	// This prevents the summary status from reintroducing semantic false positives.
	if !control.HealthLocked && control.LoadStage == "" {
		control.LoadStage = model.HealthStageFrozen
	}
}

func (e *Engine) advanceV3HealthControl(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, settings model.Settings, now time.Time) {
	decision := binding.Decision
	if decision == nil || binding.MonitorState.Phase == model.PhaseFrozen || binding.MonitorState.Phase == model.PhaseUnknown {
		return
	}
	if binding.Monitor.LastCheckedAt == nil || decision.CheckedAt.IsZero() {
		return
	}
	monitorCheckedAt := binding.Monitor.LastCheckedAt.UTC()
	decisionCheckedAt := decision.CheckedAt.UTC()
	decisionBehind := decisionCheckedAt.Before(monitorCheckedAt)
	if decision.Action == health.RecommendationPause {
		// A newer summary result may already contradict a lagging detailed-history
		// pause. Wait for telemetry to catch up before applying an adverse action.
		if decisionBehind {
			return
		}
		control.HealthLocked = true
		control.LoadStage = model.HealthStageQuarantined
		control.RecoveryStep = 0
		control.RecoveryStartedAt = nil
		return
	}
	if decisionBehind && !canUseLaggingV3Recovery(binding, decision, decisionCheckedAt, monitorCheckedAt, now) {
		return
	}
	if control.HealthLocked {
		if decision.RecoverySuccessStreak < binding.RecoveryThreshold || !accountCanBeManaged(binding.Account, now) {
			return
		}
		control.HealthLocked = false
		control.LoadStage = model.HealthStageRecovering25
		control.RecoveryStep = 1
		control.RecoveryStartedAt = &now
		e.record(ctx, model.Event{
			Type: "health_recovery_started", Severity: "info", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID,
			Message: "硬可用性连续恢复，进入百分之二十五试运行", Details: mustJSON(decision), Actor: "system", CreatedAt: now,
		})
		return
	}
	if decisionBehind {
		// A short-lag healthy decision may advance an already-running recovery
		// trial, but it may never create a new load reduction or regress a stage.
		if control.OwnsLoadFactor && isRecoveryLoadStage(control.LoadStage) {
			e.advanceV3RecoveryStage(binding, control, settings, now, false)
		}
		return
	}

	if isRecoveryLoadStage(control.LoadStage) {
		e.advanceV3RecoveryStage(binding, control, settings, now, true)
		return
	}
	targetStage := loadStageForPercent(decision.SuggestedLoadPercent)
	if targetStage == model.HealthStageHealthy && control.OwnsLoadFactor {
		switch control.LoadStage {
		case model.HealthStageLimited25:
			control.LoadStage, control.RecoveryStep = model.HealthStageRecovering25, 1
		case model.HealthStageLimited50:
			control.LoadStage, control.RecoveryStep = model.HealthStageRecovering50, 2
		case model.HealthStageLimited80:
			control.LoadStage, control.RecoveryStep = model.HealthStageRecovering80, 3
		default:
			control.LoadStage = model.HealthStageHealthy
		}
		if isRecoveryLoadStage(control.LoadStage) {
			control.RecoveryStartedAt = &now
		}
		return
	}
	control.LoadStage = targetStage
	control.RecoveryStep = 0
	control.RecoveryStartedAt = nil
}

func canUseLaggingV3Recovery(binding *model.ResolvedBinding, decision *model.HealthDecision, decisionCheckedAt, monitorCheckedAt, now time.Time) bool {
	if binding.Monitor == nil || decision == nil || binding.Monitor.PrimaryStatus != model.StatusOperational || binding.MonitorState.Phase != model.PhaseHealthy {
		return false
	}
	required := maxInt(1, binding.RecoveryThreshold)
	if decision.RecoverySuccessStreak < required || binding.MonitorState.HealthyStreak < required {
		return false
	}
	lag := monitorCheckedAt.Sub(decisionCheckedAt)
	age := now.Sub(decisionCheckedAt)
	return lag > 0 && lag <= maxLaggingV3RecoveryAge && age >= 0 && age <= maxLaggingV3RecoveryAge
}

func (e *Engine) advanceV3RecoveryStage(binding *model.ResolvedBinding, control *model.AccountControl, settings model.Settings, now time.Time, allowRegression bool) {
	decision := binding.Decision
	if decision == nil {
		return
	}
	if control.RecoveryStartedAt == nil {
		control.RecoveryStartedAt = &now
	}
	stageStarted := *control.RecoveryStartedAt
	baseHold := time.Duration(settings.HealthTrialMinutes) * time.Minute
	if baseHold < time.Minute {
		baseHold = 5 * time.Minute
	}
	switch control.LoadStage {
	case model.HealthStageRecovering25:
		if decision.SuggestedLoadPercent < 25 {
			if allowRegression {
				control.LoadStage = model.HealthStageLimited25
				control.RecoveryStep, control.RecoveryStartedAt = 0, nil
			}
			return
		}
		if decision.SuggestedLoadPercent >= 50 && now.Sub(stageStarted) >= baseHold {
			control.LoadStage, control.RecoveryStep, control.RecoveryStartedAt = model.HealthStageRecovering50, 2, &now
		}
	case model.HealthStageRecovering50:
		if decision.SuggestedLoadPercent < 50 {
			if allowRegression {
				control.LoadStage = model.HealthStageLimited25
				control.RecoveryStep, control.RecoveryStartedAt = 0, nil
			}
			return
		}
		if decision.SuggestedLoadPercent >= 80 && now.Sub(stageStarted) >= 2*baseHold {
			control.LoadStage, control.RecoveryStep, control.RecoveryStartedAt = model.HealthStageRecovering80, 3, &now
		}
	case model.HealthStageRecovering80:
		if decision.SuggestedLoadPercent < 80 {
			if allowRegression {
				control.LoadStage = loadStageForPercent(decision.SuggestedLoadPercent)
				control.RecoveryStep, control.RecoveryStartedAt = 0, nil
			}
			return
		}
		if decision.SuggestedLoadPercent >= 100 && now.Sub(stageStarted) >= 3*baseHold {
			control.LoadStage, control.RecoveryStep = model.HealthStageHealthy, 4
		}
	}
}

func (e *Engine) reconcileAdaptiveLoad(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, settings model.Settings, now time.Time) error {
	if binding.State != "bound" || binding.Monitor == nil {
		return nil
	}
	var desired *int
	restore := false
	switch control.LoadStage {
	case model.HealthStageLimited80:
		desired = desiredLoadFactor(binding.Account, control, 80)
	case model.HealthStageLimited50, model.HealthStageDegraded:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthDegradedPercent)
	case model.HealthStageLimited25, model.HealthStageRecovering25:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthTrialPercent)
	case model.HealthStageRecovering50:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthMidPercent)
	case model.HealthStageRecovering80:
		desired = desiredLoadFactor(binding.Account, control, 80)
	case model.HealthStageHealthy:
		if control.OwnsLoadFactor || loadControlExpired(*control, now) {
			desired = cloneIntPointer(control.OriginalLoadFactor)
			restore = true
		}
	default:
		return nil
	}
	if desired == nil && !restore {
		return nil
	}
	eventType, message, severity := "load_factor_adjusted", "账号负载已按渠道健康状态调整", "warning"
	if restore {
		eventType, message, severity = "load_factor_restored", "账号已恢复策略基线负载", "info"
	}
	if settings.DryRun {
		return e.store.CommitControlEvents(ctx, *control, model.Event{Type: "would_" + eventType, Severity: severity,
			MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: message,
			BeforeState: formatLoadFactor(binding.Account.LoadFactor), AfterState: formatLoadFactor(desired),
			Details: actionDetails(binding, control, message), Actor: "system", CreatedAt: now})
	}
	disposition, err := e.automaticLoadNoopDisposition(ctx, binding, *control, desired, now, restore)
	if err != nil {
		return err
	}
	if disposition == automaticLoadNoopRepairProjection {
		if err := e.reconcileAutomaticLoadProjection(ctx, binding, control, desired, restore); err != nil {
			return err
		}
	}
	if disposition != automaticLoadNoopNone {
		return e.recordDesiredAlreadyApplied(ctx, binding, *control, desired, now)
	}
	intent, safety, err := e.policyLoadIntent(ctx, binding, desired, reasonForAdaptiveLoad(restore))
	if err != nil {
		return err
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: intent.ID, Intent: intent, Safety: safety,
		Event: model.Event{Type: eventType, Severity: severity, MonitorID: bindingMonitorID(binding), Message: message}})
	if expectedPolicyBlock(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if result.VerifiedAfter != nil {
		binding.Account.LoadFactor = cloneIntPointer(result.VerifiedAfter.LoadFactor)
	}
	return nil
}

type pendingOverrideTransitionChecker interface {
	HasPendingAccountOverrideTransition(context.Context, int64, controlplane.Operation) (bool, error)
}

const (
	automaticLoadNoopNone             = ""
	automaticLoadNoopSuppress         = "suppress"
	automaticLoadNoopRepairProjection = "repair_projection"
)

func (e *Engine) automaticLoadNoopDisposition(ctx context.Context, binding *model.ResolvedBinding,
	control model.AccountControl, desired *int, now time.Time, restore bool) (string, error) {
	if !sameLoadFactor(binding.Account.LoadFactor, desired) {
		return automaticLoadNoopNone, nil
	}
	pending, err := e.store.ListPendingAccountMutations(ctx, 1000)
	if err != nil {
		return automaticLoadNoopNone, err
	}
	for _, mutation := range pending {
		if mutation.AccountID == binding.Account.ID && mutation.Operation == controlplane.OperationSetAccountLoadFactor {
			return automaticLoadNoopNone, nil
		}
	}
	overrides, err := e.store.ListActiveAccountOverrides(ctx, binding.Account.ID,
		controlplane.OperationSetAccountLoadFactor, now)
	if err != nil {
		return automaticLoadNoopNone, err
	}
	if len(overrides) > 0 {
		return automaticLoadNoopNone, nil
	}
	checker, ok := e.store.(pendingOverrideTransitionChecker)
	if !ok {
		return automaticLoadNoopNone, nil
	}
	hasPending, err := checker.HasPendingAccountOverrideTransition(ctx, binding.Account.ID,
		controlplane.OperationSetAccountLoadFactor)
	if err != nil || hasPending {
		return automaticLoadNoopNone, err
	}
	projectionMatches := sameLoadFactor(control.ExpectedLoadFactor, desired) && control.OwnsLoadFactor
	if restore {
		projectionMatches = sameLoadFactor(control.ExpectedLoadFactor, desired) && !control.OwnsLoadFactor &&
			control.OriginalLoadFactor == nil && control.LoadPinValue == nil && control.LoadPinUntil == nil &&
			control.LoadOverrideUntil == nil
	}
	if projectionMatches {
		return automaticLoadNoopSuppress, nil
	}
	// A policy-owned projection already has the durable baseline needed to
	// repair ExpectedLoadFactor. Restore can always clear ownership. Other
	// ownership gaps cannot be inferred safely and must use the normal path.
	if restore || control.OwnsLoadFactor {
		return automaticLoadNoopRepairProjection, nil
	}
	return automaticLoadNoopNone, nil
}

func (e *Engine) reconcileAutomaticLoadProjection(ctx context.Context, binding *model.ResolvedBinding,
	control *model.AccountControl, desired *int, restore bool) error {
	control.ExpectedLoadFactor = cloneIntPointer(desired)
	control.LastDecision = "projection_reconciled"
	if restore {
		control.OwnsLoadFactor = false
		control.OriginalLoadFactor = nil
		control.LoadStage = model.HealthStageHealthy
		control.RecoveryStep = 0
		control.RecoveryStartedAt = nil
		control.LoadOverrideUntil = nil
		control.LoadPinValue = nil
		control.LoadPinUntil = nil
		control.LoadPinOwner = ""
		control.LoadPinReason = ""
	}
	if err := e.store.UpsertControl(ctx, *control); err != nil {
		return err
	}
	binding.Control = *control
	return nil
}

func (e *Engine) recordDesiredAlreadyApplied(ctx context.Context, binding *model.ResolvedBinding,
	control model.AccountControl, desired *int, now time.Time) error {
	monitorID := bindingMonitorID(binding)
	accountID := binding.Account.ID
	seed := fmt.Sprintf("desired_already_applied:%d:%s:%s", accountID, control.LoadStage, formatLoadFactor(desired))
	digest := sha256.Sum256([]byte(seed))
	snapshot := model.DecisionSnapshot{DecisionID: fmt.Sprintf("%x", digest[:]), MonitorID: monitorID, AccountID: &accountID,
		CheckedAt: now, AvailabilityState: "no_change", LoadStage: control.LoadStage, Action: "no_change",
		ActionResult: "desired_already_applied", ReasonCode: "desired_already_applied", CreatedAt: now}
	event := model.Event{Type: "desired_already_applied", Severity: "info", MonitorID: monitorID, AccountID: &accountID,
		Message: "自动策略目标已由上游和本地投影满足，未创建账号 Mutation", BeforeState: formatLoadFactor(desired),
		AfterState: formatLoadFactor(desired), Actor: "system", CreatedAt: now}
	inserted, err := e.store.CommitDecisionSnapshot(ctx, snapshot, &event)
	if err == nil && inserted {
		e.logEvent(event)
	}
	return err
}

func sameLoadFactor(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (e *Engine) reconcileExpiredLoadControl(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl,
	settings model.Settings, now time.Time) error {
	if !loadControlExpired(*control, now) {
		return nil
	}
	desired := cloneIntPointer(control.OriginalLoadFactor)
	if settings.DryRun || settings.HealthMode == model.HealthModeObserve {
		return e.store.CommitControlEvents(ctx, *control, model.Event{Type: "would_load_factor_restored", Severity: "info",
			MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: "临时负载控制到期后将恢复策略基线",
			BeforeState: formatLoadFactor(binding.Account.LoadFactor), AfterState: formatLoadFactor(desired), Actor: "system", CreatedAt: now})
	}
	intent, safety, err := e.policyLoadIntent(ctx, binding, desired, "临时负载控制已到期，恢复策略基线")
	if err != nil {
		return err
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: intent.ID, Intent: intent, Safety: safety,
		Event: model.Event{Type: "load_factor_restored", Severity: "info", MonitorID: bindingMonitorID(binding), Message: "临时负载控制已到期，账号恢复策略基线"}})
	if expectedPolicyBlock(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if result.VerifiedAfter != nil {
		binding.Account.LoadFactor = cloneIntPointer(result.VerifiedAfter.LoadFactor)
	}
	return nil
}

func loadControlExpired(control model.AccountControl, now time.Time) bool {
	return control.LoadOverrideUntil != nil && !control.LoadOverrideUntil.After(now) ||
		control.LoadPinUntil != nil && !control.LoadPinUntil.After(now)
}

func desiredLoadFactor(account model.Account, control *model.AccountControl, percent int) *int {
	if percent < 1 {
		percent = 1
	}
	base := account.Concurrency
	if control.OwnsLoadFactor {
		if control.OriginalLoadFactor != nil && *control.OriginalLoadFactor > 0 {
			base = *control.OriginalLoadFactor
		}
	} else if account.LoadFactor != nil && *account.LoadFactor > 0 {
		base = *account.LoadFactor
	}
	if base < 1 {
		base = 1
	}
	target := int(math.Ceil(float64(base) * float64(percent) / 100))
	if target < 1 {
		target = 1
	}
	return &target
}

func isRecoveryLoadStage(stage string) bool {
	return stage == model.HealthStageRecovering25 || stage == model.HealthStageRecovering50 || stage == model.HealthStageRecovering80
}

func sameIntPointer(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func formatLoadFactor(value *int) string {
	if value == nil {
		return "default"
	}
	return fmt.Sprint(*value)
}

func actionDetails(binding *model.ResolvedBinding, control *model.AccountControl, reason string) string {
	details := map[string]any{
		"reason": reason,
		"locks": map[string]bool{
			"health":  control.HealthLocked,
			"balance": control.BalanceLocked,
			"cost":    control.CostLocked,
			"manual":  control.ManualLocked,
		},
		"load_stage": control.LoadStage,
	}
	if binding.Decision != nil {
		details["decision"] = binding.Decision
		details["reason_codes"] = binding.Decision.ReasonCodes
	}
	return mustJSON(details)
}

func healthStageSeverity(stage string) string {
	switch stage {
	case model.HealthStageQuarantined:
		return "error"
	case model.HealthStageDegraded, model.HealthStageFrozen:
		return "warning"
	default:
		return "info"
	}
}

func healthStageLabel(stage string) string {
	if label, ok := model.HealthStageLabels[stage]; ok {
		return label
	}
	return "未知"
}

func (e *Engine) applyPause(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, dryRun bool, reason string, now time.Time) error {
	monitorID := bindingMonitorID(binding)
	eventType := "balance_pause"
	if control.HealthLocked {
		eventType = "automatic_pause"
	} else if control.CostLocked {
		eventType = "cost_pause"
	}
	if dryRun {
		control.LastDecision = "would_pause"
		return e.store.CommitControlEvents(ctx, *control, model.Event{Type: "would_pause", Severity: "warning",
			MonitorID: monitorID, AccountID: &binding.Account.ID, Message: reason, BeforeState: "schedulable",
			AfterState: "paused", Details: actionDetails(binding, control, reason), Actor: "system", CreatedAt: now})
	}
	binding.Control = *control
	intent, safety, err := e.policySchedulableIntent(ctx, binding, false, reason)
	if err != nil {
		return err
	}
	var flap *model.FlapPolicy
	if control.HealthLocked {
		value := model.FlapPolicy{Enabled: binding.FlapEnabled, WindowMinutes: binding.FlapWindowMinutes,
			PauseThreshold:    binding.FlapPauseThreshold,
			RecoveryThreshold: maxInt(binding.BaseRecoveryThreshold, binding.FlapRecoveryThreshold)}
		flap = &value
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: intent.ID, Intent: intent, Safety: safety,
		Event: model.Event{Type: eventType, Severity: "warning", MonitorID: monitorID, Message: reason}, FlapPolicy: flap})
	if expectedPolicyBlock(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if result.VerifiedAfter != nil {
		binding.Account.Schedulable = result.VerifiedAfter.Schedulable
	}
	return nil
}

func (e *Engine) applyResume(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, dryRun bool, reason string, now time.Time) error {
	if !control.OwnsPause {
		return nil
	}
	if dryRun {
		control.LastDecision = "would_resume"
		return e.store.CommitControlEvents(ctx, *control, model.Event{Type: "would_resume", Severity: "info",
			MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: reason, BeforeState: "paused",
			AfterState: "schedulable", Details: actionDetails(binding, control, reason), Actor: "system", CreatedAt: now})
	}
	previousOwner := control.Owner
	eventType := "automatic_resume"
	if previousOwner == "balance" {
		eventType = "balance_resume"
	} else if previousOwner == "cost" {
		eventType = "cost_resume"
	}
	policyBinding := *binding
	policyBinding.Control = *control
	if policyBinding.Control.FlapActive {
		policyBinding.Control.FlapActive = false
	}
	intent, safety, err := e.policySchedulableIntent(ctx, &policyBinding, true, reason)
	if err != nil {
		return err
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: intent.ID, Intent: intent, Safety: safety,
		Event: model.Event{Type: eventType, Severity: "info", MonitorID: bindingMonitorID(binding), Message: reason}})
	if expectedPolicyBlock(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if result.VerifiedAfter != nil {
		binding.Account.Schedulable = result.VerifiedAfter.Schedulable
	}
	return nil
}

func accountCanBeManaged(account model.Account, now time.Time) bool {
	if account.Status != "active" || account.ErrorMessage != "" {
		return false
	}
	if account.ExpiresAt != nil && *account.ExpiresAt > 0 && time.Unix(*account.ExpiresAt, 0).Before(now) {
		return false
	}
	return true
}

func findAccount(accounts []model.Account, accountID int64) (model.Account, bool) {
	for _, account := range accounts {
		if account.ID == accountID {
			return account, true
		}
	}
	return model.Account{}, false
}

func (e *Engine) record(ctx context.Context, event model.Event) {
	if err := e.store.AddEvent(ctx, event); err != nil {
		e.logger.Error("event_write_failed", "type", event.Type, "error", err)
	}
	e.logEvent(event)
}

func (e *Engine) logEvent(event model.Event) {
	e.logger.Info("scheduler_event", "type", event.Type, "severity", event.Severity, "monitor_id", event.MonitorID, "account_id", event.AccountID, "message", event.Message, "actor", event.Actor)
}

func resolveFlapPolicy(settings model.Settings, policy model.Policy) model.FlapPolicy {
	resolved := model.FlapPolicy{
		Enabled: true, WindowMinutes: settings.FlapWindowMinutes,
		PauseThreshold: settings.FlapPauseThreshold, RecoveryThreshold: settings.FlapRecoveryThreshold,
	}
	if policy.FlapEnabled != nil {
		resolved.Enabled = *policy.FlapEnabled
	}
	if policy.FlapWindowMinutes != nil {
		resolved.WindowMinutes = *policy.FlapWindowMinutes
	}
	if policy.FlapPauseThreshold != nil {
		resolved.PauseThreshold = *policy.FlapPauseThreshold
	}
	if policy.FlapRecoveryThreshold != nil {
		resolved.RecoveryThreshold = *policy.FlapRecoveryThreshold
	}
	return resolved
}

func effectiveRecoveryThreshold(base int, control model.AccountControl) int {
	if !control.FlapActive {
		return base
	}
	return maxInt(base, control.FlapRecoveryRequired)
}

func clearFlapState(control *model.AccountControl) {
	control.FlapActive = false
	control.FlapTriggeredAt = nil
	control.FlapRecoveryRequired = 0
}

func bindingMonitorID(binding *model.ResolvedBinding) *int64 {
	if binding == nil || binding.Monitor == nil {
		return nil
	}
	value := binding.Monitor.ID
	return &value
}

func flapClearedEvent(accountID, monitorID *int64, actor, reason string, now time.Time) model.Event {
	return model.Event{
		Type: "flap_protection_cleared", Severity: "info", MonitorID: monitorID, AccountID: accountID,
		Message: reason, BeforeState: "flap_protected", AfterState: "normal_recovery", Actor: actor, CreatedAt: now,
	}
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func boolPtr(value bool) *bool { return &value }

func mustJSON(value any) string {
	payload, _ := json.Marshal(value)
	return string(payload)
}
