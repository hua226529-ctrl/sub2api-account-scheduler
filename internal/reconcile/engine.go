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

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/automation"
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

type Repository interface {
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
	api          Sub2API
	store        Repository
	pollInterval time.Duration
	logger       *slog.Logger
	startedAt    time.Time

	runMu      sync.Mutex
	barrier    *automation.Barrier
	snapshotMu sync.RWMutex
	snapshot   model.Snapshot
	trigger    chan struct{}
	conflicts  map[string]bool
}

func NewEngine(api Sub2API, store Repository, pollInterval time.Duration, logger *slog.Logger) *Engine {
	started := time.Now().UTC()
	return &Engine{
		api: api, store: store, pollInterval: pollInterval, logger: logger, startedAt: started,
		barrier: automation.NewBarrier(), snapshot: model.Snapshot{ServiceStarted: started}, trigger: make(chan struct{}, 1), conflicts: make(map[string]bool),
	}
}

// AutomationBarrier is shared with out-of-engine automation such as upstream
// group failover, so a global freeze has a single process-wide write boundary.
func (e *Engine) AutomationBarrier() *automation.Barrier {
	return e.barrier
}

func (e *Engine) Start(ctx context.Context) {
	go func() {
		e.reconcileLogged(ctx)
		ticker := time.NewTicker(e.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.reconcileLogged(ctx)
			case <-e.trigger:
				e.reconcileLogged(ctx)
			}
		}
	}()
}

func (e *Engine) Trigger() {
	select {
	case e.trigger <- struct{}{}:
	default:
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
	releaseBarrier := e.barrier.EnterFreeze()
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
	e.Trigger()
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
	e.Trigger()
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
	e.Trigger()
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
	for _, policy := range policies {
		accountID := policy.AccountID
		e.record(ctx, model.Event{Type: "binding_updated", Severity: "info", AccountID: &accountID,
			Message: "账号池策略已原子更新", Actor: actor, Details: mustJSON(policy)})
	}
	e.Trigger()
	return nil
}

// RunExclusive serializes a local policy publication with the 50-second
// reconcile cycle. The callback must only use store operations; calling an
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
	updated, err := e.api.SetSchedulable(ctx, accountID, false)
	if err != nil {
		return uncertainExternalMutation("管理员暂停账号", err)
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	value := false
	control.AccountID = accountID
	control.OwnsPause = true
	control.Owner = "operator"
	control.ManualLocked = true
	control.ExpectedSchedulable = &value
	control.LastObserved = &value
	control.ManualOverrideUntil = nil
	control.LastDecision = "paused"
	control.LastActionAt = &now
	event := model.Event{Type: "manual_pause", Severity: "warning", AccountID: &accountID, Message: "账号已由管理端暂停调度", BeforeState: "schedulable", AfterState: fmt.Sprint(updated.Schedulable), Actor: actor, CreatedAt: now}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		rolledBack, rollbackErr := e.api.SetSchedulable(ctx, accountID, true)
		if rollbackErr != nil || !rolledBack.Schedulable {
			return uncertainExternalMutation("暂停账号后保存归属失败且回滚未确认",
				mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认账号恢复"))
		}
		return fmt.Errorf("保存管理员暂停归属失败，外部状态已回滚: %w", err)
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

// AgentPause performs an explicit agent-owned pause. It deliberately does not
// create a manual lock, so later agent or deterministic recovery can undo it.
func (e *Engine) AgentPause(ctx context.Context, accountID int64, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}

	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if control.ManualLocked || control.Owner == "operator" {
		return errors.New("账号由人工暂停，智能体不能接管暂停归属")
	}
	if control.ManualOverrideUntil != nil && now.Before(control.ManualOverrideUntil.UTC()) {
		return errors.New("账号处于人工保护期，智能体不能暂停")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return err
	}
	account, found := findAccount(accounts, accountID)
	if !found || !accountCanBeManaged(account, now) {
		return errors.New("账号状态异常、凭据错误或已过期")
	}
	if !account.Schedulable {
		return errors.New("账号已经暂停，智能体不会覆盖现有归属")
	}
	updated, err := e.api.SetSchedulable(ctx, accountID, false)
	if err != nil {
		return uncertainExternalMutation("智能体暂停账号", err)
	}
	if updated.Schedulable {
		return errors.New("Sub2API 未确认账号暂停")
	}
	control.AccountID = accountID
	control.OwnsPause = true
	control.Owner = "agent"
	control.ManualLocked = false
	control.ExpectedSchedulable = boolPtr(false)
	control.LastObserved = boolPtr(false)
	control.ManualOverrideUntil = nil
	control.LastDecision = "agent_paused"
	control.LastActionAt = &now
	event := model.Event{Type: "agent_pause", Severity: "warning", AccountID: &accountID, Message: reason,
		BeforeState: "schedulable", AfterState: "paused", Actor: actor, CreatedAt: now}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		rolledBack, rollbackErr := e.api.SetSchedulable(ctx, accountID, true)
		if rollbackErr != nil || !rolledBack.Schedulable {
			return uncertainExternalMutation("智能体暂停后保存归属失败且回滚未确认",
				mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认账号恢复"))
		}
		return fmt.Errorf("保存智能体暂停归属失败，外部状态已回滚: %w", err)
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

func (e *Engine) AgentResume(ctx context.Context, accountID int64, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}

	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	if !control.OwnsPause || control.Owner != "agent" {
		return errors.New("该账号不是由智能体暂停，不能自动恢复")
	}
	if control.ManualLocked || control.HealthLocked || control.BalanceLocked || control.CostLocked {
		return errors.New("账号仍存在人工、健康、余额或倍率控制锁")
	}
	now := time.Now().UTC()
	if control.ManualOverrideUntil != nil && now.Before(control.ManualOverrideUntil.UTC()) {
		return errors.New("账号仍处于人工保护期")
	}
	if control.FlapActive {
		return errors.New("账号仍处于抖动保护，需先解除保护或满足恢复门槛")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return err
	}
	account, found := findAccount(accounts, accountID)
	if !found || !accountCanBeManaged(account, now) {
		return errors.New("账号状态异常、凭据错误或已过期")
	}
	if account.Schedulable {
		return errors.New("账号已经开启，拒绝改写暂停归属")
	}
	updated, err := e.api.SetSchedulable(ctx, accountID, true)
	if err != nil {
		return uncertainExternalMutation("恢复账号", err)
	}
	if !updated.Schedulable {
		return errors.New("Sub2API 未确认账号恢复")
	}
	control.OwnsPause = false
	control.Owner = ""
	control.ExpectedSchedulable = boolPtr(true)
	control.LastObserved = boolPtr(true)
	control.ManualOverrideUntil = nil
	control.LastDecision = "agent_resumed"
	control.LastActionAt = &now
	event := model.Event{Type: "agent_resume", Severity: "info", AccountID: &accountID, Message: reason,
		BeforeState: "paused", AfterState: "schedulable", Actor: actor, CreatedAt: now}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		rolledBack, rollbackErr := e.api.SetSchedulable(ctx, accountID, false)
		if rollbackErr != nil || rolledBack.Schedulable {
			return uncertainExternalMutation("恢复账号后保存归属失败且回滚未确认",
				mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认账号重新暂停"))
		}
		return fmt.Errorf("保存智能体恢复归属失败，外部状态已回滚: %w", err)
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

func (e *Engine) AgentSetLoadFactor(ctx context.Context, accountID int64, value *int, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}

	if accountID <= 0 {
		return errors.New("账号编号无效")
	}
	if value != nil && (*value < 1 || *value > 100) {
		return errors.New("负载系数必须在 1 到 100 之间")
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if control.LoadOverrideUntil != nil && now.Before(control.LoadOverrideUntil.UTC()) {
		return errors.New("负载系数处于人工保护期")
	}
	if loadPinActive(control, now) {
		return errors.New("账号负载已固定到指定时间，需先明确解除负载固定")
	}
	binding, ok := findBinding(e.Snapshot().Bindings, accountID)
	if !ok {
		return errors.New("账号不在当前调度快照中")
	}
	beforeValue := cloneIntPointer(binding.Account.LoadFactor)
	before := formatLoadFactor(beforeValue)
	updated, err := e.api.UpdateLoadFactor(ctx, accountID, value)
	if err != nil {
		return uncertainExternalMutation("调整账号负载", err)
	}
	control.AccountID = accountID
	control.OwnsLoadFactor = value != nil
	control.ExpectedLoadFactor = cloneIntPointer(updated.LoadFactor)
	control.LastActionAt = &now
	control.LastDecision = "agent_load_adjusted"
	event := model.Event{Type: "agent_load_factor", Severity: "info", AccountID: &accountID, Message: reason,
		BeforeState: before, AfterState: formatLoadFactor(updated.LoadFactor), Actor: actor, CreatedAt: now}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		rolledBack, rollbackErr := e.api.UpdateLoadFactor(ctx, accountID, beforeValue)
		if rollbackErr != nil || !sameIntPointer(rolledBack.LoadFactor, beforeValue) {
			return uncertainExternalMutation("调整负载后保存归属失败且回滚未确认",
				mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认负载回滚"))
		}
		return fmt.Errorf("保存智能体负载归属失败，外部状态已回滚: %w", err)
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

// ForceSetLoadFactor applies an administrator's exact command immediately.
// Unlike automatic load control it may replace an active pin or manual hold,
// then starts a fresh manual-protection window so the 50-second reconciler does
// not immediately overwrite the administrator's value.
func (e *Engine) ForceSetLoadFactor(ctx context.Context, accountID int64, value *int, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	actor, reason = strings.TrimSpace(actor), strings.TrimSpace(reason)
	if actor == "" || strings.HasPrefix(strings.ToLower(actor), "agent:") {
		return errors.New("强制调整负载只能由明确的管理员操作者执行")
	}
	if accountID <= 0 || (value != nil && (*value < 1 || *value > 100)) {
		return errors.New("账号编号或负载系数无效")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("强制调整负载前读取账号失败: %w", err)
	}
	account, found := findAccount(accounts, accountID)
	if !found {
		return errors.New("账号不存在，无法执行管理员负载调整")
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	settings, err := e.store.GetSettings(ctx)
	if err != nil {
		return err
	}
	before := cloneIntPointer(account.LoadFactor)
	updated, err := e.api.UpdateLoadFactor(ctx, accountID, value)
	if err != nil {
		return uncertainExternalMutation("管理员强制调整账号负载", err)
	}
	if !sameIntPointer(updated.LoadFactor, value) {
		return errors.New("Sub2API 未确认管理员负载调整")
	}
	now := time.Now().UTC()
	clearLoadPin(&control)
	control.AccountID = accountID
	control.OwnsLoadFactor = false
	control.OriginalLoadFactor = nil
	control.ExpectedLoadFactor = nil
	control.LoadStage = "manual_override"
	until := now.Add(time.Duration(settings.HealthLoadOverrideMinutes) * time.Minute)
	control.LoadOverrideUntil = &until
	control.LastDecision = "admin_force_load_adjusted"
	control.LastActionAt = &now
	event := model.Event{Type: "admin_force_load_factor", Severity: "warning", AccountID: &accountID,
		Message: "管理员已强制调整账号负载并替换既有固定或保护", BeforeState: formatLoadFactor(before),
		AfterState: formatLoadFactor(updated.LoadFactor), Actor: actor, CreatedAt: now,
		Details: mustJSON(map[string]any{"reason": reason, "protection_until": until})}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		rolledBack, rollbackErr := e.api.UpdateLoadFactor(ctx, accountID, before)
		if rollbackErr != nil || !sameIntPointer(rolledBack.LoadFactor, before) {
			return uncertainExternalMutation("管理员调整负载后保存状态失败且回滚未确认",
				mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认负载回滚"))
		}
		return fmt.Errorf("保存管理员负载状态失败，外部状态已回滚: %w", err)
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

// PinLoad fixes an account's load factor until the supplied deadline. The
// original load baseline is retained so adaptive health control does not apply
// percentages to the pinned value after the pin expires.
func (e *Engine) PinLoad(ctx context.Context, accountID int64, value int, until time.Time, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	actor, reason = strings.TrimSpace(actor), strings.TrimSpace(reason)
	if actor == "" {
		return errors.New("负载固定缺少操作者")
	}
	if accountID <= 0 || value < 1 || value > 100 {
		return errors.New("账号编号或固定负载值无效")
	}
	now := time.Now().UTC()
	until = until.UTC()
	if !until.After(now) {
		return errors.New("负载固定截止时间必须晚于当前时间")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("固定负载前读取账号失败: %w", err)
	}
	account, found := findAccount(accounts, accountID)
	isAgent := strings.HasPrefix(strings.ToLower(actor), "agent:")
	if !found || (isAgent && !accountCanBeManaged(account, now)) {
		return errors.New("账号不存在，或自动调度目标状态异常、凭据错误、已过期")
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	before := cloneIntPointer(account.LoadFactor)
	if !control.OwnsLoadFactor {
		control.OwnsLoadFactor = true
		control.OriginalLoadFactor = cloneIntPointer(before)
		if control.LoadStage == "" {
			control.LoadStage = model.HealthStageHealthy
		}
	}
	desired := value
	wroteExternal := !sameIntPointer(account.LoadFactor, &desired)
	if wroteExternal {
		updated, updateErr := e.api.UpdateLoadFactor(ctx, accountID, &desired)
		if updateErr != nil {
			return uncertainExternalMutation("固定账号负载", updateErr)
		}
		if !sameIntPointer(updated.LoadFactor, &desired) {
			return errors.New("Sub2API 未确认固定负载值")
		}
		account = updated
	}
	control.AccountID = accountID
	control.ExpectedLoadFactor = &desired
	control.LoadPinValue = &desired
	control.LoadPinUntil = &until
	control.LoadPinOwner = actor
	control.LoadPinReason = reason
	control.LoadOverrideUntil = nil
	control.LastDecision = "load_pinned"
	control.LastActionAt = &now
	event := model.Event{Type: "load_pin_set", Severity: "warning", AccountID: &accountID,
		Message: "账号负载已固定到指定截止时间", BeforeState: formatLoadFactor(before), AfterState: formatLoadFactor(&desired),
		Actor: actor, CreatedAt: now, Details: mustJSON(map[string]any{"value": value, "until": until, "reason": reason})}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		if wroteExternal {
			rolledBack, rollbackErr := e.api.UpdateLoadFactor(ctx, accountID, before)
			if rollbackErr != nil {
				return uncertainExternalMutation("固定负载后保存状态失败且回滚未确认",
					mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认负载回滚"))
			}
			if !sameIntPointer(rolledBack.LoadFactor, before) {
				return uncertainExternalMutation("固定负载后保存状态失败且回滚未确认",
					fmt.Errorf("保存状态: %w; Sub2API 未确认负载回滚", err))
			}
		}
		return err
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

func (e *Engine) ClearLoadPin(ctx context.Context, accountID int64, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	actor = strings.TrimSpace(actor)
	if actor == "" || accountID <= 0 {
		return errors.New("账号编号或操作者无效")
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	if control.LoadPinValue == nil || control.LoadPinUntil == nil {
		return errors.New("账号没有有效的负载固定")
	}
	now := time.Now().UTC()
	before := cloneIntPointer(control.LoadPinValue)
	clearLoadPin(&control)
	control.LastDecision = "load_pin_cleared"
	control.LastActionAt = &now
	event := model.Event{Type: "load_pin_cleared", Severity: "info", AccountID: &accountID,
		Message: "账号负载固定已解除", BeforeState: formatLoadFactor(before), Actor: actor, CreatedAt: now,
		Details: mustJSON(map[string]any{"reason": strings.TrimSpace(reason)})}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		return err
	}
	e.logEvent(event)
	e.Trigger()
	return nil
}

func findBinding(bindings []model.ResolvedBinding, accountID int64) (model.ResolvedBinding, bool) {
	for _, binding := range bindings {
		if binding.Account.ID == accountID {
			return binding, true
		}
	}
	return model.ResolvedBinding{}, false
}

func (e *Engine) ManualResume(ctx context.Context, accountID int64, actor string) error {
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	if !control.OwnsPause {
		return errors.New("该账号不是由本调度器暂停，禁止自动恢复")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("恢复前读取账号状态失败: %w", err)
	}
	account, found := findAccount(accounts, accountID)
	if !found {
		return errors.New("账号不存在")
	}
	if !accountCanBeManaged(account, time.Now().UTC()) {
		return errors.New("账号状态异常、凭据错误或已过期，禁止恢复")
	}
	updated, err := e.api.SetSchedulable(ctx, accountID, true)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	value := true
	balanceLock, err := e.store.GetActiveBalanceLock(ctx, accountID)
	if err != nil {
		return err
	}
	costLock, err := e.store.GetActiveCostLock(ctx, accountID)
	if err != nil {
		return err
	}
	if balanceLock != nil || costLock != nil {
		settings, settingsErr := e.store.GetSettings(ctx)
		if settingsErr != nil {
			return settingsErr
		}
		until := now.Add(time.Duration(settings.ManualHoldMinutes) * time.Minute)
		control.OwnsPause = true
		control.BalanceLocked = balanceLock != nil
		control.CostLocked = costLock != nil
		if balanceLock != nil {
			control.BalanceSourceID = &balanceLock.SourceID
		}
		if costLock != nil {
			control.CostSourceID = &costLock.SourceID
			control.CostPool = costLock.Pool
		}
		control.Owner = pauseOwner(control)
		control.ExpectedSchedulable = &value
		control.LastObserved = &value
		control.ManualOverrideUntil = &until
		control.LastDecision = "automatic_manual_override"
		control.LastActionAt = &now
		eventType := "balance_manual_override"
		message := "管理端临时开启余额锁定账号，已进入人工保护期"
		if costLock != nil && balanceLock == nil {
			eventType = "cost_manual_override"
			message = "管理端临时开启高倍率待命账号，已进入人工保护期"
		}
		event := model.Event{Type: eventType, Severity: "warning", AccountID: &accountID, Message: message, BeforeState: "paused", AfterState: fmt.Sprint(updated.Schedulable), Actor: actor, CreatedAt: now, Details: mustJSON(map[string]any{"until": until, "balance_locked": balanceLock != nil, "cost_locked": costLock != nil})}
		if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
			return err
		}
		e.logEvent(event)
		e.Trigger()
		return nil
	}
	control.OwnsPause = false
	control.Owner = ""
	control.ManualLocked = false
	control.HealthLocked = false
	control.CostLocked = false
	control.CostSourceID = nil
	control.CostPool = ""
	control.ExpectedSchedulable = &value
	control.LastObserved = &value
	control.ManualOverrideUntil = nil
	control.LastDecision = "resumed"
	control.LastActionAt = &now
	wasFlapProtected := control.FlapActive
	clearFlapState(&control)
	events := []model.Event{{Type: "manual_resume", Severity: "info", AccountID: &accountID, Message: "账号已由管理端绕过恢复门槛并恢复调度", BeforeState: "paused", AfterState: fmt.Sprint(updated.Schedulable), Actor: actor, CreatedAt: now}}
	if wasFlapProtected {
		events = append(events, flapClearedEvent(&accountID, control.MonitorID, actor, "管理端手动恢复已绕过抖动保护", now))
	}
	if err := e.store.CommitControlEvents(ctx, control, events...); err != nil {
		return err
	}
	for _, event := range events {
		e.logEvent(event)
	}
	e.Trigger()
	return nil
}

// ForceResume is the operator-only emergency recovery entry point. It may
// recover an account without scheduler pause ownership, but it keeps automatic
// locks and starts the normal protection window so an unhealthy account can be
// paused again after the operator has had time to intervene.
func (e *Engine) ForceResume(ctx context.Context, accountID int64, actor, reason string) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	actor, reason = strings.TrimSpace(actor), strings.TrimSpace(reason)
	if actor == "" || strings.HasPrefix(strings.ToLower(actor), "agent:") {
		return errors.New("强制恢复只能由明确的管理员操作者执行")
	}
	if accountID <= 0 {
		return errors.New("账号编号无效")
	}
	accounts, err := e.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("强制恢复前读取账号状态失败: %w", err)
	}
	account, found := findAccount(accounts, accountID)
	if !found {
		return errors.New("账号不存在，无法执行管理员强制恢复")
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	balanceLock, err := e.store.GetActiveBalanceLock(ctx, accountID)
	if err != nil {
		return err
	}
	costLock, err := e.store.GetActiveCostLock(ctx, accountID)
	if err != nil {
		return err
	}
	settings, err := e.store.GetSettings(ctx)
	if err != nil {
		return err
	}
	wasSchedulable := account.Schedulable
	if !wasSchedulable {
		updated, updateErr := e.api.SetSchedulable(ctx, accountID, true)
		if updateErr != nil {
			return uncertainExternalMutation("管理员强制恢复账号", updateErr)
		}
		if !updated.Schedulable {
			return errors.New("Sub2API 未确认账号强制恢复")
		}
	}
	now := time.Now().UTC()
	value := true
	control.AccountID = accountID
	control.ManualLocked = false
	control.BalanceLocked = balanceLock != nil
	control.CostLocked = costLock != nil
	if balanceLock != nil {
		control.BalanceSourceID = &balanceLock.SourceID
	} else {
		control.BalanceSourceID = nil
	}
	if costLock != nil {
		control.CostSourceID = &costLock.SourceID
		control.CostPool = costLock.Pool
	} else {
		control.CostSourceID = nil
		control.CostPool = ""
	}
	automaticLocked := control.HealthLocked || control.BalanceLocked || control.CostLocked
	var holdUntil *time.Time
	if automaticLocked {
		until := now.Add(time.Duration(settings.ManualHoldMinutes) * time.Minute)
		holdUntil = &until
		control.OwnsPause = true
		control.Owner = pauseOwner(control)
		control.ManualOverrideUntil = holdUntil
	} else {
		control.OwnsPause = false
		control.Owner = ""
		control.ManualOverrideUntil = nil
		clearFlapState(&control)
	}
	control.ExpectedSchedulable = &value
	control.LastObserved = &value
	control.LastDecision = "admin_force_resumed"
	control.LastActionAt = &now
	event := model.Event{Type: "admin_force_resume", Severity: "warning", AccountID: &accountID,
		Message: "管理员已强制恢复账号调度", BeforeState: fmt.Sprint(wasSchedulable), AfterState: "true", Actor: actor,
		CreatedAt: now, Details: mustJSON(map[string]any{"reason": reason, "protection_until": holdUntil,
			"health_locked": control.HealthLocked, "balance_locked": control.BalanceLocked, "cost_locked": control.CostLocked})}
	if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
		if !wasSchedulable {
			rolledBack, rollbackErr := e.api.SetSchedulable(ctx, accountID, false)
			if rollbackErr != nil || rolledBack.Schedulable {
				return uncertainExternalMutation("强制恢复后保存状态失败且回滚未确认",
					mutationRollbackFailure(err, rollbackErr, "Sub2API 未确认账号重新暂停"))
			}
		}
		return err
	}
	e.logEvent(event)
	e.Trigger()
	return nil
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
	if err := e.ensureActorAutomationAllowed(ctx, actor); err != nil {
		return err
	}
	control, err := e.store.GetControl(ctx, accountID)
	if err != nil {
		return err
	}
	control.ManualOverrideUntil = nil
	control.LastDecision = ""
	if err := e.store.UpsertControl(ctx, control); err != nil {
		return err
	}
	e.record(ctx, model.Event{Type: "manual_override_cleared", Severity: "info", AccountID: &accountID, Message: "人工保护已解除", Actor: actor})
	e.Trigger()
	return nil
}

func (e *Engine) reconcileLogged(ctx context.Context) {
	if err := e.Reconcile(ctx); err != nil {
		e.logger.Error("reconcile_failed", "error", err)
	}
}

func (e *Engine) Reconcile(ctx context.Context) error {
	e.runMu.Lock()
	defer e.runMu.Unlock()

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
	for i := range bindings {
		binding := &bindings[i]
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
				return e.syncFailed(decisionErr)
			}
			binding.Decision = decision
		}
		control, err := e.store.GetControl(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
		}
		recentPauses, err := e.store.CountAutomaticPauses(ctx, binding.Account.ID, now.Add(-time.Duration(flap.WindowMinutes)*time.Minute), now)
		if err != nil {
			return e.syncFailed(err)
		}
		control.RecentAutomaticPauses = recentPauses
		balanceLock, err := e.store.GetActiveBalanceLock(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
		}
		control.BalanceLocked = balanceLock != nil
		if balanceLock != nil {
			control.BalanceSourceID = &balanceLock.SourceID
		} else {
			control.BalanceSourceID = nil
		}
		costLock, err := e.store.GetActiveCostLock(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
		}
		control.CostLocked = costLock != nil
		if costLock != nil {
			control.CostSourceID = &costLock.SourceID
			control.CostPool = costLock.Pool
		} else {
			control.CostSourceID = nil
			control.CostPool = ""
		}
		if control.FlapActive && !flap.Enabled {
			clearFlapState(&control)
			event := flapClearedEvent(&binding.Account.ID, control.MonitorID, "system", "账号策略已关闭抖动保护", now)
			if err := e.store.CommitControlEvents(ctx, control, event); err != nil {
				return e.syncFailed(err)
			}
			e.logEvent(event)
		}
		if flap.Enabled && !control.FlapActive && control.HealthLocked && recentPauses >= flap.PauseThreshold {
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
				return e.syncFailed(err)
			}
			e.logEvent(event)
		}
		binding.RecoveryThreshold = effectiveRecoveryThreshold(binding.BaseRecoveryThreshold, control)
		binding.Control = control
		if err := e.reconcileAccountWithFreeze(ctx, binding, settings, now, freeze.AllAutomation); err != nil {
			e.record(ctx, model.Event{Type: "account_action_failed", Severity: "error", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: err.Error(), Details: actionDetails(binding, &binding.Control, "写入 Sub2API 失败"), Actor: "system"})
			e.logger.Error("account_reconcile_failed", "account_id", binding.Account.ID, "error", err)
			return e.syncFailed(fmt.Errorf("账号 %d 写入失败，已停止本轮后续操作: %w", binding.Account.ID, err))
		}
		control, err = e.store.GetControl(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
		}
		recentPauses, err = e.store.CountAutomaticPauses(ctx, binding.Account.ID, now.Add(-time.Duration(flap.WindowMinutes)*time.Minute), now)
		if err != nil {
			return e.syncFailed(err)
		}
		control.RecentAutomaticPauses = recentPauses
		balanceLock, err = e.store.GetActiveBalanceLock(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
		}
		control.BalanceLocked = balanceLock != nil
		if balanceLock != nil {
			control.BalanceSourceID = &balanceLock.SourceID
		} else {
			control.BalanceSourceID = nil
		}
		costLock, err = e.store.GetActiveCostLock(ctx, binding.Account.ID)
		if err != nil {
			return e.syncFailed(err)
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

	syncedAt := time.Now().UTC()
	e.snapshotMu.Lock()
	e.snapshot = model.Snapshot{Bindings: bindings, Unmatched: unmatched, Conflicts: conflicts, LastSyncAt: &syncedAt, Settings: settings, Freeze: freeze, ServiceStarted: e.startedAt}
	e.snapshotMu.Unlock()
	e.logger.Info("reconcile_complete", "monitors", len(monitors), "accounts", len(accounts), "bindings", len(bindings), "dry_run", settings.DryRun)
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
	return e.reconcileAccountWithFreeze(ctx, binding, settings, now, false)
}

func (e *Engine) reconcileAccountWithFreeze(ctx context.Context, binding *model.ResolvedBinding, settings model.Settings, now time.Time, automationFrozen bool) error {
	control := binding.Control
	control.AccountID = binding.Account.ID
	monitorID := bindingMonitorID(binding)
	if monitorID != nil {
		control.MonitorID = monitorID
	}
	actual := binding.Account.Schedulable
	effectiveDryRun := settings.DryRun || settings.HealthMode == model.HealthModeObserve

	adaptive := settings.HealthMode == model.HealthModeAdaptive
	if adaptive {
		e.advanceAdaptiveHealthControl(ctx, binding, &control, settings, now)
	} else if settings.HealthMode == model.HealthModeLegacy {
		healthUsable := binding.State == "bound" && binding.Monitor != nil && binding.MonitorState.Phase != model.PhaseFrozen && binding.MonitorState.Phase != model.PhaseUnknown
		if healthUsable && binding.MonitorState.Phase == model.PhaseUnhealthy && binding.MonitorState.UnhealthyStreak >= binding.FailureThreshold && accountCanBeManaged(binding.Account, now) {
			control.HealthLocked = true
		}
		if healthUsable && binding.MonitorState.Phase == model.PhaseHealthy && binding.MonitorState.HealthyStreak >= binding.RecoveryThreshold && accountCanBeManaged(binding.Account, now) {
			if control.HealthLocked {
				control.HealthLocked = false
				if control.FlapActive {
					clearFlapState(&control)
					e.record(ctx, flapClearedEvent(&binding.Account.ID, monitorID, "system", "连续正常检测达到锁定门槛，健康锁已解除", now))
				}
			}
		}
	}

	automaticLocked := control.HealthLocked || control.BalanceLocked || control.CostLocked
	anyLocked := automaticLocked || control.ManualLocked
	control.LastObserved = boolPtr(actual)
	if adaptive {
		if err := e.reconcileAdaptiveLoadWithFreeze(ctx, binding, &control, settings, now, automationFrozen); err != nil {
			return err
		}
	}

	if control.OwnsPause && actual {
		if control.ManualLocked && !automaticLocked {
			control.ManualLocked = false
			control.OwnsPause = false
			control.Owner = ""
			control.ExpectedSchedulable = boolPtr(true)
			control.ManualOverrideUntil = nil
			control.LastDecision = "manual_pause_reversed"
			event := model.Event{Type: "manual_resume_confirmed", Severity: "info", MonitorID: monitorID, AccountID: &binding.Account.ID, Message: "检测到后台人工开启，已释放管理端暂停归属", Actor: "system", CreatedAt: now}
			return e.store.CommitControlEvents(ctx, control, event)
		}
		if automaticLocked {
			if control.ManualOverrideUntil == nil {
				until := now.Add(time.Duration(settings.ManualHoldMinutes) * time.Minute)
				control.ManualOverrideUntil = &until
				control.LastDecision = "manual_override"
				eventType := "manual_override"
				if control.BalanceLocked {
					eventType = "balance_manual_override"
				} else if control.CostLocked {
					eventType = "cost_manual_override"
				}
				e.record(ctx, model.Event{Type: eventType, Severity: "warning", MonitorID: monitorID, AccountID: &binding.Account.ID, Message: "检测到人工开启，进入保护期", Actor: "system", Details: mustJSON(map[string]any{"until": until, "balance_locked": control.BalanceLocked, "health_locked": control.HealthLocked, "cost_locked": control.CostLocked})})
				return e.store.UpsertControl(ctx, control)
			}
			if now.Before(*control.ManualOverrideUntil) {
				return e.store.UpsertControl(ctx, control)
			}
			if automationFrozen {
				return e.persistFrozenAutomaticAction(ctx, binding, &control, "pause", "人工保护到期且控制锁仍未解除", now)
			}
			return e.applyPause(ctx, binding, &control, effectiveDryRun, "人工保护到期且控制锁仍未解除", now)
		}
		control.OwnsPause = false
		control.Owner = ""
		control.ExpectedSchedulable = boolPtr(true)
		control.ManualOverrideUntil = nil
		control.LastDecision = "manual_resume_confirmed"
		return e.store.UpsertControl(ctx, control)
	}

	if anyLocked {
		if !actual {
			return e.store.UpsertControl(ctx, control)
		}
		if control.ManualOverrideUntil != nil && now.Before(*control.ManualOverrideUntil) {
			return e.store.UpsertControl(ctx, control)
		}
		reason := "账号存在有效控制锁"
		if control.CostLocked && (control.BalanceLocked || control.HealthLocked) {
			reason = "账号同时受到健康、余额或倍率策略限制"
		} else if control.BalanceLocked && control.HealthLocked {
			reason = "余额不足且渠道连续异常"
		} else if control.BalanceLocked {
			reason = "上游余额连续低于停用阈值"
		} else if control.HealthLocked {
			reason = "上游连续检测异常"
		} else if control.CostLocked {
			reason = "倍率池已有更低成本的可用上游，该账号转入待命"
		}
		if automationFrozen {
			return e.persistFrozenAutomaticAction(ctx, binding, &control, "pause", reason, now)
		}
		return e.applyPause(ctx, binding, &control, effectiveDryRun, reason, now)
	}

	control.ManualOverrideUntil = nil
	if control.OwnsPause && !actual && accountCanBeManaged(binding.Account, now) {
		if automationFrozen {
			return e.persistFrozenAutomaticAction(ctx, binding, &control, "resume", "全部控制锁均已解除", now)
		}
		return e.applyResume(ctx, binding, &control, effectiveDryRun, "全部控制锁均已解除", now)
	}
	if control.LastDecision != "" {
		control.LastDecision = ""
	}
	return e.store.UpsertControl(ctx, control)
}

func (e *Engine) persistFrozenAutomaticAction(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, action, reason string, now time.Time) error {
	decision := "automation_frozen_" + action
	if control.LastDecision == decision {
		return e.store.UpsertControl(ctx, *control)
	}
	control.LastDecision = decision
	event := model.Event{Type: "automation_write_blocked", Severity: "warning", MonitorID: bindingMonitorID(binding),
		AccountID: &binding.Account.ID, Message: "全部自动化冻结已阻止账号写入", Actor: "system", CreatedAt: now,
		Details: mustJSON(map[string]any{"action": action, "reason": reason})}
	if err := e.store.CommitControlEvents(ctx, *control, event); err != nil {
		return err
	}
	e.logEvent(event)
	return nil
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
	return e.reconcileAdaptiveLoadWithFreeze(ctx, binding, control, settings, now, false)
}

func (e *Engine) reconcileAdaptiveLoadWithFreeze(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, settings model.Settings, now time.Time, automationFrozen bool) error {
	if binding.State != "bound" || binding.Monitor == nil || control.ManualLocked {
		return nil
	}
	if control.LoadPinValue != nil || control.LoadPinUntil != nil {
		if !loadPinActive(*control, now) {
			before := cloneIntPointer(control.LoadPinValue)
			clearLoadPin(control)
			if control.LoadStage == "" {
				control.LoadStage = model.HealthStageHealthy
			}
			event := model.Event{Type: "load_pin_expired", Severity: "info", MonitorID: bindingMonitorID(binding),
				AccountID: &binding.Account.ID, Message: "账号负载固定已到期，恢复健康调度控制", BeforeState: formatLoadFactor(before),
				Actor: "system", CreatedAt: now}
			if err := e.store.CommitControlEvents(ctx, *control, event); err != nil {
				return err
			}
			e.logEvent(event)
		} else if !sameIntPointer(binding.Account.LoadFactor, control.LoadPinValue) {
			until := now.Add(time.Duration(settings.HealthLoadOverrideMinutes) * time.Minute)
			before := cloneIntPointer(control.LoadPinValue)
			clearLoadPin(control)
			control.OwnsLoadFactor = false
			control.OriginalLoadFactor = nil
			control.ExpectedLoadFactor = nil
			control.LoadOverrideUntil = &until
			control.LoadStage = "manual_override"
			event := model.Event{Type: "load_pin_overridden", Severity: "warning", MonitorID: bindingMonitorID(binding),
				AccountID: &binding.Account.ID, Message: "检测到后台人工修改负载，已解除负载固定", BeforeState: formatLoadFactor(before),
				AfterState: formatLoadFactor(binding.Account.LoadFactor), Actor: "system", CreatedAt: now, Details: mustJSON(map[string]any{"until": until})}
			if err := e.store.CommitControlEvents(ctx, *control, event); err != nil {
				return err
			}
			e.logEvent(event)
			return nil
		} else {
			// A valid pin owns the actual load value. Health state may continue to
			// evolve, but it cannot overwrite this value until the deadline.
			return nil
		}
	}
	if control.OwnsLoadFactor && !sameIntPointer(binding.Account.LoadFactor, control.ExpectedLoadFactor) {
		until := now.Add(time.Duration(settings.HealthLoadOverrideMinutes) * time.Minute)
		control.OwnsLoadFactor = false
		control.OriginalLoadFactor = nil
		control.ExpectedLoadFactor = nil
		control.LoadOverrideUntil = &until
		control.LoadStage = "manual_override"
		e.record(ctx, model.Event{Type: "manual_load_override", Severity: "warning", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: "检测到后台人工修改负载，调度器已暂停负载控制", Details: mustJSON(map[string]any{"until": until}), Actor: "system", CreatedAt: now})
		return nil
	}
	if !binding.Account.Schedulable && !control.OwnsPause {
		return nil
	}
	if control.LoadOverrideUntil != nil {
		if now.Before(*control.LoadOverrideUntil) {
			return nil
		}
		control.LoadOverrideUntil = nil
		control.LoadStage = binding.HealthState.Stage
	}

	var desired *int
	restore := false
	switch control.LoadStage {
	case model.HealthStageLimited80:
		desired = desiredLoadFactor(binding.Account, control, 80)
	case model.HealthStageLimited50, model.HealthStageDegraded:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthDegradedPercent)
	case model.HealthStageLimited25:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthTrialPercent)
	case model.HealthStageRecovering25:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthTrialPercent)
	case model.HealthStageRecovering50:
		desired = desiredLoadFactor(binding.Account, control, settings.HealthMidPercent)
	case model.HealthStageRecovering80:
		desired = desiredLoadFactor(binding.Account, control, 80)
	case model.HealthStageHealthy:
		if control.OwnsLoadFactor {
			desired = cloneIntPointer(control.OriginalLoadFactor)
			restore = true
		}
	default:
		return nil
	}
	if desired == nil && !restore {
		return nil
	}
	if sameIntPointer(binding.Account.LoadFactor, desired) {
		if restore {
			return e.completeLoadRecovery(ctx, binding, control, now, binding.Account.LoadFactor, desired)
		}
		return nil
	}
	if settings.DryRun {
		eventType := "would_reduce_load"
		message := "智能判定建议降低账号负载"
		if restore {
			eventType = "would_restore_load"
			message = "智能判定建议恢复账号原始负载"
		}
		if control.LastDecision != eventType {
			control.LastDecision = eventType
			e.record(ctx, model.Event{Type: eventType, Severity: "info", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: message, BeforeState: formatLoadFactor(binding.Account.LoadFactor), AfterState: formatLoadFactor(desired), Details: actionDetails(binding, control, message), Actor: "system", CreatedAt: now})
		}
		return nil
	}
	if automationFrozen {
		return e.persistFrozenAutomaticAction(ctx, binding, control, "set_load_factor", "健康调度拟调整账号负载", now)
	}
	if !control.OwnsLoadFactor {
		control.OwnsLoadFactor = true
		control.OriginalLoadFactor = cloneIntPointer(binding.Account.LoadFactor)
	}
	beforeLoad := cloneIntPointer(binding.Account.LoadFactor)
	updated, err := e.api.UpdateLoadFactor(ctx, binding.Account.ID, desired)
	if err != nil {
		return err
	}
	if !sameIntPointer(updated.LoadFactor, desired) {
		return errors.New("Sub2API 未确认账号负载系数更新")
	}
	binding.Account = updated
	control.ExpectedLoadFactor = cloneIntPointer(desired)
	control.LastActionAt = &now
	if restore {
		if err := e.completeLoadRecovery(ctx, binding, control, now, beforeLoad, desired); err != nil {
			return e.rollbackLoadFactor(ctx, binding, beforeLoad, err)
		}
		return nil
	}
	event := model.Event{Type: "load_factor_adjusted", Severity: "warning", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: "账号负载已按渠道健康状态调整", BeforeState: formatLoadFactor(beforeLoad), AfterState: formatLoadFactor(desired), Details: actionDetails(binding, control, "第三版建议负载"), Actor: "system", CreatedAt: now}
	if err := e.store.CommitControlEvents(ctx, *control, event); err != nil {
		return e.rollbackLoadFactor(ctx, binding, beforeLoad, err)
	}
	e.logEvent(event)
	return nil
}

func loadPinActive(control model.AccountControl, now time.Time) bool {
	return control.LoadPinValue != nil && control.LoadPinUntil != nil && now.Before(control.LoadPinUntil.UTC())
}

func clearLoadPin(control *model.AccountControl) {
	control.LoadPinValue = nil
	control.LoadPinUntil = nil
	control.LoadPinOwner = ""
	control.LoadPinReason = ""
}

func (e *Engine) rollbackLoadFactor(ctx context.Context, binding *model.ResolvedBinding, previous *int, cause error) error {
	rolledBack, rollbackErr := e.api.UpdateLoadFactor(ctx, binding.Account.ID, previous)
	if rollbackErr != nil {
		return fmt.Errorf("保存负载控制状态失败: %w; 回滚负载失败: %v", cause, rollbackErr)
	}
	if !sameIntPointer(rolledBack.LoadFactor, previous) {
		return fmt.Errorf("保存负载控制状态失败: %w; Sub2API 未确认负载回滚", cause)
	}
	binding.Account = rolledBack
	return cause
}

func (e *Engine) completeLoadRecovery(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, now time.Time, before, after *int) error {
	details := actionDetails(binding, control, "分阶段恢复完成")
	control.OwnsLoadFactor = false
	control.OriginalLoadFactor = nil
	control.ExpectedLoadFactor = nil
	control.LoadOverrideUntil = nil
	control.LoadStage = model.HealthStageHealthy
	control.RecoveryStep = 0
	control.RecoveryStartedAt = nil
	wasFlapProtected := control.FlapActive
	clearFlapState(control)
	events := []model.Event{{Type: "load_factor_restored", Severity: "info", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: "账号已完成分阶段恢复并还原原始负载", BeforeState: formatLoadFactor(before), AfterState: formatLoadFactor(after), Details: details, Actor: "system", CreatedAt: now}}
	if wasFlapProtected {
		events = append(events, flapClearedEvent(&binding.Account.ID, bindingMonitorID(binding), "system", "账号已完成分阶段恢复", now))
	}
	if err := e.store.CommitControlEvents(ctx, *control, events...); err != nil {
		return err
	}
	for _, event := range events {
		e.logEvent(event)
	}
	return nil
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
		if control.LastDecision != "would_pause" {
			control.LastDecision = "would_pause"
			e.record(ctx, model.Event{Type: "would_pause", Severity: "warning", MonitorID: monitorID, AccountID: &binding.Account.ID, Message: reason, BeforeState: "schedulable", AfterState: "paused", Details: actionDetails(binding, control, reason), Actor: "system"})
		}
		return e.store.UpsertControl(ctx, *control)
	}
	updated, err := e.api.SetSchedulable(ctx, binding.Account.ID, false)
	if err != nil {
		return err
	}
	if updated.Schedulable {
		return errors.New("Sub2API 未确认账号暂停")
	}
	control.OwnsPause = true
	control.Owner = pauseOwner(*control)
	control.ExpectedSchedulable = boolPtr(false)
	control.LastObserved = boolPtr(false)
	control.ManualOverrideUntil = nil
	control.LastDecision = "paused"
	control.LastActionAt = &now
	event := model.Event{Type: eventType, Severity: "warning", MonitorID: monitorID, AccountID: &binding.Account.ID, Message: reason, BeforeState: "schedulable", AfterState: "paused", Details: actionDetails(binding, control, reason), Actor: "system", CreatedAt: now}
	if !control.HealthLocked {
		if err := e.store.CommitControlEvents(ctx, *control, event); err != nil {
			return err
		}
		e.logEvent(event)
		return nil
	}
	flap := model.FlapPolicy{
		Enabled: binding.FlapEnabled, WindowMinutes: binding.FlapWindowMinutes,
		PauseThreshold:    binding.FlapPauseThreshold,
		RecoveryThreshold: maxInt(binding.BaseRecoveryThreshold, binding.FlapRecoveryThreshold),
	}
	updatedControl, _, activated, err := e.store.CommitAutomaticPause(ctx, *control, event, flap)
	if err != nil {
		return err
	}
	*control = updatedControl
	e.logEvent(event)
	if activated {
		e.logEvent(model.Event{Type: "flap_protection_activated", Severity: "warning", MonitorID: monitorID, AccountID: &binding.Account.ID, Message: "账号在滚动窗口内反复暂停，已启用抖动保护", Actor: "scheduler"})
	}
	return nil
}

func (e *Engine) applyResume(ctx context.Context, binding *model.ResolvedBinding, control *model.AccountControl, dryRun bool, reason string, now time.Time) error {
	if !control.OwnsPause {
		return nil
	}
	if dryRun {
		if control.LastDecision != "would_resume" {
			control.LastDecision = "would_resume"
			e.record(ctx, model.Event{Type: "would_resume", Severity: "info", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: reason, BeforeState: "paused", AfterState: "schedulable", Details: actionDetails(binding, control, reason), Actor: "system"})
		}
		return e.store.UpsertControl(ctx, *control)
	}
	updated, err := e.api.SetSchedulable(ctx, binding.Account.ID, true)
	if err != nil {
		return err
	}
	if !updated.Schedulable {
		return errors.New("Sub2API 未确认账号恢复")
	}
	previousOwner := control.Owner
	control.OwnsPause = false
	control.Owner = ""
	control.ManualLocked = false
	control.ExpectedSchedulable = boolPtr(true)
	control.LastObserved = boolPtr(true)
	control.ManualOverrideUntil = nil
	control.LastDecision = "resumed"
	control.LastActionAt = &now
	wasFlapProtected := control.FlapActive
	clearFlapState(control)
	eventType := "automatic_resume"
	if previousOwner == "balance" {
		eventType = "balance_resume"
	} else if previousOwner == "cost" {
		eventType = "cost_resume"
	}
	events := []model.Event{{Type: eventType, Severity: "info", MonitorID: bindingMonitorID(binding), AccountID: &binding.Account.ID, Message: reason, BeforeState: "paused", AfterState: "schedulable", Details: actionDetails(binding, control, reason), Actor: "system", CreatedAt: now}}
	if wasFlapProtected {
		events = append(events, flapClearedEvent(&binding.Account.ID, bindingMonitorID(binding), "system", "连续正常检测达到锁定门槛并恢复账号", now))
	}
	if err := e.store.CommitControlEvents(ctx, *control, events...); err != nil {
		return err
	}
	for _, event := range events {
		e.logEvent(event)
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

func pauseOwner(control model.AccountControl) string {
	if control.ManualLocked {
		return "operator"
	}
	lockCount := 0
	if control.HealthLocked {
		lockCount++
	}
	if control.BalanceLocked {
		lockCount++
	}
	if control.CostLocked {
		lockCount++
	}
	if lockCount > 1 {
		return "combined"
	}
	if control.BalanceLocked {
		return "balance"
	}
	if control.CostLocked {
		return "cost"
	}
	return "automatic"
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
