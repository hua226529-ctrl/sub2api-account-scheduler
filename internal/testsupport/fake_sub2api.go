package testsupport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
)

const (
	CallListAccounts        = "list_accounts"
	CallListMonitors        = "list_monitors"
	CallSetSchedulable      = "set_schedulable"
	CallUpdateLoadFactor    = "update_load_factor"
	CallListSuccessful      = "list_successful_requests"
	CallListErrors          = "list_request_errors"
	CallListMonitorHistory  = "list_monitor_history"
	CallTransitionGroupTier = "transition_group_tier"
	CallReadGroupTier       = "read_group_tier"
)

var ErrInjected = errors.New("fake Sub2API injected failure")

type Call struct {
	Sequence int64
	Name     string
	Resource string
	Number   int
}

type Failure struct {
	AtCall           int
	Err              error
	ApplyBeforeError bool
	Always           bool
}

type CallStats struct {
	Total         int64
	ByName        map[string]int
	Order         []Call
	MaxConcurrent int
}

type FakeSub2API struct {
	mu sync.Mutex

	accounts        []model.Account
	visibleAccounts []model.Account
	monitors        []model.Monitor
	successes       []model.TrafficSuccess
	failures        []model.TrafficError
	history         map[int64][]model.MonitorHistoryRecord
	transitions     map[string]model.GroupTierTransition
	groupTiers      map[string]string

	delay           time.Duration
	delays          map[string]time.Duration
	failure         map[string]Failure
	resourceFailure map[string]Failure
	staleReads      bool
	callCount       map[string]int
	order           []Call
	sequence        int64
	active          int
	maxActive       int
	beforeCall      func(Call)
}

func NewFakeSub2API(fixture Fixture) *FakeSub2API {
	return &FakeSub2API{
		accounts: cloneAccounts(fixture.Accounts), visibleAccounts: cloneAccounts(fixture.Accounts),
		monitors: cloneMonitors(fixture.Monitors), successes: append([]model.TrafficSuccess(nil), fixture.Successes...),
		failures: append([]model.TrafficError(nil), fixture.Failures...), history: cloneHistory(fixture.History),
		transitions: make(map[string]model.GroupTierTransition), groupTiers: make(map[string]string),
		delays: make(map[string]time.Duration), failure: make(map[string]Failure), resourceFailure: make(map[string]Failure),
		callCount: make(map[string]int),
	}
}

func (f *FakeSub2API) SetDelay(delay time.Duration) {
	f.mu.Lock()
	f.delay = delay
	f.mu.Unlock()
}

func (f *FakeSub2API) SetCallDelay(name string, delay time.Duration) {
	f.mu.Lock()
	f.delays[name] = delay
	f.mu.Unlock()
}

func (f *FakeSub2API) SetFailure(name string, failure Failure) {
	f.mu.Lock()
	f.failure[name] = failure
	f.mu.Unlock()
}

func (f *FakeSub2API) SetResourceFailure(name, resource string, failure Failure) {
	f.mu.Lock()
	f.resourceFailure[name+":"+resource] = failure
	f.mu.Unlock()
}

func (f *FakeSub2API) SetStaleReads(stale bool) {
	f.mu.Lock()
	f.staleReads = stale
	if !stale {
		f.visibleAccounts = cloneAccounts(f.accounts)
	}
	f.mu.Unlock()
}

func (f *FakeSub2API) SetAccountRateLimit(accountID int64, until *time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, _ = updateAccount(f.accounts, accountID, func(account *model.Account) { account.RateLimitResetAt = cloneTime(until) })
	_, _ = updateAccount(f.visibleAccounts, accountID, func(account *model.Account) { account.RateLimitResetAt = cloneTime(until) })
}

func (f *FakeSub2API) SetBeforeCall(hook func(Call)) {
	f.mu.Lock()
	f.beforeCall = hook
	f.mu.Unlock()
}

func (f *FakeSub2API) ResetStats() {
	f.mu.Lock()
	f.callCount = make(map[string]int)
	f.order = nil
	f.sequence = 0
	f.active = 0
	f.maxActive = 0
	f.mu.Unlock()
}

func (f *FakeSub2API) Stats() CallStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := make(map[string]int, len(f.callCount))
	for name, count := range f.callCount {
		counts[name] = count
	}
	return CallStats{Total: f.sequence, ByName: counts, Order: append([]Call(nil), f.order...), MaxConcurrent: f.maxActive}
}

func (f *FakeSub2API) begin(ctx context.Context, name, resource string) (Call, Failure, error) {
	f.mu.Lock()
	f.sequence++
	f.callCount[name]++
	call := Call{Sequence: f.sequence, Name: name, Resource: resource, Number: f.callCount[name]}
	f.order = append(f.order, call)
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	delay := f.delay
	if configured, ok := f.delays[name]; ok {
		delay = configured
	}
	failure := f.failure[name]
	if configured, ok := f.resourceFailure[name+":"+resource]; ok {
		failure = configured
	}
	hook := f.beforeCall
	f.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return call, failure, ctx.Err()
		case <-timer.C:
		}
	}
	return call, failure, nil
}

func (f *FakeSub2API) finish() {
	f.mu.Lock()
	f.active--
	f.mu.Unlock()
}

func failureFor(call Call, failure Failure) error {
	if !failure.Always && (failure.AtCall <= 0 || failure.AtCall != call.Number) {
		return nil
	}
	if failure.Err != nil {
		return failure.Err
	}
	return ErrInjected
}

func (f *FakeSub2API) ListAccounts(ctx context.Context) ([]model.Account, error) {
	call, failure, err := f.begin(ctx, CallListAccounts, "accounts")
	defer f.finish()
	if err != nil {
		return nil, err
	}
	if err = failureFor(call, failure); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staleReads {
		return cloneAccounts(f.visibleAccounts), nil
	}
	return cloneAccounts(f.accounts), nil
}

func (f *FakeSub2API) ListMonitors(ctx context.Context) ([]model.Monitor, error) {
	call, failure, err := f.begin(ctx, CallListMonitors, "monitors")
	defer f.finish()
	if err != nil {
		return nil, err
	}
	if err = failureFor(call, failure); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneMonitors(f.monitors), nil
}

func (f *FakeSub2API) SetSchedulable(ctx context.Context, accountID int64, value bool) (model.Account, error) {
	call, failure, err := f.begin(ctx, CallSetSchedulable, fmt.Sprint(accountID))
	defer f.finish()
	if err != nil {
		return model.Account{}, err
	}
	injected := failureFor(call, failure)
	if injected != nil && !failure.ApplyBeforeError {
		return model.Account{}, injected
	}
	f.mu.Lock()
	account, found := updateAccount(f.accounts, accountID, func(account *model.Account) { account.Schedulable = value })
	if found && !f.staleReads {
		_, _ = updateAccount(f.visibleAccounts, accountID, func(account *model.Account) { account.Schedulable = value })
	}
	f.mu.Unlock()
	if injected != nil {
		return model.Account{}, injected
	}
	if !found {
		return model.Account{}, fmt.Errorf("account %d not found", accountID)
	}
	return account, nil
}

func (f *FakeSub2API) UpdateLoadFactor(ctx context.Context, accountID int64, value *int) (model.Account, error) {
	call, failure, err := f.begin(ctx, CallUpdateLoadFactor, fmt.Sprint(accountID))
	defer f.finish()
	if err != nil {
		return model.Account{}, err
	}
	injected := failureFor(call, failure)
	if injected != nil && !failure.ApplyBeforeError {
		return model.Account{}, injected
	}
	f.mu.Lock()
	account, found := updateAccount(f.accounts, accountID, func(account *model.Account) { account.LoadFactor = cloneInt(value) })
	if found && !f.staleReads {
		_, _ = updateAccount(f.visibleAccounts, accountID, func(account *model.Account) { account.LoadFactor = cloneInt(value) })
	}
	f.mu.Unlock()
	if injected != nil {
		return model.Account{}, injected
	}
	if !found {
		return model.Account{}, fmt.Errorf("account %d not found", accountID)
	}
	return account, nil
}

func (f *FakeSub2API) ListSuccessfulRequests(ctx context.Context, _ sub2api.TelemetryQuery) ([]model.TrafficSuccess, error) {
	call, failure, err := f.begin(ctx, CallListSuccessful, "traffic")
	defer f.finish()
	if err != nil {
		return nil, err
	}
	if err = failureFor(call, failure); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.TrafficSuccess(nil), f.successes...), nil
}

func (f *FakeSub2API) ListRequestErrors(ctx context.Context, _ sub2api.TelemetryQuery) ([]model.TrafficError, error) {
	call, failure, err := f.begin(ctx, CallListErrors, "traffic")
	defer f.finish()
	if err != nil {
		return nil, err
	}
	if err = failureFor(call, failure); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.TrafficError(nil), f.failures...), nil
}

func (f *FakeSub2API) ListMonitorHistory(ctx context.Context, monitorID int64, _ sub2api.TelemetryQuery) ([]model.MonitorHistoryRecord, error) {
	call, failure, err := f.begin(ctx, CallListMonitorHistory, fmt.Sprint(monitorID))
	defer f.finish()
	if err != nil {
		return nil, err
	}
	if err = failureFor(call, failure); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.MonitorHistoryRecord(nil), f.history[monitorID]...), nil
}

// TransitionGroupTier is the fake equivalent of SwitchGroup. It supports
// idempotency, write-before-timeout failures and an explicit readback method.
func (f *FakeSub2API) TransitionGroupTier(ctx context.Context, request model.GroupTierTransitionRequest) (model.GroupTierTransition, error) {
	resource := fmt.Sprintf("%d:%s", request.SourceID, request.KeyID)
	call, failure, err := f.begin(ctx, CallTransitionGroupTier, resource)
	defer f.finish()
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	f.mu.Lock()
	if existing, ok := f.transitions[request.IdempotencyKey]; request.IdempotencyKey != "" && ok {
		f.mu.Unlock()
		return existing, nil
	}
	from := f.groupTiers[resource]
	if from == "" {
		from = model.GroupTierMain
	}
	transition := model.GroupTierTransition{
		ID: int64(len(f.transitions) + 1), IdempotencyKey: request.IdempotencyKey,
		SourceID: request.SourceID, KeyID: request.KeyID, FromTier: from, ToTier: request.TargetTier,
		FromGroupID: from, ToGroupID: request.TargetTier, Status: model.GroupTransitionCompleted,
		Actor: request.Actor, Reason: request.Reason, Manual: request.Manual, DryRun: request.DryRun,
		CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	completedAt := transition.CreatedAt
	transition.CompletedAt = &completedAt
	f.mu.Unlock()
	injected := failureFor(call, failure)
	if injected != nil && !failure.ApplyBeforeError {
		return model.GroupTierTransition{}, injected
	}
	f.mu.Lock()
	if !request.DryRun {
		f.groupTiers[resource] = request.TargetTier
	}
	if request.IdempotencyKey != "" {
		f.transitions[request.IdempotencyKey] = transition
	}
	f.mu.Unlock()
	if injected != nil {
		return model.GroupTierTransition{}, injected
	}
	return transition, nil
}

func (f *FakeSub2API) ReadGroupTier(ctx context.Context, sourceID int64, keyID string) (string, error) {
	resource := fmt.Sprintf("%d:%s", sourceID, keyID)
	call, failure, err := f.begin(ctx, CallReadGroupTier, resource)
	defer f.finish()
	if err != nil {
		return "", err
	}
	if err = failureFor(call, failure); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	tier := f.groupTiers[resource]
	if tier == "" {
		tier = model.GroupTierMain
	}
	return tier, nil
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func updateAccount(accounts []model.Account, id int64, update func(*model.Account)) (model.Account, bool) {
	for i := range accounts {
		if accounts[i].ID == id {
			update(&accounts[i])
			return cloneAccount(accounts[i]), true
		}
	}
	return model.Account{}, false
}

func cloneAccount(account model.Account) model.Account {
	account.LoadFactor = cloneInt(account.LoadFactor)
	if account.Credentials != nil {
		credentials := account.Credentials
		account.Credentials = make(map[string]any, len(credentials))
		for key, value := range credentials {
			account.Credentials[key] = value
		}
	}
	return account
}

func cloneAccounts(accounts []model.Account) []model.Account {
	copy := make([]model.Account, len(accounts))
	for i := range accounts {
		copy[i] = cloneAccount(accounts[i])
	}
	return copy
}

func cloneMonitors(monitors []model.Monitor) []model.Monitor {
	copy := append([]model.Monitor(nil), monitors...)
	for i := range copy {
		copy[i].ExtraModels = append([]model.MonitorModelStatus(nil), copy[i].ExtraModels...)
		copy[i].ExtraModelsStatus = append([]model.MonitorModelStatus(nil), copy[i].ExtraModelsStatus...)
	}
	return copy
}

func cloneHistory(history map[int64][]model.MonitorHistoryRecord) map[int64][]model.MonitorHistoryRecord {
	copy := make(map[int64][]model.MonitorHistoryRecord, len(history))
	for monitorID, items := range history {
		copy[monitorID] = append([]model.MonitorHistoryRecord(nil), items...)
	}
	return copy
}
