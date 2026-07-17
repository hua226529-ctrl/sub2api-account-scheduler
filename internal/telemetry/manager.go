package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
)

type API interface {
	ListMonitors(context.Context) ([]model.Monitor, error)
	ListSuccessfulRequests(context.Context, sub2api.TelemetryQuery) ([]model.TrafficSuccess, error)
	ListRequestErrors(context.Context, sub2api.TelemetryQuery) ([]model.TrafficError, error)
	ListMonitorHistory(context.Context, int64, sub2api.TelemetryQuery) ([]model.MonitorHistoryRecord, error)
}

type Store interface {
	InsertMonitorHistoryBatch(context.Context, []model.MonitorHistoryRecord) (bool, error)
	InsertTrafficBatch(context.Context, []model.TrafficSuccess, []model.TrafficError) (int, error)
	GetModelTrafficWindow(context.Context, int64, string, time.Time, time.Time) (model.TrafficWindow, error)
	UpsertAccountModelCapability(context.Context, model.AccountModelCapability) error
	DeleteTelemetryBefore(context.Context, time.Time, time.Time, time.Time) error
	AddEvent(context.Context, model.Event) error
}

type ReconcileRequester interface {
	RequestAccounts(...int64)
	RequestFull()
}

type MonitorAccountResolver interface {
	AccountIDsForMonitors(...int64) []int64
}

type sourcedReconcileRequester interface {
	RequestAccountsFrom(string, ...int64)
	RequestFullFrom(string)
}

type Option func(*Manager)

func WithReconcileRequester(requester ReconcileRequester) Option {
	return func(manager *Manager) { manager.requester = requester }
}

func WithMonitorAccountResolver(resolver MonitorAccountResolver) Option {
	return func(manager *Manager) { manager.resolver = resolver }
}

type TelemetryError struct {
	Code string
	Err  error
}

func (e *TelemetryError) Error() string {
	if e == nil || e.Err == nil {
		return e.Code
	}
	return e.Code + ": " + e.Err.Error()
}

func (e *TelemetryError) Unwrap() error { return e.Err }

type MonitorIssue struct {
	Code      string
	MonitorID int64
	Err       error
}

type PartialError struct {
	Issues []MonitorIssue
}

func (e *PartialError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "telemetry_partial_success"
	}
	return "telemetry_partial_success: " + e.Issues[0].Code
}

func (e *PartialError) Code() string { return "telemetry_partial_success" }

// Manager imports evidence without holding a mutex across network or SQLite
// work. The atomic running bit only prevents overlapping rounds.
type Manager struct {
	api      API
	store    Store
	interval time.Duration
	logger   *slog.Logger

	requester ReconcileRequester
	resolver  MonitorAccountResolver

	mu              sync.Mutex
	lastSuccessAt   time.Time
	lastError       string
	lastMonitorKeys map[int64]time.Time
	running         atomic.Bool
}

func (m *Manager) Status() (*time.Time, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var last *time.Time
	if !m.lastSuccessAt.IsZero() {
		value := m.lastSuccessAt
		last = &value
	}
	return last, m.lastError
}

func NewManager(api API, store Store, interval time.Duration, logger *slog.Logger, options ...Option) *Manager {
	if interval < time.Minute {
		interval = 2 * time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	manager := &Manager{api: api, store: store, interval: interval, logger: logger, lastMonitorKeys: make(map[int64]time.Time)}
	for _, option := range options {
		if option != nil {
			option(manager)
		}
	}
	return manager
}

func (m *Manager) Start(ctx context.Context) {
	go func() {
		m.runLogged(ctx)
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.runLogged(ctx)
			}
		}
	}()
}

func (m *Manager) RunOnce(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return &TelemetryError{Code: "telemetry_run_in_progress"}
	}
	defer m.running.Store(false)

	now := time.Now().UTC()
	trafficSince := now.Add(-time.Hour)
	m.mu.Lock()
	lastSuccessAt := m.lastSuccessAt
	cursors := make(map[int64]time.Time, len(m.lastMonitorKeys))
	for id, cursor := range m.lastMonitorKeys {
		cursors[id] = cursor
	}
	m.mu.Unlock()
	if !lastSuccessAt.IsZero() {
		trafficSince = lastSuccessAt.Add(-2 * time.Minute)
	}
	query := sub2api.TelemetryQuery{Since: trafficSince, Until: now, PageSize: 200}
	successes, err := m.api.ListSuccessfulRequests(ctx, query)
	if err != nil {
		return &TelemetryError{Code: "traffic_fetch_failed", Err: fmt.Errorf("读取真实请求记录失败: %w", err)}
	}
	failures, err := m.api.ListRequestErrors(ctx, query)
	if err != nil {
		return &TelemetryError{Code: "traffic_fetch_failed", Err: fmt.Errorf("读取真实请求错误失败: %w", err)}
	}
	trafficInserted, err := m.store.InsertTrafficBatch(ctx, successes, failures)
	if err != nil {
		return &TelemetryError{Code: "monitor_store_failed", Err: fmt.Errorf("保存真实请求证据失败: %w", err)}
	}
	triggerAccounts := make(map[int64]struct{})
	if trafficInserted > 0 {
		for _, item := range successes {
			if item.AccountID > 0 {
				triggerAccounts[item.AccountID] = struct{}{}
			}
		}
		for _, item := range failures {
			if item.AccountID > 0 {
				triggerAccounts[item.AccountID] = struct{}{}
			}
		}
	}

	monitors, err := m.api.ListMonitors(ctx)
	if err != nil {
		return &TelemetryError{Code: "monitor_fetch_failed", Err: fmt.Errorf("读取监控列表失败: %w", err)}
	}
	type monitorResult struct {
		monitorID int64
		items     []model.MonitorHistoryRecord
		inserted  bool
		issue     *MonitorIssue
	}
	results := make(chan monitorResult, len(monitors))
	jobs := make(chan model.Monitor)
	workerCount := len(monitors)
	if workerCount > 4 {
		workerCount = 4
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case monitor, ok := <-jobs:
					if !ok {
						return
					}
					since := now.Add(-24 * time.Hour)
					if cursor := cursors[monitor.ID]; !cursor.IsZero() {
						since = cursor.Add(-2 * time.Minute)
					}
					items, historyErr := m.api.ListMonitorHistory(ctx, monitor.ID, sub2api.TelemetryQuery{Since: since, Until: now, PageSize: 200})
					if historyErr != nil {
						results <- monitorResult{monitorID: monitor.ID, issue: &MonitorIssue{Code: "monitor_fetch_failed", MonitorID: monitor.ID, Err: historyErr}}
						continue
					}
					valid := true
					for _, item := range items {
						if (item.MonitorID != 0 && item.MonitorID != monitor.ID) || item.CheckedAt.IsZero() {
							valid = false
							break
						}
					}
					if !valid {
						results <- monitorResult{monitorID: monitor.ID, issue: &MonitorIssue{Code: "monitor_history_invalid", MonitorID: monitor.ID, Err: errors.New("monitor history contains an invalid monitor id or timestamp")}}
						continue
					}
					inserted, storeErr := m.store.InsertMonitorHistoryBatch(ctx, items)
					if storeErr != nil {
						results <- monitorResult{monitorID: monitor.ID, issue: &MonitorIssue{Code: "monitor_store_failed", MonitorID: monitor.ID, Err: storeErr}}
						continue
					}
					results <- monitorResult{monitorID: monitor.ID, items: items, inserted: inserted}
				}
			}
		}()
	}
	for _, monitor := range monitors {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return ctx.Err()
		case jobs <- monitor:
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	issues := make([]MonitorIssue, 0)
	for result := range results {
		if result.issue != nil {
			issues = append(issues, *result.issue)
			continue
		}
		if result.inserted && m.resolver != nil {
			for _, accountID := range m.resolver.AccountIDsForMonitors(result.monitorID) {
				if accountID > 0 {
					triggerAccounts[accountID] = struct{}{}
				}
			}
		}
		m.mu.Lock()
		cursor := cursors[result.monitorID]
		for _, item := range result.items {
			if item.CheckedAt.After(cursor) {
				cursor = item.CheckedAt
			}
		}
		if !cursor.IsZero() {
			m.lastMonitorKeys[result.monitorID] = cursor
		}
		m.mu.Unlock()
	}

	if err := m.refreshCapabilities(ctx, successes, failures, now); err != nil {
		issues = append(issues, MonitorIssue{Code: "monitor_store_failed", Err: err})
	}
	if err := m.store.DeleteTelemetryBefore(ctx, now.Add(-14*24*time.Hour), now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour)); err != nil {
		issues = append(issues, MonitorIssue{Code: "monitor_store_failed", Err: fmt.Errorf("清理过期证据失败: %w", err)})
	}
	if trafficInserted > 0 || len(issues) < len(monitors) {
		m.mu.Lock()
		m.lastSuccessAt = now
		m.mu.Unlock()
	}
	if len(triggerAccounts) > 0 && m.requester != nil {
		ids := make([]int64, 0, len(triggerAccounts))
		for accountID := range triggerAccounts {
			ids = append(ids, accountID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if sourced, ok := m.requester.(sourcedReconcileRequester); ok {
			sourced.RequestAccountsFrom("telemetry", ids...)
		} else {
			m.requester.RequestAccounts(ids...)
		}
	}
	if len(issues) > 0 {
		sort.Slice(issues, func(i, j int) bool {
			if issues[i].MonitorID != issues[j].MonitorID {
				return issues[i].MonitorID < issues[j].MonitorID
			}
			return issues[i].Code < issues[j].Code
		})
		return &PartialError{Issues: issues}
	}
	return nil
}

func (m *Manager) refreshCapabilities(ctx context.Context, successes []model.TrafficSuccess, failures []model.TrafficError, now time.Time) error {
	type key struct {
		accountID int64
		model     string
	}
	keys := make(map[key]struct{})
	lastErrors := make(map[key]model.TrafficError)
	for _, item := range successes {
		if item.AccountID > 0 && item.Model != "" {
			keys[key{item.AccountID, item.Model}] = struct{}{}
		}
	}
	for _, item := range failures {
		if item.AccountID <= 0 || item.Model == "" {
			continue
		}
		current := key{item.AccountID, item.Model}
		keys[current] = struct{}{}
		if previous, ok := lastErrors[current]; !ok || item.CreatedAt.After(previous.CreatedAt) {
			lastErrors[current] = item
		}
	}
	ordered := make([]key, 0, len(keys))
	for current := range keys {
		ordered = append(ordered, current)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].accountID == ordered[j].accountID {
			return ordered[i].model < ordered[j].model
		}
		return ordered[i].accountID < ordered[j].accountID
	})
	for _, current := range ordered {
		window, err := m.store.GetModelTrafficWindow(ctx, current.accountID, current.model, now.Add(-24*time.Hour), now)
		if err != nil {
			return fmt.Errorf("汇总账号 %d 模型 %s 能力失败: %w", current.accountID, current.model, err)
		}
		last := lastErrors[current]
		observedAt := last.CreatedAt
		if observedAt.IsZero() {
			observedAt = now
		}
		capability := model.AccountModelCapability{AccountID: current.accountID, Model: current.model,
			Supported: window.SuccessCount > 0 || window.CapabilityErrors == 0, SuccessCount: window.SuccessCount,
			FailureCount: window.CapabilityErrors, LastErrorClass: last.ErrorClass, LastReasonCode: last.ReasonCode,
			LastObservedAt: observedAt, UpdatedAt: now}
		if err := m.store.UpsertAccountModelCapability(ctx, capability); err != nil {
			return fmt.Errorf("保存账号 %d 模型 %s 能力失败: %w", current.accountID, current.model, err)
		}
	}
	return nil
}

func (m *Manager) runLogged(ctx context.Context) {
	err := m.RunOnce(ctx)
	if err != nil {
		message := err.Error()
		m.logger.Error("telemetry_sync_failed", "error", err)
		m.mu.Lock()
		changed := message != m.lastError
		m.lastError = message
		m.mu.Unlock()
		if changed {
			eventType := "telemetry_sync_failed"
			var partial *PartialError
			if errors.As(err, &partial) {
				eventType = "telemetry_partial_success"
			}
			_ = m.store.AddEvent(context.Background(), model.Event{Type: eventType, Severity: "error", Message: message, Actor: "system"})
		}
		return
	}
	m.mu.Lock()
	recovered := m.lastError != ""
	m.lastError = ""
	m.mu.Unlock()
	if recovered {
		_ = m.store.AddEvent(context.Background(), model.Event{Type: "telemetry_sync_recovered", Severity: "info", Message: "监控历史与真实流量证据读取已恢复", Actor: "system"})
	}
	m.logger.Info("telemetry_sync_complete")
}
