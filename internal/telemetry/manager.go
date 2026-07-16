package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
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

// Manager continuously imports read-only evidence from Sub2API. The scheduler
// can keep reconciling if this importer is unavailable; existing evidence and
// control locks remain untouched until a later successful poll.
type Manager struct {
	api      API
	store    Store
	interval time.Duration
	logger   *slog.Logger

	mu              sync.Mutex
	lastSuccessAt   time.Time
	lastError       string
	lastMonitorKeys map[int64]time.Time
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

func NewManager(api API, store Store, interval time.Duration, logger *slog.Logger) *Manager {
	if interval < time.Minute {
		interval = 2 * time.Minute
	}
	return &Manager{api: api, store: store, interval: interval, logger: logger, lastMonitorKeys: make(map[int64]time.Time)}
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
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	trafficSince := now.Add(-time.Hour)
	if !m.lastSuccessAt.IsZero() {
		trafficSince = m.lastSuccessAt.Add(-2 * time.Minute)
	}
	query := sub2api.TelemetryQuery{Since: trafficSince, Until: now, PageSize: 200}
	successes, err := m.api.ListSuccessfulRequests(ctx, query)
	if err != nil {
		return fmt.Errorf("读取真实请求记录失败: %w", err)
	}
	failures, err := m.api.ListRequestErrors(ctx, query)
	if err != nil {
		return fmt.Errorf("读取真实请求错误失败: %w", err)
	}
	if _, err := m.store.InsertTrafficBatch(ctx, successes, failures); err != nil {
		return fmt.Errorf("保存真实请求证据失败: %w", err)
	}

	monitors, err := m.api.ListMonitors(ctx)
	if err != nil {
		return fmt.Errorf("读取监控列表失败: %w", err)
	}
	for _, monitor := range monitors {
		since := now.Add(-24 * time.Hour)
		if cursor := m.lastMonitorKeys[monitor.ID]; !cursor.IsZero() {
			since = cursor.Add(-2 * time.Minute)
		}
		items, historyErr := m.api.ListMonitorHistory(ctx, monitor.ID, sub2api.TelemetryQuery{Since: since, Until: now, PageSize: 200})
		if historyErr != nil {
			return fmt.Errorf("读取监控 %d 历史失败: %w", monitor.ID, historyErr)
		}
		if _, historyErr = m.store.InsertMonitorHistoryBatch(ctx, items); historyErr != nil {
			return fmt.Errorf("保存监控 %d 历史失败: %w", monitor.ID, historyErr)
		}
		for _, item := range items {
			if item.CheckedAt.After(m.lastMonitorKeys[monitor.ID]) {
				m.lastMonitorKeys[monitor.ID] = item.CheckedAt
			}
		}
	}
	if err := m.refreshCapabilities(ctx, successes, failures, now); err != nil {
		return err
	}
	if err := m.store.DeleteTelemetryBefore(ctx, now.Add(-14*24*time.Hour), now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour)); err != nil {
		return fmt.Errorf("清理过期证据失败: %w", err)
	}
	m.lastSuccessAt = now
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
		capability := model.AccountModelCapability{
			AccountID: current.accountID, Model: current.model,
			Supported:    window.SuccessCount > 0 || window.CapabilityErrors == 0,
			SuccessCount: window.SuccessCount, FailureCount: window.CapabilityErrors,
			LastErrorClass: last.ErrorClass, LastReasonCode: last.ReasonCode,
			LastObservedAt: observedAt, UpdatedAt: now,
		}
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
			_ = m.store.AddEvent(context.Background(), model.Event{Type: "telemetry_sync_failed", Severity: "error", Message: message, Actor: "system"})
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
