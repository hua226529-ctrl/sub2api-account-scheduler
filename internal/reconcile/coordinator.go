package reconcile

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultReconcileDebounce = 100 * time.Millisecond
	defaultReconcileInterval = 50 * time.Second
)

// PassRunner is the only work a Coordinator may start. Keeping this small
// prevents the coordinator from becoming a general event or workflow bus.
type PassRunner interface {
	ReconcileFull(context.Context) error
	ReconcileAccounts(context.Context, []int64) error
}

type CoordinatorOption func(*Coordinator)

func WithReconcileDebounce(value time.Duration) CoordinatorOption {
	return func(coordinator *Coordinator) {
		if value >= 0 && value <= 500*time.Millisecond {
			coordinator.debounce = value
		}
	}
}

func WithReconcileInterval(value time.Duration) CoordinatorOption {
	return func(coordinator *Coordinator) {
		if value > 0 {
			coordinator.interval = value
		}
	}
}

// Coordinator coalesces wakeups and serializes Reconcile passes. Request
// methods only mutate a small in-memory set and perform a non-blocking wake.
type Coordinator struct {
	pass     PassRunner
	logger   *slog.Logger
	debounce time.Duration
	interval time.Duration

	mu          sync.Mutex
	pendingFull bool
	pending     map[int64]struct{}
	sources     map[string]struct{}
	wake        chan struct{}
	requests    uint64
	coalesced   uint64
	requestedAt time.Time
}

func NewCoordinator(pass PassRunner, logger *slog.Logger, options ...CoordinatorOption) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	coordinator := &Coordinator{
		pass: pass, logger: logger, debounce: defaultReconcileDebounce,
		interval: defaultReconcileInterval, pending: make(map[int64]struct{}), sources: make(map[string]struct{}), wake: make(chan struct{}, 1),
	}
	for _, option := range options {
		if option != nil {
			option(coordinator)
		}
	}
	return coordinator
}

func (c *Coordinator) RequestAccounts(accountIDs ...int64) {
	c.RequestAccountsFrom("unspecified", accountIDs...)
}

func (c *Coordinator) RequestAccountsFrom(source string, accountIDs ...int64) {
	c.mu.Lock()
	c.addSource(source)
	if c.pendingFull {
		c.coalesced++
		c.mu.Unlock()
		return
	}
	before := len(c.pending)
	for _, accountID := range accountIDs {
		if accountID > 0 {
			c.pending[accountID] = struct{}{}
		}
	}
	if len(c.pending) == before {
		c.coalesced++
	} else {
		if c.requestedAt.IsZero() {
			c.requestedAt = time.Now()
		}
		c.requests++
	}
	c.mu.Unlock()
	c.signal()
}

func (c *Coordinator) RequestFull() {
	c.RequestFullFrom("unspecified")
}

func (c *Coordinator) RequestFullFrom(source string) {
	c.mu.Lock()
	c.addSource(source)
	if c.pendingFull {
		c.coalesced++
	} else {
		if c.requestedAt.IsZero() {
			c.requestedAt = time.Now()
		}
		c.requests++
		c.pendingFull = true
		clear(c.pending)
	}
	c.mu.Unlock()
	c.signal()
}

func (c *Coordinator) addSource(source string) {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "unspecified"
	}
	c.sources[source] = struct{}{}
}

func (c *Coordinator) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Run owns the only production pass loop. It keeps requests arriving during a
// pass pending and immediately drains them after that pass completes.
func (c *Coordinator) Run(ctx context.Context) {
	if c == nil || c.pass == nil {
		return
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.RequestFullFrom("periodic")
			c.runPending(ctx, false)
		case <-c.wake:
			c.waitDebounce(ctx)
			c.runPending(ctx, true)
		}
	}
}

func (c *Coordinator) waitDebounce(ctx context.Context) {
	if c.debounce <= 0 {
		return
	}
	timer := time.NewTimer(c.debounce)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			return
		case <-c.wake:
			timer.Reset(c.debounce)
		}
	}
}

func (c *Coordinator) runPending(ctx context.Context, _ bool) {
	for ctx.Err() == nil {
		full, accountIDs, sources, requests, coalesced, queueWait := c.takePending()
		if !full && len(accountIDs) == 0 {
			return
		}
		started := time.Now()
		var err error
		if full {
			err = c.pass.ReconcileFull(ctx)
		} else {
			err = c.pass.ReconcileAccounts(ctx, accountIDs)
		}
		attrs := []any{"trigger", "coordinator", "trigger_sources", sources, "full", full, "target_account_count", len(accountIDs),
			"coalesced_request_count", coalesced, "queue_requests", requests, "queue_wait", queueWait.String(),
			"run_duration", time.Since(started).String()}
		if err != nil {
			attrs = append(attrs, "error", err)
			c.logger.Error("reconcile_pass_failed", attrs...)
		} else {
			c.logger.Info("reconcile_pass_complete", attrs...)
		}
	}
}

func (c *Coordinator) takePending() (bool, []int64, []string, uint64, uint64, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	full := c.pendingFull
	ids := make([]int64, 0, len(c.pending))
	if !full {
		for accountID := range c.pending {
			ids = append(ids, accountID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	sources := make([]string, 0, len(c.sources))
	for source := range c.sources {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	requests, coalesced := c.requests, c.coalesced
	var queueWait time.Duration
	if !c.requestedAt.IsZero() {
		queueWait = time.Since(c.requestedAt)
	}
	c.pendingFull = false
	clear(c.pending)
	clear(c.sources)
	c.requests, c.coalesced = 0, 0
	c.requestedAt = time.Time{}
	return full, ids, sources, requests, coalesced, queueWait
}
