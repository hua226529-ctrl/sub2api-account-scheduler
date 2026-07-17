package reconcile

import (
	"context"
	"log/slog"
	"sort"
	"time"
)

type overrideExpiryRepository interface {
	NextActiveAccountOverrideExpiry(context.Context) (*time.Time, error)
	ExpireActiveAccountOverrides(context.Context, time.Time) ([]int64, error)
}

// OverrideExpiryWorker owns one timer for all temporary override rows. Expiration is
// a local durable state transition; Reconcile and AccountControl decide the
// resulting upstream state afterward.
type OverrideExpiryWorker struct {
	repository overrideExpiryRepository
	requester  interface{ RequestAccounts(...int64) }
	logger     *slog.Logger
	wake       chan struct{}
	clock      func() time.Time
	newTimer   func(time.Duration) expiryTimer
}

type expiryTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realExpiryTimer struct{ timer *time.Timer }

func (t realExpiryTimer) C() <-chan time.Time { return t.timer.C }
func (t realExpiryTimer) Stop() bool          { return t.timer.Stop() }

type expiryWaitResult uint8

const (
	expiryWaitCanceled expiryWaitResult = iota
	expiryWaitTimerFired
	expiryWaitChanged
)

type OverrideExpiryOption func(*OverrideExpiryWorker)

func WithOverrideExpiryClock(clock func() time.Time) OverrideExpiryOption {
	return func(worker *OverrideExpiryWorker) {
		if clock != nil {
			worker.clock = clock
		}
	}
}

func withOverrideExpiryTimerFactory(factory func(time.Duration) expiryTimer) OverrideExpiryOption {
	return func(worker *OverrideExpiryWorker) {
		if factory != nil {
			worker.newTimer = factory
		}
	}
}

func NewOverrideExpiryWorker(repository overrideExpiryRepository, requester interface{ RequestAccounts(...int64) }, logger *slog.Logger, options ...OverrideExpiryOption) *OverrideExpiryWorker {
	if repository == nil || requester == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	worker := &OverrideExpiryWorker{repository: repository, requester: requester, logger: logger, wake: make(chan struct{}, 1),
		clock: func() time.Time { return time.Now().UTC() }, newTimer: func(duration time.Duration) expiryTimer {
			return realExpiryTimer{timer: time.NewTimer(duration)}
		}}
	for _, option := range options {
		if option != nil {
			option(worker)
		}
	}
	return worker
}

func (w *OverrideExpiryWorker) Wake() {
	if w == nil {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *OverrideExpiryWorker) Run(ctx context.Context) {
	if w == nil {
		return
	}
	for {
		next, err := w.repository.NextActiveAccountOverrideExpiry(ctx)
		if err != nil {
			w.logger.Warn("override_expiry_next_failed", "error", err)
			if w.wait(ctx, time.Minute) == expiryWaitCanceled {
				return
			}
			continue
		}
		if next == nil {
			if !w.waitWake(ctx) {
				return
			}
			continue
		}
		delay := next.UTC().Sub(w.clock().UTC())
		if delay > 0 {
			switch w.wait(ctx, delay) {
			case expiryWaitCanceled:
				return
			case expiryWaitChanged:
				continue
			case expiryWaitTimerFired:
			}
		}
		if ctx.Err() != nil {
			return
		}
		ids, err := w.repository.ExpireActiveAccountOverrides(ctx, w.clock().UTC())
		if err != nil {
			w.logger.Warn("override_expiry_commit_failed", "error", err)
			continue
		}
		if len(ids) > 0 {
			seen := make(map[int64]struct{}, len(ids))
			merged := ids[:0]
			for _, accountID := range ids {
				if accountID > 0 {
					if _, ok := seen[accountID]; !ok {
						seen[accountID] = struct{}{}
						merged = append(merged, accountID)
					}
				}
			}
			sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
			w.logger.Info("override_expired", "account_count", len(merged))
			if sourced, ok := w.requester.(interface {
				RequestAccountsFrom(string, ...int64)
			}); ok {
				sourced.RequestAccountsFrom("override_expiry", merged...)
			} else {
				w.requester.RequestAccounts(merged...)
			}
		}
	}
}

func (w *OverrideExpiryWorker) waitWake(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-w.wake:
		return true
	}
}

func (w *OverrideExpiryWorker) wait(ctx context.Context, duration time.Duration) expiryWaitResult {
	timer := w.newTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return expiryWaitCanceled
	case <-w.wake:
		return expiryWaitChanged
	case <-timer.C():
		return expiryWaitTimerFired
	}
}
