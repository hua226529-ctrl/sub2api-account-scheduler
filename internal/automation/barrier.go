package automation

import (
	"context"
	"sync"
)

// Barrier serializes freeze-state publication with automated external
// mutations. Freeze waiters have priority over new mutations so publication
// cannot starve under a steady write load.
type Barrier struct {
	mu              sync.Mutex
	changed         chan struct{}
	activeMutations int
	freezeActive    bool
	freezeWaiters   int
}

func NewBarrier() *Barrier {
	return &Barrier{changed: make(chan struct{})}
}

// EnterMutation holds the shared side of the barrier until release is called.
// Callers must re-read freeze state while the lease is held and before writing.
func (b *Barrier) EnterMutation(ctx context.Context) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		b.mu.Lock()
		if !b.freezeActive && b.freezeWaiters == 0 {
			b.activeMutations++
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					b.mu.Lock()
					b.activeMutations--
					b.signalLocked()
					b.mu.Unlock()
				})
			}, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// EnterFreeze waits for in-flight mutations, then excludes new mutations until
// release is called. Waiting is canceled when ctx ends.
func (b *Barrier) EnterFreeze(ctx context.Context) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	b.freezeWaiters++
	b.signalLocked()
	b.mu.Unlock()
	waiting := true
	for {
		b.mu.Lock()
		if !b.freezeActive && b.activeMutations == 0 {
			b.freezeWaiters--
			waiting = false
			b.freezeActive = true
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					b.mu.Lock()
					b.freezeActive = false
					b.signalLocked()
					b.mu.Unlock()
				})
			}, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			if waiting {
				b.mu.Lock()
				b.freezeWaiters--
				b.signalLocked()
				b.mu.Unlock()
			}
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (b *Barrier) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
