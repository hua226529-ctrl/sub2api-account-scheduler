package automation

import "sync"

// Barrier serializes a freeze-state publication with automated external
// mutations owned by modules outside the deterministic reconcile engine.
type Barrier struct {
	mu sync.RWMutex
}

func NewBarrier() *Barrier {
	return &Barrier{}
}

// EnterMutation holds the shared side of the barrier until the returned
// release function is called. Callers must re-read the freeze state while the
// lease is held and before starting the external write.
func (b *Barrier) EnterMutation() func() {
	if b == nil {
		return func() {}
	}
	b.mu.RLock()
	return b.mu.RUnlock
}

// EnterFreeze blocks until every in-flight automated external mutation has
// finished, then prevents a new one from starting while freeze state changes.
func (b *Barrier) EnterFreeze() func() {
	if b == nil {
		return func() {}
	}
	b.mu.Lock()
	return b.mu.Unlock
}
