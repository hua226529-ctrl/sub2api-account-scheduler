package testsupport

import (
	"sync"
	"time"
)

// FixedClock is a concurrency-safe clock for deterministic tests.
type FixedClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewFixedClock(now time.Time) *FixedClock {
	return &FixedClock{now: now.UTC()}
}

func (c *FixedClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *FixedClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now.UTC()
	c.mu.Unlock()
}

func (c *FixedClock) Advance(delta time.Duration) time.Time {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	now := c.now
	c.mu.Unlock()
	return now
}
