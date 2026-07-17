package balance

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type groupLockEntry struct {
	token chan struct{}
	refs  int
}

type groupKeyLocks struct {
	mu      sync.Mutex
	entries map[string]*groupLockEntry
}

func newGroupKeyLocks() *groupKeyLocks {
	return &groupKeyLocks{entries: make(map[string]*groupLockEntry)}
}

func groupResourceKey(sourceID int64, keyID string) string {
	return fmt.Sprintf("%d:%s", sourceID, strings.TrimSpace(keyID))
}

func (l *groupKeyLocks) acquire(ctx context.Context, sourceID int64, keyID string) (func(), error) {
	key := groupResourceKey(sourceID, keyID)
	l.mu.Lock()
	entry := l.entries[key]
	if entry == nil {
		entry = &groupLockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		l.entries[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.releaseRef(key, entry)
		return nil, ctx.Err()
	case <-entry.token:
	}
	return func() {
		entry.token <- struct{}{}
		l.releaseRef(key, entry)
	}, nil
}

func (l *groupKeyLocks) releaseRef(key string, entry *groupLockEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && len(entry.token) == 1 && l.entries[key] == entry {
		delete(l.entries, key)
	}
}
