package accountcontrol

import (
	"context"
	"strings"
	"sync"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

type lockEntry struct {
	token chan struct{}
	refs  int
}

type accountLocks struct {
	mu      sync.Mutex
	entries map[int64]*lockEntry
}

func newAccountLocks() *accountLocks {
	return &accountLocks{entries: make(map[int64]*lockEntry)}
}

func (l *accountLocks) acquire(ctx context.Context, accountID int64) (func(), error) {
	l.mu.Lock()
	entry := l.entries[accountID]
	if entry == nil {
		entry = &lockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		l.entries[accountID] = entry
	}
	entry.refs++
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.releaseRef(accountID, entry)
		return nil, ctx.Err()
	case <-entry.token:
	}
	return func() {
		entry.token <- struct{}{}
		l.releaseRef(accountID, entry)
	}, nil
}

func (l *accountLocks) releaseRef(accountID int64, entry *lockEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && len(entry.token) == 1 && l.entries[accountID] == entry {
		delete(l.entries, accountID)
	}
}

func (l *accountLocks) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

type queuedCandidate struct {
	token      uint64
	requestKey string
	intent     controlplane.Intent
	kind       OverrideKind
	completed  bool
}

type candidateKey struct {
	accountID int64
	operation controlplane.Operation
}

// candidateQueue covers only the interval between submission and acquisition
// of the account lock. Persistent active overrides remain the sole durable
// override source; queued candidates are discarded when Submit returns.
type candidateQueue struct {
	mu      sync.Mutex
	next    uint64
	entries map[candidateKey][]queuedCandidate
}

func newCandidateQueue() *candidateQueue {
	return &candidateQueue{entries: make(map[candidateKey][]queuedCandidate)}
}

func (q *candidateQueue) add(submission Submission) (queuedCandidate, func(bool)) {
	accountID, _ := submission.Intent.Resource.AccountID()
	key := candidateKey{accountID: accountID, operation: submission.Intent.Operation}
	requestKey := strings.TrimSpace(submission.RequestIdempotencyKey)
	if requestKey == "" {
		requestKey = strings.TrimSpace(submission.Intent.IdempotencyKey)
	}
	q.mu.Lock()
	q.next++
	entry := queuedCandidate{token: q.next, requestKey: requestKey, intent: submission.Intent, kind: overrideKindForSubmission(submission)}
	q.entries[key] = append(q.entries[key], entry)
	q.mu.Unlock()
	return entry, func(retain bool) { q.finish(key, entry.token, retain) }
}

func (q *candidateQueue) finish(key candidateKey, token uint64, retain bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.entries[key]
	foundIndex := -1
	for index := range items {
		if items[index].token == token {
			foundIndex = index
			break
		}
	}
	if foundIndex < 0 {
		return
	}
	if retain && hasIncompleteCandidate(items, token) {
		items[foundIndex].completed = true
		q.entries[key] = items
		return
	}
	items = append(items[:foundIndex], items[foundIndex+1:]...)
	if len(items) == 0 || allCandidatesCompleted(items) {
		delete(q.entries, key)
		return
	}
	q.entries[key] = items
}

func hasIncompleteCandidate(items []queuedCandidate, except uint64) bool {
	for _, item := range items {
		if item.token != except && !item.completed {
			return true
		}
	}
	return false
}

func allCandidatesCompleted(items []queuedCandidate) bool {
	for _, item := range items {
		if !item.completed {
			return false
		}
	}
	return true
}

func (q *candidateQueue) competing(entry queuedCandidate) []queuedCandidate {
	accountID, _ := entry.intent.Resource.AccountID()
	key := candidateKey{accountID: accountID, operation: entry.intent.Operation}
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.entries[key]
	result := make([]queuedCandidate, 0, len(items))
	for _, candidate := range items {
		if candidate.token == entry.token || candidate.requestKey == entry.requestKey {
			continue
		}
		candidate.intent = cloneControlplaneIntent(candidate.intent)
		result = append(result, candidate)
	}
	return result
}
