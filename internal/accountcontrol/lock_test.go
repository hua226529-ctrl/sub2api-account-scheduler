package accountcontrol

import (
	"context"
	"testing"
)

func TestAccountLockSerializesSameAccountWithBarrier(t *testing.T) {
	locks := newAccountLocks()
	releaseFirst, err := locks.acquire(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	attempting := make(chan struct{})
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(attempting)
		releaseSecond, acquireErr := locks.acquire(context.Background(), 42)
		if acquireErr == nil {
			close(acquired)
			releaseSecond()
		}
		close(done)
	}()
	<-attempting
	select {
	case <-acquired:
		t.Fatal("same-account lock was acquired before the first holder released it")
	default:
	}
	releaseFirst()
	<-acquired
	<-done
}

func TestAccountLockEntriesAreReclaimed(t *testing.T) {
	locks := newAccountLocks()
	for accountID := int64(1); accountID <= 100; accountID++ {
		release, err := locks.acquire(context.Background(), accountID)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if size := locks.size(); size != 0 {
		t.Fatalf("lock map retained %d unused entries", size)
	}
}

func TestAccountLockWaitHonorsContextCancellation(t *testing.T) {
	locks := newAccountLocks()
	release, err := locks.acquire(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := locks.acquire(ctx, 42); err != context.Canceled {
		t.Fatalf("canceled waiter returned %v", err)
	}
	release()
	if size := locks.size(); size != 0 {
		t.Fatalf("canceled waiter leaked lock entry: size=%d", size)
	}
}
