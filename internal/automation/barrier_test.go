package automation

import (
	"testing"
	"time"
)

func TestFreezeWaitsForInflightMutationAndBlocksNewMutation(t *testing.T) {
	barrier := NewBarrier()
	releaseFirst := barrier.EnterMutation()

	freezeAcquired := make(chan struct{})
	freezeRelease := make(chan struct{})
	go func() {
		release := barrier.EnterFreeze()
		close(freezeAcquired)
		<-freezeRelease
		release()
	}()

	select {
	case <-freezeAcquired:
		t.Fatal("freeze crossed an in-flight mutation")
	case <-time.After(30 * time.Millisecond):
	}
	releaseFirst()
	select {
	case <-freezeAcquired:
	case <-time.After(time.Second):
		t.Fatal("freeze did not acquire after the mutation completed")
	}

	secondAcquired := make(chan struct{})
	go func() {
		release := barrier.EnterMutation()
		close(secondAcquired)
		release()
	}()
	select {
	case <-secondAcquired:
		t.Fatal("a new mutation crossed an active freeze publication")
	case <-time.After(30 * time.Millisecond):
	}
	close(freezeRelease)
	select {
	case <-secondAcquired:
	case <-time.After(time.Second):
		t.Fatal("mutation did not resume after freeze publication completed")
	}
}
