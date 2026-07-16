package reconcile

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestExternalMutationErrorClassification(t *testing.T) {
	t.Run("transport EOF is uncertain", func(t *testing.T) {
		err := uncertainExternalMutation("test write", io.EOF)
		if !IsExternalMutationUncertain(err) || !errors.Is(err, io.EOF) {
			t.Fatalf("EOF should remain identifiable and uncertain: %v", err)
		}
	})
	t.Run("timeout is uncertain", func(t *testing.T) {
		err := uncertainExternalMutation("test write", context.DeadlineExceeded)
		if !IsExternalMutationUncertain(err) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout should remain identifiable and uncertain: %v", err)
		}
	})
	t.Run("HTTP business rejection is definitive", func(t *testing.T) {
		upstream := errors.New("sub2api returned 409: account state rejects mutation")
		err := uncertainExternalMutation("test write", upstream)
		if IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed 4xx rejection must not be marked uncertain: %v", err)
		}
		if !errors.Is(err, upstream) {
			t.Fatalf("business rejection should preserve its cause: %v", err)
		}
	})
	t.Run("server failure remains uncertain", func(t *testing.T) {
		err := uncertainExternalMutation("test write", errors.New("sub2api returned 503: unavailable"))
		if !IsExternalMutationUncertain(err) {
			t.Fatalf("5xx may follow an applied write and must be reconciled: %v", err)
		}
	})
}

func TestAgentMutationEntrypointsClassifyAmbiguousAPIErrors(t *testing.T) {
	t.Run("pause", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.scheduleErr = io.EOF
		err := engine.AgentPause(context.Background(), 225, "agent:test", "test")
		assertUncertainMutation(t, err, io.EOF)
	})

	t.Run("resume", func(t *testing.T) {
		engine, database, api := newEngineTest(t, false)
		prepareAgentPausedAccount(t, database, api)
		api.scheduleErr = context.DeadlineExceeded
		err := engine.AgentResume(context.Background(), 225, "agent:test", "test")
		assertUncertainMutation(t, err, context.DeadlineExceeded)
	})

	t.Run("load factor", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		if err := engine.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		api.loadErr = io.ErrUnexpectedEOF
		value := 35
		err := engine.AgentSetLoadFactor(context.Background(), 225, &value, "agent:test", "test")
		assertUncertainMutation(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("load pin", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.loadErr = context.DeadlineExceeded
		err := engine.PinLoad(context.Background(), 225, 35, time.Now().UTC().Add(time.Hour), "agent:test", "test")
		assertUncertainMutation(t, err, context.DeadlineExceeded)
	})

	t.Run("force resume", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.mu.Lock()
		api.accounts[0].Schedulable = false
		api.mu.Unlock()
		api.scheduleErr = io.EOF
		err := engine.ForceResume(context.Background(), 225, "web", "test")
		assertUncertainMutation(t, err, io.EOF)
	})
}

func TestConfirmedBusinessRejectionIsNotUncertain(t *testing.T) {
	engine, _, api := newEngineTest(t, false)
	api.scheduleErr = errors.New("sub2api returned 422: account is disabled")
	err := engine.AgentPause(context.Background(), 225, "agent:test", "test")
	if err == nil || IsExternalMutationUncertain(err) {
		t.Fatalf("confirmed business rejection should be a definitive failure: %v", err)
	}
}

func TestControlCommitFailureRollsBackExternalMutations(t *testing.T) {
	t.Run("pause", func(t *testing.T) {
		engine, _, api, path := newEngineTestWithPath(t, false)
		failEventCommit(t, path, "agent_pause")
		err := engine.AgentPause(context.Background(), 225, "agent:test", "test")
		if err == nil || IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed rollback should return a definitive local commit failure: %v", err)
		}
		assertScheduleActions(t, api, false, true)
	})

	t.Run("resume", func(t *testing.T) {
		engine, database, api, path := newEngineTestWithPath(t, false)
		prepareAgentPausedAccount(t, database, api)
		failEventCommit(t, path, "agent_resume")
		err := engine.AgentResume(context.Background(), 225, "agent:test", "test")
		if err == nil || IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed rollback should return a definitive local commit failure: %v", err)
		}
		assertScheduleActions(t, api, true, false)
	})

	t.Run("load factor", func(t *testing.T) {
		engine, _, api, path := newEngineTestWithPath(t, false)
		original := 80
		api.mu.Lock()
		api.accounts[0].LoadFactor = &original
		api.mu.Unlock()
		if err := engine.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		failEventCommit(t, path, "agent_load_factor")
		value := 35
		err := engine.AgentSetLoadFactor(context.Background(), 225, &value, "agent:test", "test")
		if err == nil || IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed rollback should return a definitive local commit failure: %v", err)
		}
		assertLoadActions(t, api, intPointer(35), intPointer(80))
	})

	t.Run("load pin", func(t *testing.T) {
		engine, _, api, path := newEngineTestWithPath(t, false)
		original := 70
		api.mu.Lock()
		api.accounts[0].LoadFactor = &original
		api.mu.Unlock()
		failEventCommit(t, path, "load_pin_set")
		err := engine.PinLoad(context.Background(), 225, 35, time.Now().UTC().Add(time.Hour), "agent:test", "test")
		if err == nil || IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed rollback should return a definitive local commit failure: %v", err)
		}
		assertLoadActions(t, api, intPointer(35), intPointer(70))
	})

	t.Run("force resume", func(t *testing.T) {
		engine, _, api, path := newEngineTestWithPath(t, false)
		api.mu.Lock()
		api.accounts[0].Schedulable = false
		api.mu.Unlock()
		failEventCommit(t, path, "admin_force_resume")
		err := engine.ForceResume(context.Background(), 225, "web", "test")
		if err == nil || IsExternalMutationUncertain(err) {
			t.Fatalf("confirmed rollback should return a definitive local commit failure: %v", err)
		}
		assertScheduleActions(t, api, true, false)
	})
}

func TestFailedRollbackReturnsUncertainMutation(t *testing.T) {
	engine, _, api, path := newEngineTestWithPath(t, false)
	failEventCommit(t, path, "agent_pause")
	api.scheduleErrs = []error{nil, io.EOF}
	err := engine.AgentPause(context.Background(), 225, "agent:test", "test")
	assertUncertainMutation(t, err, io.EOF)
	assertScheduleActions(t, api, false, true)
}

func prepareAgentPausedAccount(t *testing.T, database interface {
	UpsertControl(context.Context, model.AccountControl) error
}, api *fakeAPI) {
	t.Helper()
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	paused := false
	control := model.AccountControl{
		AccountID: 225, OwnsPause: true, Owner: "agent",
		ExpectedSchedulable: &paused, LastObserved: &paused,
	}
	if err := database.UpsertControl(context.Background(), control); err != nil {
		t.Fatal(err)
	}
}

func assertUncertainMutation(t *testing.T, err, cause error) {
	t.Helper()
	if err == nil || !IsExternalMutationUncertain(err) || !errors.Is(err, cause) {
		t.Fatalf("expected uncertain mutation wrapping %v, got %v", cause, err)
	}
}

func assertScheduleActions(t *testing.T, api *fakeAPI, expected ...bool) {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.actions) != len(expected) {
		t.Fatalf("unexpected schedulable writes: got %v want %v", api.actions, expected)
	}
	for index := range expected {
		if api.actions[index] != expected[index] {
			t.Fatalf("unexpected schedulable writes: got %v want %v", api.actions, expected)
		}
	}
}

func assertLoadActions(t *testing.T, api *fakeAPI, expected ...*int) {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.loadActions) != len(expected) {
		t.Fatalf("unexpected load writes: got %v want %v", api.loadActions, expected)
	}
	for index := range expected {
		if !sameIntPointer(api.loadActions[index], expected[index]) {
			t.Fatalf("unexpected load writes at %d: got %v want %v", index, api.loadActions[index], expected[index])
		}
	}
}

func intPointer(value int) *int { return &value }
