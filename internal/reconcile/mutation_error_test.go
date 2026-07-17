package reconcile

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
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

func TestAccountMutationEntrypointsClassifyAmbiguousAPIErrors(t *testing.T) {
	t.Run("agent pause", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.scheduleErr = io.EOF
		ctx := autonomousAgentTestContext("ambiguous-agent-pause", time.Now().UTC())
		err := engine.AgentPause(ctx, 225, "agent:test", "test")
		assertUncertainMutation(t, err, io.EOF)
		assertScheduleActions(t, api, false, false)
	})

	t.Run("agent resume", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.mu.Lock()
		api.accounts[0].Schedulable = false
		api.mu.Unlock()
		api.scheduleErr = context.DeadlineExceeded
		ctx := autonomousAgentTestContext("ambiguous-agent-resume", time.Now().UTC())
		err := engine.AgentResume(ctx, 225, "agent:test", "test")
		assertUncertainMutation(t, err, context.DeadlineExceeded)
		assertScheduleActions(t, api, true, true)
	})

	t.Run("agent load factor", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.loadErr = io.ErrUnexpectedEOF
		value := 35
		ctx := autonomousAgentTestContext("ambiguous-agent-load", time.Now().UTC())
		err := engine.AgentSetLoadFactor(ctx, 225, &value, "agent:test", "test")
		assertUncertainMutation(t, err, io.ErrUnexpectedEOF)
		assertLoadActions(t, api, intPointer(35), intPointer(35))
	})

	t.Run("administrator load pin", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.loadErr = context.DeadlineExceeded
		err := engine.PinLoad(context.Background(), 225, 35, time.Now().UTC().Add(time.Hour), "web", "test")
		assertUncertainMutation(t, err, context.DeadlineExceeded)
		assertLoadActions(t, api, intPointer(35), intPointer(35))
	})

	t.Run("administrator force resume", func(t *testing.T) {
		engine, _, api := newEngineTest(t, false)
		api.mu.Lock()
		api.accounts[0].Schedulable = false
		api.mu.Unlock()
		api.scheduleErr = io.EOF
		err := engine.ForceResume(context.Background(), 225, "web", "test")
		assertUncertainMutation(t, err, io.EOF)
		assertScheduleActions(t, api, true, true)
	})
}

func TestConfirmedBusinessRejectionIsNotUncertain(t *testing.T) {
	engine, _, api := newEngineTest(t, false)
	api.scheduleErr = errors.New("sub2api returned 422: account is disabled")
	ctx := autonomousAgentTestContext("definitive-agent-pause", time.Now().UTC())
	err := engine.AgentPause(ctx, 225, "agent:test", "test")
	if err == nil || IsExternalMutationUncertain(err) {
		t.Fatalf("confirmed business rejection should be a definitive failure: %v", err)
	}
	assertScheduleActions(t, api, false)
}

func TestLocalCommitFailureDoesNotRollBackExternalMutation(t *testing.T) {
	engine, _, api, path := newEngineTestWithPath(t, false)
	failEventCommit(t, path, "agent_pause")
	ctx := autonomousAgentTestContext("commit-failure-agent-pause", time.Now().UTC())
	err := engine.AgentPause(ctx, 225, "agent:test", "test")
	if err == nil || !IsExternalMutationUncertain(err) {
		t.Fatalf("applied upstream write with failed local commit must be uncertain: %v", err)
	}
	assertScheduleActions(t, api, false)
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
