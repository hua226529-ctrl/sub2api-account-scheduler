package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestManualPauseReleaseAndResumeOverrideSemantics(t *testing.T) {
	t.Run("manual pause is a permanent hold and release re-arbitrates", func(t *testing.T) {
		ctx := context.Background()
		engine, _, api := newEngineTest(t, false)
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		pause, err := engine.ManualPauseCommand(ctx, 225, "web", "manual-hold-1")
		if err != nil || pause.Status != accountcontrol.StatusApplied {
			t.Fatalf("pause=%+v err=%v", pause, err)
		}
		hold, err := engine.accountControl.FindActiveOverride(ctx, 225, controlplane.OperationSetAccountSchedulable,
			controlplane.AuthorityManualHold)
		if err != nil || hold == nil || hold.ExpiresAt != nil || hold.Kind != accountcontrol.OverrideKindManualHold {
			t.Fatalf("hold=%+v err=%v", hold, err)
		}
		released, err := engine.ReleaseManualHoldCommand(ctx, 225, "web", "release-hold-1")
		if err != nil || released.VerifiedAfter == nil || !released.VerifiedAfter.Schedulable {
			t.Fatalf("released=%+v err=%v", released, err)
		}
		replay, err := engine.ReleaseManualHoldCommand(ctx, 225, "web", "release-hold-1")
		if err != nil || !replay.IdempotentReplay || replay.MutationID != released.MutationID {
			t.Fatalf("release replay=%+v err=%v", replay, err)
		}
		if len(api.actions) != 2 || api.actions[0] || !api.actions[1] {
			t.Fatalf("unexpected writes: %v", api.actions)
		}
	})

	t.Run("release does not force recovery while policy still pauses", func(t *testing.T) {
		ctx := context.Background()
		engine, _, api := newEngineTest(t, false)
		checkedAt := time.Now().UTC()
		applyChecks(t, engine, api, model.StatusFailed, &checkedAt, 3)
		if err := engine.ManualPause(ctx, 225, "web"); err != nil {
			t.Fatal(err)
		}
		writesBefore := len(api.actions)
		result, err := engine.ReleaseManualHoldCommand(ctx, 225, "web", "release-still-paused")
		if err != nil || result.VerifiedAfter == nil || result.VerifiedAfter.Schedulable {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if len(api.actions) != writesBefore {
			t.Fatalf("release forced an unsafe resume: before=%d actions=%v", writesBefore, api.actions)
		}
	})

	t.Run("manual resume revokes hold and creates thirty minute override", func(t *testing.T) {
		ctx := context.Background()
		engine, _, _ := newEngineTest(t, false)
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if err := engine.ManualPause(ctx, 225, "web"); err != nil {
			t.Fatal(err)
		}
		started := time.Now().UTC()
		result, err := engine.ManualResumeCommand(ctx, 225, "web", "manual-resume-1", 0)
		if err != nil || result.ExpiresAt == nil {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if result.ExpiresAt.Before(started.Add(29*time.Minute)) || result.ExpiresAt.After(time.Now().UTC().Add(31*time.Minute)) {
			t.Fatalf("unexpected default expiration: %v", result.ExpiresAt)
		}
		hold, err := engine.accountControl.FindActiveOverride(ctx, 225, controlplane.OperationSetAccountSchedulable,
			controlplane.AuthorityManualHold)
		if err != nil || hold != nil {
			t.Fatalf("manual hold remained active: hold=%+v err=%v", hold, err)
		}
		resume, err := engine.accountControl.FindActiveOverride(ctx, 225, controlplane.OperationSetAccountSchedulable,
			controlplane.AuthorityAdministratorCommand)
		if err != nil || resume == nil || resume.ExpiresAt == nil || resume.Kind != accountcontrol.OverrideKindTemporary {
			t.Fatalf("resume override=%+v err=%v", resume, err)
		}
	})
}

func TestPinLoadPermanentAndTemporaryKinds(t *testing.T) {
	t.Run("permanent", func(t *testing.T) {
		ctx := context.Background()
		engine, _, _ := newEngineTest(t, false)
		result, err := engine.PinLoadCommand(ctx, 225, 40, nil, true, "web", "capacity hold", "pin-permanent")
		if err != nil || result.ExpiresAt != nil {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		pin, err := engine.accountControl.FindActiveOverride(ctx, 225, controlplane.OperationSetAccountLoadFactor,
			controlplane.AuthorityManualHold)
		if err != nil || pin == nil || pin.Kind != accountcontrol.OverrideKindLoadPin || pin.ExpiresAt != nil {
			t.Fatalf("pin=%+v err=%v", pin, err)
		}
	})

	t.Run("temporary", func(t *testing.T) {
		ctx := context.Background()
		engine, _, _ := newEngineTest(t, false)
		until := time.Now().UTC().Add(time.Hour)
		result, err := engine.PinLoadCommand(ctx, 225, 45, &until, false, "web", "temporary capacity", "pin-temporary")
		if err != nil || result.ExpiresAt == nil {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		pin, err := engine.accountControl.FindActiveOverride(ctx, 225, controlplane.OperationSetAccountLoadFactor,
			controlplane.AuthorityAdministratorCommand)
		if err != nil || pin == nil || pin.Kind != accountcontrol.OverrideKindLoadPin || pin.ExpiresAt == nil {
			t.Fatalf("pin=%+v err=%v", pin, err)
		}
	})

	t.Run("release idempotency includes audit reason", func(t *testing.T) {
		ctx := context.Background()
		engine, _, _ := newEngineTest(t, false)
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.PinLoadCommand(ctx, 225, 40, nil, true, "web", "capacity hold", "pin-for-release-conflict"); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ClearLoadPinCommand(ctx, 225, "web", "capacity restored", "clear-pin-conflict"); err != nil {
			t.Fatal(err)
		}
		_, err := engine.ClearLoadPinCommand(ctx, 225, "web", "different audit reason", "clear-pin-conflict")
		var conflict *accountcontrol.IdempotencyConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("same release command with different audit reason did not conflict: %v", err)
		}
	})
}

func TestExpiredTemporaryLoadOverrideReturnsToPolicyBaseline(t *testing.T) {
	ctx := context.Background()
	engine, database, api, path := newEngineTestWithPath(t, false)
	original := 90
	api.mu.Lock()
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()
	value := 35
	if _, err := engine.ForceSetLoadFactorCommand(ctx, 225, &value, "web", "temporary capacity", "temporary-load-expiry", time.Hour); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE account_overrides SET expires_at=? WHERE account_id=225 AND operation=?`, past,
		controlplane.OperationSetAccountLoadFactor.String()); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE account_controls SET load_override_until=? WHERE account_id=225`, past); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 2 || api.loadActions[0] == nil || *api.loadActions[0] != 35 ||
		api.loadActions[1] == nil || *api.loadActions[1] != 90 {
		t.Fatalf("expired load override did not restore baseline: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.LoadOverrideUntil != nil || control.LoadPinUntil != nil || control.OriginalLoadFactor != nil {
		t.Fatalf("expired load projection was not cleared: %+v", control)
	}
}

func TestExpiredPauseOverrideLetsUnchangedPolicySnapshotReclaimAccount(t *testing.T) {
	ctx := context.Background()
	engine, database, api, path := newEngineTestWithPath(t, false)
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	paused := false
	if err := database.UpsertControl(ctx, model.AccountControl{AccountID: 225, OwnsPause: true, Owner: "automatic",
		ExpectedSchedulable: &paused, LastObserved: &paused}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC()
	expires := created.Add(time.Hour)
	adminCtx := accountcontrol.WithCommandContext(ctx, accountcontrol.CommandContext{CommandID: "temporary-admin-pause",
		CreatedAt: created, ExpiresAt: &expires, Administrator: true, GrantConsumptionID: "temporary-admin-pause"})
	if err := engine.AgentPause(adminCtx, 225, "administrator:agent", "temporary maintenance pause"); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE account_overrides SET expires_at=? WHERE account_id=225 AND operation=?`, past,
		controlplane.OperationSetAccountSchedulable.String()); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE account_controls SET manual_override_until=? WHERE account_id=225`, past); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 3 || !api.actions[0] || api.actions[1] || !api.actions[2] {
		t.Fatalf("unchanged policy snapshot did not reclaim expired pause override: %v", api.actions)
	}
}

func TestAgentAccountOverrideAuthorityTTLAndEvidence(t *testing.T) {
	t.Run("autonomous defaults to fifteen minutes", func(t *testing.T) {
		engine, _, _ := newEngineTest(t, false)
		created := time.Now().UTC()
		ctx := accountcontrol.WithCommandContext(context.Background(), accountcontrol.CommandContext{
			CommandID: "agent-goal-1-step-1-action-1", CreatedAt: created, SnapshotVersion: "snapshot-1",
			EvidenceRefs: []string{"packet:1", "observation:7"},
		})
		if err := engine.AgentPause(ctx, 225, "agent:v2", "autonomous evidence"); err != nil {
			t.Fatal(err)
		}
		override, err := engine.accountControl.FindActiveOverride(context.Background(), 225,
			controlplane.OperationSetAccountSchedulable, controlplane.AuthorityAutonomousAgent)
		if err != nil || override == nil || override.ExpiresAt == nil {
			t.Fatalf("override=%+v err=%v", override, err)
		}
		if override.ExpiresAt.Before(created.Add(14*time.Minute)) || override.ExpiresAt.After(created.Add(16*time.Minute)) {
			t.Fatalf("unexpected autonomous TTL: %v", override.ExpiresAt)
		}
	})

	t.Run("administrator grant defaults to thirty minutes", func(t *testing.T) {
		engine, _, _ := newEngineTest(t, false)
		created := time.Now().UTC()
		ctx := accountcontrol.WithCommandContext(context.Background(), accountcontrol.CommandContext{
			CommandID: "grant-consumption-1", GrantConsumptionID: "grant-consumption-1", Administrator: true, CreatedAt: created,
		})
		if err := engine.AgentPause(ctx, 225, "administrator:agent", "exact administrator command"); err != nil {
			t.Fatal(err)
		}
		override, err := engine.accountControl.FindActiveOverride(context.Background(), 225,
			controlplane.OperationSetAccountSchedulable, controlplane.AuthorityAdministratorCommand)
		if err != nil || override == nil || override.ExpiresAt == nil || override.Producer != controlplane.ProducerAgentOperator {
			t.Fatalf("override=%+v err=%v", override, err)
		}
		if override.ExpiresAt.Before(created.Add(29*time.Minute)) || override.ExpiresAt.After(created.Add(31*time.Minute)) {
			t.Fatalf("unexpected administrator TTL: %v", override.ExpiresAt)
		}
	})

	for _, test := range []struct {
		name    string
		command accountcontrol.CommandContext
	}{
		{"missing evidence", accountcontrol.CommandContext{CommandID: "agent-missing-evidence", CreatedAt: time.Now().UTC(), SnapshotVersion: "snapshot"}},
		{"exceeds maximum TTL", func() accountcontrol.CommandContext {
			created := time.Now().UTC()
			expires := created.Add(accountcontrol.MaximumAutonomousTTL + time.Minute)
			return accountcontrol.CommandContext{CommandID: "agent-too-long", CreatedAt: created, ExpiresAt: &expires,
				SnapshotVersion: "snapshot", EvidenceRefs: []string{"evidence"}}
		}()},
		{"legacy missing identity", accountcontrol.CommandContext{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, _, api := newEngineTest(t, false)
			ctx := accountcontrol.WithCommandContext(context.Background(), test.command)
			if err := engine.AgentPause(ctx, 225, "agent:v2", test.name); err == nil {
				t.Fatal("invalid autonomous action was accepted")
			}
			if len(api.actions) != 0 {
				t.Fatalf("invalid autonomous action reached upstream: %v", api.actions)
			}
		})
	}
}

func TestAgentCommandIdentityCannotBeReusedForDifferentAccountAction(t *testing.T) {
	engine, _, _ := newEngineTest(t, false)
	ctx := autonomousAgentTestContext("agent-command-conflict", time.Now().UTC())
	if err := engine.AgentPause(ctx, 225, "agent:test", "pause evidence"); err != nil {
		t.Fatal(err)
	}
	value := 25
	err := engine.AgentSetLoadFactor(ctx, 225, &value, "agent:test", "different action")
	var conflict *accountcontrol.IdempotencyConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("reused agent command identity did not conflict: %v", err)
	}
}

func TestForceResumeCannotBypassWritesFreeze(t *testing.T) {
	ctx := context.Background()
	engine, _, api := newEngineTest(t, false)
	if err := engine.UpdateFreezeState(ctx, model.FreezeState{AllAutomation: true, Reason: "incident"}, "web"); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ForceResumeCommand(ctx, 225, "web", "emergency", "force-frozen", time.Minute)
	var blocked *accountcontrol.BlockedError
	if !errors.As(err, &blocked) || result.BlockedReason != accountcontrol.BlockWritesFrozen {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("force resume bypassed writes freeze: %v", api.actions)
	}
}
