package agent

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestEvaluateAccountReconciliationRequiresFreshEvidence(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	before := reconciliationAccountState(t, now.Add(-time.Minute), true, nil, model.AccountControl{})
	applied := reconciliationAccountState(t, now, false, nil, model.AccountControl{OwnsPause: true, Owner: "agent"})
	unchanged := reconciliationAccountState(t, now, true, nil, model.AccountControl{})

	verdict, _ := evaluateCapabilityReconciliation("pause_account", json.RawMessage(`{"account_id":225,"reason":"outage"}`), before, applied, true)
	if verdict != reconciliationApplied {
		t.Fatalf("fresh target state was not confirmed: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("pause_account", json.RawMessage(`{"account_id":225,"reason":"outage"}`), before, unchanged, true)
	if verdict != reconciliationNotApplied {
		t.Fatalf("fresh unchanged state was not made retryable: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("pause_account", json.RawMessage(`{"account_id":225,"reason":"outage"}`), before, unchanged, false)
	if verdict != reconciliationInconclusive {
		t.Fatalf("stale unchanged state was incorrectly made retryable: %s", verdict)
	}
}

func TestEvaluateAccountReconciliationDetectsConcurrentControlChange(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	before := reconciliationAccountState(t, now.Add(-time.Minute), true, nil, model.AccountControl{})
	current := reconciliationAccountState(t, now, true, nil, model.AccountControl{ManualLocked: true, Owner: "operator"})
	verdict, _ := evaluateCapabilityReconciliation("pause_account", json.RawMessage(`{"account_id":225}`), before, current, true)
	if verdict != reconciliationInconclusive {
		t.Fatalf("concurrent operator state was incorrectly considered safe to retry: %s", verdict)
	}
}

func TestEvaluateLoadPinReconciliationRejectsPartialEffect(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	until := now.Add(time.Hour)
	baselineLoad := 100
	pinnedLoad := 25
	before := reconciliationAccountState(t, now.Add(-time.Minute), true, &baselineLoad, model.AccountControl{})
	complete := reconciliationAccountState(t, now, true, &pinnedLoad, model.AccountControl{
		OwnsLoadFactor: true, ExpectedLoadFactor: &pinnedLoad, LoadPinValue: &pinnedLoad, LoadPinUntil: &until,
	})
	partial := reconciliationAccountState(t, now, true, &pinnedLoad, model.AccountControl{})
	args := marshalRaw(map[string]any{"account_id": 225, "load_factor": pinnedLoad, "until": until})

	verdict, _ := evaluateCapabilityReconciliation("pin_load_until", args, before, complete, true)
	if verdict != reconciliationApplied {
		t.Fatalf("complete load pin was not confirmed: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("pin_load_until", args, before, partial, true)
	if verdict != reconciliationInconclusive {
		t.Fatalf("partial external/local load pin was incorrectly resolved: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("pin_load_until", args, before, before, true)
	if verdict != reconciliationNotApplied {
		t.Fatalf("unchanged load pin baseline was not made retryable: %s", verdict)
	}
}

func TestEvaluateTokenTierReconciliationUsesOnlyConfirmedTier(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	before := reconciliationSourceState(t, now.Add(-time.Minute), "token-1", model.GroupTierMain)
	applied := reconciliationSourceState(t, now, "token-1", model.GroupTierBackup)
	unchanged := reconciliationSourceState(t, now, "token-1", model.GroupTierMain)
	args := json.RawMessage(`{"source_id":9,"key_id":"token-1","target_tier":"backup"}`)

	verdict, _ := evaluateCapabilityReconciliation("transition_token_group_tier", args, before, applied, true)
	if verdict != reconciliationApplied {
		t.Fatalf("confirmed backup tier was not accepted: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("transition_token_group_tier", args, before, unchanged, true)
	if verdict != reconciliationNotApplied {
		t.Fatalf("fresh unchanged tier was not made retryable: %s", verdict)
	}
	verdict, _ = evaluateCapabilityReconciliation("transition_token_group_tier", args, before, json.RawMessage(`{}`), true)
	if verdict != reconciliationInconclusive {
		t.Fatalf("missing token readback was incorrectly resolved: %s", verdict)
	}
}

func TestGroupTransitionReconciliationAcceptsAppliedAndLegacyCompleted(t *testing.T) {
	t.Parallel()
	for _, status := range []string{model.GroupTransitionApplied, model.GroupTransitionCompleted} {
		if !groupTransitionWasApplied(status) {
			t.Errorf("applied group transition status %q was not accepted", status)
		}
	}
	for _, status := range []string{model.GroupTransitionPending, model.GroupTransitionFailed, model.GroupTransitionSimulated} {
		if groupTransitionWasApplied(status) {
			t.Errorf("non-applied group transition status %q was accepted", status)
		}
	}
}

func TestEvaluateUnobservableMutationStaysQuarantined(t *testing.T) {
	t.Parallel()
	verdict, _ := evaluateCapabilityReconciliation("activate_policy_version", json.RawMessage(`{"policy_id":7}`),
		json.RawMessage(`{}`), json.RawMessage(`{}`), true)
	if verdict != reconciliationInconclusive {
		t.Fatalf("unobservable capability escaped quarantine: %s", verdict)
	}
}

func TestCharacterizationRestartReconciliationOnlyAcceptsObservableExternalMutations(t *testing.T) {
	t.Parallel()
	for _, capability := range []string{"pause_account", "resume_account", "set_load_factor", "pin_load_until",
		"clear_load_pin", "clear_flap_protection", "clear_manual_override", "update_binding",
		"update_upstream_control", "transition_token_group_tier"} {
		if !store.CapabilitySupportsRestartReadback(capability) {
			t.Errorf("external mutation %q lost restart readback protection", capability)
		}
	}
	for _, capability := range []string{"update_dispatch_policy", "activate_policy_version", "schedule_command",
		"cancel_scheduled_command", "refresh_upstream", "trigger_reconcile"} {
		if store.CapabilitySupportsRestartReadback(capability) {
			t.Errorf("local/unobservable mutation %q would remain stuck in reconciliation", capability)
		}
	}
}

func reconciliationAccountState(t *testing.T, syncedAt time.Time, schedulable bool, loadFactor *int,
	control model.AccountControl) json.RawMessage {
	t.Helper()
	return marshalRaw(map[string]any{"account_id": int64(225), "schedulable": schedulable, "load_factor": loadFactor,
		"control": control, "policy": model.Policy{AccountID: 225, Enabled: true}, "snapshot_synced_at": syncedAt})
}

func reconciliationSourceState(t *testing.T, updatedAt time.Time, keyID, tier string) json.RawMessage {
	t.Helper()
	return marshalRaw(map[string]any{"id": int64(9), "updated_at": updatedAt, "last_success_at": updatedAt,
		"failover_policies": []map[string]any{{"key_id": keyID, "current_tier": tier,
			"observed_group_id": tier + "-group", "state_updated_at": updatedAt}}})
}
