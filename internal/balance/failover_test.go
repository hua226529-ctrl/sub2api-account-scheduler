package balance

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/automation"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestGroupFailoverPolicyVersionConfirmationAndIdempotentTransition(t *testing.T) {
	ctx := context.Background()
	var currentGroup atomic.Value
	currentGroup.Store("main")
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/login":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "ok"})
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 7}})
		case "/api/status":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 500000, "quota_display_type": "USD"}})
		case "/api/user/self":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"username": "owner", "quota": 5000000}})
		case "/api/user/self/groups":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{
				"main": map[string]any{"ratio": 0.5}, "backup": map[string]any{"ratio": 0.8}, "emergency": map[string]any{"ratio": 1.2}, "emergency-2": map[string]any{"ratio": 1.4},
			}})
		case "/api/token/1":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup.Load().(string), "status": 1, "expired_time": -1, "unlimited_quota": true}})
		case "/api/token/":
			if r.Method == http.MethodPut {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				currentGroup.Store(body["group"].(string))
				writes.Add(1)
				writeTestJSON(w, map[string]any{"success": true, "data": true})
				return
			}
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"items": []map[string]any{{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup.Load().(string), "status": 1}}, "total": 1}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	api := &fakeAccountAPI{accounts: []model.Account{{ID: 11, Name: "primary", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": server.URL + "/v1"}}}}
	manager := NewManager(database, api, fakeTrigger{}, NewFetcher(3*time.Second, true), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	freeze := &testFreezeReader{}
	manager.barrier = automation.NewBarrier()
	manager.freeze = freeze
	source, err := manager.Create(ctx, SourceInput{Name: "test", Provider: "newapi", BaseURL: server.URL, Username: "owner", Password: "secret", PauseBelow: 1, ResumeAt: 2, Enabled: true}, "test")
	if err != nil {
		t.Fatal(err)
	}

	policy, err := manager.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{SourceID: source.ID, KeyID: "1", Enabled: true, Pool: "gpt", MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency", AccountIDs: []int64{11}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if policy.Version != 1 || policy.Confirmed || policy.State.CurrentTier != model.GroupTierMain {
		t.Fatalf("unexpected new policy: %+v", policy)
	}
	unchanged, err := manager.SaveGroupFailoverPolicy(ctx, policy, "test")
	if err != nil || unchanged.Version != policy.Version {
		t.Fatalf("idempotent policy save changed version: %+v, %v", unchanged, err)
	}
	confirmed, err := manager.ConfirmGroupFailoverPolicy(ctx, source.ID, "1", policy.Version, "test")
	if err != nil || !confirmed.Confirmed {
		t.Fatalf("policy was not confirmed: %+v, %v", confirmed, err)
	}
	dispatch, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dispatch.FailoverManualProtectionMinutes = 7
	dispatch.FailoverSwitchCooldownMinutes = 9
	dispatch.FailoverReturnRetryMinutes = 17
	if err := database.UpdateSettings(ctx, dispatch); err != nil {
		t.Fatal(err)
	}

	request := model.GroupTierTransitionRequest{SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierBackup, IdempotencyKey: "transition-1", Actor: "web", Reason: "manual test", Manual: true}
	transition, err := manager.TransitionGroupTier(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if transition.Status != model.GroupTransitionCompleted || currentGroup.Load().(string) != "backup" || writes.Load() != 1 {
		t.Fatalf("unexpected transition: %+v group=%s writes=%d", transition, currentGroup.Load(), writes.Load())
	}
	repeated, err := manager.TransitionGroupTier(ctx, request)
	if err != nil || repeated.ID != transition.ID || writes.Load() != 1 {
		t.Fatalf("idempotency failed: %+v, %v, writes=%d", repeated, err, writes.Load())
	}
	stored, err := database.GetGroupFailoverPolicy(ctx, source.ID, "1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.State.CurrentTier != model.GroupTierBackup || stored.State.ManualOverrideUntil == nil || stored.State.CooldownUntil == nil {
		t.Fatalf("transition state was not persisted: %+v", stored.State)
	}
	if remaining := time.Until(stored.State.ManualOverrideUntil.UTC()); remaining < 6*time.Minute || remaining > 8*time.Minute {
		t.Fatalf("configured manual protection was not applied: %s", remaining)
	}
	if remaining := time.Until(stored.State.CooldownUntil.UTC()); remaining < 8*time.Minute || remaining > 10*time.Minute {
		t.Fatalf("configured switch cooldown was not applied: %s", remaining)
	}
	_, err = manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierEmergency, IdempotencyKey: "automatic-during-hold", Actor: "agent", Reason: "test"})
	if err == nil {
		t.Fatal("automatic transition bypassed manual protection")
	}

	stored.State.ManualHoldUntil = nil
	stored.State.ManualOverrideUntil = nil
	stored.State.CooldownUntil = nil
	if err := database.SaveGroupFailoverState(ctx, stored.State); err != nil {
		t.Fatal(err)
	}
	returned, err := manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierMain,
		IdempotencyKey: "return-main", Actor: "system", Reason: "stable", Trigger: "stable_return_main",
	})
	if err != nil || returned.Status != model.GroupTransitionCompleted || currentGroup.Load().(string) != "main" {
		t.Fatalf("return to main failed: transition=%+v err=%v group=%s", returned, err, currentGroup.Load())
	}
	rolledBack, err := manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierBackup,
		IdempotencyKey: "rollback-main", Actor: "system", Reason: "main failed", Trigger: "main_trial_rollback",
	})
	if err != nil || rolledBack.Status != model.GroupTransitionCompleted || currentGroup.Load().(string) != "backup" {
		t.Fatalf("main trial rollback did not bypass cooldown: transition=%+v err=%v group=%s", rolledBack, err, currentGroup.Load())
	}
	stored, err = database.GetGroupFailoverPolicy(ctx, source.ID, "1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.State.ReturnBlockedUntil == nil || stored.State.PreviousStableTier != "" {
		t.Fatalf("rollback state not finalized safely: %+v", stored.State)
	}
	if remaining := time.Until(stored.State.ReturnBlockedUntil.UTC()); remaining < 16*time.Minute || remaining > 18*time.Minute {
		t.Fatalf("configured return retry delay was not applied: %s", remaining)
	}
	cutoff := time.Now().UTC().Add(-time.Minute)
	poolChanged, err := database.HasAutomaticGroupTransitionInPoolSince(ctx, "gpt", cutoff)
	if err != nil || !poolChanged {
		t.Fatalf("pool transition lease was not visible: changed=%v err=%v", poolChanged, err)
	}
	_, err = manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierEmergency,
		IdempotencyKey: "stale-agent", Actor: "agent", Reason: "stale packet", Trigger: "agent",
		ExpectedPool: "gpt", ExpectedFromTier: model.GroupTierBackup, EvidenceCutoffAt: &cutoff,
	})
	if err == nil {
		t.Fatal("stale agent packet ignored a newer automatic transition in the same pool")
	}

	currentGroup.Store("emergency")
	_, err = manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierEmergency,
		IdempotencyKey: "stale-cache", Actor: "system", Reason: "stale", Trigger: "global_outage",
	})
	if err == nil {
		t.Fatal("automatic transition overwrote an upstream-side manual group change")
	}
	stored, err = database.GetGroupFailoverPolicy(ctx, source.ID, "1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.State.CurrentTier != model.GroupTierEmergency || stored.State.ManualHoldUntil == nil {
		t.Fatalf("upstream-side manual group change was not protected: %+v", stored.State)
	}

	stored.EmergencyGroupID = "emergency-2"
	changed, err := manager.SaveGroupFailoverPolicy(ctx, stored, "test")
	if err != nil {
		t.Fatal(err)
	}
	if changed.Version != 2 || changed.Confirmed {
		t.Fatalf("changed policy retained stale confirmation: %+v", changed)
	}

	freeze.state = model.FreezeState{AllAutomation: true}
	writesBeforeFreeze := writes.Load()
	credentials := model.UpstreamCredentials{AuthMode: "password", Username: "owner", Password: "secret"}
	if _, err := manager.switchAutomatedGroup(ctx, false, false, source, credentials, "1", "backup"); err == nil {
		t.Fatal("automatic group write crossed the global freeze")
	}
	if writes.Load() != writesBeforeFreeze {
		t.Fatalf("frozen group write reached the upstream: before=%d after=%d", writesBeforeFreeze, writes.Load())
	}
}

func TestManualGroupTransitionBypassesAutomaticEligibilityOnly(t *testing.T) {
	ctx := context.Background()
	var currentGroup atomic.Value
	currentGroup.Store("main")
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/login":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "ok"})
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 7}})
		case "/api/status":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 500000, "quota_display_type": "USD"}})
		case "/api/user/self":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"username": "owner", "quota": 5000000}})
		case "/api/user/self/groups":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{
				"main": map[string]any{"ratio": 0.5}, "backup": map[string]any{"ratio": 0.8}, "emergency": map[string]any{"ratio": 1.2},
			}})
		case "/api/token/1":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup.Load().(string), "status": 1, "expired_time": -1, "unlimited_quota": true}})
		case "/api/token/":
			if r.Method == http.MethodPut {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				currentGroup.Store(body["group"].(string))
				writes.Add(1)
				writeTestJSON(w, map[string]any{"success": true, "data": true})
				return
			}
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"items": []map[string]any{{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup.Load().(string), "status": 1}}, "total": 1}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	api := &fakeAccountAPI{accounts: []model.Account{{ID: 11, Name: "primary", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": server.URL + "/v1"}}}}
	manager := NewManager(database, api, fakeTrigger{}, NewFetcher(3*time.Second, true), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	manager.barrier = automation.NewBarrier()
	manager.freeze = &testFreezeReader{}
	source, err := manager.Create(ctx, SourceInput{Name: "test", Provider: "newapi", BaseURL: server.URL, Username: "owner", Password: "secret", PauseBelow: 1, ResumeAt: 2, Enabled: true}, "test")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := manager.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{SourceID: source.ID, KeyID: "1", Enabled: false, Pool: "gpt", MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency", AccountIDs: []int64{11}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ConfirmGroupFailoverPolicy(ctx, source.ID, "1", policy.Version, "test"); err != nil {
		t.Fatal(err)
	}

	source, err = database.GetUpstreamSource(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	staleAt := time.Now().UTC().Add(-time.Hour)
	zeroBalance := 0.0
	source.Balance = &zeroBalance
	source.BalanceLocked = true
	source.LastSuccessAt = &staleAt
	if err := database.SaveUpstreamSuccess(ctx, source,
		[]model.KeyRate{{ExternalID: "1", Name: "router", GroupID: "main", Status: "disabled"}},
		[]model.UpstreamGroup{{ExternalID: "main", Name: "main"}, {ExternalID: "backup", Name: "backup"}, {ExternalID: "emergency", Name: "emergency"}}); err != nil {
		t.Fatal(err)
	}
	source.Enabled = false
	source.MigrationRequired = true
	if err := database.UpdateUpstreamSource(ctx, source); err != nil {
		t.Fatal(err)
	}

	transition, err := manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierBackup, IdempotencyKey: "administrator-bypass",
		Actor: "administrator:agent", Reason: "exact administrator command", Manual: true,
	})
	if err != nil || transition.Status != model.GroupTransitionCompleted || currentGroup.Load().(string) != "backup" || writes.Load() != 1 {
		t.Fatalf("administrator command was blocked by automatic eligibility: transition=%+v err=%v group=%s writes=%d", transition, err, currentGroup.Load(), writes.Load())
	}
	_, err = manager.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{
		SourceID: source.ID, KeyID: "1", TargetTier: model.GroupTierEmergency, IdempotencyKey: "automatic-still-blocked",
		Actor: "agent:v2", Reason: "automatic attempt",
	})
	if err == nil || writes.Load() != 1 {
		t.Fatalf("ordinary automation bypassed disabled/migration eligibility: err=%v writes=%d", err, writes.Load())
	}
}

func TestUpstreamIdentityChangeInvalidatesConfirmedFailoverPolicy(t *testing.T) {
	ctx := context.Background()
	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	payload, _ := json.Marshal(model.UpstreamCredentials{AuthMode: "password", Username: "old@example.com", Password: "old-secret"})
	nonce, ciphertext, _ := box.Encrypt(payload)
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{
		Name: "source", Provider: "newapi", BaseURL: "https://old.example", NormalizedURL: "https://old.example",
		CredentialNonce: nonce, CredentialCiphertext: ciphertext, CredentialMode: "password",
		PauseBelow: 1, ResumeAt: 2, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := database.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{
		SourceID: source.ID, KeyID: "1", KeyName: "router", Enabled: true, Pool: "pool",
		MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency", AccountIDs: []int64{11},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ConfirmGroupFailoverPolicy(ctx, source.ID, "1", policy.Version, "test"); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(database, &fakeAccountAPI{}, fakeTrigger{}, NewFetcher(time.Second, false), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err = manager.Update(ctx, source.ID, SourceInput{
		Name: "source", Provider: "newapi", BaseURL: "https://new.example", Username: "new@example.com", Password: "new-secret",
		PauseBelow: 1, ResumeAt: 2, Enabled: false,
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	updated, err := database.GetGroupFailoverPolicy(ctx, source.ID, "1")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Confirmed || !updated.State.Frozen || updated.State.FreezeReason == "" {
		t.Fatalf("identity change retained an active failover confirmation: %+v", updated)
	}
}

func TestLegacyAccessKeySourceIsMarkedForPasswordMigration(t *testing.T) {
	ctx := context.Background()
	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	payload, _ := json.Marshal(model.UpstreamCredentials{AuthMode: "access_key", AccessKey: "legacy", UserID: "7"})
	nonce, ciphertext, err := box.Encrypt(payload)
	if err != nil {
		t.Fatal(err)
	}
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "legacy", Provider: "newapi", BaseURL: "https://legacy.example", NormalizedURL: "https://legacy.example", CredentialNonce: nonce, CredentialCiphertext: ciphertext, CredentialMode: "unknown", PauseBelow: 1, ResumeAt: 2, Enabled: true, RoutingEnabled: true, RoutingPool: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(database, &fakeAccountAPI{}, fakeTrigger{}, NewFetcher(time.Second, false), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := manager.Refresh(ctx, source.ID); err == nil {
		t.Fatal("legacy access key was accepted for refresh")
	}
	updated, err := database.GetUpstreamSource(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.MigrationRequired || updated.CredentialMode != "access_key" || updated.Enabled || updated.RoutingEnabled {
		t.Fatalf("legacy source was not safely disabled: %+v", updated)
	}
}

func TestPasswordCredentialIsNotReplacedByRotatedSessionToken(t *testing.T) {
	ctx := context.Background()
	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	payload, _ := json.Marshal(model.UpstreamCredentials{AuthMode: "password", Username: "owner@example.com", Password: "secret"})
	nonce, ciphertext, _ := box.Encrypt(payload)
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "password", Provider: "sub2", BaseURL: "https://sub2.example", NormalizedURL: "https://sub2.example", CredentialNonce: nonce, CredentialCiphertext: ciphertext, CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(database, &fakeAccountAPI{}, fakeTrigger{}, NewFetcher(time.Second, false), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := manager.applySuccess(ctx, &source, model.UpstreamResult{Balance: 10, Unit: "USD", FetchedAt: time.Now().UTC(), RotatedAccessKey: "short-lived-refresh"}); err != nil {
		t.Fatal(err)
	}
	stored, err := database.GetUpstreamSource(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.decrypt(stored)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.AuthMode != "password" || credentials.Username != "owner@example.com" || credentials.Password != "secret" || credentials.AccessKey != "" {
		t.Fatalf("password credential was replaced: %+v", credentials)
	}
}

func TestGroupFailoverPolicyRejectsUnmatchedAccount(t *testing.T) {
	ctx := context.Background()
	database := openFailoverTestStore(t)
	defer database.Close()
	box := newFailoverTestSecretBox(t)
	payload, _ := json.Marshal(model.UpstreamCredentials{AuthMode: "password", Username: "owner", Password: "secret"})
	nonce, ciphertext, _ := box.Encrypt(payload)
	now := time.Now().UTC()
	balance := 10.0
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "source", Provider: "newapi", BaseURL: "https://source.example", NormalizedURL: "https://source.example", CredentialNonce: nonce, CredentialCiphertext: ciphertext, CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	source.Balance, source.LastSuccessAt, source.LastAttemptAt = &balance, &now, &now
	rate := 1.0
	if err := database.SaveUpstreamSuccess(ctx, source, []model.KeyRate{{ExternalID: "1", Name: "key", GroupID: "main", Status: "active", RateMultiplier: &rate}}, []model.UpstreamGroup{{ExternalID: "main"}, {ExternalID: "backup"}, {ExternalID: "emergency"}}); err != nil {
		t.Fatal(err)
	}
	api := &fakeAccountAPI{accounts: []model.Account{{ID: 99, Name: "other", Credentials: map[string]any{"base_url": "https://other.example/v1"}}}}
	manager := NewManager(database, api, fakeTrigger{}, NewFetcher(time.Second, false), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err = manager.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{SourceID: source.ID, KeyID: "1", Enabled: true, Pool: "pool", MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency", AccountIDs: []int64{99}}, "test")
	if err == nil {
		t.Fatal("unmatched Sub2API account was accepted")
	}
}

func openFailoverTestStore(t *testing.T) *store.Store {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10, FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func newFailoverTestSecretBox(t *testing.T) *SecretBox {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := NewSecretBox(key)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

type testFreezeReader struct {
	state model.FreezeState
}

func (r *testFreezeReader) FreezeState(context.Context) (model.FreezeState, error) {
	return r.state, nil
}
