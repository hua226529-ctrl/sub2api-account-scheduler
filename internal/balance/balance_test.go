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
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestSecretBoxRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := NewSecretBox(key)
	if err != nil {
		t.Fatal(err)
	}
	nonce, ciphertext, err := box.Encrypt([]byte(`{"username":"user","password":"secret"}`))
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := box.Decrypt(nonce, ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != `{"username":"user","password":"secret"}` {
		t.Fatalf("unexpected plaintext %q", plaintext)
	}
}

func TestSourceInputAlwaysBuildsPasswordCredentials(t *testing.T) {
	credentials := credentialsFromInput(SourceInput{Username: "owner@example.com", Password: "secret"})
	if credentials.AuthMode != "password" || credentials.Username != "owner@example.com" || credentials.Password != "secret" {
		t.Fatalf("unexpected password credentials: %+v", credentials)
	}
	if credentials.AccessKey != "" || credentials.UserID != "" {
		t.Fatalf("legacy access-key fields were populated: %+v", credentials)
	}
}

func TestNewAPIFetchBalanceAndEffectiveRate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/user/login":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "ok"})
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 7}})
		case "/api/status":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 500000, "quota_display_type": "CNY", "usd_exchange_rate": 7.2}})
		case "/api/user/self":
			if r.Header.Get("New-Api-User") != "7" {
				t.Fatal("missing New-Api-User header")
			}
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"username": "alice", "quota": 1000000}})
		case "/api/user/self/groups":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"vip": map[string]any{"ratio": 0.8}, "auto": map[string]any{"ratio": "自动"}}})
		case "/api/token/":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"items": []map[string]any{{"id": 1, "name": "vip-key", "key": "sk-1234567890", "group": "vip", "status": 1}, {"id": 2, "name": "auto-key", "key": "sk-0987654321", "group": "auto", "status": 1}}, "total": 2}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	result, err := fetcher.fetchNewAPI(context.Background(), server.URL, model.UpstreamCredentials{Username: "alice", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Balance != 14.4 || result.Unit != "CNY" {
		t.Fatalf("unexpected balance: %+v", result)
	}
	if len(result.KeyRates) != 2 || result.KeyRates[0].RateMultiplier == nil || *result.KeyRates[0].RateMultiplier != 0.8 || !result.KeyRates[1].Dynamic {
		t.Fatalf("unexpected key rates: %+v", result.KeyRates)
	}
}

func TestNewAPIAccessKeyCanReadAndSwitchTokenGroup(t *testing.T) {
	currentGroup := "cheap"
	applyGroupWrite := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/status" {
			if r.Header.Get("Authorization") != "Bearer management-token" || r.Header.Get("New-Api-User") != "17" {
				t.Fatalf("missing management headers for %s", r.URL.Path)
			}
		}
		switch r.URL.Path {
		case "/api/status":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 500000, "quota_display_type": "USD"}})
		case "/api/user/self":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"username": "operator", "quota": 5000000}})
		case "/api/user/self/groups":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"cheap": map[string]any{"ratio": 0.5}, "backup": map[string]any{"ratio": 1.2}}})
		case "/api/token/":
			if r.Method == http.MethodPut {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if applyGroupWrite {
					currentGroup, _ = body["group"].(string)
				}
				writeTestJSON(w, map[string]any{"success": true, "data": true})
				return
			}
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"items": []map[string]any{{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup, "status": 1}}, "total": 1}})
		case "/api/token/1":
			writeTestJSON(w, map[string]any{"success": true, "data": map[string]any{"id": 1, "name": "router", "key": "sk-router1234", "group": currentGroup, "status": 1, "expired_time": -1, "remain_quota": 0, "unlimited_quota": true}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	credentials := model.UpstreamCredentials{AccessKey: "management-token", UserID: "17"}
	result, err := fetcher.Fetch(context.Background(), "newapi", server.URL, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if result.Balance != 10 || len(result.Groups) != 2 || result.KeyRates[0].GroupID != "cheap" {
		t.Fatalf("unexpected access-key result: %+v", result)
	}
	result, err = fetcher.SwitchGroup(context.Background(), "newapi", server.URL, credentials, "1", "backup")
	if err != nil {
		t.Fatal(err)
	}
	if currentGroup != "backup" || result.KeyRates[0].GroupID != "backup" {
		t.Fatalf("token group was not confirmed: %+v", result.KeyRates)
	}
	applyGroupWrite = false
	_, err = fetcher.SwitchGroup(context.Background(), "newapi", server.URL, credentials, "1", "cheap")
	if err == nil || !mutation.IsUncertain(err) {
		t.Fatalf("eventually consistent old-group readback must remain uncertain: %v", err)
	}
}

func TestSub2FetchUsesUserRateOverride(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"access_token": "access", "refresh_token": "refresh", "expires_in": 900}})
		case "/api/v1/auth/me":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"email": "owner@example.com", "balance": 23.5}})
		case "/api/v1/groups/available":
			writeTestJSON(w, map[string]any{"code": 0, "data": []map[string]any{{"id": 1, "name": "default", "rate_multiplier": 1.2}}})
		case "/api/v1/groups/rates":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"1": 0.8}})
		case "/api/v1/keys":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"items": []map[string]any{{"id": 1, "name": "fixed", "key": "sk-1234567890", "group_id": 1, "status": "active"}, {"id": 2, "name": "dynamic", "key": "short", "group_id": nil, "status": "active"}}, "total": 2}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	result, err := fetcher.fetchSub2(context.Background(), server.URL, model.UpstreamCredentials{Username: "owner@example.com", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Balance != 23.5 || result.Unit != "USD" || len(result.KeyRates) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.KeyRates[0].RateMultiplier == nil || *result.KeyRates[0].RateMultiplier != 0.8 {
		t.Fatalf("user rate override was not applied: %+v", result.KeyRates[0])
	}
	if !result.KeyRates[1].Dynamic || result.KeyRates[1].KeyHint == "short" {
		t.Fatalf("dynamic key was not marked or masked: %+v", result.KeyRates[1])
	}
}

func TestSub2RefreshKeyCanReadAndSwitchTokenGroup(t *testing.T) {
	currentGroup := int64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/auth/refresh" && r.Header.Get("Authorization") != "Bearer access-token" {
			t.Fatalf("missing Sub2 access token for %s", r.URL.Path)
		}
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"access_token": "access-token", "refresh_token": "rotated-refresh", "expires_in": 900}})
		case "/api/v1/auth/me":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"email": "owner@example.com", "balance": 18.5}})
		case "/api/v1/groups/available":
			writeTestJSON(w, map[string]any{"code": 0, "data": []map[string]any{{"id": 1, "name": "低倍率", "rate_multiplier": 0.6}, {"id": 2, "name": "备用", "rate_multiplier": 1.1}}})
		case "/api/v1/groups/rates":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{}})
		case "/api/v1/keys":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"items": []map[string]any{{"id": 9, "name": "router", "key": "sk-sub2router", "group_id": currentGroup, "status": "active"}}, "total": 1}})
		case "/api/v1/keys/9":
			if r.Method != http.MethodPut {
				http.NotFound(w, r)
				return
			}
			var body struct {
				GroupID int64 `json:"group_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			currentGroup = body.GroupID
			writeTestJSON(w, map[string]any{"code": 0, "data": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	credentials := model.UpstreamCredentials{AccessKey: "refresh-token"}
	result, err := fetcher.Fetch(context.Background(), "sub2", server.URL, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if result.Balance != 18.5 || result.KeyRates[0].GroupID != "1" || result.RotatedAccessKey != "rotated-refresh" {
		t.Fatalf("unexpected refresh-key result: %+v", result)
	}
	result, err = fetcher.SwitchGroup(context.Background(), "sub2", server.URL, credentials, "9", "2")
	if err != nil {
		t.Fatal(err)
	}
	if currentGroup != 2 || result.KeyRates[0].GroupID != "2" {
		t.Fatalf("Sub2 token group was not confirmed: %+v", result.KeyRates)
	}
}

func TestSub2FetchFallsBackToLegacyProfileAndKeyPaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"access_token": "access", "expires_in": 900}})
		case "/api/v1/user/profile":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"email": "legacy@example.com", "balance": 7.5}})
		case "/api/v1/groups/available":
			writeTestJSON(w, map[string]any{"code": 0, "data": []map[string]any{{"id": 2, "name": "legacy", "rate_multiplier": 1.5}}})
		case "/api/v1/groups/rates":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{}})
		case "/api/v1/api-keys":
			writeTestJSON(w, map[string]any{"code": 0, "data": map[string]any{"items": []map[string]any{{"id": 9, "name": "legacy-key", "key": "sk-legacy1234", "group_id": 2, "status": "active"}}, "total": 1}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	result, err := fetcher.fetchSub2(context.Background(), server.URL, model.UpstreamCredentials{Username: "legacy@example.com", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Balance != 7.5 || len(result.KeyRates) != 1 || result.KeyRates[0].RateMultiplier == nil || *result.KeyRates[0].RateMultiplier != 1.5 {
		t.Fatalf("unexpected legacy result: %+v", result)
	}
}

func TestDisableSourceDoesNotRequireUpstreamConnection(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10, FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := NewSecretBox(key)
	if err != nil {
		t.Fatal(err)
	}
	credentials, _ := json.Marshal(model.UpstreamCredentials{Username: "owner", Password: "secret"})
	nonce, ciphertext, err := box.Encrypt(credentials)
	if err != nil {
		t.Fatal(err)
	}
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "offline", Provider: "newapi", BaseURL: "https://offline.example", NormalizedURL: "https://offline.example", CredentialNonce: nonce, CredentialCiphertext: ciphertext, PauseBelow: 5, ResumeAt: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	source.BalanceLocked = true
	source.LowStreak = 2
	if err := database.SaveUpstreamSuccess(ctx, source, nil, nil); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(database, &fakeAccountAPI{}, fakeTrigger{}, NewFetcher(100*time.Millisecond, false), box, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	updated, err := manager.Update(ctx, source.ID, SourceInput{Name: source.Name, Provider: source.Provider, BaseURL: source.BaseURL, PauseBelow: source.PauseBelow, ResumeAt: source.ResumeAt, Enabled: false}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Enabled || updated.BalanceLocked || updated.LowStreak != 0 || updated.RecoveryStreak != 0 {
		t.Fatalf("disabled source retained control state: %+v", updated)
	}
}

func TestBalanceLockNeedsTwoLowAndTwoRecoveryResults(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10, FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "test", Provider: "newapi", BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{1}, PauseBelow: 5, ResumeAt: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	api := &fakeAccountAPI{accounts: []model.Account{{ID: 225, Name: "account", Schedulable: true, Credentials: map[string]any{"base_url": "https://upstream.example/v1"}}}}
	manager := NewManager(database, api, fakeTrigger{}, NewFetcher(time.Second, false), nil, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for i := 0; i < 2; i++ {
		if err := manager.applySuccess(ctx, &source, model.UpstreamResult{Balance: 4, Unit: "USD", FetchedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		lock, err := database.GetActiveBalanceLock(ctx, 225)
		if err != nil {
			t.Fatal(err)
		}
		if (i == 0 && lock != nil) || (i == 1 && lock == nil) {
			t.Fatalf("unexpected lock after low result %d: %+v", i+1, lock)
		}
	}
	for i := 0; i < 2; i++ {
		if err := manager.applySuccess(ctx, &source, model.UpstreamResult{Balance: 12, Unit: "USD", FetchedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	lock, err := database.GetActiveBalanceLock(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if lock != nil || source.BalanceLocked {
		t.Fatalf("balance lock was not cleared: %+v", lock)
	}
}

func TestCostRoutingPrefersLowRateAndUsesTwoPhaseFailover(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10, FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC()
	low, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "低倍率", Provider: "newapi", BaseURL: "https://low.example", NormalizedURL: "https://low.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{1}, PauseBelow: 5, ResumeAt: 10, Enabled: true, SelectedKeyID: "1", RoutingEnabled: true, RoutingPool: "主池"})
	if err != nil {
		t.Fatal(err)
	}
	high, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "高倍率", Provider: "sub2", BaseURL: "https://high.example", NormalizedURL: "https://high.example", CredentialNonce: []byte{2}, CredentialCiphertext: []byte{2}, PauseBelow: 5, ResumeAt: 10, Enabled: true, SelectedKeyID: "2", RoutingEnabled: true, RoutingPool: "主池"})
	if err != nil {
		t.Fatal(err)
	}
	lowRate, highRate := 0.5, 1.2
	lowBalance, highBalance := 20.0, 30.0
	low.Balance, low.LastSuccessAt, low.LastAttemptAt, low.RecoveryStreak = &lowBalance, &now, &now, 2
	high.Balance, high.LastSuccessAt, high.LastAttemptAt, high.RecoveryStreak = &highBalance, &now, &now, 2
	if err := database.SaveUpstreamSuccess(ctx, low, []model.KeyRate{{ExternalID: "1", GroupID: "cheap", RateMultiplier: &lowRate, Status: "active"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := database.SaveUpstreamSuccess(ctx, high, []model.KeyRate{{ExternalID: "2", GroupID: "backup", RateMultiplier: &highRate, Status: "active"}}, nil); err != nil {
		t.Fatal(err)
	}
	api := &fakeAccountAPI{accounts: []model.Account{
		{ID: 101, Name: "low-account", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://low.example/v1"}},
		{ID: 202, Name: "high-account", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://high.example/v1"}},
	}}
	manager := NewManager(database, api, fakeTrigger{}, NewFetcher(time.Second, false), nil, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := manager.reconcileCostRouting(ctx); err != nil {
		t.Fatal(err)
	}
	if lock, _ := database.GetActiveCostLock(ctx, 202); lock == nil || lock.RateMultiplier != highRate {
		t.Fatalf("high-rate account was not placed on standby: %+v", lock)
	}
	if lock, _ := database.GetActiveCostLock(ctx, 101); lock != nil {
		t.Fatalf("low-rate account was unexpectedly locked: %+v", lock)
	}

	lowBalance = 1
	low.BalanceLocked = true
	low.LowStreak = 2
	if err := database.SaveUpstreamSuccess(ctx, low, []model.KeyRate{{ExternalID: "1", GroupID: "cheap", RateMultiplier: &lowRate, Status: "active"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileCostRouting(ctx); err != nil {
		t.Fatal(err)
	}
	if lock, _ := database.GetActiveCostLock(ctx, 202); lock != nil {
		t.Fatalf("backup remained locked during failover activation: %+v", lock)
	}
	if lock, _ := database.GetActiveCostLock(ctx, 101); lock != nil {
		t.Fatalf("low-rate source was locked before backup became available: %+v", lock)
	}
	if err := manager.reconcileCostRouting(ctx); err != nil {
		t.Fatal(err)
	}
	if lock, _ := database.GetActiveCostLock(ctx, 101); lock == nil || lock.SourceID != low.ID {
		t.Fatalf("low-rate source did not enter standby after backup was available: %+v", lock)
	}
}

type fakeAccountAPI struct{ accounts []model.Account }

func (f *fakeAccountAPI) ListAccounts(context.Context) ([]model.Account, error) {
	return f.accounts, nil
}

type fakeTrigger struct{}

func (fakeTrigger) Trigger() {}

func writeTestJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}
