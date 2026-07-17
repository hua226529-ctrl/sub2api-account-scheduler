package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestAccountActionIdempotencyConflictAndGeneratedCommand(t *testing.T) {
	server, _, api := newAccountActionServer(t)
	first := performAccountAction(t, server, 1, "pause", "http-command-1", `{"confirm":true}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	var firstResult accountcontrol.Result
	if err := json.Unmarshal(first.Body.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	second := performAccountAction(t, server, 1, "pause", "http-command-1", `{"confirm":true}`)
	var secondResult accountcontrol.Result
	if err := json.Unmarshal(second.Body.Bytes(), &secondResult); err != nil {
		t.Fatal(err)
	}
	if second.Code != http.StatusOK || !secondResult.IdempotentReplay || secondResult.MutationID != firstResult.MutationID {
		t.Fatalf("second status=%d result=%+v", second.Code, secondResult)
	}
	if api.Stats().ByName[testsupport.CallSetSchedulable] != 1 {
		t.Fatal("idempotent HTTP replay repeated the upstream write")
	}

	conflict := performAccountAction(t, server, 1, "resume", "http-command-1", `{"confirm":true}`)
	if conflict.Code != http.StatusConflict || !bytes.Contains(conflict.Body.Bytes(), []byte("idempotency_conflict")) {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	releaseConflict := performAccountAction(t, server, 1, "release-manual-hold", "http-command-1", `{"confirm":true}`)
	if releaseConflict.Code != http.StatusConflict || !bytes.Contains(releaseConflict.Body.Bytes(), []byte("idempotency_conflict")) {
		t.Fatalf("release conflict status=%d body=%s", releaseConflict.Code, releaseConflict.Body.String())
	}

	generatedServer, _, _ := newAccountActionServer(t)
	generated := performAccountAction(t, generatedServer, 1, "pause", "", `{"confirm":true}`)
	var generatedResult accountcontrol.Result
	if err := json.Unmarshal(generated.Body.Bytes(), &generatedResult); err != nil {
		t.Fatal(err)
	}
	if generated.Code != http.StatusOK || generatedResult.CommandID == "" {
		t.Fatalf("generated status=%d result=%+v", generated.Code, generatedResult)
	}
}

func TestAccountActionBlockedUncertainTTLAndReleaseReplay(t *testing.T) {
	t.Run("blocked is structured", func(t *testing.T) {
		server, database, api := newAccountActionServer(t)
		state := model.AgentFreezeState{ScopeType: "global", Mode: model.AgentFreezeModeWritesFrozen, Actor: "test"}
		if err := database.Store.SetAgentFreezeState(context.Background(), &state); err != nil {
			t.Fatal(err)
		}
		response := performAccountAction(t, server, 1, "pause", "blocked-http", `{"confirm":true}`)
		var result accountcontrol.Result
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusConflict || result.BlockedReason != accountcontrol.BlockWritesFrozen || result.Status != accountcontrol.StatusBlocked {
			t.Fatalf("status=%d result=%+v", response.Code, result)
		}
		if api.Stats().ByName[testsupport.CallSetSchedulable] != 0 {
			t.Fatal("blocked HTTP request reached upstream")
		}
	})

	t.Run("uncertain is service unavailable", func(t *testing.T) {
		server, _, api := newAccountActionServer(t)
		api.SetFailure(testsupport.CallSetSchedulable, testsupport.Failure{AtCall: 1, Err: io.EOF})
		api.SetFailure(testsupport.CallListAccounts, testsupport.Failure{AtCall: 2, Err: io.EOF})
		response := performAccountAction(t, server, 1, "pause", "uncertain-http", `{"confirm":true}`)
		var result accountcontrol.Result
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusServiceUnavailable || !result.Uncertain || result.Status != accountcontrol.StatusUncertain {
			t.Fatalf("status=%d result=%+v", response.Code, result)
		}
	})

	t.Run("resume TTL and release replay", func(t *testing.T) {
		server, database, _ := newAccountActionServer(t)
		monitorID := int64(10_000)
		if err := database.Store.UpsertPolicy(context.Background(), model.Policy{AccountID: 1, MonitorID: &monitorID, Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := server.engine.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if snapshot := server.engine.Snapshot(); len(snapshot.Bindings) != 1 {
			t.Fatalf("expected one policy binding before release test: %+v", snapshot)
		}
		pause := performAccountAction(t, server, 1, "pause", "hold-for-release", `{"confirm":true}`)
		if pause.Code != http.StatusOK {
			t.Fatalf("pause status=%d body=%s", pause.Code, pause.Body.String())
		}
		release := performAccountAction(t, server, 1, "release-manual-hold", "release-http", `{"confirm":true}`)
		var releaseResult accountcontrol.Result
		if err := json.Unmarshal(release.Body.Bytes(), &releaseResult); err != nil {
			t.Fatal(err)
		}
		replay := performAccountAction(t, server, 1, "release-manual-hold", "release-http", `{"confirm":true}`)
		var replayResult accountcontrol.Result
		if err := json.Unmarshal(replay.Body.Bytes(), &replayResult); err != nil {
			t.Fatal(err)
		}
		if release.Code != http.StatusOK || replay.Code != http.StatusOK || !replayResult.IdempotentReplay || replayResult.MutationID != releaseResult.MutationID {
			t.Fatalf("release status=%d body=%s result=%+v; replay status=%d body=%s result=%+v",
				release.Code, release.Body.String(), releaseResult, replay.Code, replay.Body.String(), replayResult)
		}

		if response := performAccountAction(t, server, 1, "pause", "hold-for-ttl", `{"confirm":true}`); response.Code != http.StatusOK {
			t.Fatalf("second pause status=%d body=%s", response.Code, response.Body.String())
		}
		started := time.Now().UTC()
		resume := performAccountAction(t, server, 1, "resume", "resume-with-ttl", `{"confirm":true,"ttl_minutes":45}`)
		var resumeResult accountcontrol.Result
		if err := json.Unmarshal(resume.Body.Bytes(), &resumeResult); err != nil {
			t.Fatal(err)
		}
		if resume.Code != http.StatusOK || resumeResult.ExpiresAt == nil ||
			resumeResult.ExpiresAt.Before(started.Add(44*time.Minute)) || resumeResult.ExpiresAt.After(time.Now().UTC().Add(46*time.Minute)) {
			t.Fatalf("status=%d result=%+v", resume.Code, resumeResult)
		}
	})
}

func TestReadOnlyHealthRequestCompletesWhileAccountWriteWaitsOnUpstream(t *testing.T) {
	server, _, api := newAccountActionServer(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallSetSchedulable {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	mutationDone := make(chan error, 1)
	go func() { mutationDone <- server.engine.ManualPause(context.Background(), 1, "web") }()
	<-entered

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	readDone := make(chan struct{})
	go func() {
		server.health(response, request)
		close(readDone)
	}()
	select {
	case <-readDone:
		if response.Code != http.StatusOK {
			t.Fatalf("health status=%d body=%s", response.Code, response.Body.String())
		}
	case <-time.After(time.Second):
		close(release)
		<-mutationDone
		t.Fatal("read-only HTTP request blocked behind upstream account write")
	}
	close(release)
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
}

func newAccountActionServer(t *testing.T) (*Server, *testsupport.TempDatabase, *testsupport.FakeSub2API) {
	t.Helper()
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	now := time.Now().UTC()
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 1, Monitors: 1, Now: now})
	api := testsupport.NewFakeSub2API(fixture)
	engine := reconcile.NewEngine(api, database.Store, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &Server{store: database.Store, engine: engine}, database, api
}

func performAccountAction(t *testing.T, server *Server, accountID int64, action, idempotencyKey, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/actions", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	request.SetPathValue("accountID", strconv.FormatInt(accountID, 10))
	request.SetPathValue("action", action)
	response := httptest.NewRecorder()
	server.accountAction(response, request)
	return response
}
