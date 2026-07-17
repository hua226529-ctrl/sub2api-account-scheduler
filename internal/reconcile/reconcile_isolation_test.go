package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestReconcileContinuesAfterPerAccountMutationErrors(t *testing.T) {
	tests := []struct {
		name       string
		failure    testsupport.Failure
		wantStatus accountcontrol.MutationStatus
		wantCode   string
	}{
		{"failed", testsupport.Failure{Always: true, Err: errors.New("sub2api returned 422: rejected")},
			accountcontrol.StatusFailed, "mutation_failed"},
		{"uncertain", testsupport.Failure{Always: true, Err: io.EOF},
			accountcontrol.StatusUncertain, "controlled_retry_uncertain"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, _, api := newIsolationEngine(t, true, false)
			api.SetResourceFailure(testsupport.CallSetSchedulable, "1", test.failure)
			err := engine.Reconcile(context.Background())
			var aggregate *AccountReconcileErrors
			if !errors.As(err, &aggregate) || len(aggregate.Issues) != 1 {
				t.Fatalf("aggregate=%+v err=%v", aggregate, err)
			}
			issue := aggregate.Issues[0]
			if issue.AccountID != 1 || issue.Status != test.wantStatus || issue.Code != test.wantCode {
				t.Fatalf("issue=%+v", issue)
			}
			accounts, readErr := api.ListAccounts(context.Background())
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !accounts[0].Schedulable || accounts[1].Schedulable {
				t.Fatalf("account isolation failed: %+v", accounts)
			}
			if engine.Snapshot().LastSyncAt == nil || engine.Snapshot().LastSyncError == "" {
				t.Fatalf("partial reconcile snapshot was not published: %+v", engine.Snapshot())
			}
		})
	}
}

func TestReconcileContinuesAfterPerAccountGuardBlock(t *testing.T) {
	engine, _, api := newIsolationEngine(t, false, true)
	err := engine.Reconcile(context.Background())
	var aggregate *AccountReconcileErrors
	if !errors.As(err, &aggregate) || len(aggregate.Issues) != 1 {
		t.Fatalf("aggregate=%+v err=%v", aggregate, err)
	}
	issue := aggregate.Issues[0]
	if issue.AccountID != 1 || issue.Status != accountcontrol.StatusBlocked || issue.Code != string(accountcontrol.BlockRateLimited) {
		t.Fatalf("issue=%+v", issue)
	}
	accounts, readErr := api.ListAccounts(context.Background())
	if readErr != nil {
		t.Fatal(readErr)
	}
	if accounts[0].Schedulable || !accounts[1].Schedulable {
		t.Fatalf("guard isolation failed: %+v", accounts)
	}
}

func TestReconcileGlobalSnapshotFailureStillStopsCycle(t *testing.T) {
	engine, _, api := newIsolationEngine(t, true, false)
	api.SetFailure(testsupport.CallListAccounts, testsupport.Failure{AtCall: 1, Err: testsupport.ErrInjected})
	err := engine.Reconcile(context.Background())
	if err == nil {
		t.Fatal("global account snapshot failure returned nil")
	}
	var aggregate *AccountReconcileErrors
	if errors.As(err, &aggregate) {
		t.Fatalf("global failure was reported as per-account aggregate: %+v", aggregate)
	}
	if writes := api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 0 {
		t.Fatalf("global snapshot failure wrote %d accounts", writes)
	}
}

func newIsolationEngine(t *testing.T, unhealthy, resume bool) (*Engine, *testsupport.TempDatabase, *testsupport.FakeSub2API) {
	t.Helper()
	now := time.Now().UTC()
	config := testsupport.FixtureConfig{Accounts: 2, Monitors: 2, PolicyEvery: 1, Now: now}
	if unhealthy {
		config.UnhealthyEvery = 1
	}
	if resume {
		config.RateLimitedEvery = 2
		config.Seed = 1
	}
	fixture := testsupport.GenerateFixture(config)
	if resume {
		for index := range fixture.Accounts {
			fixture.Accounts[index].Schedulable = false
			fixture.Controls = append(fixture.Controls, model.AccountControl{AccountID: fixture.Accounts[index].ID,
				OwnsPause: true, Owner: "automatic", ExpectedSchedulable: boolPointer(false), LastObserved: boolPointer(false), UpdatedAt: now})
		}
	}
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	settings, err := database.Store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings.FailureThreshold = 1
	settings.RecoveryThreshold = 1
	settings.HealthMode = model.HealthModeLegacy
	if err := database.Store.UpdateSettings(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	for _, policy := range fixture.Policies {
		if err := database.Store.UpsertPolicy(context.Background(), policy); err != nil {
			t.Fatal(err)
		}
	}
	for _, control := range fixture.Controls {
		if err := database.Store.UpsertControl(context.Background(), control); err != nil {
			t.Fatal(err)
		}
	}
	api := testsupport.NewFakeSub2API(fixture)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(api, database.Store, time.Minute, logger), database, api
}

func boolPointer(value bool) *bool { return &value }
