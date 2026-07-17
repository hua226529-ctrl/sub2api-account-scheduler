package testsupport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestFakeSub2APIWriteSucceededButResponseFailed(t *testing.T) {
	fixture := GenerateFixture(FixtureConfig{Accounts: 1, Monitors: 1})
	api := NewFakeSub2API(fixture)
	timeout := errors.New("response timeout")
	api.SetFailure(CallSetSchedulable, Failure{AtCall: 1, Err: timeout, ApplyBeforeError: true})

	if _, err := api.SetSchedulable(context.Background(), 1, false); !errors.Is(err, timeout) {
		t.Fatalf("write error = %v, want response timeout", err)
	}
	accounts, err := api.ListAccounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].Schedulable {
		t.Fatal("write was not applied before the injected response failure")
	}
}

func TestFakeSub2APIStaleReadAndCallOrder(t *testing.T) {
	api := NewFakeSub2API(GenerateFixture(FixtureConfig{Accounts: 1, Monitors: 1}))
	api.SetStaleReads(true)
	if _, err := api.SetSchedulable(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	accounts, err := api.ListAccounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !accounts[0].Schedulable {
		t.Fatal("stale read unexpectedly exposed the applied write")
	}
	stats := api.Stats()
	if stats.Total != 2 || len(stats.Order) != 2 || stats.Order[0].Name != CallSetSchedulable || stats.Order[1].Name != CallListAccounts {
		t.Fatalf("call order = %+v", stats.Order)
	}
}

func TestFakeSub2APIDelayHonorsContext(t *testing.T) {
	api := NewFakeSub2API(GenerateFixture(FixtureConfig{Accounts: 1, Monitors: 1}))
	api.SetCallDelay(CallListAccounts, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := api.ListAccounts(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListAccounts error = %v, want context cancellation", err)
	}
}

func TestFixedClockCanAdvanceAndReset(t *testing.T) {
	initial := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	clock := NewFixedClock(initial)
	if got := clock.Advance(5 * time.Minute); !got.Equal(initial.Add(5 * time.Minute)) {
		t.Fatalf("advanced time = %s", got)
	}
	clock.Set(initial)
	if got := clock.Now(); !got.Equal(initial) {
		t.Fatalf("reset time = %s", got)
	}
}

func TestFakeSub2APIGroupTransitionSupportsIdempotencyAndReadback(t *testing.T) {
	api := NewFakeSub2API(GenerateFixture(FixtureConfig{Accounts: 1, Monitors: 1}))
	request := model.GroupTierTransitionRequest{SourceID: 9, KeyID: "token-1", TargetTier: model.GroupTierBackup, IdempotencyKey: "transition-1"}
	first, err := api.TransitionGroupTier(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := api.TransitionGroupTier(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || second.Status != model.GroupTransitionApplied {
		t.Fatalf("idempotent transitions differ: first=%+v second=%+v", first, second)
	}
	tier, err := api.ReadGroupTier(context.Background(), 9, "token-1")
	if err != nil {
		t.Fatal(err)
	}
	if tier != model.GroupTierBackup {
		t.Fatalf("readback tier = %q, want backup", tier)
	}
}

func TestTempDatabaseCountsRealSQLOperationsAndReopens(t *testing.T) {
	database := OpenTempDatabase(t, DefaultSettings())
	if counts := database.SQLCounter.Snapshot(); counts.Total() != 0 {
		t.Fatalf("schema initialization leaked into SQL counts: %+v", counts)
	}
	ctx := context.Background()
	settings, err := database.Store.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.DryRun = true
	settings.SchedulerMode = model.SchedulerModeObserve
	if err := database.Store.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	counts := database.SQLCounter.Snapshot()
	if counts.Queries == 0 || counts.Execs == 0 {
		t.Fatalf("SQL counts = %+v, want at least one query and exec", counts)
	}
	if err := database.Reopen(); err != nil {
		t.Fatal(err)
	}
	if counts := database.SQLCounter.Snapshot(); counts.Total() != 0 {
		t.Fatalf("reopen initialization leaked into SQL counts: %+v", counts)
	}
	settings, err = database.Store.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !settings.DryRun {
		t.Fatal("temporary database did not preserve state across reopen")
	}
}

func TestGenerateFixtureIsDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	fixture := GenerateFixture(FixtureConfig{
		Accounts: 500, Monitors: 100, UnhealthyEvery: 10, CredentialFailureEvery: 25,
		RateLimitedEvery: 40, PoolCount: 4, LockedEvery: 20, BalanceLockedEvery: 30,
		CostLockedEvery: 50, PolicyEvery: 5, Now: now,
	})
	if len(fixture.Accounts) != 500 || len(fixture.Monitors) != 100 || len(fixture.Policies) != 100 || len(fixture.Controls) != 36 {
		t.Fatalf("unexpected fixture dimensions: accounts=%d monitors=%d policies=%d controls=%d",
			len(fixture.Accounts), len(fixture.Monitors), len(fixture.Policies), len(fixture.Controls))
	}
	if fixture.Monitors[9].PrimaryStatus != model.StatusFailed || fixture.Accounts[499].ID != 500 {
		t.Fatal("deterministic signal or identity assignment changed")
	}
	if fixture.Accounts[24].CredentialStatus != "invalid" || fixture.Accounts[39].RateLimitResetAt == nil || fixture.Pools[500] != "pool-03" {
		t.Fatal("credential, rate-limit or pool assignment changed")
	}
}
