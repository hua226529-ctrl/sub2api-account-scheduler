package reconcile

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkCoreA10AccountPaths(b *testing.B) {
	b.Run("full_reconcile_10", func(b *testing.B) {
		engine, database, api := coreABenchmarkEngine(b, 10)
		database.SQLCounter.Reset()
		api.ResetStats()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := engine.Reconcile(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportCoreABenchmarkMetrics(b, database, api)
	})

	b.Run("manual_action", func(b *testing.B) {
		engine, database, api := coreABenchmarkEngine(b, 1)
		database.SQLCounter.Reset()
		api.ResetStats()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			commandID := fmt.Sprintf("benchmark-manual-%d", i)
			var err error
			if i%2 == 0 {
				_, err = engine.ManualPauseCommand(context.Background(), 1, "web", commandID)
			} else {
				_, err = engine.ManualResumeCommand(context.Background(), 1, "web", commandID, 0)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportCoreABenchmarkMetrics(b, database, api)
	})

	b.Run("agent_action", func(b *testing.B) {
		engine, database, api := coreABenchmarkEngine(b, 1)
		base := time.Now().UTC()
		database.SQLCounter.Reset()
		api.ResetStats()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			created := base.Add(time.Duration(i) * time.Second)
			expires := created.Add(accountcontrol.DefaultAutonomousTTL)
			commandID := fmt.Sprintf("benchmark-agent-%d", i)
			ctx := accountcontrol.WithCommandContext(context.Background(), accountcontrol.CommandContext{
				CommandID: commandID, CreatedAt: created, ExpiresAt: &expires,
				SnapshotVersion: "benchmark-snapshot-" + commandID, EvidenceRefs: []string{"benchmark-evidence-" + commandID},
			})
			var err error
			if i%2 == 0 {
				err = engine.AgentPause(ctx, 1, "agent:benchmark", "benchmark")
			} else {
				err = engine.AgentResume(ctx, 1, "agent:benchmark", "benchmark")
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportCoreABenchmarkMetrics(b, database, api)
	})

	b.Run("startup_recovery_empty", func(b *testing.B) {
		engine, database, api := coreABenchmarkEngine(b, 10)
		database.SQLCounter.Reset()
		api.ResetStats()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := engine.accountControl.ReconcilePendingAccountMutations(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportCoreABenchmarkMetrics(b, database, api)
	})
}

func coreABenchmarkEngine(b *testing.B, accounts int) (*Engine, *testsupport.TempDatabase, *testsupport.FakeSub2API) {
	b.Helper()
	database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: accounts, Monitors: accounts, PolicyEvery: 4, Now: time.Now().UTC()})
	for _, policy := range fixture.Policies {
		if err := database.Store.UpsertPolicy(context.Background(), policy); err != nil {
			b.Fatal(err)
		}
	}
	api := testsupport.NewFakeSub2API(fixture)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(api, database.Store, time.Minute, logger), database, api
}

func reportCoreABenchmarkMetrics(b *testing.B, database *testsupport.TempDatabase, api *testsupport.FakeSub2API) {
	b.Helper()
	queries := database.SQLCounter.Snapshot()
	calls := api.Stats()
	b.ReportMetric(float64(queries.Queries)/float64(b.N), "sql_queries/op")
	b.ReportMetric(float64(queries.Execs)/float64(b.N), "sql_execs/op")
	b.ReportMetric(float64(calls.Total)/float64(b.N), "upstream_calls/op")
}
