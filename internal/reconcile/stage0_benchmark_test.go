package reconcile

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkReconcileCurrentBehavior(b *testing.B) {
	scenarios := []struct {
		name     string
		accounts int
		delay    time.Duration
	}{
		{name: "accounts_10", accounts: 10},
		{name: "accounts_100", accounts: 100},
		{name: "accounts_500", accounts: 500},
		{name: "accounts_100_upstream_delay_1ms", accounts: 100, delay: time.Millisecond},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
			fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{
				Accounts: scenario.accounts, Monitors: scenario.accounts, PolicyEvery: 4,
			})
			for _, policy := range fixture.Policies {
				if err := database.Store.UpsertPolicy(context.Background(), policy); err != nil {
					b.Fatal(err)
				}
			}
			api := testsupport.NewFakeSub2API(fixture)
			api.SetDelay(scenario.delay)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			engine := NewEngine(api, database.Store, 50*time.Second, logger)
			database.SQLCounter.Reset()
			api.ResetStats()
			b.ReportAllocs()
			b.ReportMetric(float64(scenario.accounts), "accounts/op")
			b.ResetTimer()
			for range b.N {
				if err := engine.Reconcile(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			sqlCounts := database.SQLCounter.Snapshot()
			apiStats := api.Stats()
			b.ReportMetric(float64(sqlCounts.Queries)/float64(b.N), "sql_queries/op")
			b.ReportMetric(float64(sqlCounts.Execs)/float64(b.N), "sql_execs/op")
			b.ReportMetric(float64(apiStats.Total)/float64(b.N), "upstream_calls/op")
			if got := apiStats.ByName[testsupport.CallListAccounts]; got > 0 {
				b.ReportMetric(float64(got)/float64(b.N), "list_accounts/op")
			}
			if got := apiStats.ByName[testsupport.CallListMonitors]; got > 0 {
				b.ReportMetric(float64(got)/float64(b.N), "list_monitors/op")
			}
		})
	}
}

func BenchmarkResolveBindingsLocal(b *testing.B) {
	for _, count := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("accounts_%d", count), func(b *testing.B) {
			fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: count, Monitors: count, PolicyEvery: 4})
			policies := make(map[int64]model.Policy, len(fixture.Policies))
			for _, policy := range fixture.Policies {
				policies[policy.AccountID] = policy
			}
			b.ReportAllocs()
			b.ReportMetric(float64(count), "accounts/op")
			b.ResetTimer()
			for range b.N {
				ResolveBindings(fixture.Monitors, fixture.Accounts, policies)
			}
		})
	}
}
