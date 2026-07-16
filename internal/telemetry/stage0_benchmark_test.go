package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkTelemetryRunOnceCurrentBehavior(b *testing.B) {
	for _, monitorCount := range []int{10, 100} {
		b.Run(fmt.Sprintf("monitors_%d", monitorCount), func(b *testing.B) {
			database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
			fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: monitorCount, Monitors: monitorCount, UnhealthyEvery: 10})
			api := testsupport.NewFakeSub2API(fixture)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			manager := NewManager(api, database.Store, 2*time.Minute, logger)
			database.SQLCounter.Reset()
			api.ResetStats()
			b.ReportAllocs()
			b.ReportMetric(float64(monitorCount), "monitors/op")
			b.ResetTimer()
			for range b.N {
				if err := manager.RunOnce(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			sqlCounts := database.SQLCounter.Snapshot()
			apiStats := api.Stats()
			b.ReportMetric(float64(sqlCounts.Queries)/float64(b.N), "sql_queries/op")
			b.ReportMetric(float64(sqlCounts.Execs)/float64(b.N), "sql_execs/op")
			b.ReportMetric(float64(apiStats.Total)/float64(b.N), "upstream_calls/op")
			b.ReportMetric(float64(apiStats.ByName[testsupport.CallListSuccessful])/float64(b.N), "list_success/op")
			b.ReportMetric(float64(apiStats.ByName[testsupport.CallListErrors])/float64(b.N), "list_errors/op")
			b.ReportMetric(float64(apiStats.ByName[testsupport.CallListMonitors])/float64(b.N), "list_monitors/op")
			b.ReportMetric(float64(apiStats.ByName[testsupport.CallListMonitorHistory])/float64(b.N), "list_history/op")
		})
	}
}
