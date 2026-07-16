package health

import (
	"fmt"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkPolicyEvaluationLocal(b *testing.B) {
	for _, count := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("monitors_%d", count), func(b *testing.B) {
			fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: count, Monitors: count})
			settings := testsupport.DefaultSettings()
			now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
			states := make([]model.MonitorHealthState, len(fixture.Monitors))
			b.ReportAllocs()
			b.ReportMetric(float64(count), "monitors/op")
			b.ResetTimer()
			for range b.N {
				for index, monitor := range fixture.Monitors {
					states[index], _ = Evaluate(monitor, nil, states[index], settings, now)
				}
			}
		})
	}
}
