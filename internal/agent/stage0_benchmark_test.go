package agent

import (
	"context"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkChatAsyncEnqueueCurrentBehavior(b *testing.B) {
	database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
	manager := &Manager{store: database.Store, interactiveWake: make(chan struct{}, 1), backgroundWake: make(chan struct{}, 1)}
	database.SQLCounter.Reset()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, _, _, err := manager.ChatAsync(context.Background(), 0, "分析当前账号状态"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	counts := database.SQLCounter.Snapshot()
	b.ReportMetric(float64(counts.Queries)/float64(b.N), "sql_queries/op")
	b.ReportMetric(float64(counts.Execs)/float64(b.N), "sql_execs/op")
}

func BenchmarkAgentInteractiveWakeNonBlocking(b *testing.B) {
	manager := &Manager{interactiveWake: make(chan struct{}, 1)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		manager.wakeLane(model.AgentLaneInteractive)
	}
	b.StopTimer()
}
