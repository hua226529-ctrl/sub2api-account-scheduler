package agent

import (
	"context"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkChatAsyncEnqueueCurrentBehavior(b *testing.B) {
	database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
	manager := &Manager{store: database.Store, runtimeWake: make(chan struct{}, 1)}
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

func BenchmarkAgentInteractiveQueueWaitCurrentBehavior(b *testing.B) {
	manager := &Manager{}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		manager.runtimeMu.Lock()
		ready := make(chan struct{})
		acquired := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			close(ready)
			manager.runtimeMu.Lock()
			acquired <- time.Since(start)
			manager.runtimeMu.Unlock()
		}()
		<-ready
		time.Sleep(time.Millisecond)
		manager.runtimeMu.Unlock()
		total += <-acquired
	}
	b.StopTimer()
	b.ReportMetric(float64(time.Millisecond.Nanoseconds()), "configured_background_hold_ns/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "queue_wait_ns/op")
}
