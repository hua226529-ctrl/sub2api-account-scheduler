package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func BenchmarkStoreReadCurrentControls(b *testing.B) {
	for _, count := range []int{100, 500} {
		b.Run(fmt.Sprintf("accounts_%d", count), func(b *testing.B) {
			database := testsupport.OpenTempDatabase(b, testsupport.DefaultSettings())
			ctx := context.Background()
			now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
			for accountID := 1; accountID <= count; accountID++ {
				if err := database.Store.UpsertControl(ctx, model.AccountControl{AccountID: int64(accountID), UpdatedAt: now}); err != nil {
					b.Fatal(err)
				}
			}
			database.SQLCounter.Reset()
			b.ReportAllocs()
			b.ReportMetric(float64(count), "accounts/op")
			b.ResetTimer()
			for range b.N {
				for accountID := 1; accountID <= count; accountID++ {
					if _, err := database.Store.GetControl(ctx, int64(accountID)); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			counts := database.SQLCounter.Snapshot()
			b.ReportMetric(float64(counts.Queries)/float64(b.N), "sql_queries/op")
			b.ReportMetric(float64(counts.Execs)/float64(b.N), "sql_execs/op")
		})
	}
}
