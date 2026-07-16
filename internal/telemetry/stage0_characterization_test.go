package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestCurrentBehaviorSingleMonitorFailureStopsTelemetryRound(t *testing.T) {
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	api := testsupport.NewFakeSub2API(testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 3, Monitors: 3}))
	api.SetFailure(testsupport.CallListMonitorHistory, testsupport.Failure{AtCall: 2})
	manager := NewManager(api, database.Store, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := manager.RunOnce(context.Background()); err == nil {
		t.Fatal("telemetry round unexpectedly continued after a monitor history failure")
	}
	stats := api.Stats()
	if stats.ByName[testsupport.CallListMonitorHistory] != 2 {
		t.Fatalf("monitor history calls = %d, want current stop-on-first-failure behavior 2", stats.ByName[testsupport.CallListMonitorHistory])
	}
}
