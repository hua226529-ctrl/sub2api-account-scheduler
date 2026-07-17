package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestCurrentBehaviorSingleMonitorFailureIsolatedFromTelemetryRound(t *testing.T) {
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	api := testsupport.NewFakeSub2API(testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 3, Monitors: 3}))
	api.SetFailure(testsupport.CallListMonitorHistory, testsupport.Failure{AtCall: 2})
	manager := NewManager(api, database.Store, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := manager.RunOnce(context.Background())
	if err == nil {
		t.Fatal("telemetry round unexpectedly reported full success after a monitor history failure")
	}
	var partial *PartialError
	if !errors.As(err, &partial) || len(partial.Issues) != 1 || partial.Issues[0].Code != "monitor_fetch_failed" {
		t.Fatalf("unexpected partial telemetry error: %#v", err)
	}
	stats := api.Stats()
	if stats.ByName[testsupport.CallListMonitorHistory] != 3 {
		t.Fatalf("monitor history calls = %d, want all three monitors attempted", stats.ByName[testsupport.CallListMonitorHistory])
	}
}
