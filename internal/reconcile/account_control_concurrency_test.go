package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestPolicyAndInteractiveAccountWritesUseSameResourceLock(t *testing.T) {
	tests := []struct {
		name   string
		action func(*Engine) error
	}{
		{
			name: "reconcile and manual pause",
			action: func(engine *Engine) error {
				return engine.ManualPause(context.Background(), 1, "web")
			},
		},
		{
			name: "reconcile and agent pause",
			action: func(engine *Engine) error {
				created := time.Now().UTC()
				expires := created.Add(accountcontrol.DefaultAutonomousTTL)
				ctx := accountcontrol.WithCommandContext(context.Background(), accountcontrol.CommandContext{
					CommandID: "concurrent-agent-pause", CreatedAt: created, ExpiresAt: &expires,
					SnapshotVersion: "concurrent-snapshot", EvidenceRefs: []string{"concurrent-health-evidence"},
				})
				return engine.AgentPause(ctx, 1, "agent:test", "concurrent safety action")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, database, api := stage0Engine(t)
			ctx := context.Background()
			if err := engine.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			binding, ok := findBinding(engine.Snapshot().Bindings, 1)
			if !ok {
				t.Fatal("account is missing from reconciled snapshot")
			}
			now := time.Now().UTC()
			binding.Account.UpdatedAt = now
			binding.MonitorState.UpdatedAt = now
			binding.Monitor.LastCheckedAt = &now
			control := binding.Control
			control.LoadStage = model.HealthStageLimited25
			binding.Control = control
			settings, err := database.Store.GetSettings(ctx)
			if err != nil {
				t.Fatal(err)
			}
			settings.HealthMode = model.HealthModeAdaptive

			policyEntered := make(chan struct{})
			releasePolicy := make(chan struct{})
			var once sync.Once
			api.ResetStats()
			api.SetBeforeCall(func(call testsupport.Call) {
				if call.Name == testsupport.CallUpdateLoadFactor {
					once.Do(func() { close(policyEntered) })
					<-releasePolicy
				}
			})
			policyDone := make(chan error, 1)
			go func() {
				policyDone <- engine.reconcileAdaptiveLoad(ctx, &binding, &control, settings, now)
			}()
			select {
			case <-policyEntered:
			case <-time.After(time.Second):
				t.Fatal("policy mutation did not reach the blocked fake write")
			}

			interactiveStarted := make(chan struct{})
			interactiveDone := make(chan error, 1)
			go func() {
				close(interactiveStarted)
				interactiveDone <- test.action(engine)
			}()
			<-interactiveStarted
			close(releasePolicy)
			if err := <-policyDone; err != nil {
				t.Fatal(err)
			}
			if err := <-interactiveDone; err != nil {
				t.Fatal(err)
			}

			stats := api.Stats()
			if stats.MaxConcurrent != 1 {
				t.Fatalf("same-account policy and interactive calls overlapped: max concurrent=%d", stats.MaxConcurrent)
			}
			writes := make([]string, 0, 2)
			for _, call := range stats.Order {
				if call.Name == testsupport.CallUpdateLoadFactor || call.Name == testsupport.CallSetSchedulable {
					writes = append(writes, call.Name)
				}
			}
			if len(writes) != 2 || writes[0] != testsupport.CallUpdateLoadFactor || writes[1] != testsupport.CallSetSchedulable {
				t.Fatalf("unexpected write sequence: %v", writes)
			}
		})
	}
}
