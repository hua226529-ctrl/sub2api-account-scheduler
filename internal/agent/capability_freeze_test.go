package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestAdministratorCapabilityWaitsForFreezePublication(t *testing.T) {
	manager, database, api := newAdministratorTestManager(t)
	arguments := json.RawMessage(`{"account_id":298,"reason":"administrator request"}`)
	grant := mintAdministratorGrant(administratorCommandHash("freeze-test-scope"),
		administratorCommandHash("立即暂停account-example"), "immediate",
		"pause_account", arguments, []string{"account:298"}, "", nil, nil)

	releaseFreeze := manager.engine.AutomationBarrier().EnterFreeze()
	done := make(chan error, 1)
	go func() {
		_, err := manager.ExecuteCapability(context.Background(), CapabilityInvocation{
			Name: "pause_account", Arguments: arguments, GoalID: 101, StepID: 201,
			Actor: "administrator:agent", AdministratorGrant: grant,
		})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("administrator capability crossed freeze publication: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := database.SetAgentFreezeState(context.Background(), &model.AgentFreezeState{
		ScopeType: "global", Mode: model.AgentFreezeModeWritesFrozen, Actor: "web", Reason: "test",
	}); err != nil {
		releaseFreeze()
		t.Fatal(err)
	}
	releaseFreeze()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("administrator capability ignored the newly published freeze")
		}
	case <-time.After(time.Second):
		t.Fatal("administrator capability did not resume after freeze publication")
	}
	if len(api.actions) != 0 {
		t.Fatalf("frozen administrator capability reached Sub2API: %v", api.actions)
	}
}
