package agent

import (
	"encoding/json"
	"testing"
)

func TestUpstreamToggleGrantCannotModifyOtherControlFields(t *testing.T) {
	enabled := true
	intent := AdministratorIntent{
		Version: administratorGrantVersion, CommandHash: administratorCommandHash("开启上游9"),
		GrantScopeID: administratorCommandHash("upstream-control-test-scope"), Explicit: true,
		Grants: []AdministratorIntentGrant{{
			Capability: "update_upstream_control", Clause: "immediate", ResourceKeys: []string{"source:9"}, Enabled: &enabled,
		}},
	}
	manager := &Manager{}
	safe := json.RawMessage(`{"source_id":9,"enabled":true,"reason":"administrator request"}`)
	grant, err := manager.administratorGrantForInvocation(intent, "update_upstream_control", safe)
	if err != nil || grant == nil {
		t.Fatalf("plain upstream toggle did not receive its exact grant: grant=%+v err=%v", grant, err)
	}
	for _, unsafe := range []json.RawMessage{
		json.RawMessage(`{"source_id":9,"enabled":true,"pause_below":1,"reason":"smuggled threshold"}`),
		json.RawMessage(`{"source_id":9,"enabled":true,"routing_pool":"other","reason":"smuggled pool"}`),
		json.RawMessage(`{"source_id":9,"enabled":true,"selected_key_id":"secret-target","reason":"smuggled key"}`),
	} {
		grant, err := manager.administratorGrantForInvocation(intent, "update_upstream_control", unsafe)
		if err != nil || grant != nil {
			t.Fatalf("upstream toggle elevated unrelated control changes: grant=%+v err=%v args=%s", grant, err, unsafe)
		}
	}
}
