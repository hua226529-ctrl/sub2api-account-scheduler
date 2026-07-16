package agent

import (
	"encoding/json"
	"testing"
)

func TestValidateDispatchPolicyPatchRejectsControlFields(t *testing.T) {
	for _, test := range []struct {
		scope string
		patch string
	}{
		{"global", `{"dry_run":true}`},
		{"account", `{"enabled":false}`},
		{"account", `{"monitor_id":8}`},
		{"pool", `{"excluded":true}`},
	} {
		if err := validateDispatchPolicyPatch(test.scope, json.RawMessage(test.patch)); err == nil {
			t.Fatalf("expected %s patch %s to be rejected", test.scope, test.patch)
		}
	}
}

func TestValidateDispatchPolicyPatchAllowsTypedDispatchFields(t *testing.T) {
	cases := []struct {
		scope string
		patch string
	}{
		{"global", `{"failure_threshold":4,"recovery_initial_percent":25,"health_engine_mode":"adaptive_v3"}`},
		{"global", `{"group_failover_traffic_min_samples":12,"group_failover_switch_cooldown_minutes":20,"group_failover_recovery_success_at":99}`},
		{"pool", `{"healthy_score_threshold":82,"persistent_slow_rate":35}`},
		{"account", `{"flap_enabled":true,"flap_recovery_threshold":12}`},
	}
	for _, test := range cases {
		if err := validateDispatchPolicyPatch(test.scope, json.RawMessage(test.patch)); err != nil {
			t.Fatalf("expected %s patch to pass: %v", test.scope, err)
		}
	}
}

func TestValidateDispatchPolicyPatchKeepsFailoverSettingsGlobal(t *testing.T) {
	for _, scope := range []string{"pool", "account"} {
		if err := validateDispatchPolicyPatch(scope, json.RawMessage(`{"group_failover_traffic_min_samples":12}`)); err == nil {
			t.Fatalf("expected %s failover patch to be rejected", scope)
		}
	}
}
