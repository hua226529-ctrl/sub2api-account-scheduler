package agent

import (
	"strings"
	"testing"
)

func TestCapabilityRegistryIsTypedAndContainsNoGenericEscapeTool(t *testing.T) {
	items := CapabilitySpecs()
	if len(items) < 20 {
		t.Fatalf("expected complete domain capability catalog, got %d", len(items))
	}
	seen := map[string]bool{}
	for _, item := range items {
		if item.Name == "" || item.Description == "" || item.Version < 1 || item.Parameters["type"] != "object" {
			t.Fatalf("invalid capability: %#v", item)
		}
		if seen[item.Name] {
			t.Fatalf("duplicate capability %s", item.Name)
		}
		seen[item.Name] = true
		name := strings.ToLower(item.Name)
		for _, forbidden := range []string{"shell", "exec", "sql", "http", "secret", "password", "filesystem"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("capability %s exposes forbidden surface", item.Name)
			}
		}
	}
}

func TestCapabilityCatalogContainsRequiredV2Operations(t *testing.T) {
	for _, name := range []string{"pin_load_until", "schedule_command", "update_dispatch_policy", "transition_token_group_tier", "search_memory"} {
		if _, ok := capabilitySpec(name); !ok {
			t.Fatalf("missing %s", name)
		}
	}
}
