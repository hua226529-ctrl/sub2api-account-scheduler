package reconcile

import (
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestNormalizeEndpoint(t *testing.T) {
	tests := map[string]string{
		"https://Example.com/v1/":     "https://example.com",
		"https://example.com:443/api": "https://example.com",
		"http://example.com:8080/v1":  "http://example.com:8080",
	}
	for input, expected := range tests {
		actual, err := NormalizeEndpoint(input)
		if err != nil {
			t.Fatalf("NormalizeEndpoint(%q): %v", input, err)
		}
		if actual != expected {
			t.Fatalf("NormalizeEndpoint(%q)=%q, want %q", input, actual, expected)
		}
	}
}

func TestResolveBindingsAutoManualAndConflict(t *testing.T) {
	monitors := []model.Monitor{
		{ID: 1, Provider: "openai", Endpoint: "https://one.example", Enabled: true},
		{ID: 2, Provider: "openai", Endpoint: "https://two.example", Enabled: true},
		{ID: 3, Provider: "openai", Endpoint: "https://two.example", Enabled: true},
	}
	accounts := []model.Account{
		{ID: 10, Platform: "openai", Credentials: map[string]any{"base_url": "https://one.example/v1"}},
		{ID: 11, Platform: "openai", Credentials: map[string]any{"base_url": "https://two.example/v1"}},
		{ID: 12, Platform: "openai", Credentials: map[string]any{"base_url": "https://missing.example/v1"}},
	}
	manualID := int64(1)
	policies := map[int64]model.Policy{12: {AccountID: 12, MonitorID: &manualID, Enabled: true}}
	bindings, _, conflicts := ResolveBindings(monitors, accounts, policies)
	if bindings[0].Monitor == nil || bindings[0].Monitor.ID != 1 || bindings[0].Source != "auto" {
		t.Fatalf("account 10 did not auto bind: %+v", bindings[0])
	}
	if bindings[1].State != "conflict" || len(conflicts) != 1 {
		t.Fatalf("account 11 should conflict: %+v conflicts=%v", bindings[1], conflicts)
	}
	if bindings[2].Monitor == nil || bindings[2].Monitor.ID != 1 || bindings[2].Source != "manual" {
		t.Fatalf("account 12 did not use manual binding: %+v", bindings[2])
	}
}
