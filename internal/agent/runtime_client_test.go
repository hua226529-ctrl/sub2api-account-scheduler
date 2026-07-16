package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestCompleteRuntimeNativeParsesToolCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if len(request["tools"].([]any)) == 0 {
			t.Fatal("missing native tools")
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"get_system_snapshot","arguments":"{}"}}]}}]}`))
	}))
	defer server.Close()
	turn, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10, MaxOutputTokens: 1024,
	}, "secret", []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, CapabilitySpecs())
	if err != nil {
		t.Fatal(err)
	}
	if len(turn.ToolCalls) != 1 || turn.ToolCalls[0].Function.Name != "get_system_snapshot" {
		t.Fatalf("unexpected turn: %#v", turn)
	}
}

func TestCompleteRuntimeNativeFallsBackWhenToolsUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	_, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "secret", []RuntimeMessage{{Role: "user", Content: "test"}}, CapabilitySpecs())
	if !errors.Is(err, errNativeToolsUnsupported) {
		t.Fatalf("expected fallback error, got %v", err)
	}
}
