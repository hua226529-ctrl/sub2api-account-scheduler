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
		format, ok := request["response_format"].(map[string]any)
		if !ok || format["type"] != "json_schema" {
			t.Fatalf("missing strict response schema: %#v", request["response_format"])
		}
		schema, ok := format["json_schema"].(map[string]any)
		if !ok || schema["strict"] != true {
			t.Fatalf("response schema is not strict: %#v", format)
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

func TestCompleteRuntimeNativeRejectsStringEvidenceRequest(t *testing.T) {
	server := runtimeDecisionServer(t, http.StatusOK,
		`{"summary":"需要证据","conclusion":"等待","confidence":0.4,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":["需要更多审计事件"]}`)
	defer server.Close()

	_, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "secret", []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, CapabilitySpecs())
	if runtimeErrorClassOf(err) != runtimeErrorModelContractInvalid {
		t.Fatalf("expected model_contract_invalid, got %v", err)
	}
}

func TestDecodeRuntimeDecisionRejectsMarkdownWrappedJSON(t *testing.T) {
	_, err := decodeRuntimeDecision("```json\n" +
		`{"summary":"完成","conclusion":"无需变更","confidence":1,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":[]}` + "\n```")
	if runtimeErrorClassOf(err) != runtimeErrorModelContractInvalid {
		t.Fatalf("markdown-wrapped decision class=%s err=%v", runtimeErrorClassOf(err), err)
	}
}

func TestCompleteRuntimeNativeRejectsFinalEvidenceRequest(t *testing.T) {
	server := runtimeDecisionServer(t, http.StatusOK,
		`{"summary":"需要证据","conclusion":"等待","confidence":0.4,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":[{"tool":"get_audit_events","limit":10}]}`)
	defer server.Close()

	_, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "secret", []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, CapabilitySpecs())
	if runtimeErrorClassOf(err) != runtimeErrorModelContractInvalid {
		t.Fatalf("expected model_contract_invalid, got %v", err)
	}
}

func TestCompleteRuntimeNativeDowngradesSchemaOnce(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		format := request["response_format"].(map[string]any)
		if requests == 1 {
			if format["type"] != "json_schema" {
				t.Fatalf("first request did not use json_schema: %#v", format)
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"response_format json_schema unsupported"}}`))
			return
		}
		if format["type"] != "json_object" {
			t.Fatalf("fallback did not use json_object: %#v", format)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{\"summary\":\"完成\",\"conclusion\":\"无需变更\",\"confidence\":1,\"no_change\":true,\"actions\":[],\"advice\":[],\"data_limitations\":[],\"evidence_requests\":[]}"}}]}`))
	}))
	defer server.Close()

	turn, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "secret", []RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, CapabilitySpecs())
	if err != nil || turn.Decision == nil {
		t.Fatalf("schema fallback failed: turn=%+v err=%v", turn, err)
	}
	if requests != 2 {
		t.Fatalf("expected exactly one downgrade, got %d requests", requests)
	}
}

func runtimeDecisionServer(t *testing.T, status int, content string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		payload, err := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": map[string]any{
			"role": "assistant", "content": content,
		}}}})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(payload)
	}))
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
