package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestRuntimeDecisionJSONSchemaClosesAndRequiresEveryObjectProperty(t *testing.T) {
	payload, err := json.Marshal(runtimeDecisionJSONSchema())
	if err != nil {
		t.Fatal(err)
	}
	var schema any
	if err := json.Unmarshal(payload, &schema); err != nil {
		t.Fatal(err)
	}
	assertStrictSchemaNode(t, "$", schema)
}

func TestDecodeRuntimeDecisionNormalizesEncodedActionArguments(t *testing.T) {
	content, err := json.Marshal(map[string]any{
		"summary": "需要暂停账号", "conclusion": "证据满足暂停条件", "confidence": .95, "no_change": false,
		"actions": []any{map[string]any{
			"type": "pause_account", "arguments": `{"account_id":225,"reason":"连续失败超过阈值"}`,
			"reason": "连续失败超过阈值", "prediction": map[string]any{
				"success_rate_delta": 10, "latency_delta_ms": int64(0), "cost_delta": 0,
			},
		}},
		"advice": []string{}, "data_limitations": []string{}, "evidence_requests": []string{},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := decodeRuntimeDecision(string(content))
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Actions) != 1 || decision.Actions[0].AccountID != 225 {
		t.Fatalf("encoded arguments were not hydrated: %+v", decision.Actions)
	}
	call, err := decisionActionToolCall(decision.Actions[0], decision.Confidence)
	if err != nil {
		t.Fatal(err)
	}
	if call.Function.Arguments != `{"account_id":225,"reason":"连续失败超过阈值"}` {
		t.Fatalf("normalized arguments = %s", call.Function.Arguments)
	}
}

func TestDecodeRuntimeDecisionHydratesConfigFromEncodedArguments(t *testing.T) {
	content, err := json.Marshal(map[string]any{
		"summary": "创建策略提案", "conclusion": "生成待审核版本", "confidence": .9, "no_change": false,
		"actions": []any{map[string]any{
			"type":      "propose_dispatch_policy",
			"arguments": `{"scope_type":"global","config":{"minimum_samples":10},"reason":"使用当前证据创建低风险提案"}`,
			"reason":    "使用当前证据创建低风险提案",
			"prediction": map[string]any{
				"success_rate_delta": 0, "latency_delta_ms": int64(0), "cost_delta": 0,
			},
		}},
		"advice": []string{}, "data_limitations": []string{}, "evidence_requests": []string{},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := decodeRuntimeDecision(string(content))
	if err != nil {
		t.Fatal(err)
	}
	action := decision.Actions[0]
	if action.ScopeType != "global" || string(action.Config) != `{"minimum_samples":10}` {
		t.Fatalf("encoded policy arguments were not hydrated: %+v", action)
	}
	if err := ValidateRuntimeDecision(model.AnalysisPacket{}, runtimeGoalContext{}, decision); err != nil {
		t.Fatalf("hydrated policy action rejected: %v", err)
	}
}

func TestDecodeRuntimeDecisionRejectsConflictingEncodedActionTarget(t *testing.T) {
	_, err := decodeRuntimeDecision(`{"summary":"暂停账号","conclusion":"执行暂停","confidence":0.95,"no_change":false,` +
		`"actions":[{"type":"pause_account","arguments":"{\"account_id\":226}","account_id":225,` +
		`"reason":"连续失败超过阈值","prediction":{"success_rate_delta":10,"latency_delta_ms":0,"cost_delta":0}}],` +
		`"advice":[],"data_limitations":[],"evidence_requests":[]}`)
	if runtimeErrorClassOf(err) != runtimeErrorModelContractInvalid {
		t.Fatalf("conflicting target class=%s err=%v", runtimeErrorClassOf(err), err)
	}
}

func assertStrictSchemaNode(t *testing.T, path string, node any) {
	t.Helper()
	switch value := node.(type) {
	case map[string]any:
		if schemaContainsType(value["type"], "object") {
			closed, ok := value["additionalProperties"].(bool)
			if !ok || closed {
				t.Fatalf("%s additionalProperties = %#v, want false", path, value["additionalProperties"])
			}
			properties, ok := value["properties"].(map[string]any)
			if !ok {
				t.Fatalf("%s object has no properties map", path)
			}
			requiredValues, ok := value["required"].([]any)
			if !ok {
				t.Fatalf("%s object has no required array", path)
			}
			required := make(map[string]bool, len(requiredValues))
			for _, item := range requiredValues {
				name, ok := item.(string)
				if !ok {
					t.Fatalf("%s required contains non-string %#v", path, item)
				}
				required[name] = true
			}
			for name := range properties {
				if !required[name] {
					t.Fatalf("%s.properties.%s is not required", path, name)
				}
			}
			if len(required) != len(properties) {
				t.Fatalf("%s required/property count mismatch: %d/%d", path, len(required), len(properties))
			}
		}
		for key, child := range value {
			assertStrictSchemaNode(t, path+"."+key, child)
		}
	case []any:
		for index, child := range value {
			assertStrictSchemaNode(t, fmt.Sprintf("%s[%d]", path, index), child)
		}
	}
}

func schemaContainsType(value any, expected string) bool {
	if value == expected {
		return true
	}
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if item == expected {
			return true
		}
	}
	return false
}

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
