package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestRedactAgentTextRemovesCommonCredentials(t *testing.T) {
	t.Parallel()
	input := "立即恢复账号225\n密码：demo-labeled-pass\napi_key=sk-example1234567890abcdef\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz.123\nadmin-" + strings.Repeat("c", 64)
	redacted := redactAgentText(input)
	for _, forbidden := range []string{"demo-labeled-pass", "sk-example", "abcdefghijklmnopqrstuvwxyz", strings.Repeat("c", 16)} {
		if strings.Contains(redacted, forbidden) {
			t.Fatalf("credential fragment %q survived redaction: %s", forbidden, redacted)
		}
	}
	if !strings.Contains(redacted, "立即恢复账号225") || !strings.Contains(redacted, "[已脱敏]") {
		t.Fatalf("redaction removed the command or omitted its marker: %s", redacted)
	}
}

func TestRedactAgentTextKeepsOperationalTokenLanguage(t *testing.T) {
	t.Parallel()
	input := "把主上游的令牌切换到备用分组，负载保持25到早上6点"
	if got := redactAgentText(input); got != input {
		t.Fatalf("ordinary scheduling language was changed: %q", got)
	}
}

func TestRedactAgentTextRemovesURLAndUnlabeledSecrets(t *testing.T) {
	t.Parallel()
	rawHex := strings.Repeat("a1", 32)
	bareToken := "AbCDef0123456789_AbCDef0123456789_AbCd"
	base64Token := "AbCDef0123456789/AbCDef0123456789+AbCd=="
	jwtToken := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbmlzdHJhdG9yIn0.signature0123456789"
	input := strings.Join([]string{
		"立即恢复账号225",
		"https://alice:s3cr3t@api.example.com/v1",
		rawHex,
		bareToken,
		base64Token,
		jwtToken,
		"admin123",
		"Demo#Server9$Pass",
	}, "\n")
	redacted := redactAgentText(input)
	for _, forbidden := range []string{"alice", "s3cr3t", rawHex, bareToken, base64Token, jwtToken, "admin123", "Demo#Server9$Pass"} {
		if strings.Contains(redacted, forbidden) {
			t.Fatalf("unlabeled secret %q survived redaction: %s", forbidden, redacted)
		}
	}
	for _, preserved := range []string{"立即恢复账号225", "https://[已脱敏]@api.example.com/v1"} {
		if !strings.Contains(redacted, preserved) {
			t.Fatalf("non-secret context %q was removed: %s", preserved, redacted)
		}
	}
}

func TestRedactAgentTextDoesNotMistakeOperationalLinesForPasswords(t *testing.T) {
	t.Parallel()
	input := "把 account-example 负载设为25并保持到早上6点\naccount-example\ngpt-5.5\n25\n立即恢复账号225\nhttps://api.example.com/a-very-long-nonsensitive-route-name-for-diagnostics"
	if got := redactAgentText(input); got != input {
		t.Fatalf("ordinary scheduling text was changed: %q", got)
	}
}

func TestRedactAgentTextRemovesStandalonePasswordFromCRLFInput(t *testing.T) {
	t.Parallel()
	redacted := redactAgentText("立即暂停账号225\r\nadmin123\r\n随后检查")
	if strings.Contains(redacted, "admin123") || !strings.Contains(redacted, "立即暂停账号225") {
		t.Fatalf("CRLF password line was not safely redacted: %q", redacted)
	}
}

func TestRedactRuntimeMessagesSanitizesHistoricalAndToolContent(t *testing.T) {
	t.Parallel()
	rawHex := strings.Repeat("ab", 32)
	messages := []RuntimeMessage{{
		Role: "tool",
		Content: map[string]any{
			"freeze_reason": "password: old-database-secret",
			"nested":        []any{"https://alice:s3cr3t@example.com/v1", rawHex},
			"password":      "demo-tool-password",
		},
		ToolCalls: []RuntimeToolCall{{ID: "call-1", Type: "function", Function: RuntimeFunctionCall{
			Name: "trigger_reconcile", Arguments: `{"reason":"api_key=sk-abcdefghijklmnopqrstuvwxyz123456","password":"demo-argument-password"}`,
		}}},
	}}
	redacted := redactRuntimeMessages(messages)
	payload, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"old-database-secret", "alice", "s3cr3t", rawHex, "sk-abcdefghijklmnopqrstuvwxyz", "demo-tool-password", "demo-argument-password"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("model-bound payload retained %q: %s", forbidden, payload)
		}
	}
	if !json.Valid([]byte(redacted[0].ToolCalls[0].Function.Arguments)) {
		t.Fatalf("tool arguments became invalid JSON: %s", redacted[0].ToolCalls[0].Function.Arguments)
	}
}

func TestRedactAgentTextKeepsJSONWellFormed(t *testing.T) {
	t.Parallel()
	redacted := redactAgentText(`{"password":"demo-json-password","note":"keep"}`)
	if !json.Valid([]byte(redacted)) || strings.Contains(redacted, "demo-json-password") || !strings.Contains(redacted, "keep") {
		t.Fatalf("JSON redaction was unsafe: %s", redacted)
	}
}

func TestRedactAgentPromptRecursivelySanitizesNestedJSON(t *testing.T) {
	t.Parallel()
	prompt := `{"password":"pw-short","nested":{"access_token":"tk","payload":"{\"secret\":\"inner-short\",\"keep\":\"yes\"}"},"keep":"visible"}`
	redacted := redactAgentPrompt(prompt)
	if !json.Valid([]byte(redacted)) {
		t.Fatalf("redacted prompt is not valid JSON: %s", redacted)
	}
	for _, forbidden := range []string{"pw-short", `\"tk\"`, "inner-short"} {
		if strings.Contains(redacted, forbidden) {
			t.Fatalf("nested secret %q survived prompt redaction: %s", forbidden, redacted)
		}
	}
	if !strings.Contains(redacted, "visible") || !strings.Contains(redacted, "yes") {
		t.Fatalf("non-secret JSON data was removed: %s", redacted)
	}
}

func TestRedactAgentPromptSanitizesPrimitiveAndEmbeddedEscapedSecrets(t *testing.T) {
	t.Parallel()
	if redacted := redactAgentPrompt("123456"); strings.Contains(redacted, "123456") {
		t.Fatalf("numeric standalone password survived prompt redaction: %s", redacted)
	}
	input := `上游错误：{\"password\":\"tiny-pw\",\"keep\":\"diagnostic\"}`
	redacted := redactAgentPrompt(input)
	if strings.Contains(redacted, "tiny-pw") || !strings.Contains(redacted, "diagnostic") {
		t.Fatalf("embedded escaped JSON was not safely redacted: %s", redacted)
	}
}

func TestCompletionClientRedactsStructuredPromptsAtHTTPBoundary(t *testing.T) {
	t.Parallel()
	var captured chatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"summary\":\"完成\",\"conclusion\":\"完成\",\"confidence\":1,\"no_change\":true,\"actions\":[],\"advice\":[],\"data_limitations\":[]}"}}]}`))
	}))
	defer server.Close()

	_, err := (completionClient{}).Complete(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "provider-secret", `{"password":"system-pw","keep":"system"}`,
		`{"nested":{"secret":"user-pw"},"keep":"user"}`)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"system-pw", "user-pw"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("secret %q reached model request: %s", forbidden, payload)
		}
	}
	if !strings.Contains(string(payload), "system") || !strings.Contains(string(payload), "user") {
		t.Fatalf("non-secret prompt context was lost: %s", payload)
	}
}

func TestCompleteRuntimeFallbackRedactsMessagesAndToolDataAtHTTPBoundary(t *testing.T) {
	t.Parallel()
	var captured chatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"summary\":\"完成\",\"conclusion\":\"完成\",\"confidence\":1,\"no_change\":true,\"actions\":[],\"advice\":[],\"data_limitations\":[]}"}}]}`))
	}))
	defer server.Close()

	messages := []RuntimeMessage{
		{Role: "system", Content: map[string]any{"password": "system-short", "keep": "system-context"}},
		{Role: "user", Content: `{"nested":{"password":"user-short"},"keep":"user-context"}`},
		{Role: "tool", ToolCallID: "call-1", Content: map[string]any{
			"result":  map[string]any{"access_token": "output-short", "keep": "tool-context"},
			"escaped": `错误详情：{\"password\":\"escaped-short\",\"keep\":\"escaped-context\"}`,
		}, ToolCalls: []RuntimeToolCall{{ID: "call-2", Type: "function", Function: RuntimeFunctionCall{
			Name: "get_system_snapshot", Arguments: `{"password":"argument-short","nested":{"secret":"argument-inner"}}`,
		}}}},
	}
	turn, err := (&Manager{}).completeRuntimeFallback(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test", TimeoutSeconds: 10,
	}, "provider-secret", messages)
	if err != nil {
		t.Fatal(err)
	}
	if turn.Decision == nil || turn.Decision.Summary != "完成" {
		t.Fatalf("unexpected fallback result: %#v", turn)
	}
	payload, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"system-short", "user-short", "output-short", "argument-short", "argument-inner", "escaped-short"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("fallback secret %q reached model request: %s", forbidden, payload)
		}
	}
	for _, preserved := range []string{"system-context", "user-context", "tool-context", "escaped-context"} {
		if !strings.Contains(string(payload), preserved) {
			t.Fatalf("fallback context %q was lost: %s", preserved, payload)
		}
	}
}
