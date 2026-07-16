package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestAgentFreezeRequiresConfirmationToRelease(t *testing.T) {
	server, database := newAgentV2HTTPTestServer(t)
	defer database.Close()

	response := callJSONHandler(t, server.updateAgentFreezeState, http.MethodPut, "/api/agent/freeze", map[string]any{
		"scope_type": "global", "scope_id": "", "mode": model.AgentFreezeModeWritesFrozen, "reason": "incident",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("freeze status = %d body=%s", response.Code, response.Body.String())
	}

	response = callJSONHandler(t, server.updateAgentFreezeState, http.MethodPut, "/api/agent/freeze", map[string]any{
		"scope_type": "global", "scope_id": "", "mode": model.AgentFreezeModeActive,
	})
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "二次确认") {
		t.Fatalf("unconfirmed release status = %d body=%s", response.Code, response.Body.String())
	}

	response = callJSONHandler(t, server.updateAgentFreezeState, http.MethodPut, "/api/agent/freeze", map[string]any{
		"scope_type": "global", "scope_id": "", "mode": model.AgentFreezeModeActive, "confirm": true,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("confirmed release status = %d body=%s", response.Code, response.Body.String())
	}
	var state model.AgentFreezeState
	if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Mode != model.AgentFreezeModeActive || state.ScopeType != "global" || state.Actor != "web" {
		t.Fatalf("released state = %+v", state)
	}
}

func TestAgentEventStreamResumesAfterEventID(t *testing.T) {
	server, database := newAgentV2HTTPTestServer(t)
	defer database.Close()
	ctx := context.Background()
	for index := 1; index <= 2; index++ {
		item := model.AgentEvent{EventKey: "http-event-" + string(rune('0'+index)), Type: "goal_updated",
			Severity: "info", Actor: "agent", Payload: json.RawMessage(`{"status":"running"}`)}
		if _, err := database.AppendAgentEvent(ctx, &item); err != nil {
			t.Fatal(err)
		}
	}

	requestContext, cancel := context.WithCancel(context.Background())
	recorder := &cancelOnFlushRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	request := httptest.NewRequest(http.MethodGet, "/api/agent/stream?after_id=1", nil).WithContext(requestContext)
	server.agentEventStream(recorder, request)

	body := recorder.Body.String()
	if recorder.Header().Get("Content-Type") != "text/event-stream; charset=utf-8" ||
		!strings.Contains(body, "id: 2") || !strings.Contains(body, "event: agent_event") || strings.Contains(body, "id: 1\n") {
		t.Fatalf("unexpected stream headers=%v body=%q", recorder.Header(), body)
	}
}

func TestAgentRuntimeEventsInitialPageReturnsNewestTail(t *testing.T) {
	server, database := newAgentV2HTTPTestServer(t)
	defer database.Close()
	for index := 1; index <= 3; index++ {
		item := model.AgentEvent{EventKey: "tail-event-" + string(rune('0'+index)), Type: "step_updated",
			Severity: "info", Actor: "agent", Payload: json.RawMessage(`{}`)}
		if _, err := database.AppendAgentEvent(context.Background(), &item); err != nil {
			t.Fatal(err)
		}
	}
	response := httptest.NewRecorder()
	server.agentRuntimeEvents(response, httptest.NewRequest(http.MethodGet, "/api/agent/events?limit=2", nil))
	var payload struct {
		Items []model.AgentEvent `json:"items"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &payload) != nil {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(payload.Items) != 2 || payload.Items[0].ID != 2 || payload.Items[1].ID != 3 {
		t.Fatalf("initial event tail = %+v", payload.Items)
	}
}

func TestAgentCapabilitiesUseFrontendEnvelope(t *testing.T) {
	response := httptest.NewRecorder()
	(&Server{}).agentCapabilities(response, httptest.NewRequest(http.MethodGet, "/api/agent/capabilities", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var payload struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) == 0 || payload.Items[0]["version"] == nil || payload.Items[0]["input_schema"] == nil {
		t.Fatalf("capability envelope is incomplete: %s", response.Body.String())
	}
}

type cancelOnFlushRecorder struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (r *cancelOnFlushRecorder) Flush() {
	r.ResponseRecorder.Flush()
	r.cancel()
}

func newAgentV2HTTPTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := reconcile.NewEngine(nil, database, 50*time.Second, slog.Default())
	return &Server{store: database, engine: engine}, database
}

func callJSONHandler(t *testing.T, handler http.HandlerFunc, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler(response, httptest.NewRequest(method, target, bytes.NewReader(payload)))
	return response
}
