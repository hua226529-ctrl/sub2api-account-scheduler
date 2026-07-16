package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/agent"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func (s *Server) agentCapabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": agent.CapabilitySpecs()})
}

func (s *Server) agentGoals(w http.ResponseWriter, r *http.Request) {
	limit := queryLimit(r, 30, 500)
	items, err := s.store.ListAgentGoals(r.Context(), r.URL.Query().Get("status"), limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	steps := make([]model.AgentStep, 0)
	if queryIncludes(r, "steps") {
		for _, goal := range items {
			goalSteps, listErr := s.store.ListAgentSteps(r.Context(), goal.ID)
			if listErr != nil {
				writeError(w, http.StatusInternalServerError, "读取智能体目标步骤失败")
				return
			}
			steps = append(steps, goalSteps...)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "steps": steps})
}

func (s *Server) agentRuntimeEvents(w http.ResponseWriter, r *http.Request) {
	rawAfterID := strings.TrimSpace(r.URL.Query().Get("after_id"))
	afterID, ok := queryNonNegativeInt64(w, r, "after_id")
	if !ok {
		return
	}
	goalID, ok := queryNonNegativeInt64(w, r, "goal_id")
	if !ok {
		return
	}
	limit := queryLimit(r, 100, 1000)
	var items []model.AgentEvent
	var err error
	if rawAfterID == "" && goalID == 0 {
		items, err = s.latestAgentEvents(r.Context(), limit)
	} else {
		items, err = s.store.ListAgentEvents(r.Context(), goalID, afterID, limit)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取智能体运行事件失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) agentTasks(w http.ResponseWriter, r *http.Request) {
	goalID, ok := queryNonNegativeInt64(w, r, "goal_id")
	if !ok {
		return
	}
	items, err := s.store.ListScheduledCommands(r.Context(), r.URL.Query().Get("status"), goalID, queryLimit(r, 50, 1000))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) agentMemories(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListAgentMemories(r.Context(), r.URL.Query().Get("scope_type"), r.URL.Query().Get("scope_id"), queryLimit(r, 50, 1000))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取智能体记忆失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) agentFreezeState(w http.ResponseWriter, r *http.Request) {
	state, err := s.engine.FreezeState(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取智能体冻结状态失败")
		return
	}
	writeJSON(w, http.StatusOK, freezeResponse(state))
}

func (s *Server) updateAgentFreezeState(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ScopeType string     `json:"scope_type"`
		ScopeID   string     `json:"scope_id"`
		Mode      string     `json:"mode"`
		Reason    string     `json:"reason"`
		ExpiresAt *time.Time `json:"expires_at"`
		Confirm   bool       `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "冻结状态格式无效")
		return
	}
	body.ScopeType = strings.TrimSpace(body.ScopeType)
	body.ScopeID = strings.TrimSpace(body.ScopeID)
	body.Mode = strings.TrimSpace(body.Mode)
	if body.ScopeType != "global" || body.ScopeID != "" || !validAgentFreezeMode(body.Mode) {
		writeError(w, http.StatusBadRequest, "仅支持有效的全局冻结状态")
		return
	}
	current, err := s.engine.FreezeState(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取当前冻结状态失败")
		return
	}
	if body.Mode == model.AgentFreezeModeActive && current.Mode != model.AgentFreezeModeActive && !body.Confirm {
		writeError(w, http.StatusBadRequest, "解除冻结需要二次确认")
		return
	}
	if body.ExpiresAt != nil && !body.ExpiresAt.After(time.Now().UTC()) {
		writeError(w, http.StatusBadRequest, "冻结截止时间必须晚于当前时间")
		return
	}
	state := model.FreezeState{Mode: body.Mode, Reason: strings.TrimSpace(body.Reason), ExpiresAt: body.ExpiresAt}
	if err := s.engine.UpdateFreezeState(r.Context(), state, "web"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.engine.FreezeState(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "冻结状态已更新但回读失败")
		return
	}
	writeJSON(w, http.StatusOK, freezeResponse(updated))
}

func (s *Server) agentEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "当前连接不支持事件流")
		return
	}
	afterID := int64(0)
	if raw := strings.TrimSpace(r.Header.Get("Last-Event-ID")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "事件游标无效")
			return
		}
		afterID = parsed
	} else if raw := strings.TrimSpace(r.URL.Query().Get("after_id")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "事件游标无效")
			return
		}
		afterID = parsed
	}
	var initial []model.AgentEvent
	var err error
	if afterID == 0 && r.Header.Get("Last-Event-ID") == "" && r.URL.Query().Get("after_id") == "" {
		initial, err = s.latestAgentEvents(r.Context(), 200)
	} else {
		initial, err = s.store.ListAgentEvents(r.Context(), 0, afterID, 200)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "建立智能体事件流失败")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	for _, event := range initial {
		if !writeAgentStreamEvent(w, event) {
			return
		}
		afterID = event.ID
	}
	flusher.Flush()

	poll := time.NewTicker(2 * time.Second)
	heartbeat := time.NewTicker(15 * time.Second)
	defer poll.Stop()
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-poll.C:
			items, listErr := s.store.ListAgentEvents(r.Context(), 0, afterID, 200)
			if listErr != nil {
				_, _ = fmt.Fprintf(w, "event: stream_error\ndata: %s\n\n", mustStreamJSON(map[string]string{"error": "读取事件失败"}))
				flusher.Flush()
				return
			}
			for _, event := range items {
				if !writeAgentStreamEvent(w, event) {
					return
				}
				afterID = event.ID
			}
			if len(items) > 0 {
				flusher.Flush()
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprintf(w, ": keepalive %d\n\n", time.Now().UTC().Unix()); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func freezeResponse(state model.FreezeState) map[string]any {
	result := map[string]any{"scope_type": "global", "scope_id": "", "mode": state.Mode, "reason": state.Reason, "actor": state.Actor}
	if state.ExpiresAt != nil {
		result["expires_at"] = state.ExpiresAt
	}
	if !state.UpdatedAt.IsZero() {
		result["updated_at"] = state.UpdatedAt
	}
	return result
}

// ListAgentEvents is cursor-oriented. For an initial page the control room
// needs the newest tail, so walk bounded database pages while retaining only
// the requested number of events. Subsequent polling uses after_id directly.
func (s *Server) latestAgentEvents(ctx context.Context, limit int) ([]model.AgentEvent, error) {
	const pageSize = 1000
	afterID := int64(0)
	tail := make([]model.AgentEvent, 0, limit)
	for {
		items, err := s.store.ListAgentEvents(ctx, 0, afterID, pageSize)
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			return tail, nil
		}
		tail = append(tail, items...)
		if len(tail) > limit {
			tail = append([]model.AgentEvent(nil), tail[len(tail)-limit:]...)
		}
		afterID = items[len(items)-1].ID
		if len(items) < pageSize {
			return tail, nil
		}
	}
}

func validAgentFreezeMode(mode string) bool {
	switch mode {
	case model.AgentFreezeModeActive, model.AgentFreezeModeAgentPaused, model.AgentFreezeModeReadOnly, model.AgentFreezeModeWritesFrozen:
		return true
	default:
		return false
	}
}

func queryLimit(r *http.Request, fallback, maximum int) int {
	value, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
	if err != nil || value < 1 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func queryNonNegativeInt64(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, true
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		writeError(w, http.StatusBadRequest, key+" 参数无效")
		return 0, false
	}
	return value, true
}

func queryIncludes(r *http.Request, value string) bool {
	for _, item := range strings.Split(r.URL.Query().Get("include"), ",") {
		if strings.TrimSpace(item) == value {
			return true
		}
	}
	return false
}

func writeAgentStreamEvent(w http.ResponseWriter, event model.AgentEvent) bool {
	payload, err := json.Marshal(event)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: agent_event\ndata: %s\n\n", event.ID, payload)
	return err == nil
}

func mustStreamJSON(value any) string {
	payload, _ := json.Marshal(value)
	return string(payload)
}
