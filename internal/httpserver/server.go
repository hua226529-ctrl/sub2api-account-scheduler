package httpserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/agent"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/config"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/webui"
)

const sessionCookie = "scheduler_session"

type Server struct {
	cfg      config.Config
	store    *store.Store
	engine   *reconcile.Engine
	balances *balance.Manager
	client   *sub2api.Client
	agent    *agent.Manager
	logger   *slog.Logger
	mux      *http.ServeMux
	loginMu  sync.Mutex
	loginLog map[string][]time.Time
	writeMu  sync.Mutex
	writeLog map[string][]time.Time
}

type sessionContextKey struct{}

type sessionInfo struct {
	CSRF string
}

func New(cfg config.Config, database *store.Store, engine *reconcile.Engine, balances *balance.Manager, client *sub2api.Client, logger *slog.Logger, agents ...*agent.Manager) *Server {
	s := &Server{cfg: cfg, store: database, engine: engine, balances: balances, client: client, logger: logger, mux: http.NewServeMux(), loginLog: map[string][]time.Time{}, writeLog: map[string][]time.Time{}}
	if len(agents) > 0 {
		s.agent = agents[0]
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.BasePath != "/" {
			bareBasePath := strings.TrimSuffix(s.cfg.BasePath, "/")
			if r.URL.Path == bareBasePath {
				http.Redirect(w, r, s.cfg.BasePath, http.StatusMovedPermanently)
				return
			}
			if strings.HasPrefix(r.URL.Path, s.cfg.BasePath) {
				request := r.Clone(r.Context())
				requestURL := *r.URL
				requestURL.Path = "/" + strings.TrimPrefix(r.URL.Path, s.cfg.BasePath)
				requestURL.RawPath = ""
				request.URL = &requestURL
				s.mux.ServeHTTP(w, request)
				return
			}
		}
		s.mux.ServeHTTP(w, r)
	})
	return securityHeaders(handler)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.Handle("POST /api/session", s.source(http.HandlerFunc(s.login)))
	s.mux.Handle("GET /api/session", s.auth(http.HandlerFunc(s.session)))
	s.mux.Handle("DELETE /api/session", s.auth(http.HandlerFunc(s.logout)))
	s.mux.Handle("GET /api/overview", s.auth(http.HandlerFunc(s.overview)))
	s.mux.Handle("GET /api/diagnostics", s.auth(http.HandlerFunc(s.diagnostics)))
	s.mux.Handle("GET /api/events", s.auth(http.HandlerFunc(s.events)))
	s.mux.Handle("PUT /api/settings", s.mutation(http.HandlerFunc(s.updateSettings)))
	s.mux.Handle("PUT /api/policies/{accountID}", s.mutation(http.HandlerFunc(s.updatePolicy)))
	s.mux.Handle("POST /api/actions/{accountID}/{action}", s.mutation(http.HandlerFunc(s.accountAction)))
	s.mux.Handle("POST /api/reconcile", s.mutation(http.HandlerFunc(s.triggerReconcile)))
	s.mux.Handle("POST /api/upstreams/validate", s.mutation(http.HandlerFunc(s.validateUpstream)))
	s.mux.Handle("GET /api/upstreams", s.auth(http.HandlerFunc(s.listUpstreams)))
	s.mux.Handle("POST /api/upstreams", s.mutation(http.HandlerFunc(s.createUpstream)))
	s.mux.Handle("PUT /api/upstreams/{id}", s.mutation(http.HandlerFunc(s.updateUpstream)))
	s.mux.Handle("DELETE /api/upstreams/{id}", s.mutation(http.HandlerFunc(s.deleteUpstream)))
	s.mux.Handle("POST /api/upstreams/{id}/refresh", s.mutation(http.HandlerFunc(s.refreshUpstream)))
	s.mux.Handle("POST /api/upstreams/{id}/keys/{keyID}/group", s.mutation(http.HandlerFunc(s.switchUpstreamKeyGroup)))
	s.mux.Handle("PUT /api/upstreams/{id}/failover/policies/{keyID}", s.mutation(http.HandlerFunc(s.saveUpstreamFailoverPolicy)))
	s.mux.Handle("DELETE /api/upstreams/{id}/failover/policies/{keyID}", s.mutation(http.HandlerFunc(s.deleteUpstreamFailoverPolicy)))
	s.mux.Handle("POST /api/upstreams/{id}/failover/policies/{keyID}/confirm", s.mutation(http.HandlerFunc(s.confirmUpstreamFailoverPolicy)))
	s.mux.Handle("GET /api/upstreams/{id}/failover/transitions", s.auth(http.HandlerFunc(s.listUpstreamFailoverTransitions)))
	s.mux.Handle("POST /api/upstreams/{id}/keys/{keyID}/tier", s.mutation(http.HandlerFunc(s.switchUpstreamKeyTier)))
	s.mux.Handle("GET /api/agent/overview", s.auth(http.HandlerFunc(s.agentOverview)))
	s.mux.Handle("PUT /api/agent/settings", s.mutation(http.HandlerFunc(s.updateAgentSettings)))
	s.mux.Handle("POST /api/agent/providers/validate", s.mutation(http.HandlerFunc(s.validateAgentProvider)))
	s.mux.Handle("PUT /api/agent/providers/{slot}", s.mutation(http.HandlerFunc(s.saveAgentProvider)))
	s.mux.Handle("POST /api/agent/run", s.mutation(http.HandlerFunc(s.runAgent)))
	s.mux.Handle("POST /api/agent/chat", s.mutation(http.HandlerFunc(s.chatAgent)))
	s.mux.Handle("GET /api/agent/conversations/{id}/messages", s.auth(http.HandlerFunc(s.agentMessages)))
	s.mux.Handle("POST /api/agent/policies/{id}/activate", s.mutation(http.HandlerFunc(s.activateAgentPolicy)))
	s.mux.Handle("GET /api/agent/capabilities", s.auth(http.HandlerFunc(s.agentCapabilities)))
	s.mux.Handle("GET /api/agent/goals", s.auth(http.HandlerFunc(s.agentGoals)))
	s.mux.Handle("GET /api/agent/events", s.auth(http.HandlerFunc(s.agentRuntimeEvents)))
	s.mux.Handle("GET /api/agent/tasks", s.auth(http.HandlerFunc(s.agentTasks)))
	s.mux.Handle("GET /api/agent/memories", s.auth(http.HandlerFunc(s.agentMemories)))
	s.mux.Handle("GET /api/agent/freeze", s.auth(http.HandlerFunc(s.agentFreezeState)))
	s.mux.Handle("PUT /api/agent/freeze", s.mutation(http.HandlerFunc(s.updateAgentFreezeState)))
	s.mux.Handle("GET /api/agent/stream", s.auth(http.HandlerFunc(s.agentEventStream)))
	s.mux.Handle("/", webui.Handler())
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "数据库不可用")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	snapshot := s.engine.Snapshot()
	if snapshot.LastSyncAt == nil || time.Since(*snapshot.LastSyncAt) > 2*s.cfg.PollInterval || snapshot.LastSyncError != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "last_error": snapshot.LastSyncError})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "last_sync_at": snapshot.LastSyncAt})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !s.allowLogin(ip) {
		writeError(w, http.StatusTooManyRequests, "登录尝试过于频繁，请稍后再试")
		return
	}
	var body struct {
		APIKey string `json:"api_key"`
	}
	if err := decodeJSON(r, &body); err != nil || strings.TrimSpace(body.APIKey) == "" {
		writeError(w, http.StatusBadRequest, "请输入管理员密钥")
		return
	}
	candidate := strings.TrimSpace(body.APIKey)
	if subtle.ConstantTimeCompare([]byte(candidate), []byte(s.cfg.AdminAPIKey)) != 1 {
		writeError(w, http.StatusUnauthorized, "管理员密钥无效")
		return
	}
	if err := s.client.Validate(r.Context(), candidate); err != nil {
		s.logger.Warn("admin_key_validation_failed", "error", err)
		writeError(w, http.StatusUnauthorized, "管理员密钥无法通过 Sub2API 验证")
		return
	}
	token := randomToken(32)
	csrf := randomToken(24)
	if err := s.store.CreateSession(r.Context(), token, csrf, time.Now().UTC().Add(s.cfg.SessionIdleTimeout)); err != nil {
		writeError(w, http.StatusInternalServerError, "创建会话失败")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: s.cfg.BasePath, HttpOnly: true, Secure: s.cfg.CookieSecure, SameSite: http.SameSiteStrictMode})
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "csrf_token": csrf, "expires_in_seconds": int(s.cfg.SessionIdleTimeout.Seconds())})
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	info := r.Context().Value(sessionContextKey{}).(sessionInfo)
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "csrf_token": info.CSRF, "expires_in_seconds": int(s.cfg.SessionIdleTimeout.Seconds())})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		_ = s.store.DeleteSession(r.Context(), cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: s.cfg.BasePath, HttpOnly: true, Secure: s.cfg.CookieSecure, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Snapshot())
}

func (s *Server) diagnostics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.engine.Snapshot()
	databaseStatus := "ok"
	if err := s.store.Ping(r.Context()); err != nil {
		databaseStatus = "error"
	}
	ready := snapshot.LastSyncAt != nil && time.Since(*snapshot.LastSyncAt) <= 2*s.cfg.PollInterval && snapshot.LastSyncError == ""
	writeJSON(w, http.StatusOK, map[string]any{
		"alive":                         true,
		"ready":                         ready,
		"database":                      databaseStatus,
		"last_sync_at":                  snapshot.LastSyncAt,
		"last_sync_error":               snapshot.LastSyncError,
		"service_started_at":            snapshot.ServiceStarted,
		"poll_interval_seconds":         int(s.cfg.PollInterval.Seconds()),
		"dry_run":                       snapshot.Settings.DryRun,
		"balance_poll_interval_seconds": int(s.cfg.BalancePollInterval.Seconds()),
		"balance_last_run_at":           s.balances.LastRunAt(),
	})
}

func (s *Server) validateUpstream(w http.ResponseWriter, r *http.Request) {
	var input balance.SourceInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "上游配置格式无效")
		return
	}
	result, err := s.balances.Validate(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listUpstreams(w http.ResponseWriter, r *http.Request) {
	items, err := s.balances.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取上游余额账户失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) createUpstream(w http.ResponseWriter, r *http.Request) {
	var body struct {
		balance.SourceInput
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	item, err := s.balances.Create(r.Context(), body.SourceInput, "web")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (s *Server) updateUpstream(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	var body struct {
		balance.SourceInput
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	item, err := s.balances.Update(r.Context(), id, body.SourceInput, "web")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) deleteUpstream(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	if err := s.balances.Delete(r.Context(), id, "web"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) refreshUpstream(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	if err := s.balances.RefreshManual(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	item, err := s.balances.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) switchUpstreamKeyGroup(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.PathValue("keyID"))
	var body struct {
		GroupID string `json:"group_id"`
		Confirm bool   `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm || keyID == "" || strings.TrimSpace(body.GroupID) == "" {
		writeError(w, http.StatusBadRequest, "切换令牌分组需要二次确认")
		return
	}
	item, err := s.balances.SwitchGroup(r.Context(), id, keyID, strings.TrimSpace(body.GroupID), "web")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) saveUpstreamFailoverPolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.PathValue("keyID"))
	var body struct {
		model.GroupFailoverPolicy
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm || keyID == "" {
		writeError(w, http.StatusBadRequest, "保存三级分组策略需要二次确认")
		return
	}
	body.SourceID = id
	body.KeyID = keyID
	item, err := s.balances.SaveGroupFailoverPolicy(r.Context(), body.GroupFailoverPolicy, "web")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) deleteUpstreamFailoverPolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.PathValue("keyID"))
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm || keyID == "" {
		writeError(w, http.StatusBadRequest, "删除三级分组策略需要二次确认")
		return
	}
	if err := s.balances.DeleteGroupFailoverPolicy(r.Context(), id, keyID, "web"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) confirmUpstreamFailoverPolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.PathValue("keyID"))
	var body struct {
		Version int64 `json:"version"`
		Confirm bool  `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm || keyID == "" || body.Version <= 0 {
		writeError(w, http.StatusBadRequest, "确认三级分组策略需要有效版本和二次确认")
		return
	}
	item, err := s.balances.ConfirmGroupFailoverPolicy(r.Context(), id, keyID, body.Version, "web")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) listUpstreamFailoverTransitions(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.URL.Query().Get("key_id"))
	items, err := s.store.ListGroupTierTransitions(r.Context(), id, keyID, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取分组切换记录失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) switchUpstreamKeyTier(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUpstreamID(w, r)
	if !ok {
		return
	}
	keyID := strings.TrimSpace(r.PathValue("keyID"))
	var body struct {
		Tier    string `json:"tier"`
		Confirm bool   `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm || keyID == "" {
		writeError(w, http.StatusBadRequest, "手动切换分组层级需要二次确认")
		return
	}
	tier := strings.ToLower(strings.TrimSpace(body.Tier))
	if tier != model.GroupTierMain && tier != model.GroupTierBackup && tier != model.GroupTierEmergency {
		writeError(w, http.StatusBadRequest, "目标层级只能是主分组、备用分组或紧急分组")
		return
	}
	_, err := s.balances.TransitionGroupTier(r.Context(), model.GroupTierTransitionRequest{
		SourceID: id, KeyID: keyID, TargetTier: tier, IdempotencyKey: "web-" + randomToken(18),
		Actor: "web", Reason: "管理员在余额中心手动切换令牌分组层级", Trigger: "manual", Manual: true,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	items, err := s.balances.ListGroupFailoverPolicies(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "切换成功但读取策略状态失败")
		return
	}
	for _, item := range items {
		if item.KeyID == keyID {
			writeJSON(w, http.StatusOK, item)
			return
		}
	}
	writeError(w, http.StatusNotFound, "切换成功但三级分组策略不存在")
}

func (s *Server) requireAgent(w http.ResponseWriter) (*agent.Manager, bool) {
	if s.agent == nil {
		writeError(w, http.StatusServiceUnavailable, "智能体服务未初始化")
		return nil, false
	}
	return s.agent, true
}

func (s *Server) agentOverview(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	result, err := manager.Overview(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取智能调度状态失败")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) updateAgentSettings(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	var body struct {
		model.AgentSettings
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	if err := manager.UpdateSettings(r.Context(), body.AgentSettings); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, body.AgentSettings)
}

func (s *Server) validateAgentProvider(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	var input agent.ProviderInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "模型配置格式无效")
		return
	}
	result, err := manager.ValidateProvider(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) saveAgentProvider(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	var body struct {
		agent.ProviderInput
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	body.Slot = strings.TrimSpace(r.PathValue("slot"))
	result, err := manager.SaveProvider(r.Context(), body.ProviderInput)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) runAgent(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	goal, err := manager.EnqueueAnalysisGoal(r.Context(), model.AgentRunManual, "管理员手动触发", 80)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"goal_id": goal.ID, "run_id": 0, "status": goal.Status})
}

func (s *Server) chatAgent(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	var body struct {
		ConversationID int64  `json:"conversation_id"`
		Message        string `json:"message"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "对话内容格式无效")
		return
	}
	conversationID, goalID, runID, status, err := manager.ChatAsync(r.Context(), body.ConversationID, body.Message)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"conversation_id": conversationID, "goal_id": goalID, "run_id": runID, "status": status})
}

func (s *Server) agentMessages(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "对话编号无效")
		return
	}
	items, err := manager.Messages(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取对话失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) activateAgentPolicy(w http.ResponseWriter, r *http.Request) {
	manager, ok := s.requireAgent(w)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err != nil || id <= 0 || decodeJSON(r, &body) != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "策略回退需要二次确认")
		return
	}
	if err := manager.ActivatePolicy(r.Context(), id, "管理端"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"activated": true})
}

func parseUpstreamID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "上游编号无效")
		return 0, false
	}
	return id, true
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.engine.Events(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取操作记录失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) updateSettings(w http.ResponseWriter, r *http.Request) {
	var body struct {
		model.Settings
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	if err := s.engine.UpdateSettings(r.Context(), body.Settings, "web"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, body.Settings)
}

func (s *Server) updatePolicy(w http.ResponseWriter, r *http.Request) {
	accountID, err := strconv.ParseInt(r.PathValue("accountID"), 10, 64)
	if err != nil || accountID <= 0 {
		writeError(w, http.StatusBadRequest, "账号编号无效")
		return
	}
	var body struct {
		MonitorID                *int64 `json:"monitor_id"`
		Excluded                 bool   `json:"excluded"`
		Enabled                  bool   `json:"enabled"`
		FailureThreshold         *int   `json:"failure_threshold"`
		RecoveryThreshold        *int   `json:"recovery_threshold"`
		FlapEnabled              *bool  `json:"flap_enabled"`
		FlapWindowMinutes        *int   `json:"flap_window_minutes"`
		FlapPauseThreshold       *int   `json:"flap_pause_threshold"`
		FlapRecoveryThreshold    *int   `json:"flap_recovery_threshold"`
		HealthHealthyScore       *int   `json:"healthy_score_threshold"`
		HealthWatchScore         *int   `json:"watch_score_threshold"`
		HealthQuarantineScore    *int   `json:"quarantine_score_threshold"`
		HealthMinSamples         *int   `json:"minimum_samples"`
		HealthLatencyWarningMS   *int64 `json:"latency_warning_ms"`
		HealthLatencyCriticalMS  *int64 `json:"latency_critical_ms"`
		HealthTrafficPauseBelow  *int   `json:"traffic_pause_below"`
		HealthTrafficHealthyAt   *int   `json:"traffic_healthy_at"`
		HealthHardFailures10     *int   `json:"hard_failures_10_threshold"`
		HealthPersistentSlowRate *int   `json:"persistent_slow_rate"`
		Confirm                  bool   `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	policy := model.Policy{
		AccountID: accountID, MonitorID: body.MonitorID, Excluded: body.Excluded, Enabled: body.Enabled,
		FailureThreshold: body.FailureThreshold, RecoveryThreshold: body.RecoveryThreshold,
		FlapEnabled: body.FlapEnabled, FlapWindowMinutes: body.FlapWindowMinutes,
		FlapPauseThreshold: body.FlapPauseThreshold, FlapRecoveryThreshold: body.FlapRecoveryThreshold,
		HealthHealthyScore: body.HealthHealthyScore, HealthWatchScore: body.HealthWatchScore,
		HealthQuarantineScore: body.HealthQuarantineScore, HealthMinSamples: body.HealthMinSamples,
		HealthLatencyWarningMS: body.HealthLatencyWarningMS, HealthLatencyCriticalMS: body.HealthLatencyCriticalMS,
		HealthTrafficPauseBelow: body.HealthTrafficPauseBelow, HealthTrafficHealthyAt: body.HealthTrafficHealthyAt,
		HealthHardFailures10: body.HealthHardFailures10, HealthPersistentSlowRate: body.HealthPersistentSlowRate,
	}
	if err := s.engine.UpdatePolicy(r.Context(), policy, "web"); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) accountAction(w http.ResponseWriter, r *http.Request) {
	accountID, err := strconv.ParseInt(r.PathValue("accountID"), 10, 64)
	if err != nil || accountID <= 0 {
		writeError(w, http.StatusBadRequest, "账号编号无效")
		return
	}
	var body struct {
		Confirm    bool `json:"confirm"`
		TTLMinutes *int `json:"ttl_minutes,omitempty"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	commandID := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if commandID == "" {
		commandID, err = accountcontrol.NewCommandID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "生成命令编号失败")
			return
		}
	}
	if err := accountcontrol.ValidateCommandID(commandID); err != nil {
		writeError(w, http.StatusBadRequest, "Idempotency-Key 无效")
		return
	}
	ttl := accountcontrol.DefaultAdministratorTTL
	if body.TTLMinutes != nil {
		if *body.TTLMinutes < 1 || *body.TTLMinutes > 24*60 {
			writeError(w, http.StatusBadRequest, "TTL 必须在 1 到 1440 分钟之间")
			return
		}
		ttl = time.Duration(*body.TTLMinutes) * time.Minute
	}
	var result accountcontrol.Result
	switch r.PathValue("action") {
	case "pause":
		result, err = s.engine.ManualPauseCommand(r.Context(), accountID, "web", commandID)
	case "resume":
		result, err = s.engine.ManualResumeCommand(r.Context(), accountID, "web", commandID, ttl)
	case "release-manual-hold", "clear-override":
		result, err = s.engine.ReleaseManualHoldCommand(r.Context(), accountID, "web", commandID)
	case "clear-flap":
		err = s.engine.ClearFlapProtection(r.Context(), accountID, "web")
	default:
		writeError(w, http.StatusNotFound, "未知操作")
		return
	}
	if err != nil {
		var idempotencyConflict *accountcontrol.IdempotencyConflictError
		var blocked *accountcontrol.BlockedError
		var mutationState *accountcontrol.MutationStateError
		switch {
		case errors.As(err, &idempotencyConflict):
			writeJSON(w, http.StatusConflict, map[string]any{"error": "idempotency_conflict", "command_id": commandID})
		case errors.As(err, &blocked):
			writeJSON(w, http.StatusConflict, blocked.Result)
		case errors.As(err, &mutationState):
			status := http.StatusConflict
			if mutationState.Result.Uncertain {
				status = http.StatusServiceUnavailable
			}
			writeJSON(w, status, mutationState.Result)
		case result.Uncertain:
			writeJSON(w, http.StatusServiceUnavailable, result)
		case result.MutationID != "":
			writeJSON(w, http.StatusConflict, result)
		default:
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}
	if r.PathValue("action") == "clear-flap" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "command_id": commandID})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) triggerReconcile(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeJSON(r, &body); err != nil || !body.Confirm {
		writeError(w, http.StatusBadRequest, "需要二次确认")
		return
	}
	s.engine.Trigger()
	writeJSON(w, http.StatusAccepted, map[string]any{"queued": true})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(sessionCookie)
		if err != nil || cookie.Value == "" {
			writeError(w, http.StatusUnauthorized, "请重新登录")
			return
		}
		csrf, ok, err := s.store.ValidateSession(r.Context(), cookie.Value, s.cfg.SessionIdleTimeout)
		if err != nil || !ok {
			writeError(w, http.StatusUnauthorized, "会话已失效")
			return
		}
		ctx := context.WithValue(r.Context(), sessionContextKey{}, sessionInfo{CSRF: csrf})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) csrf(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := r.Context().Value(sessionContextKey{}).(sessionInfo)
		provided := r.Header.Get("X-CSRF-Token")
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(info.CSRF)) != 1 {
			writeError(w, http.StatusForbidden, "请求校验失败")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) mutation(next http.Handler) http.Handler {
	return s.auth(s.source(s.writeRateLimit(s.csrf(next))))
}

func (s *Server) source(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !sameOriginRequest(r) {
			writeError(w, http.StatusForbidden, "请求来源校验失败")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) writeRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := clientIP(r)
		if cookie, err := r.Cookie(sessionCookie); err == nil {
			key += "|" + cookie.Value
		}
		if !s.allowWrite(key) {
			writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowLogin(ip string) bool {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	now := time.Now()
	cutoff := now.Add(-5 * time.Minute)
	entries := s.loginLog[ip][:0]
	for _, entry := range s.loginLog[ip] {
		if entry.After(cutoff) {
			entries = append(entries, entry)
		}
	}
	if len(entries) >= 5 {
		s.loginLog[ip] = entries
		return false
	}
	s.loginLog[ip] = append(entries, now)
	return true
}

func (s *Server) allowWrite(key string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now()
	cutoff := now.Add(-time.Minute)
	entries := s.writeLog[key][:0]
	for _, entry := range s.writeLog[key] {
		if entry.After(cutoff) {
			entries = append(entries, entry)
		}
	}
	if len(entries) >= 30 {
		s.writeLog[key] = entries
		return false
	}
	s.writeLog[key] = append(entries, now)
	return true
}

func sameOriginRequest(r *http.Request) bool {
	if site := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")); site != "" && site != "same-origin" {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Path != "" {
		return false
	}
	scheme := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0])
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return strings.EqualFold(parsed.Scheme, scheme) && strings.EqualFold(parsed.Host, r.Host)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(r *http.Request, out any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func randomToken(size int) string {
	buffer := make([]byte, size)
	_, _ = rand.Read(buffer)
	return base64.RawURLEncoding.EncodeToString(buffer)
}

func clientIP(r *http.Request) string {
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0]); forwarded != "" {
		return forwarded
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}
