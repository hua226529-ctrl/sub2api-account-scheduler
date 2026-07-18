package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
)

type CapabilityInvocation struct {
	Name               string
	Arguments          json.RawMessage
	RunID              int64
	GoalID             int64
	StepID             int64
	PacketID           int64
	PacketHash         string
	Actor              string
	IdempotencyKey     string
	AdministratorGrant *AdministratorGrant
	DryRun             bool
	CreatedAt          time.Time
	ExpiresAt          *time.Time
	SnapshotVersion    string
	EvidenceRefs       []string
}

type CapabilityExecution struct {
	Capability  string          `json:"capability"`
	Status      string          `json:"status"`
	DryRun      bool            `json:"dry_run"`
	BeforeState json.RawMessage `json:"before_state"`
	AfterState  json.RawMessage `json:"after_state"`
	Output      json.RawMessage `json:"output"`
	Retryable   bool            `json:"retryable"`
	Message     string          `json:"message"`
}

func (m *Manager) ExecuteCapability(ctx context.Context, invocation CapabilityInvocation) (CapabilityExecution, error) {
	invocation.Name = strings.TrimSpace(invocation.Name)
	invocation.Actor = strings.TrimSpace(invocation.Actor)
	if invocation.Actor == "" {
		invocation.Actor = "agent"
	}
	spec, ok := capabilitySpec(invocation.Name)
	if !ok {
		return CapabilityExecution{}, fmt.Errorf("未授权能力 %s", invocation.Name)
	}
	arguments, err := normalizedArguments(invocation.Arguments)
	if err != nil {
		if invocation.DryRun && spec.Mutating {
			_ = m.store.RecordAgentObservation(ctx, 1, 0, 0, 0)
		}
		return CapabilityExecution{}, err
	}
	invocation.Arguments = arguments
	if invocation.AdministratorGrant != nil {
		if !spec.AdministratorDirect {
			return CapabilityExecution{}, errors.New("该能力不接受管理员直达授权")
		}
		if err := validateAdministratorGrant(invocation.AdministratorGrant, invocation.Name, invocation.Arguments); err != nil {
			return CapabilityExecution{}, fmt.Errorf("管理员精确授权无效: %w", err)
		}
		invocation.Actor = "administrator:agent"
	} else if invocation.Actor == "administrator:agent" {
		// Actor text is audit metadata, not authority. Without a typed grant an
		// injected or legacy administrator actor receives ordinary agent rules.
		invocation.Actor = "agent:v2"
	}
	if err := applyExecutionPolicyDefaults(&invocation, spec); err != nil {
		return CapabilityExecution{}, err
	}
	if spec.Mutating && invocation.AdministratorGrant == nil {
		if spec.ExecutionPolicy.RequiresConfirmation {
			return CapabilityExecution{}, errors.New("该写能力需要载荷精确绑定的管理员确认")
		}
		if !spec.ExecutionPolicy.SupportsAutonomous {
			return CapabilityExecution{}, errors.New("该写能力不支持自主执行")
		}
	}
	if spec.Mutating {
		if spec.ExecutionPolicy.RequiresEvidence && len(invocation.EvidenceRefs) == 0 {
			return CapabilityExecution{}, errors.New("写能力缺少 EvidenceRefs")
		}
		if spec.ExecutionPolicy.RequiresFreshSnapshot && strings.TrimSpace(invocation.SnapshotVersion) == "" {
			return CapabilityExecution{}, errors.New("写能力缺少 SnapshotVersion")
		}
		if spec.ExecutionPolicy.MaxScope > 0 {
			count, scopeErr := capabilityInvocationScopeCount(invocation.Name, invocation.Arguments)
			if scopeErr != nil {
				return CapabilityExecution{}, scopeErr
			}
			if count > spec.ExecutionPolicy.MaxScope {
				return CapabilityExecution{}, fmt.Errorf("写能力作用域超过上限 %d", spec.ExecutionPolicy.MaxScope)
			}
		}
		if spec.ExecutionPolicy.MaxTTLSeconds > 0 {
			if invocation.ExpiresAt == nil {
				return CapabilityExecution{}, errors.New("写能力缺少 TTL")
			}
			createdAt := invocation.CreatedAt.UTC()
			if createdAt.IsZero() {
				return CapabilityExecution{}, errors.New("写能力缺少创建时间")
			}
			ttl := invocation.ExpiresAt.Sub(createdAt)
			if ttl <= 0 || ttl > time.Duration(spec.ExecutionPolicy.MaxTTLSeconds)*time.Second {
				return CapabilityExecution{}, errors.New("写能力 TTL 超出执行策略")
			}
		}
	}
	if spec.Mutating && !invocation.DryRun {
		// Every model-driven write, including one performed on behalf of an
		// administrator, shares the same barrier as global freeze publication.
		// Browser-originated manual operations deliberately remain outside it.
		release, err := m.engine.AutomationBarrier().EnterMutation(ctx)
		if err != nil {
			return CapabilityExecution{}, fmt.Errorf("等待智能体 mutation 冻结屏障: %w", err)
		}
		defer release()
	}
	if spec.Mutating {
		freeze, freezeErr := m.engine.FreezeState(ctx)
		if freezeErr != nil {
			return CapabilityExecution{}, fmt.Errorf("无法确认自动化冻结状态: %w", freezeErr)
		}
		if freeze.AllAutomation {
			return CapabilityExecution{}, errors.New("全部自动化已被冻结")
		}
		if freeze.Agent {
			return CapabilityExecution{}, errors.New("智能体已被冻结")
		}
	}
	if spec.Mutating && !invocation.DryRun && invocation.AdministratorGrant != nil {
		if invocation.GoalID <= 0 || invocation.StepID <= 0 {
			return CapabilityExecution{}, errors.New("管理员精确授权缺少持久目标或步骤编号，已拒绝执行")
		}
		if _, err := m.store.ConsumeAdministratorGrant(ctx, invocation.AdministratorGrant.GrantID,
			invocation.GoalID, invocation.StepID, invocation.Name, administratorArgumentsHash(invocation.Arguments)); err != nil {
			return CapabilityExecution{}, fmt.Errorf("管理员精确授权已失效或已被其他步骤消费: %w", err)
		}
	}
	if spec.Mutating {
		ctx = withAccountControlContext(ctx, invocation)
	}
	before := m.capabilityState(ctx, invocation)
	output, retryable, err := m.executeCapabilityBody(ctx, invocation)
	after := m.capabilityState(ctx, invocation)
	execution := CapabilityExecution{Capability: invocation.Name, DryRun: invocation.DryRun, BeforeState: before,
		AfterState: after, Output: marshalRaw(output), Retryable: retryable}
	if err != nil {
		if invocation.DryRun && spec.Mutating {
			_ = m.store.RecordAgentObservation(ctx, 1, 0, 0, 0)
		}
		execution.Status, execution.Message = "failed", err.Error()
		m.recordEventWithProvenance(ctx, "agent_capability_failed", "error", capabilityAccountID(invocation.Arguments),
			invocation.Name+" 执行失败: "+err.Error(), invocation.RunID, invocation.GoalID, invocation.StepID)
		return execution, err
	}
	if invocation.DryRun && spec.Mutating {
		_ = m.store.RecordAgentObservation(ctx, 1, 1, 0, 0)
		execution.Status, execution.Message = "proposed", "观察模式：前置检查通过，未执行写入"
	} else {
		execution.Status, execution.Message = "completed", "已执行并回读确认"
	}
	return execution, nil
}

func applyExecutionPolicyDefaults(invocation *CapabilityInvocation, spec CapabilitySpec) error {
	if invocation == nil || !spec.Mutating {
		return nil
	}
	argumentExpiry, err := capabilityArgumentExpiry(invocation.Arguments)
	if err != nil {
		return err
	}
	if argumentExpiry != nil {
		if invocation.ExpiresAt != nil && !invocation.ExpiresAt.Equal(*argumentExpiry) {
			return errors.New("能力参数 TTL 与目标授权 TTL 不一致")
		}
		invocation.ExpiresAt = argumentExpiry
	}
	if invocation.ExpiresAt != nil || spec.ExecutionPolicy.DefaultTTLSeconds <= 0 {
		return nil
	}
	if invocation.CreatedAt.IsZero() {
		return errors.New("写能力缺少创建时间，无法应用默认 TTL")
	}
	expiresAt := invocation.CreatedAt.UTC().Add(time.Duration(spec.ExecutionPolicy.DefaultTTLSeconds) * time.Second)
	invocation.ExpiresAt = &expiresAt
	return nil
}

func capabilityArgumentExpiry(arguments json.RawMessage) (*time.Time, error) {
	if len(arguments) == 0 {
		return nil, nil
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &values); err != nil {
		return nil, fmt.Errorf("能力参数无效: %w", err)
	}
	var result *time.Time
	for _, key := range []string{"expires_at", "until"} {
		raw, exists := values[key]
		if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			continue
		}
		var value time.Time
		if err := json.Unmarshal(raw, &value); err != nil || value.IsZero() {
			return nil, fmt.Errorf("能力参数 %s 不是有效时间", key)
		}
		value = value.UTC()
		if result != nil && !result.Equal(value) {
			return nil, errors.New("能力参数包含冲突的 TTL")
		}
		result = &value
	}
	return result, nil
}

func capabilityInvocationScopeCount(name string, arguments json.RawMessage) (int, error) {
	var values map[string]any
	if err := json.Unmarshal(arguments, &values); err != nil {
		return 0, fmt.Errorf("能力参数无效: %w", err)
	}
	for _, key := range []string{"account_id", "source_id", "policy_id", "command_id", "scope_id"} {
		if value, exists := values[key]; exists && value != nil && fmt.Sprint(value) != "" && fmt.Sprint(value) != "0" {
			return 1, nil
		}
	}
	if name == "trigger_reconcile" || name == "schedule_command" {
		return 1, nil
	}
	return 0, nil
}

func (m *Manager) executeCapabilityBody(ctx context.Context, invocation CapabilityInvocation) (any, bool, error) {
	switch invocation.Name {
	case "get_system_snapshot":
		settings, err := m.store.GetAgentSettings(ctx)
		if err != nil {
			return nil, true, err
		}
		packet, err := m.builder.Build(ctx, "capability_snapshot", settings)
		if err != nil {
			return nil, true, err
		}
		freeze, _ := m.engine.FreezeState(ctx)
		goals, _ := m.store.ListAgentGoals(ctx, "", 50)
		commands, _ := m.store.ListScheduledCommands(ctx, "", 0, 50)
		return map[string]any{"packet": compactPacketForModel(packet), "freeze": freeze, "goals": goals, "scheduled_commands": commands}, false, nil
	case "get_account_evidence":
		var args struct {
			AccountID int64 `json:"account_id"`
			Limit     int   `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		settings, err := m.store.GetAgentSettings(ctx)
		if err != nil {
			return nil, true, err
		}
		packet, err := m.builder.Build(ctx, "account_evidence", settings)
		if err != nil {
			return nil, true, err
		}
		for _, state := range packet.AccountCompactStates {
			if state.AccountID == args.AccountID {
				records, listErr := m.store.ListAccountEvidence(ctx, args.AccountID, boundedLimit(args.Limit, 20, 50))
				return map[string]any{"account_state": state, "records": records}, listErr != nil, listErr
			}
		}
		return nil, false, errors.New("账号不在当前调度快照中")
	case "get_pool_evidence":
		var args struct {
			Pool  string `json:"pool"`
			Limit int    `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || strings.TrimSpace(args.Pool) == "" {
			return nil, false, firstError(err, errors.New("调度池名称无效"))
		}
		settings, err := m.store.GetAgentSettings(ctx)
		if err != nil {
			return nil, true, err
		}
		packet, err := m.builder.Build(ctx, "pool_evidence", settings)
		if err != nil {
			return nil, true, err
		}
		limit := boundedLimit(args.Limit, 20, 50)
		accounts := make([]model.AgentAccountState, 0)
		for _, state := range packet.AccountCompactStates {
			if state.Pool == strings.TrimSpace(args.Pool) && len(accounts) < limit {
				accounts = append(accounts, state)
			}
		}
		tokens := make([]model.AgentGroupFailoverToken, 0)
		for _, token := range packet.GroupFailoverTokens {
			if token.Pool == strings.TrimSpace(args.Pool) {
				tokens = append(tokens, token)
			}
		}
		if len(accounts) == 0 {
			return nil, false, errors.New("调度池不存在或没有账号")
		}
		return map[string]any{"accounts": accounts, "group_failover_tokens": tokens}, false, nil
	case "get_upstream_state":
		var args struct {
			SourceID int64 `json:"source_id"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.balances.List(ctx)
		if err != nil {
			return nil, true, err
		}
		result := make([]map[string]any, 0)
		for _, item := range items {
			if args.SourceID > 0 && item.ID != args.SourceID {
				continue
			}
			result = append(result, sanitizeUpstream(item))
		}
		return map[string]any{"items": result}, false, nil
	case "get_audit_events":
		var args struct {
			Limit int `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.engine.Events(ctx, boundedLimit(args.Limit, 50, 200))
		return map[string]any{"items": items}, err != nil, err
	case "get_policy_history":
		var args struct {
			ScopeType string `json:"scope_type"`
			ScopeID   string `json:"scope_id"`
			Limit     int    `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.store.ListPolicyLifecycle(ctx, boundedLimit(args.Limit, 50, 200))
		if err != nil {
			return nil, true, err
		}
		filtered := make([]model.ScorePolicyVersion, 0)
		for _, item := range items {
			if (args.ScopeType == "" || item.ScopeType == args.ScopeType) && (args.ScopeID == "" || item.ScopeID == args.ScopeID) {
				filtered = append(filtered, item)
			}
		}
		return map[string]any{"items": filtered}, false, nil
	case "get_action_outcomes":
		var args struct {
			RunID int64 `json:"run_id"`
			Limit int   `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.store.ListRecentDecisionOutcomes(ctx, boundedLimit(args.Limit, 50, 200))
		if err != nil {
			return nil, true, err
		}
		filtered := make([]model.DecisionOutcome, 0)
		for _, item := range items {
			if args.RunID == 0 || item.RunID == args.RunID {
				filtered = append(filtered, item)
			}
		}
		return map[string]any{"items": filtered}, false, nil
	case "list_goals":
		var args struct {
			Status string `json:"status"`
			Limit  int    `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.store.ListAgentGoals(ctx, args.Status, boundedLimit(args.Limit, 50, 200))
		return map[string]any{"items": items}, err != nil, err
	case "list_scheduled_commands":
		var args struct {
			Status string `json:"status"`
			Limit  int    `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		items, err := m.store.ListScheduledCommands(ctx, args.Status, 0, boundedLimit(args.Limit, 50, 200))
		return map[string]any{"items": items}, err != nil, err
	case "search_memory":
		var args struct {
			Query string `json:"query"`
			Limit int    `json:"limit"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || strings.TrimSpace(args.Query) == "" {
			return nil, false, firstError(err, errors.New("记忆查询不能为空"))
		}
		items, err := m.store.ListAgentMemories(ctx, "", "", 1000)
		if err != nil {
			return nil, true, err
		}
		needle, limit := strings.ToLower(strings.TrimSpace(args.Query)), boundedLimit(args.Limit, 20, 100)
		matched := make([]model.AgentMemory, 0)
		for _, item := range items {
			if strings.Contains(strings.ToLower(item.Summary+" "+item.Key+" "+string(item.Content)), needle) {
				matched = append(matched, item)
				if len(matched) >= limit {
					break
				}
			}
		}
		return map[string]any{"items": matched}, false, nil
	}
	return m.executeMutationCapability(ctx, invocation)
}

func (m *Manager) executeMutationCapability(ctx context.Context, invocation CapabilityInvocation) (any, bool, error) {
	actor := invocation.Actor
	switch invocation.Name {
	case "pause_account":
		var args temporaryAccountReasonArgs
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		if _, ok := findBinding(m.engine.Snapshot(), args.AccountID); !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		if invocation.DryRun {
			return map[string]any{"would_pause": args.AccountID}, false, nil
		}
		err := m.engine.AgentPause(ctx, args.AccountID, actor, args.Reason)
		return map[string]any{"account_id": args.AccountID, "schedulable": false}, retryableExternal(err), err
	case "resume_account":
		var args temporaryAccountReasonArgs
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		if _, ok := findBinding(m.engine.Snapshot(), args.AccountID); !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		if invocation.DryRun {
			return map[string]any{"would_resume": args.AccountID, "administrator_direct": administratorGrantedInvocation(invocation)}, false, nil
		}
		err := m.engine.AgentResume(ctx, args.AccountID, actor, args.Reason)
		return map[string]any{"account_id": args.AccountID, "schedulable": true}, retryableExternal(err), err
	case "manual_hold_account":
		var args accountReasonArgs
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		if _, ok := findBinding(m.engine.Snapshot(), args.AccountID); !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		if !administratorGrantedInvocation(invocation) {
			return nil, false, errors.New("永久人工保持需要管理员精确授权和一次性确认")
		}
		if invocation.DryRun {
			return map[string]any{"would_manual_hold": args.AccountID}, false, nil
		}
		command := accountcontrol.CommandContextFrom(ctx)
		result, err := m.engine.ManualPauseCommand(ctx, args.AccountID, "administrator", command.CommandID)
		return map[string]any{"account_id": args.AccountID, "schedulable": false, "manual_hold": true, "result": result}, retryableExternal(err), err
	case "set_load_factor":
		var args struct {
			AccountID  int64  `json:"account_id"`
			LoadFactor *int   `json:"load_factor"`
			Reason     string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 || (args.LoadFactor != nil && (*args.LoadFactor < 1 || *args.LoadFactor > 100)) {
			return nil, false, firstError(err, errors.New("负载系数参数无效"))
		}
		if _, ok := findBinding(m.engine.Snapshot(), args.AccountID); !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		if invocation.DryRun {
			return map[string]any{"would_set_load_factor": args.LoadFactor}, false, nil
		}
		err := m.engine.AgentSetLoadFactor(ctx, args.AccountID, args.LoadFactor, actor, args.Reason)
		return map[string]any{"account_id": args.AccountID, "load_factor": args.LoadFactor}, retryableExternal(err), err
	case "pin_load_until":
		var args struct {
			AccountID  int64     `json:"account_id"`
			LoadFactor int       `json:"load_factor"`
			Until      time.Time `json:"until"`
			Reason     string    `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 || args.LoadFactor < 1 || args.LoadFactor > 100 || !args.Until.After(time.Now().UTC()) {
			return nil, false, firstError(err, errors.New("负载固定参数无效或截止时间不在未来"))
		}
		if _, ok := findBinding(m.engine.Snapshot(), args.AccountID); !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		if invocation.DryRun {
			return map[string]any{"would_pin": args.AccountID, "load_factor": args.LoadFactor, "until": args.Until}, false, nil
		}
		command := accountcontrol.CommandContextFrom(ctx)
		command.ExpiresAt = cloneInvocationTime(&args.Until)
		command.OverrideKind = accountcontrol.OverrideKindLoadPin
		ctx = accountcontrol.WithCommandContext(ctx, command)
		err := m.engine.AgentSetLoadFactor(ctx, args.AccountID, &args.LoadFactor, actor, args.Reason)
		return map[string]any{"account_id": args.AccountID, "load_factor": args.LoadFactor, "until": args.Until}, retryableExternal(err), err
	case "clear_load_pin":
		var args accountReasonArgs
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		if invocation.DryRun {
			return map[string]any{"would_clear_load_pin": args.AccountID}, false, nil
		}
		if !administratorGrantedInvocation(invocation) {
			return nil, false, errors.New("解除负载固定需要管理员精确授权")
		}
		err := m.engine.AgentReleaseLoadPin(ctx, args.AccountID, actor, args.Reason)
		return map[string]any{"account_id": args.AccountID}, retryableExternal(err), err
	case "clear_flap_protection", "clear_manual_override":
		var args accountReasonArgs
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("账号编号无效"))
		}
		if invocation.DryRun {
			return map[string]any{"would_clear": invocation.Name, "account_id": args.AccountID}, false, nil
		}
		var err error
		if invocation.Name == "clear_flap_protection" {
			err = m.engine.ClearFlapProtection(ctx, args.AccountID, actor)
		} else {
			if !administratorGrantedInvocation(invocation) {
				return nil, false, errors.New("解除人工保持需要管理员精确授权")
			}
			err = m.engine.AgentReleaseManualHold(ctx, args.AccountID, actor, args.Reason)
		}
		return map[string]any{"account_id": args.AccountID}, retryableExternal(err), err
	case "update_binding":
		var args struct {
			AccountID    int64  `json:"account_id"`
			MonitorID    *int64 `json:"monitor_id"`
			ClearMonitor bool   `json:"clear_monitor"`
			Excluded     *bool  `json:"excluded"`
			Enabled      *bool  `json:"enabled"`
			Reason       string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.AccountID <= 0 {
			return nil, false, firstError(err, errors.New("绑定参数无效"))
		}
		binding, ok := findBinding(m.engine.Snapshot(), args.AccountID)
		if !ok {
			return nil, false, errors.New("账号不在当前调度快照中")
		}
		policy := binding.Policy
		if policy.AccountID == 0 {
			policy = model.Policy{AccountID: args.AccountID, Enabled: true}
		}
		if args.ClearMonitor {
			policy.MonitorID = nil
		} else if args.MonitorID != nil {
			policy.MonitorID = args.MonitorID
		}
		if args.Excluded != nil {
			policy.Excluded = *args.Excluded
		}
		if args.Enabled != nil {
			policy.Enabled = *args.Enabled
		}
		if invocation.DryRun {
			return map[string]any{"would_update_binding": policy}, false, nil
		}
		err := m.engine.UpdatePolicy(ctx, policy, actor)
		return policy, false, err
	case "update_upstream_control":
		var args struct {
			SourceID       int64    `json:"source_id"`
			Enabled        *bool    `json:"enabled"`
			PauseBelow     *float64 `json:"pause_below"`
			ResumeAt       *float64 `json:"resume_at"`
			RoutingEnabled *bool    `json:"routing_enabled"`
			RoutingPool    *string  `json:"routing_pool"`
			SelectedKeyID  *string  `json:"selected_key_id"`
			Reason         string   `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.SourceID <= 0 {
			return nil, false, firstError(err, errors.New("上游控制参数无效"))
		}
		current, err := m.balances.Get(ctx, args.SourceID)
		if err != nil {
			return nil, false, errors.New("上游不存在")
		}
		input := balance.SourceInput{Name: current.Name, Provider: current.Provider, BaseURL: current.BaseURL,
			PauseBelow: current.PauseBelow, ResumeAt: current.ResumeAt, Enabled: current.Enabled,
			SelectedKeyID: current.SelectedKeyID, RoutingEnabled: current.RoutingEnabled, RoutingPool: current.RoutingPool}
		if args.Enabled != nil {
			input.Enabled = *args.Enabled
		}
		if args.PauseBelow != nil {
			input.PauseBelow = *args.PauseBelow
		}
		if args.ResumeAt != nil {
			input.ResumeAt = *args.ResumeAt
		}
		if args.RoutingEnabled != nil {
			input.RoutingEnabled = *args.RoutingEnabled
		}
		if args.RoutingPool != nil {
			input.RoutingPool = strings.TrimSpace(*args.RoutingPool)
		}
		if args.SelectedKeyID != nil {
			input.SelectedKeyID = strings.TrimSpace(*args.SelectedKeyID)
		}
		if input.ResumeAt <= input.PauseBelow {
			return nil, false, errors.New("恢复阈值必须大于暂停阈值")
		}
		if invocation.DryRun {
			return map[string]any{"would_update_upstream": sanitizeUpstream(current), "new_control": input}, false, nil
		}
		updated, err := m.balances.Update(ctx, args.SourceID, input, actor)
		return sanitizeUpstream(updated), retryableExternal(err), err
	case "refresh_upstream":
		var args struct {
			SourceID int64  `json:"source_id"`
			Reason   string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.SourceID <= 0 {
			return nil, false, firstError(err, errors.New("上游编号无效"))
		}
		if _, err := m.balances.Get(ctx, args.SourceID); err != nil {
			return nil, false, errors.New("上游不存在")
		}
		if invocation.DryRun {
			return map[string]any{"would_refresh": args.SourceID}, false, nil
		}
		err := m.balances.RefreshManual(ctx, args.SourceID)
		return map[string]any{"source_id": args.SourceID}, retryableExternal(err), err
	case "transition_token_group_tier":
		var args struct {
			SourceID   int64   `json:"source_id"`
			KeyID      string  `json:"key_id"`
			TargetTier string  `json:"target_tier"`
			Confidence float64 `json:"confidence"`
			Reason     string  `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.SourceID <= 0 || strings.TrimSpace(args.KeyID) == "" {
			return nil, false, firstError(err, errors.New("令牌分组参数无效"))
		}
		if args.TargetTier != model.GroupTierMain && args.TargetTier != model.GroupTierBackup && args.TargetTier != model.GroupTierEmergency {
			return nil, false, errors.New("目标分组层级无效")
		}
		if !administratorGrantedInvocation(invocation) && args.Confidence < 0.90 {
			return nil, false, errors.New("自动令牌分组切换置信度必须达到0.90")
		}
		if !administratorGrantedInvocation(invocation) {
			if err := m.validateAutonomousGroupTransition(ctx, args.SourceID, args.KeyID, args.TargetTier); err != nil {
				return nil, false, err
			}
		}
		if invocation.DryRun {
			return map[string]any{"would_transition": args}, false, nil
		}
		expectedPool, expectedTier, evidenceCutoff := "", "", (*time.Time)(nil)
		if !administratorGrantedInvocation(invocation) {
			expectedPool, expectedTier, evidenceCutoff = m.autonomousGroupTransitionFence(ctx, args.SourceID, args.KeyID)
		}
		transition, err := m.balances.TransitionGroupTier(ctx, model.GroupTierTransitionRequest{SourceID: args.SourceID,
			KeyID: args.KeyID, TargetTier: args.TargetTier, IdempotencyKey: nonEmptyIdempotency(invocation), Actor: actor,
			Producer: groupCapabilityProducer(invocation), Authority: groupCapabilityAuthority(invocation), Reason: args.Reason,
			Evidence: strings.Join(invocation.EvidenceRefs, ","), SnapshotVersion: invocation.SnapshotVersion,
			Trigger: "agent_v2", Manual: administratorGrantedInvocation(invocation), PacketID: invocation.PacketID, PacketHash: invocation.PacketHash, RunID: invocation.RunID,
			GoalID: invocation.GoalID, StepID: invocation.StepID,
			ExpectedPool: expectedPool, ExpectedFromTier: expectedTier, EvidenceCutoffAt: evidenceCutoff, AutomationLeaseHeld: true})
		return transition, retryableExternal(err), err
	case "update_dispatch_policy", "propose_dispatch_policy":
		var args struct {
			ScopeType string          `json:"scope_type"`
			ScopeID   string          `json:"scope_id"`
			Config    json.RawMessage `json:"config"`
			Reason    string          `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		if invocation.DryRun {
			if _, _, err := decodeDispatchPolicyPatch(args.ScopeType, args.Config); err != nil {
				return nil, false, err
			}
			return map[string]any{"would_propose": args}, false, nil
		}
		version, err := m.ProposeDispatchPolicy(ctx, PolicyProposalInput{ScopeType: args.ScopeType, ScopeID: args.ScopeID,
			Patch: args.Config, Reason: args.Reason, Actor: actor, RunID: invocation.RunID, GoalID: invocation.GoalID,
			StepID: invocation.StepID, PacketID: invocation.PacketID, PacketHash: invocation.PacketHash,
			IdempotencyKey: nonEmptyIdempotency(invocation)})
		return version, false, err
	case "activate_policy_version":
		var args struct {
			PolicyID int64  `json:"policy_id"`
			Reason   string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.PolicyID <= 0 {
			return nil, false, firstError(err, errors.New("策略版本编号无效"))
		}
		version, err := m.store.GetPolicyLifecycle(ctx, args.PolicyID)
		if err != nil {
			return nil, false, errors.New("策略版本不存在")
		}
		if err := validateDispatchPolicyPatch(version.ScopeType, version.Config); err != nil {
			return nil, false, err
		}
		if invocation.DryRun {
			return map[string]any{"would_activate": version}, false, nil
		}
		err = m.ActivatePolicyProposal(ctx, args.PolicyID, actor, false)
		return version, retryableExternal(err), err
	case "trigger_reconcile":
		var args struct {
			Reason string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil {
			return nil, false, err
		}
		if !invocation.DryRun {
			m.engine.Trigger()
		}
		return map[string]any{"queued": !invocation.DryRun}, false, nil
	case "schedule_command":
		var args struct {
			Capability   string          `json:"capability"`
			Arguments    json.RawMessage `json:"arguments"`
			ExecuteAt    time.Time       `json:"execute_at"`
			Timezone     string          `json:"timezone"`
			ExpiresAt    *time.Time      `json:"expires_at"`
			MissedPolicy string          `json:"missed_policy"`
			Reason       string          `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.ExecuteAt.IsZero() {
			return nil, false, firstError(err, errors.New("定时命令参数无效"))
		}
		target, ok := capabilitySpec(args.Capability)
		if !ok || !target.Mutating || !target.ExecutionPolicy.SupportsScheduling || args.Capability == "schedule_command" || args.Capability == "cancel_scheduled_command" {
			return nil, false, errors.New("定时命令目标能力无效")
		}
		targetArguments, err := normalizedArguments(args.Arguments)
		if err != nil {
			return nil, false, fmt.Errorf("定时命令参数无效: %w", err)
		}
		args.Arguments = targetArguments
		if args.Timezone == "" {
			args.Timezone = model.AgentDefaultTimezone
		}
		if args.MissedPolicy == "" {
			args.MissedPolicy = "catch_up_once"
		}
		if args.MissedPolicy != "skip" && args.MissedPolicy != "catch_up_once" {
			return nil, false, errors.New("定时命令 missed_policy 只能是 skip 或 catch_up_once")
		}
		if args.MissedPolicy == "skip" && args.ExpiresAt == nil {
			return nil, false, errors.New("skip 定时命令必须设置 expires_at")
		}
		var targetGrant *AdministratorGrant
		if administratorGrantedInvocation(invocation) {
			targetGrant, err = scheduledTargetAdministratorGrant(invocation.AdministratorGrant, args.Capability, args.Arguments)
			if err != nil {
				return nil, false, err
			}
		}
		conditions := map[string]any{"reason": args.Reason}
		if invocation.SnapshotVersion != "" {
			conditions["snapshot_version"] = invocation.SnapshotVersion
		}
		if len(invocation.EvidenceRefs) > 0 {
			conditions["evidence_refs"] = append([]string(nil), invocation.EvidenceRefs...)
		}
		if targetGrant != nil {
			conditions["administrator_grant"] = targetGrant
		}
		resourceType, resourceIDs, operation, desiredState, err := scheduledCommandMetadata(args.Capability, args.Arguments)
		if err != nil {
			return nil, false, err
		}
		occurrenceID := scheduledOccurrenceID(nonEmptyIdempotency(invocation), args.Capability, args.Arguments, args.ExecuteAt.UTC(), args.Timezone)
		command := model.ScheduledCommand{GoalID: optionalPositiveInt64(invocation.GoalID), StepID: optionalPositiveInt64(invocation.StepID),
			Capability: args.Capability, Arguments: args.Arguments, Status: model.AgentCommandStatusPending, Timezone: args.Timezone,
			ExecuteAt: args.ExecuteAt.UTC(), ExpiresAt: args.ExpiresAt, IdempotencyKey: occurrenceID, OccurrenceID: occurrenceID,
			MissedPolicy: args.MissedPolicy, Authority: scheduledCommandAuthority(invocation), IntentType: "scheduled_action",
			ResourceType: resourceType, ResourceIDs: resourceIDs, Operation: operation, DesiredState: desiredState, MaxAttempts: 100,
			Conditions: marshalRaw(conditions), CreatedBy: actor}
		if invocation.DryRun {
			return map[string]any{"would_schedule": command}, false, nil
		}
		err = m.store.CreateScheduledCommand(ctx, &command)
		return command, false, err
	case "cancel_scheduled_command":
		var args struct {
			CommandID int64  `json:"command_id"`
			Reason    string `json:"reason"`
		}
		if err := decodeCapabilityArguments(invocation.Arguments, &args); err != nil || args.CommandID <= 0 {
			return nil, false, firstError(err, errors.New("定时命令编号无效"))
		}
		command, err := m.store.GetScheduledCommand(ctx, args.CommandID)
		if err != nil {
			return nil, false, errors.New("定时命令不存在")
		}
		if invocation.DryRun {
			return map[string]any{"would_cancel": command}, false, nil
		}
		err = m.store.CancelScheduledCommand(ctx, args.CommandID, actor, args.Reason)
		return command, false, err
	}
	return nil, false, errors.New("能力尚未实现")
}

type accountReasonArgs struct {
	AccountID int64  `json:"account_id"`
	Reason    string `json:"reason"`
}

type temporaryAccountReasonArgs struct {
	AccountID int64      `json:"account_id"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Reason    string     `json:"reason"`
}

func decodeCapabilityArguments(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("能力参数无效: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("能力参数包含多余 JSON 内容")
	}
	return nil
}

func normalizedArguments(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage("{}"), nil
	}
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("能力参数必须是 JSON 对象")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("能力参数包含多余 JSON 内容")
	}
	encoded, err := json.Marshal(object)
	return encoded, err
}

func administratorGrantedInvocation(invocation CapabilityInvocation) bool {
	return invocation.AdministratorGrant != nil &&
		validateAdministratorGrant(invocation.AdministratorGrant, invocation.Name, invocation.Arguments) == nil
}

func groupCapabilityProducer(invocation CapabilityInvocation) string {
	if administratorGrantedInvocation(invocation) {
		return "agent_operator"
	}
	return "autonomous_agent"
}

func groupCapabilityAuthority(invocation CapabilityInvocation) string {
	if administratorGrantedInvocation(invocation) {
		return "administrator_command"
	}
	return "autonomous_agent"
}

func scheduledCommandAuthority(invocation CapabilityInvocation) string {
	if administratorGrantedInvocation(invocation) {
		return "administrator_command"
	}
	return "autonomous_agent"
}

func scheduledOccurrenceID(baseKey, capability string, arguments json.RawMessage, executeAt time.Time, timezone string) string {
	hash := sha256.Sum256([]byte(strings.Join([]string{strings.TrimSpace(baseKey), capability, string(arguments), executeAt.UTC().Format(time.RFC3339Nano), strings.TrimSpace(timezone)}, "\x00")))
	return "occ-" + hex.EncodeToString(hash[:16])
}

func scheduledCommandMetadata(capability string, arguments json.RawMessage) (string, json.RawMessage, string, json.RawMessage, error) {
	var values map[string]any
	if err := json.Unmarshal(arguments, &values); err != nil {
		return "", nil, "", nil, errors.New("定时命令目标参数必须是对象")
	}
	delete(values, "reason")
	resourceType := "scheduler"
	resourceIDs := []any{}
	if accountID, ok := numericID(values["account_id"]); ok {
		resourceType, resourceIDs = "account", []any{accountID}
	} else if sourceID, ok := numericID(values["source_id"]); ok {
		resourceType = "upstream"
		resourceIDs = []any{sourceID}
		if keyID, exists := values["key_id"].(string); exists && strings.TrimSpace(keyID) != "" {
			resourceType = "upstream_key"
			resourceIDs = append(resourceIDs, strings.TrimSpace(keyID))
		}
	} else if policyID, ok := numericID(values["policy_id"]); ok {
		resourceType, resourceIDs = "policy_version", []any{policyID}
	}
	encodedResources, err := json.Marshal(resourceIDs)
	if err != nil {
		return "", nil, "", nil, err
	}
	desiredState, err := json.Marshal(values)
	if err != nil {
		return "", nil, "", nil, err
	}
	return resourceType, encodedResources, capability, desiredState, nil
}

func numericID(value any) (int64, bool) {
	switch number := value.(type) {
	case float64:
		if number > 0 && number == float64(int64(number)) {
			return int64(number), true
		}
	case json.Number:
		value, err := number.Int64()
		return value, err == nil && value > 0
	}
	return 0, false
}

func withAccountControlContext(ctx context.Context, invocation CapabilityInvocation) context.Context {
	createdAt := invocation.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	actionContext := accountcontrol.CommandContext{CommandID: strings.TrimSpace(invocation.IdempotencyKey),
		CreatedAt: createdAt, ExpiresAt: cloneInvocationTime(invocation.ExpiresAt),
		SnapshotVersion: strings.TrimSpace(invocation.SnapshotVersion), EvidenceRefs: append([]string(nil), invocation.EvidenceRefs...),
		RunID: invocation.RunID, GoalID: invocation.GoalID, StepID: invocation.StepID, AutomationLeaseHeld: true}
	if invocation.AdministratorGrant != nil {
		grantID := strings.TrimSpace(invocation.AdministratorGrant.GrantID)
		actionContext.CommandID = grantID
		actionContext.Administrator = true
		actionContext.GrantConsumptionID = grantID
	}
	return accountcontrol.WithCommandContext(ctx, actionContext)
}

func cloneInvocationTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func (m *Manager) capabilityState(ctx context.Context, invocation CapabilityInvocation) json.RawMessage {
	accountID := capabilityAccountID(invocation.Arguments)
	if accountID > 0 {
		snapshot := m.engine.Snapshot()
		if binding, ok := findBinding(snapshot, accountID); ok {
			return marshalRaw(map[string]any{"account_id": accountID, "schedulable": binding.Account.Schedulable,
				"load_factor": binding.Account.LoadFactor, "status": binding.Account.Status, "control": binding.Control,
				"policy": binding.Policy, "snapshot_synced_at": snapshot.LastSyncAt})
		}
	}
	var source struct {
		SourceID int64 `json:"source_id"`
	}
	// Reconciliation only needs the source identity here. The capability's full
	// argument validation happens before execution; using the strict decoder on
	// a projection would incorrectly reject valid fields such as key_id.
	if json.Unmarshal(invocation.Arguments, &source) == nil && source.SourceID > 0 {
		if item, err := m.balances.Get(ctx, source.SourceID); err == nil {
			return marshalRaw(sanitizeUpstream(item))
		}
	}
	if invocation.Name == "schedule_command" || invocation.Name == "cancel_scheduled_command" {
		items, _ := m.store.ListScheduledCommands(ctx, "", 0, 50)
		return marshalRaw(map[string]any{"scheduled_commands": items})
	}
	return json.RawMessage("{}")
}

func findBinding(snapshot model.Snapshot, accountID int64) (model.ResolvedBinding, bool) {
	for _, binding := range snapshot.Bindings {
		if binding.Account.ID == accountID {
			return binding, true
		}
	}
	return model.ResolvedBinding{}, false
}

func sanitizeUpstream(item model.UpstreamSource) map[string]any {
	keys := make([]map[string]any, 0, len(item.KeyRates))
	for _, key := range item.KeyRates {
		keys = append(keys, map[string]any{"key_id": key.ExternalID, "name": key.Name, "hint": key.KeyHint,
			"group_name": key.GroupName, "rate_multiplier": key.RateMultiplier, "dynamic": key.Dynamic})
	}
	policies := make([]map[string]any, 0, len(item.FailoverPolicies))
	for _, policy := range item.FailoverPolicies {
		policies = append(policies, map[string]any{"key_id": policy.KeyID, "key_name": policy.KeyName, "key_hint": policy.KeyHint,
			"enabled": policy.Enabled, "confirmed": policy.Confirmed, "pool": policy.Pool, "account_ids": policy.AccountIDs,
			"current_tier": policy.State.CurrentTier, "observed_group_id": policy.State.ObservedGroupID,
			"state_updated_at": policy.State.UpdatedAt, "frozen": policy.State.Frozen, "last_error": policy.State.LastError})
	}
	return map[string]any{"id": item.ID, "name": item.Name, "provider": item.Provider, "enabled": item.Enabled,
		"balance": item.Balance, "unit": item.Unit, "pause_below": item.PauseBelow, "resume_at": item.ResumeAt,
		"balance_locked": item.BalanceLocked, "stale": item.Stale, "last_success_at": item.LastSuccessAt,
		"last_error": item.LastError, "selected_key_id": item.SelectedKeyID, "routing_enabled": item.RoutingEnabled,
		"routing_pool": item.RoutingPool, "matched_accounts": item.MatchedAccounts, "keys": keys, "failover_policies": policies,
		"updated_at": item.UpdatedAt}
}

func capabilityAccountID(payload json.RawMessage) int64 {
	var value struct {
		AccountID int64 `json:"account_id"`
	}
	_ = json.Unmarshal(payload, &value)
	return value.AccountID
}

func marshalRaw(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"error":"结果无法序列化"}`)
	}
	return payload
}

func boundedLimit(value, fallback, maximum int) int {
	if value < 1 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func firstError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

func retryableExternal(err error) bool {
	if err == nil {
		return false
	}
	if reconcile.IsExternalMutationUncertain(err) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "temporary") || strings.Contains(message, "connection") ||
		strings.Contains(message, "超时") || strings.Contains(message, "连接") || strings.Contains(message, "temporarily")
}

func nonEmptyIdempotency(invocation CapabilityInvocation) string {
	if value := strings.TrimSpace(invocation.IdempotencyKey); value != "" {
		return value
	}
	hash := sha256.Sum256([]byte(strings.Join([]string{invocation.Name, strconv.FormatInt(invocation.GoalID, 10),
		strconv.FormatInt(invocation.StepID, 10), string(invocation.Arguments)}, "|")))
	return "agent-v2-" + hex.EncodeToString(hash[:16])
}

func optionalPositiveInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}
