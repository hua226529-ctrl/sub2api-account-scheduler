package agent

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

// CapabilitySpec is the complete surface visible to the model. There is no
// generic HTTP, SQL, filesystem or command capability by design.
type CapabilitySpec struct {
	Name                 string          `json:"name"`
	Version              int             `json:"-"`
	VersionLabel         string          `json:"version"`
	Title                string          `json:"title"`
	Description          string          `json:"description"`
	RiskLevel            string          `json:"risk_level"`
	Mutating             bool            `json:"mutating"`
	AdministratorDirect  bool            `json:"administrator_direct"`
	Parameters           map[string]any  `json:"input_schema"`
	Scopes               []string        `json:"scopes"`
	AutoExecutable       bool            `json:"auto_executable"`
	ApprovalRequired     bool            `json:"approval_required"`
	SupportsSchedule     bool            `json:"supports_schedule"`
	SupportsCompensation bool            `json:"supports_compensation"`
	ExecutionPolicy      ExecutionPolicy `json:"execution_policy"`
}

type ExecutionPolicy struct {
	Risk                  string `json:"risk"`
	ReadOnly              bool   `json:"read_only"`
	SupportsAutonomous    bool   `json:"supports_autonomous"`
	RequiresExactGrant    bool   `json:"requires_exact_grant"`
	RequiresConfirmation  bool   `json:"requires_confirmation"`
	RequiresFreshSnapshot bool   `json:"requires_fresh_snapshot"`
	RequiresEvidence      bool   `json:"requires_evidence"`
	SupportsScheduling    bool   `json:"supports_scheduling"`
	MaxScope              int    `json:"max_scope"`
	DefaultTTLSeconds     int64  `json:"default_ttl_seconds"`
	MaxTTLSeconds         int64  `json:"max_ttl_seconds"`
}

var capabilityRegistry = map[string]CapabilitySpec{}

func init() {
	registerCapability("get_system_snapshot", "读取脱敏的调度、余额、冻结与数据新鲜度总览", "read_only", false, false, objectSchema(nil, nil))
	registerCapability("get_account_evidence", "读取一个账号的预统计状态和脱敏证据", "read_only", false, false,
		objectSchema(map[string]any{"account_id": integerSchema(1), "limit": integerSchema(1)}, []string{"account_id"}))
	registerCapability("get_pool_evidence", "读取一个调度池的账号、流量与救灾状态", "read_only", false, false,
		objectSchema(map[string]any{"pool": stringSchema(), "limit": integerSchema(1)}, []string{"pool"}))
	registerCapability("get_upstream_state", "读取余额、倍率和已确认三级分组的脱敏状态", "read_only", false, false,
		objectSchema(map[string]any{"source_id": integerSchema(1)}, nil))
	registerCapability("get_audit_events", "读取最近的调度中心审计事件", "read_only", false, false,
		objectSchema(map[string]any{"limit": integerSchema(1)}, nil))
	registerCapability("get_policy_history", "读取全局、池或账号的调度策略版本", "read_only", false, false,
		objectSchema(map[string]any{"scope_type": enumSchema("global", "pool", "account"), "scope_id": stringSchema(), "limit": integerSchema(1)}, nil))
	registerCapability("get_action_outcomes", "读取历史动作预测与实际效果", "read_only", false, false,
		objectSchema(map[string]any{"run_id": integerSchema(1), "limit": integerSchema(1)}, nil))
	registerCapability("list_goals", "读取当前和历史目标及执行状态", "read_only", false, false,
		objectSchema(map[string]any{"status": stringSchema(), "limit": integerSchema(1)}, nil))
	registerCapability("list_scheduled_commands", "读取持久化定时命令", "read_only", false, false,
		objectSchema(map[string]any{"status": stringSchema(), "limit": integerSchema(1)}, nil))
	registerCapability("search_memory", "查询最近90天的智能体记忆", "read_only", false, false,
		objectSchema(map[string]any{"query": stringSchema(), "limit": integerSchema(1)}, []string{"query"}))

	registerCapability("pause_account", "临时暂停一个 Sub2API 账号并回读确认", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "expires_at": dateTimeSchema(), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("resume_account", "临时恢复一个 Sub2API 账号并回读确认", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "expires_at": dateTimeSchema(), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("manual_hold_account", "永久暂停一个账号，直到管理员显式解除", "critical", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("set_load_factor", "设置账号负载系数，空值表示恢复上游默认", "medium", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "load_factor": nullableIntegerSchema(1, 100), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("pin_load_until", "设置账号负载并固定到一个绝对时间", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "load_factor": integerRangeSchema(1, 100), "until": dateTimeSchema(), "reason": stringSchema()}, []string{"account_id", "load_factor", "until", "reason"}))
	registerCapability("clear_load_pin", "解除账号负载固定", "medium", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("clear_flap_protection", "解除账号本次抖动保护", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("clear_manual_override", "解除账号人工保护期", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("update_binding", "修改账号显式监控绑定、排除或规则启用状态", "high", true, true,
		objectSchema(map[string]any{"account_id": integerSchema(1), "monitor_id": nullableIntegerSchema(1, 0), "clear_monitor": boolSchema(), "excluded": boolSchema(), "enabled": boolSchema(), "reason": stringSchema()}, []string{"account_id", "reason"}))
	registerCapability("update_upstream_control", "修改已有上游的启用、余额阈值、路由池或受控令牌，不接触凭据", "high", true, true,
		objectSchema(map[string]any{"source_id": integerSchema(1), "enabled": boolSchema(), "pause_below": numberSchema(), "resume_at": numberSchema(), "routing_enabled": boolSchema(), "routing_pool": stringSchema(), "selected_key_id": stringSchema(), "reason": stringSchema()}, []string{"source_id", "reason"}))
	registerCapability("refresh_upstream", "立即刷新已有上游的余额、倍率和分组", "medium", true, true,
		objectSchema(map[string]any{"source_id": integerSchema(1), "reason": stringSchema()}, []string{"source_id", "reason"}))
	registerCapability("transition_token_group_tier", "在已确认策略内切换令牌主、备用或紧急分组", "critical", true, true,
		objectSchema(map[string]any{"source_id": integerSchema(1), "key_id": stringSchema(), "target_tier": enumSchema("main", "backup", "emergency"), "confidence": numberRangeSchema(0, 1), "reason": stringSchema()}, []string{"source_id", "key_id", "target_tier", "reason"}))
	registerCapability("propose_dispatch_policy", "创建、验证并模拟类型化调度策略提案，不立即激活", "medium", true, true,
		objectSchema(map[string]any{"scope_type": enumSchema("global", "pool", "account"), "scope_id": stringSchema(), "config": map[string]any{"type": "object"}, "reason": stringSchema()}, []string{"scope_type", "config", "reason"}))
	registerCapability("update_dispatch_policy", "兼容别名：创建策略提案，不立即激活", "medium", true, true,
		objectSchema(map[string]any{"scope_type": enumSchema("global", "pool", "account"), "scope_id": stringSchema(), "config": map[string]any{"type": "object"}, "reason": stringSchema()}, []string{"scope_type", "config", "reason"}))
	registerCapability("activate_policy_version", "激活一个已有调度策略版本", "critical", true, true,
		objectSchema(map[string]any{"policy_id": integerSchema(1), "reason": stringSchema()}, []string{"policy_id", "reason"}))
	registerCapability("trigger_reconcile", "立即触发一次确定性协调", "low", true, true,
		objectSchema(map[string]any{"reason": stringSchema()}, []string{"reason"}))
	registerCapability("schedule_command", "创建北京时间持久化定时业务命令", "high", true, true,
		objectSchema(map[string]any{"capability": stringSchema(), "arguments": map[string]any{"type": "object"}, "execute_at": dateTimeSchema(), "timezone": stringSchema(), "expires_at": map[string]any{"type": []string{"string", "null"}, "format": "date-time"}, "missed_policy": enumSchema("skip", "catch_up_once"), "reason": stringSchema()}, []string{"capability", "arguments", "execute_at", "missed_policy", "reason"}))
	registerCapability("cancel_scheduled_command", "取消尚未完成的定时命令", "medium", true, true,
		objectSchema(map[string]any{"command_id": integerSchema(1), "reason": stringSchema()}, []string{"command_id", "reason"}))
}

func registerCapability(name, description, _ string, mutating, administratorDirect bool, parameters map[string]any) {
	policy := executionPolicyFor(name, mutating, administratorDirect)
	scopes := []string{"scheduler"}
	if mutating {
		scopes = append(scopes, "write")
	} else {
		scopes = append(scopes, "read")
	}
	capabilityRegistry[name] = CapabilitySpec{Name: name, Version: 1, VersionLabel: "v1", Title: name,
		Description: description, RiskLevel: policy.Risk, Mutating: mutating, AdministratorDirect: administratorDirect,
		Parameters: parameters, Scopes: scopes, AutoExecutable: policy.ReadOnly || (policy.SupportsAutonomous && !policy.RequiresConfirmation),
		ApprovalRequired: policy.RequiresConfirmation, SupportsSchedule: policy.SupportsScheduling,
		SupportsCompensation: mutating, ExecutionPolicy: policy}
}

func executionPolicyFor(name string, mutating, administratorDirect bool) ExecutionPolicy {
	if !mutating {
		return ExecutionPolicy{Risk: "read_only", ReadOnly: true, SupportsAutonomous: true, MaxScope: 100}
	}
	policy := ExecutionPolicy{Risk: model.AgentRiskHigh, RequiresExactGrant: administratorDirect,
		RequiresFreshSnapshot: true, SupportsScheduling: true, MaxScope: 1}
	switch name {
	case "pause_account", "resume_account", "set_load_factor", "pin_load_until":
		policy.SupportsAutonomous, policy.RequiresEvidence = true, true
		policy.DefaultTTLSeconds, policy.MaxTTLSeconds = int64((15*time.Minute)/time.Second), int64((2*time.Hour)/time.Second)
	case "manual_hold_account", "clear_manual_override", "clear_load_pin":
		policy.Risk, policy.RequiresConfirmation = model.AgentRiskCritical, true
	case "propose_dispatch_policy", "update_dispatch_policy":
		policy.Risk, policy.SupportsAutonomous, policy.RequiresEvidence = model.AgentRiskMedium, true, true
		policy.RequiresExactGrant, policy.SupportsScheduling, policy.MaxScope = false, false, 100
	case "activate_policy_version":
		policy.Risk, policy.SupportsAutonomous, policy.RequiresConfirmation = model.AgentRiskCritical, false, true
		policy.SupportsScheduling = false
	case "transition_token_group_tier":
		policy.Risk, policy.SupportsAutonomous, policy.RequiresEvidence, policy.RequiresConfirmation = model.AgentRiskCritical, false, true, true
		policy.DefaultTTLSeconds, policy.MaxTTLSeconds = int64((15*time.Minute)/time.Second), int64((2*time.Hour)/time.Second)
	case "schedule_command":
		policy.Risk, policy.RequiresConfirmation, policy.SupportsScheduling = model.AgentRiskCritical, true, false
	case "trigger_reconcile":
		policy.Risk, policy.SupportsAutonomous, policy.RequiresFreshSnapshot = model.AgentRiskLow, true, false
	case "refresh_upstream":
		policy.Risk, policy.SupportsAutonomous = model.AgentRiskMedium, true
	}
	return policy
}

func CapabilitySpecs() []CapabilitySpec {
	items := make([]CapabilitySpec, 0, len(capabilityRegistry))
	for _, item := range capabilityRegistry {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items
}

func capabilitySpec(name string) (CapabilitySpec, bool) {
	item, ok := capabilityRegistry[name]
	return item, ok
}

func capabilityCatalogJSON() json.RawMessage {
	payload, _ := json.Marshal(CapabilitySpecs())
	return payload
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	result := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		result["required"] = required
	}
	return result
}

func stringSchema() map[string]any { return map[string]any{"type": "string"} }
func boolSchema() map[string]any   { return map[string]any{"type": "boolean"} }
func numberSchema() map[string]any { return map[string]any{"type": "number"} }
func numberRangeSchema(min, max float64) map[string]any {
	return map[string]any{"type": "number", "minimum": min, "maximum": max}
}
func dateTimeSchema() map[string]any { return map[string]any{"type": "string", "format": "date-time"} }
func integerSchema(min int) map[string]any {
	return map[string]any{"type": "integer", "minimum": min}
}
func integerRangeSchema(min, max int) map[string]any {
	return map[string]any{"type": "integer", "minimum": min, "maximum": max}
}
func nullableIntegerSchema(min, max int) map[string]any {
	result := map[string]any{"type": []string{"integer", "null"}, "minimum": min}
	if max > 0 {
		result["maximum"] = max
	}
	return result
}
func enumSchema(values ...string) map[string]any {
	return map[string]any{"type": "string", "enum": values}
}
