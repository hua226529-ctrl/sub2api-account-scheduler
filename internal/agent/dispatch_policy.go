package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var globalDispatchPolicyFields = map[string]struct{}{
	"failure_threshold": {}, "recovery_threshold": {}, "manual_hold_minutes": {},
	"flap_window_minutes": {}, "flap_pause_threshold": {}, "flap_recovery_threshold": {},
	"health_engine_mode": {}, "healthy_score_threshold": {}, "watch_score_threshold": {},
	"quarantine_score_threshold": {}, "minimum_samples": {}, "latency_warning_ms": {},
	"latency_critical_ms": {}, "traffic_pause_below": {}, "traffic_healthy_at": {},
	"hard_failures_10_threshold": {}, "persistent_slow_rate": {}, "quarantine_minutes": {},
	"recovery_window_size": {}, "recovery_required_successes": {}, "recovery_initial_percent": {},
	"recovery_mid_percent": {}, "degraded_load_percent": {}, "recovery_stage_minutes": {},
	"load_manual_hold_minutes":             {},
	"group_failover_account_fresh_minutes": {}, "group_failover_telemetry_fresh_minutes": {},
	"group_failover_data_fresh_minutes": {}, "group_failover_agent_grace_seconds": {},
	"group_failover_monitor_failures": {}, "group_failover_no_traffic_failures": {},
	"group_failover_traffic_window_minutes": {}, "group_failover_traffic_min_samples": {},
	"group_failover_traffic_success_below": {}, "group_failover_consecutive_hard_errors": {},
	"group_failover_backup_verify_minutes": {}, "group_failover_post_switch_monitors": {},
	"group_failover_post_switch_requests": {}, "group_failover_main_verify_minutes": {},
	"group_failover_switch_cooldown_minutes": {}, "group_failover_manual_protection_minutes": {},
	"group_failover_short_limit_window_minutes": {}, "group_failover_short_limit_count": {},
	"group_failover_long_limit_window_minutes": {}, "group_failover_long_limit_count": {},
	"group_failover_recovery_window_minutes": {}, "group_failover_recovery_stable_minutes": {},
	"group_failover_recovery_monitor_successes": {}, "group_failover_recovery_min_samples": {},
	"group_failover_recovery_success_at": {}, "group_failover_return_retry_minutes": {},
}

var scopedDispatchPolicyFields = map[string]struct{}{
	"failure_threshold": {}, "recovery_threshold": {}, "flap_enabled": {},
	"flap_window_minutes": {}, "flap_pause_threshold": {}, "flap_recovery_threshold": {},
	"healthy_score_threshold": {}, "watch_score_threshold": {}, "quarantine_score_threshold": {},
	"minimum_samples": {}, "latency_warning_ms": {}, "latency_critical_ms": {},
	"traffic_pause_below": {}, "traffic_healthy_at": {}, "hard_failures_10_threshold": {},
	"persistent_slow_rate": {},
}

// validateDispatchPolicyPatch keeps the model inside the scheduling-policy
// surface. Binding, enablement, dry-run and identity fields have dedicated
// capabilities and cannot be smuggled through a policy version.
func validateDispatchPolicyPatch(scope string, patch json.RawMessage) error {
	if len(patch) == 0 || !json.Valid(patch) {
		return errors.New("策略配置不是合法 JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(patch, &fields); err != nil {
		return errors.New("策略配置必须是 JSON 对象")
	}
	if len(fields) == 0 {
		return errors.New("策略配置不能为空")
	}
	allowed := scopedDispatchPolicyFields
	if scope == "global" {
		allowed = globalDispatchPolicyFields
	} else if scope != "pool" && scope != "account" {
		return errors.New("策略作用域必须是 global、pool 或 account")
	}
	for field := range fields {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("字段 %s 不属于可修改的调度策略", strings.TrimSpace(field))
		}
	}
	return nil
}
