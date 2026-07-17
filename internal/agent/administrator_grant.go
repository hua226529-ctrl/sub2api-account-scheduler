package agent

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const administratorGrantVersion = 1

// AdministratorIntent is a conservative projection of one administrator
// message. It is persisted with the goal, but is not itself an execution
// credential. The runtime may mint a one-call grant only when a proposed
// capability and its resource keys match one of these clauses.
type AdministratorIntent struct {
	Version      int                        `json:"version"`
	CommandHash  string                     `json:"command_hash"`
	GrantScopeID string                     `json:"grant_scope_id,omitempty"`
	Explicit     bool                       `json:"explicit"`
	Grants       []AdministratorIntentGrant `json:"grants,omitempty"`
	Issues       []string                   `json:"issues,omitempty"`
}

type AdministratorIntentGrant struct {
	Capability    string     `json:"capability"`
	Clause        string     `json:"clause"` // immediate or scheduled
	ResourceKeys  []string   `json:"resource_keys,omitempty"`
	LoadFactor    *int       `json:"load_factor,omitempty"`
	TargetTier    string     `json:"target_tier,omitempty"`
	Enabled       *bool      `json:"enabled,omitempty"`
	ExecuteAt     *time.Time `json:"execute_at,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	PermanentHold bool       `json:"permanent_hold,omitempty"`
}

// AdministratorGrant is the typed, single-invocation privilege envelope.
// ArgumentsHash and ResourceKeys bind it to the exact normalized call. For a
// schedule_command, Target* additionally binds the durable command which may
// receive administrator-direct semantics when it becomes due.
type AdministratorGrant struct {
	Version             int      `json:"version"`
	GrantID             string   `json:"grant_id"`
	GrantScopeID        string   `json:"grant_scope_id"`
	CommandHash         string   `json:"command_hash"`
	Clause              string   `json:"clause"`
	Capability          string   `json:"capability"`
	ArgumentsHash       string   `json:"arguments_hash"`
	ResourceKeys        []string `json:"resource_keys,omitempty"`
	TargetCapability    string   `json:"target_capability,omitempty"`
	TargetArgumentsHash string   `json:"target_arguments_hash,omitempty"`
	TargetResourceKeys  []string `json:"target_resource_keys,omitempty"`
}

type administratorScheduleArguments struct {
	Capability string          `json:"capability"`
	Arguments  json.RawMessage `json:"arguments"`
	ExecuteAt  time.Time       `json:"execute_at"`
	Timezone   string          `json:"timezone"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Reason     string          `json:"reason"`
}

var (
	administratorClauseSeparators = []string{"，", ",", "；", ";", "。", "\n", "然后", "随后", "之后", "同时"}
	administratorActionJoins      = []string{"并恢复", "并暂停", "并开启", "并关闭", "并切换", "并刷新", "并解除", "并回退"}
	administratorLoadFactorRE     = regexp.MustCompile(`(?:负载(?:系数)?\s*)?(?:设为|设置为|调到|调整为)\s*(\d{1,3})`)
	administratorClockRE          = regexp.MustCompile(`(?:(明天|今天|今晚|早上|上午|中午|下午|晚上|凌晨)\s*)?([一二两三四五六七八九十\d]{1,3})\s*(?:点|:)(?:([一二两三四五六七八九十\d]{1,3})\s*分?)?`)
	administratorNegatedActionRE  = regexp.MustCompile(`(?i)(?:先\s*别|暂(?:时)?\s*不|请勿|别|不(?:要|再|得|许|能|可|应(?:该)?|准|允许|必|需要)?|禁止|不得|无需|无须|never|do\s+not|don't|dont)[^，,；;。\n]{0,40}?(?:暂停|恢复|设置|设为|固定|切换|开启|启用|关闭|禁用|解除|回退|执行|pause|resume|set|switch|enable|disable)`)
	administratorConditionalRE    = regexp.MustCompile(`(?:如果|假如|假设|若(?:是)?|倘若|一旦|除非|只要|仅当|只在|前提是|视情况|看情况|酌情|必要时|合适时|有需要时|条件允许|当[^，,；;。\n]{0,40}时|等(?:到)?[^，,；;。\n]{0,40}(?:后|再)|确认[^，,；;。\n]{0,40}后|(?:正常|异常|可用|不可用|失败|成功|满足|达到)[^，,；;。\n]{0,30}(?:时|后)[^，,；;。\n]{0,30}(?:暂停|恢复|切换|设置|开启|关闭))`)
	administratorAmbiguousRE      = regexp.MustCompile(`(?:或者|或是|还是|任意|任选|随便|其中一个|可能|也许|或许|大概|尽量|尝试|考虑|建议|你决定|自行判断)`)
)

func (m *Manager) parseAdministratorIntent(ctx context.Context, message string) AdministratorIntent {
	intent := AdministratorIntent{Version: administratorGrantVersion, CommandHash: administratorCommandHash(message)}
	if !explicitAdministratorCommand(message) {
		return intent
	}
	intent.Explicit = true
	if reason := unsafeAdministratorCommandReason(message); reason != "" {
		intent.Issues = append(intent.Issues, reason)
		return intent
	}
	grantScopeID, err := newAdministratorGrantScopeID()
	if err != nil {
		intent.Issues = append(intent.Issues, "无法生成管理员授权批次，命令已失败关闭")
		return intent
	}
	intent.GrantScopeID = grantScopeID
	now := time.Now().UTC()
	clauses := splitAdministratorClauses(message)
	globalTime, _ := parseAdministratorClock(message, now)
	for _, clause := range clauses {
		capabilities := administratorClauseCapabilities(clause.Text)
		if len(capabilities) == 0 {
			continue
		}
		for _, capability := range capabilities {
			resources, err := m.resolveAdministratorResources(ctx, capability, clause.Text)
			if err != nil {
				intent.Issues = append(intent.Issues, fmt.Sprintf("%s：%v", truncateRunes(clause.Text, 80), err))
				continue
			}
			if capability == "transition_token_group_tier" {
				resources, err = m.bindAdministratorTokenResource(ctx, resources, clause.Text)
				if err != nil {
					intent.Issues = append(intent.Issues, fmt.Sprintf("%s：%v", truncateRunes(clause.Text, 80), err))
					continue
				}
			}
			grant := AdministratorIntentGrant{Capability: capability, Clause: "immediate", ResourceKeys: resources}
			if capability == "pin_load_until" || capability == "set_load_factor" {
				grant.LoadFactor = administratorLoadFactor(clause.Text)
				if grant.LoadFactor == nil {
					intent.Issues = append(intent.Issues, fmt.Sprintf("%s：没有明确负载值", truncateRunes(clause.Text, 80)))
					continue
				}
			}
			if capability == "transition_token_group_tier" {
				grant.TargetTier = administratorTargetTier(clause.Text)
				if grant.TargetTier == "" {
					intent.Issues = append(intent.Issues, fmt.Sprintf("%s：没有明确主、备用或紧急层级", truncateRunes(clause.Text, 80)))
					continue
				}
			}
			if capability == "update_upstream_control" {
				grant.Enabled = administratorEnabledConstraint(clause.Text)
			}
			if capability == "pin_load_until" {
				until, ok := parseAdministratorClock(clause.Text, now)
				if !ok {
					until, ok = globalTime, !globalTime.IsZero()
				}
				if !ok {
					intent.Issues = append(intent.Issues, fmt.Sprintf("%s：没有可解析的固定截止时间", truncateRunes(clause.Text, 80)))
					continue
				}
				grant.ExecuteAt = &until
			} else if clause.Scheduled {
				grant.Clause = "scheduled"
				executeAt, ok := parseAdministratorClock(clause.Text, now)
				if !ok {
					executeAt, ok = globalTime, !globalTime.IsZero()
				}
				if !ok {
					intent.Issues = append(intent.Issues, fmt.Sprintf("%s：定时子句没有可解析的执行时间", truncateRunes(clause.Text, 80)))
					continue
				}
				grant.ExecuteAt = &executeAt
			}
			intent.Grants = append(intent.Grants, grant)
		}
	}
	if len(intent.Grants) == 0 && len(intent.Issues) == 0 {
		intent.Issues = append(intent.Issues, "没有识别出可精确授权的业务动作")
	}
	return intent
}

// unsafeAdministratorCommandReason rejects command-shaped text whose intended
// effect is not an unambiguous positive action. A rejected message may still be
// discussed by the model, but it can never mint an administrator grant.
func unsafeAdministratorCommandReason(message string) string {
	message = strings.TrimSpace(message)
	if administratorNegatedActionRE.MatchString(message) {
		return "命令包含否定或禁止语义，未生成管理员执行授权"
	}
	if administratorConditionalRE.MatchString(message) {
		return "命令包含条件语义，需改为目标、动作和时间均明确的直接命令"
	}
	if administratorAmbiguousRE.MatchString(message) {
		return "命令包含含糊或选择语义，需改为唯一明确的直接命令"
	}
	return ""
}

func (m *Manager) bindAdministratorTokenResource(ctx context.Context, resources []string, clause string) ([]string, error) {
	if m.balances == nil {
		return nil, errors.New("上游令牌快照不可用")
	}
	var sourceID int64
	for _, resource := range resources {
		if !strings.HasPrefix(resource, "source:") {
			continue
		}
		parsed, err := strconv.ParseInt(strings.TrimPrefix(resource, "source:"), 10, 64)
		if err == nil {
			sourceID = parsed
		}
	}
	if sourceID <= 0 {
		return nil, errors.New("切组授权缺少明确上游")
	}
	source, err := m.balances.Get(ctx, sourceID)
	if err != nil {
		return nil, errors.New("切组授权的上游不存在")
	}
	candidates := make([]model.GroupFailoverPolicy, 0)
	for _, policy := range source.FailoverPolicies {
		if policy.Enabled && policy.Confirmed && strings.TrimSpace(policy.KeyID) != "" {
			candidates = append(candidates, policy)
		}
	}
	if len(candidates) == 0 {
		return nil, errors.New("上游没有已确认的受控令牌")
	}
	selected := make([]model.GroupFailoverPolicy, 0, 1)
	for _, policy := range candidates {
		for _, identity := range []string{policy.KeyID, policy.KeyName, policy.KeyHint} {
			identity = strings.TrimSpace(identity)
			if identity != "" && strings.Contains(clause, identity) {
				selected = append(selected, policy)
				break
			}
		}
	}
	if len(selected) == 0 && len(candidates) == 1 {
		selected = candidates
	}
	if len(selected) != 1 {
		return nil, errors.New("上游存在多个受控令牌，必须使用唯一精确的令牌名称或编号")
	}
	return normalizeResourceKeys(append(resources, "key:"+strings.TrimSpace(selected[0].KeyID))), nil
}

type administratorClause struct {
	Text      string
	Scheduled bool
}

func splitAdministratorClauses(message string) []administratorClause {
	normalized := strings.TrimSpace(message)
	for _, join := range administratorActionJoins {
		normalized = strings.ReplaceAll(normalized, join, "|"+strings.TrimPrefix(join, "并"))
	}
	for _, separator := range administratorClauseSeparators {
		normalized = strings.ReplaceAll(normalized, separator, "|")
	}
	parts := strings.Split(normalized, "|")
	result := make([]administratorClause, 0, len(parts))
	globalScheduled := strings.Contains(message, "定时")
	anchor := strings.Index(message, "保持到")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		index := strings.Index(message, part)
		scheduled := globalScheduled || (index >= 0 && anchor >= 0 && index > anchor && !strings.Contains(part, "保持到"))
		if !scheduled && !strings.Contains(part, "保持到") {
			_, scheduled = parseAdministratorClock(part, time.Now().UTC())
		}
		result = append(result, administratorClause{Text: part, Scheduled: scheduled})
	}
	return result
}

func administratorClauseCapabilities(clause string) []string {
	text := strings.ToLower(clause)
	capabilities := make([]string, 0, 2)
	add := func(value string) {
		for _, existing := range capabilities {
			if existing == value {
				return
			}
		}
		capabilities = append(capabilities, value)
	}
	if (strings.Contains(text, "负载") || strings.Contains(text, "设为") || strings.Contains(text, "调到")) &&
		(strings.Contains(text, "保持到") || strings.Contains(text, "固定到") || strings.Contains(text, "固定")) {
		add("pin_load_until")
	} else if strings.Contains(text, "负载") || strings.Contains(text, "设为") || strings.Contains(text, "调到") {
		add("set_load_factor")
	}
	if strings.Contains(text, "切换") && strings.Contains(text, "分组") {
		add("transition_token_group_tier")
	}
	if strings.Contains(text, "上游") {
		switch {
		case strings.Contains(text, "刷新"):
			add("refresh_upstream")
		case strings.Contains(text, "开启") || strings.Contains(text, "启用") || strings.Contains(text, "关闭") || strings.Contains(text, "禁用"):
			add("update_upstream_control")
		}
	} else {
		if strings.Contains(text, "恢复") || strings.Contains(text, "开启调度") || strings.Contains(text, "resume") {
			add("resume_account")
		}
		if strings.Contains(text, "暂停") || strings.Contains(text, "关闭调度") || strings.Contains(text, "pause") {
			add("pause_account")
		}
	}
	if strings.Contains(text, "解除") && strings.Contains(text, "抖动") {
		add("clear_flap_protection")
	}
	if strings.Contains(text, "解除") && (strings.Contains(text, "人工保护") || strings.Contains(text, "人工覆盖")) {
		add("clear_manual_override")
	}
	if strings.Contains(text, "解除") && strings.Contains(text, "负载固定") {
		add("clear_load_pin")
	}
	if strings.Contains(text, "重新匹配") || strings.Contains(text, "立即协调") || strings.Contains(text, "执行协调") {
		add("trigger_reconcile")
	}
	if strings.Contains(text, "回退") && strings.Contains(text, "策略") || strings.Contains(text, "激活策略") {
		add("activate_policy_version")
	}
	if strings.Contains(text, "取消") && strings.Contains(text, "定时") {
		add("cancel_scheduled_command")
	}
	return capabilities
}

func administratorLoadFactor(text string) *int {
	match := administratorLoadFactorRE.FindStringSubmatch(text)
	if len(match) != 2 {
		return nil
	}
	value, err := strconv.Atoi(match[1])
	if err != nil || value < 1 || value > 100 {
		return nil
	}
	return &value
}

func administratorTargetTier(text string) string {
	switch {
	case strings.Contains(text, "紧急"):
		return model.GroupTierEmergency
	case strings.Contains(text, "备用"):
		return model.GroupTierBackup
	case strings.Contains(text, "主分组") || strings.Contains(text, "主组"):
		return model.GroupTierMain
	default:
		return ""
	}
}

func administratorEnabledConstraint(text string) *bool {
	if strings.Contains(text, "关闭") || strings.Contains(text, "禁用") {
		value := false
		return &value
	}
	if strings.Contains(text, "开启") || strings.Contains(text, "启用") {
		value := true
		return &value
	}
	return nil
}

func parseAdministratorClock(text string, now time.Time) (time.Time, bool) {
	match := administratorClockRE.FindStringSubmatch(text)
	if len(match) != 4 {
		return time.Time{}, false
	}
	hour, hourOK := parseAdministratorClockNumber(match[2])
	minute := 0
	minuteOK := true
	if match[3] != "" {
		minute, minuteOK = parseAdministratorClockNumber(match[3])
	}
	if !hourOK || !minuteOK || hour > 23 || minute > 59 {
		return time.Time{}, false
	}
	period := match[1]
	if (period == "下午" || period == "晚上") && hour < 12 {
		hour += 12
	}
	if period == "中午" && hour < 11 {
		hour += 12
	}
	shanghai := time.FixedZone(model.AgentDefaultTimezone, 8*60*60)
	localNow := now.In(shanghai)
	day := localNow
	if period == "明天" {
		day = day.AddDate(0, 0, 1)
	}
	result := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, shanghai)
	if period != "明天" && !result.After(localNow) {
		result = result.AddDate(0, 0, 1)
	}
	return result.UTC(), true
}

func parseAdministratorClockNumber(value string) (int, bool) {
	value = strings.ReplaceAll(value, "两", "二")
	return parseChatInteger(value)
}

func (m *Manager) resolveAdministratorResources(ctx context.Context, capability, clause string) ([]string, error) {
	kind := administratorResourceKind(capability)
	if kind == "" {
		return nil, nil
	}
	if kind == "global" {
		return []string{"global:scheduler"}, nil
	}
	explicitIDs := prefixedAdministratorIDs(clause, kind)
	candidates, err := m.administratorNamedResources(ctx, kind)
	if err != nil {
		return nil, err
	}
	matched := make(map[string]bool)
	for _, id := range explicitIDs {
		key := kind + ":" + id
		if _, exists := candidates[key]; !exists {
			return nil, fmt.Errorf("%s不存在", key)
		}
		matched[key] = true
	}
	for key, name := range candidates {
		if name != "" && strings.Contains(clause, name) {
			matched[key] = true
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("没有找到带%s前缀的编号或唯一精确名称", administratorKindLabel(kind))
	}
	if len(matched) != 1 {
		return nil, fmt.Errorf("目标不明确或命中多个%s", administratorKindLabel(kind))
	}
	result := make([]string, 0, 1)
	for key := range matched {
		result = append(result, key)
	}
	return result, nil
}

func administratorResourceKind(capability string) string {
	switch capability {
	case "pause_account", "resume_account", "manual_hold_account", "set_load_factor", "pin_load_until", "clear_load_pin",
		"clear_flap_protection", "clear_manual_override", "update_binding":
		return "account"
	case "update_upstream_control", "refresh_upstream", "transition_token_group_tier":
		return "source"
	case "activate_policy_version":
		return "policy"
	case "cancel_scheduled_command":
		return "command"
	case "trigger_reconcile":
		return "global"
	default:
		return ""
	}
}

func administratorKindLabel(kind string) string {
	switch kind {
	case "account":
		return "账号"
	case "source":
		return "上游"
	case "policy":
		return "策略"
	case "command":
		return "定时命令"
	default:
		return kind
	}
}

func prefixedAdministratorIDs(text, kind string) []string {
	labels := map[string]string{
		"account": `(?:账号|账户)`, "source": `上游`, "policy": `(?:策略|策略版本)`, "command": `(?:定时命令|命令)`,
	}
	label := labels[kind]
	if label == "" {
		return nil
	}
	re := regexp.MustCompile(label + `\s*(?:(?:ID|id|编号)\s*)?(?:#|：|:)?\s*(\d+)`)
	matches := re.FindAllStringSubmatch(text, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 2 {
			result = append(result, match[1])
		}
	}
	return result
}

func (m *Manager) administratorNamedResources(ctx context.Context, kind string) (map[string]string, error) {
	result := make(map[string]string)
	switch kind {
	case "account":
		if m.engine == nil {
			return nil, errors.New("账号快照不可用")
		}
		for _, binding := range m.engine.Snapshot().Bindings {
			key := fmt.Sprintf("account:%d", binding.Account.ID)
			if existing, duplicate := result[key]; !duplicate || existing == "" {
				result[key] = strings.TrimSpace(binding.Account.Name)
			}
		}
	case "source":
		if m.balances == nil {
			return nil, errors.New("上游快照不可用")
		}
		items, err := m.balances.List(ctx)
		if err != nil {
			return nil, fmt.Errorf("读取上游列表失败: %w", err)
		}
		for _, item := range items {
			result[fmt.Sprintf("source:%d", item.ID)] = strings.TrimSpace(item.Name)
		}
	case "policy":
		if m.store == nil {
			return nil, errors.New("策略存储不可用")
		}
		items, err := m.store.ListPolicyLifecycle(ctx, 500)
		if err != nil {
			return nil, fmt.Errorf("读取策略版本失败: %w", err)
		}
		for _, item := range items {
			result[fmt.Sprintf("policy:%d", item.ID)] = ""
		}
	case "command":
		if m.store == nil {
			return nil, errors.New("定时任务存储不可用")
		}
		items, err := m.store.ListScheduledCommands(ctx, "", 0, 500)
		if err != nil {
			return nil, fmt.Errorf("读取定时命令失败: %w", err)
		}
		for _, item := range items {
			result[fmt.Sprintf("command:%d", item.ID)] = ""
		}
	}
	return result, nil
}

func (m *Manager) administratorGrantForInvocation(intent AdministratorIntent, capability string,
	arguments json.RawMessage) (*AdministratorGrant, error) {
	if !intent.Explicit || intent.Version != administratorGrantVersion || !validAdministratorCommandHash(intent.CommandHash) {
		return nil, nil
	}
	if len(intent.Issues) > 0 || len(intent.Grants) == 0 {
		return nil, nil
	}
	if !validAdministratorGrantScopeID(intent.GrantScopeID) {
		return nil, errors.New("管理员授权批次缺失或无效，请重新确认命令")
	}
	normalized, err := normalizedArguments(arguments)
	if err != nil {
		return nil, err
	}
	if capability == "schedule_command" {
		var schedule administratorScheduleArguments
		if err := decodeCapabilityArguments(normalized, &schedule); err != nil || schedule.ExecuteAt.IsZero() {
			return nil, firstError(err, errors.New("定时命令参数无效"))
		}
		targetArguments, err := normalizedArguments(schedule.Arguments)
		if err != nil {
			return nil, err
		}
		targetResources, err := capabilityResourceKeys(schedule.Capability, targetArguments)
		if err != nil {
			return nil, err
		}
		clause, ok := matchingAdministratorIntentGrant(intent, schedule.Capability, "scheduled", targetResources, targetArguments, schedule.ExecuteAt)
		if !ok {
			return nil, nil
		}
		return mintAdministratorGrant(intent.GrantScopeID, intent.CommandHash, clause.Clause, capability, normalized, targetResources,
			schedule.Capability, targetArguments, targetResources), nil
	}
	resources, err := capabilityResourceKeys(capability, normalized)
	if err != nil {
		return nil, err
	}
	clause, ok := matchingAdministratorIntentGrant(intent, capability, "immediate", resources, normalized, time.Time{})
	if !ok {
		return nil, nil
	}
	return mintAdministratorGrant(intent.GrantScopeID, intent.CommandHash, clause.Clause, capability, normalized, resources, "", nil, nil), nil
}

func matchingAdministratorIntentGrant(intent AdministratorIntent, capability, clause string, resources []string,
	arguments json.RawMessage, executeAt time.Time) (AdministratorIntentGrant, bool) {
	for _, candidate := range intent.Grants {
		if candidate.Capability != capability || candidate.Clause != clause ||
			!administratorResourcesMatch(candidate.ResourceKeys, resources) ||
			!administratorConstraintsMatch(candidate, arguments, executeAt) {
			continue
		}
		return candidate, true
	}
	return AdministratorIntentGrant{}, false
}

func administratorResourcesMatch(expected, actual []string) bool {
	if len(expected) == 0 {
		return len(actual) == 0
	}
	actualSet := make(map[string]bool, len(actual))
	for _, key := range actual {
		actualSet[key] = true
	}
	for _, key := range expected {
		if !actualSet[key] {
			return false
		}
	}
	// A grant resolved to one account/source may include a subordinate key,
	// but it may never switch to another resource of the same primary kind.
	for _, key := range actual {
		kind := strings.SplitN(key, ":", 2)[0]
		for _, wanted := range expected {
			if strings.HasPrefix(wanted, kind+":") && key != wanted && kind != "key" {
				return false
			}
		}
	}
	return true
}

func administratorConstraintsMatch(grant AdministratorIntentGrant, arguments json.RawMessage, executeAt time.Time) bool {
	var values map[string]any
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.UseNumber()
	if decoder.Decode(&values) != nil {
		return false
	}
	if grant.LoadFactor != nil {
		value, ok := integerMapValue(values, "load_factor")
		if !ok || value != *grant.LoadFactor {
			return false
		}
	}
	if grant.TargetTier != "" {
		if value, _ := values["target_tier"].(string); value != grant.TargetTier {
			return false
		}
	}
	if grant.Enabled != nil {
		if value, ok := values["enabled"].(bool); !ok || value != *grant.Enabled {
			return false
		}
		if grant.Capability == "update_upstream_control" {
			// A plain "enable/disable upstream" command authorizes only that
			// switch. Optional control fields must not be smuggled into the same
			// exact call by the model.
			for _, field := range []string{"pause_below", "resume_at", "routing_enabled", "routing_pool", "selected_key_id"} {
				if value, exists := values[field]; exists && value != nil {
					return false
				}
			}
		}
	}
	if grant.ExecuteAt != nil {
		actual := executeAt
		if actual.IsZero() {
			if raw, ok := values["until"].(string); ok {
				actual, _ = time.Parse(time.RFC3339, raw)
			}
		}
		if actual.IsZero() || actual.UTC().Truncate(time.Minute) != grant.ExecuteAt.UTC().Truncate(time.Minute) {
			return false
		}
	}
	if grant.ExpiresAt != nil {
		raw, ok := values["expires_at"].(string)
		actual, err := time.Parse(time.RFC3339, raw)
		if !ok || err != nil || actual.UTC().Truncate(time.Minute) != grant.ExpiresAt.UTC().Truncate(time.Minute) {
			return false
		}
	}
	return true
}

func integerMapValue(values map[string]any, key string) (int, bool) {
	value, ok := values[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		return parsed, err == nil
	case float64:
		return int(typed), typed == float64(int(typed))
	default:
		return 0, false
	}
}

func mintAdministratorGrant(grantScopeID, commandHash, clause, capability string, arguments json.RawMessage, resources []string,
	targetCapability string, targetArguments json.RawMessage, targetResources []string) *AdministratorGrant {
	grant := &AdministratorGrant{Version: administratorGrantVersion, GrantScopeID: grantScopeID, CommandHash: commandHash, Clause: clause,
		Capability: capability, ArgumentsHash: administratorArgumentsHash(arguments), ResourceKeys: normalizeResourceKeys(resources),
		TargetCapability: targetCapability, TargetArgumentsHash: administratorArgumentsHash(targetArguments),
		TargetResourceKeys: normalizeResourceKeys(targetResources)}
	grant.GrantID = deriveAdministratorGrantID(grant)
	return grant
}

func validateAdministratorGrant(grant *AdministratorGrant, capability string, arguments json.RawMessage) error {
	if grant == nil {
		return nil
	}
	if grant.Version != administratorGrantVersion || !validAdministratorCommandHash(grant.CommandHash) {
		return errors.New("管理员精确授权版本或命令哈希无效")
	}
	if !validAdministratorGrantScopeID(grant.GrantScopeID) || grant.GrantID == "" ||
		grant.GrantID != deriveAdministratorGrantID(grant) {
		return errors.New("管理员精确授权编号缺失、无效或已被篡改")
	}
	if grant.Clause != "immediate" && grant.Clause != "scheduled" {
		return errors.New("管理员精确授权执行子句无效")
	}
	if grant.Capability != capability || grant.ArgumentsHash != administratorArgumentsHash(arguments) {
		return errors.New("管理员精确授权与当前能力或参数不匹配")
	}
	resources, err := capabilityResourceKeys(capability, arguments)
	if err != nil {
		return err
	}
	if !equalResourceKeys(grant.ResourceKeys, resources) {
		return errors.New("管理员精确授权与当前资源不匹配")
	}
	if capability == "schedule_command" {
		var schedule administratorScheduleArguments
		if err := decodeCapabilityArguments(arguments, &schedule); err != nil {
			return err
		}
		targetArguments, err := normalizedArguments(schedule.Arguments)
		if err != nil {
			return err
		}
		targetResources, err := capabilityResourceKeys(schedule.Capability, targetArguments)
		if err != nil {
			return err
		}
		if grant.TargetCapability != schedule.Capability || grant.TargetArgumentsHash != administratorArgumentsHash(targetArguments) ||
			!equalResourceKeys(grant.TargetResourceKeys, targetResources) {
			return errors.New("管理员精确授权与定时目标能力、参数或资源不匹配")
		}
	} else if grant.TargetCapability != "" || grant.TargetArgumentsHash != "" || len(grant.TargetResourceKeys) > 0 {
		return errors.New("非定时管理员授权包含非法嵌套目标")
	}
	return nil
}

func scheduledTargetAdministratorGrant(outer *AdministratorGrant, capability string, arguments json.RawMessage) (*AdministratorGrant, error) {
	if outer == nil {
		return nil, nil
	}
	if outer.Capability != "schedule_command" || outer.TargetCapability != capability {
		return nil, errors.New("定时管理员授权目标能力不匹配")
	}
	resources, err := capabilityResourceKeys(capability, arguments)
	if err != nil {
		return nil, err
	}
	grant := &AdministratorGrant{Version: outer.Version, GrantScopeID: outer.GrantScopeID, CommandHash: outer.CommandHash,
		Clause: "scheduled", Capability: capability, ArgumentsHash: administratorArgumentsHash(arguments), ResourceKeys: resources}
	if grant.ArgumentsHash != outer.TargetArgumentsHash || !equalResourceKeys(grant.ResourceKeys, outer.TargetResourceKeys) {
		return nil, errors.New("定时管理员授权目标参数或资源不匹配")
	}
	grant.GrantID = deriveAdministratorGrantID(grant)
	if grant.GrantID == outer.GrantID {
		return nil, errors.New("定时命令与目标动作的管理员授权编号发生冲突")
	}
	return grant, nil
}

func newAdministratorGrantScopeID() (string, error) {
	value := make([]byte, sha256.Size)
	if _, err := cryptorand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func validAdministratorGrantScopeID(value string) bool {
	return validAdministratorCommandHash(value)
}

func deriveAdministratorGrantID(grant *AdministratorGrant) string {
	if grant == nil || !validAdministratorGrantScopeID(grant.GrantScopeID) {
		return ""
	}
	canonical := struct {
		Version             int      `json:"version"`
		GrantScopeID        string   `json:"grant_scope_id"`
		CommandHash         string   `json:"command_hash"`
		Clause              string   `json:"clause"`
		Capability          string   `json:"capability"`
		ArgumentsHash       string   `json:"arguments_hash"`
		ResourceKeys        []string `json:"resource_keys"`
		TargetCapability    string   `json:"target_capability"`
		TargetArgumentsHash string   `json:"target_arguments_hash"`
		TargetResourceKeys  []string `json:"target_resource_keys"`
	}{
		Version: grant.Version, GrantScopeID: grant.GrantScopeID, CommandHash: grant.CommandHash,
		Clause: grant.Clause, Capability: grant.Capability, ArgumentsHash: grant.ArgumentsHash,
		ResourceKeys: normalizeResourceKeys(grant.ResourceKeys), TargetCapability: grant.TargetCapability,
		TargetArgumentsHash: grant.TargetArgumentsHash, TargetResourceKeys: normalizeResourceKeys(grant.TargetResourceKeys),
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(append([]byte("sub2api-scheduler/administrator-grant/v1\x00"), payload...))
	return "ag1_" + hex.EncodeToString(hash[:])
}

func capabilityResourceKeys(capability string, arguments json.RawMessage) ([]string, error) {
	var values map[string]any
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.UseNumber()
	if err := decoder.Decode(&values); err != nil {
		return nil, errors.New("无法提取能力资源")
	}
	keys := make([]string, 0, 3)
	appendInteger := func(field, prefix string) error {
		if _, exists := values[field]; !exists {
			return nil
		}
		value, ok := integerMapValue(values, field)
		if !ok || value <= 0 {
			return fmt.Errorf("%s 资源编号无效", field)
		}
		keys = append(keys, fmt.Sprintf("%s:%d", prefix, value))
		return nil
	}
	if err := appendInteger("account_id", "account"); err != nil {
		return nil, err
	}
	if err := appendInteger("source_id", "source"); err != nil {
		return nil, err
	}
	if err := appendInteger("policy_id", "policy"); err != nil {
		return nil, err
	}
	if err := appendInteger("command_id", "command"); err != nil {
		return nil, err
	}
	if value, ok := values["key_id"].(string); ok && strings.TrimSpace(value) != "" {
		keys = append(keys, "key:"+strings.TrimSpace(value))
	}
	if capability == "trigger_reconcile" {
		keys = append(keys, "global:scheduler")
	}
	if capability == "update_dispatch_policy" || capability == "propose_dispatch_policy" {
		scopeType, _ := values["scope_type"].(string)
		scopeID, _ := values["scope_id"].(string)
		keys = append(keys, "policy_scope:"+scopeType+":"+scopeID)
	}
	if capability == "schedule_command" {
		var schedule administratorScheduleArguments
		if err := decodeCapabilityArguments(arguments, &schedule); err != nil {
			return nil, err
		}
		targetArguments, err := normalizedArguments(schedule.Arguments)
		if err != nil {
			return nil, err
		}
		return capabilityResourceKeys(schedule.Capability, targetArguments)
	}
	return normalizeResourceKeys(keys), nil
}

func administratorArgumentsHash(arguments json.RawMessage) string {
	if len(arguments) == 0 {
		return ""
	}
	hash := sha256.Sum256(arguments)
	return hex.EncodeToString(hash[:])
}

func validAdministratorCommandHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func normalizeResourceKeys(values []string) []string {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = true
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func equalResourceKeys(left, right []string) bool {
	left, right = normalizeResourceKeys(left), normalizeResourceKeys(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
