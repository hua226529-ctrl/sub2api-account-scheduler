package agent

import (
	"context"
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

const (
	ChatIntentQuery           = "query"
	ChatIntentAnalysis        = "analysis"
	ChatIntentDirectAction    = "direct_action"
	ChatIntentPolicyChange    = "policy_change"
	ChatIntentScheduledAction = "scheduled_action"
	ChatIntentAmbiguous       = "ambiguous"

	ChatResourceAccount = "account"
	ChatResourcePool    = "pool"
	ChatResourceGroup   = "group"
	ChatResourceSystem  = "system"
)

type ChatDesiredState struct {
	Schedulable      *bool  `json:"schedulable,omitempty"`
	LoadFactor       *int   `json:"load_factor,omitempty"`
	TargetTier       string `json:"target_tier,omitempty"`
	FailureThreshold *int   `json:"failure_threshold,omitempty"`
}

// ChatIntent is the closed, locally validated contract between an
// administrator message and the persistent Agent runtime. Model output may
// refine a plan, but it cannot widen this envelope.
type ChatIntent struct {
	IntentType           string           `json:"intent_type"`
	ResourceType         string           `json:"resource_type"`
	ResourceIDs          []string         `json:"resource_ids,omitempty"`
	Operation            string           `json:"operation"`
	DesiredState         ChatDesiredState `json:"desired_state"`
	DurationSeconds      int64            `json:"duration_seconds,omitempty"`
	ExpiresAt            *time.Time       `json:"expires_at,omitempty"`
	PermanentHold        bool             `json:"permanent_hold"`
	ScheduledAt          *time.Time       `json:"scheduled_at,omitempty"`
	Timezone             string           `json:"timezone,omitempty"`
	ReadOnly             bool             `json:"read_only"`
	RequiresConfirmation bool             `json:"requires_confirmation"`
	Confirmed            bool             `json:"confirmed"`
	RiskLevel            string           `json:"risk_level"`
	Autonomous           bool             `json:"autonomous"`
	AllowedCapabilities  []string         `json:"allowed_capabilities,omitempty"`
	UserFacingSummary    string           `json:"user_facing_summary"`
	Clarification        string           `json:"clarification,omitempty"`
}

func (i ChatIntent) Validate() error {
	switch i.IntentType {
	case ChatIntentQuery, ChatIntentAnalysis:
		if !i.ReadOnly || len(i.AllowedCapabilities) != 0 {
			return errors.New("query and analysis intents must be read-only")
		}
	case ChatIntentDirectAction:
		if i.ReadOnly || i.ResourceType == "" || len(i.ResourceIDs) == 0 || i.Operation == "" || len(i.AllowedCapabilities) == 0 {
			return errors.New("direct action intent is incomplete")
		}
	case ChatIntentPolicyChange:
		if i.ReadOnly || i.Operation != "propose_policy" || len(i.ResourceIDs) != 1 || !onlyCapability(i.AllowedCapabilities, "propose_dispatch_policy") {
			return errors.New("policy change intent must create one proposal")
		}
	case ChatIntentScheduledAction:
		if i.ReadOnly || i.ScheduledAt == nil || i.Timezone == "" || !onlyCapability(i.AllowedCapabilities, "schedule_command") {
			return errors.New("scheduled action intent is incomplete")
		}
	case ChatIntentAmbiguous:
		if !i.ReadOnly || len(i.AllowedCapabilities) != 0 || i.Clarification == "" {
			return errors.New("ambiguous intent must fail closed")
		}
	default:
		return errors.New("unknown chat intent type")
	}
	if i.PermanentHold && (!i.RequiresConfirmation || i.Operation != "manual_hold") {
		return errors.New("permanent hold requires explicit confirmation")
	}
	if i.Autonomous {
		if i.PermanentHold || i.ExpiresAt == nil || i.DurationSeconds <= 0 || i.DurationSeconds > int64((2*time.Hour)/time.Second) {
			return errors.New("autonomous action requires a bounded TTL")
		}
	}
	return nil
}

func onlyCapability(values []string, expected string) bool {
	return len(values) == 1 && values[0] == expected
}

func chatIntentConfirmationHash(intent ChatIntent) string {
	intent.Confirmed = false
	payload, _ := json.Marshal(struct {
		IntentType           string           `json:"intent_type"`
		ResourceType         string           `json:"resource_type"`
		ResourceIDs          []string         `json:"resource_ids"`
		Operation            string           `json:"operation"`
		DesiredState         ChatDesiredState `json:"desired_state"`
		DurationSeconds      int64            `json:"duration_seconds,omitempty"`
		ExpiresAt            *time.Time       `json:"expires_at,omitempty"`
		PermanentHold        bool             `json:"permanent_hold"`
		ScheduledAt          *time.Time       `json:"scheduled_at,omitempty"`
		Timezone             string           `json:"timezone,omitempty"`
		RequiresConfirmation bool             `json:"requires_confirmation"`
		RiskLevel            string           `json:"risk_level"`
		Autonomous           bool             `json:"autonomous"`
		AllowedCapabilities  []string         `json:"allowed_capabilities"`
	}{
		IntentType: intent.IntentType, ResourceType: intent.ResourceType,
		ResourceIDs: normalizeResourceKeys(intent.ResourceIDs), Operation: intent.Operation, DesiredState: intent.DesiredState,
		DurationSeconds: intent.DurationSeconds, ExpiresAt: intent.ExpiresAt, PermanentHold: intent.PermanentHold,
		ScheduledAt: intent.ScheduledAt, Timezone: intent.Timezone, RequiresConfirmation: intent.RequiresConfirmation,
		RiskLevel: intent.RiskLevel, Autonomous: intent.Autonomous,
		AllowedCapabilities: normalizeResourceKeys(intent.AllowedCapabilities),
	})
	hash := sha256.Sum256(append([]byte("sub2api-scheduler/chat-confirmation/v1\x00"), payload...))
	return hex.EncodeToString(hash[:])
}

func (m *Manager) bindChatIntentAdministratorGrants(intent AdministratorIntent, chat ChatIntent) AdministratorIntent {
	if chat.ReadOnly || chat.Autonomous || chat.IntentType == ChatIntentAmbiguous {
		return intent
	}
	if intent.CommandHash == "" {
		return intent
	}
	if intent.GrantScopeID == "" {
		grantScopeID, err := newAdministratorGrantScopeID()
		if err != nil {
			intent.Issues = append(intent.Issues, "无法生成管理员授权批次，命令已失败关闭")
			return intent
		}
		intent.GrantScopeID = grantScopeID
	}
	intent.Explicit = true
	add := func(grant AdministratorIntentGrant) {
		grant.ResourceKeys = normalizeResourceKeys(grant.ResourceKeys)
		for _, existing := range intent.Grants {
			if existing.Capability == grant.Capability && existing.Clause == grant.Clause &&
				equalResourceKeys(existing.ResourceKeys, grant.ResourceKeys) {
				return
			}
		}
		intent.Grants = append(intent.Grants, grant)
	}
	accountResources := func() []string {
		resources := make([]string, 0, len(chat.ResourceIDs))
		for _, id := range chat.ResourceIDs {
			resources = append(resources, "account:"+id)
		}
		return resources
	}
	switch chat.Operation {
	case "pause":
		for _, resource := range accountResources() {
			add(AdministratorIntentGrant{Capability: "pause_account", Clause: "immediate", ResourceKeys: []string{resource}, ExpiresAt: chat.ExpiresAt})
		}
	case "resume":
		for _, resource := range accountResources() {
			add(AdministratorIntentGrant{Capability: "resume_account", Clause: "immediate", ResourceKeys: []string{resource}, ExpiresAt: chat.ExpiresAt})
		}
	case "manual_hold":
		for _, resource := range accountResources() {
			add(AdministratorIntentGrant{Capability: "manual_hold_account", Clause: "immediate", ResourceKeys: []string{resource}, PermanentHold: true})
		}
	case "bulk_pause":
		for _, resource := range accountResources() {
			add(AdministratorIntentGrant{Capability: "pause_account", Clause: "immediate", ResourceKeys: []string{resource}, ExpiresAt: chat.ExpiresAt})
		}
	case "set_load_factor":
		for _, resource := range accountResources() {
			add(AdministratorIntentGrant{Capability: "pin_load_until", Clause: "immediate", ResourceKeys: []string{resource},
				LoadFactor: chat.DesiredState.LoadFactor, ExecuteAt: chat.ExpiresAt})
		}
	case "propose_policy":
		if len(chat.ResourceIDs) == 1 {
			add(AdministratorIntentGrant{Capability: "propose_dispatch_policy", Clause: "immediate",
				ResourceKeys: []string{"policy_scope:pool:" + chat.ResourceIDs[0]}})
		}
	case "transition_group_tier":
		add(AdministratorIntentGrant{Capability: "transition_token_group_tier", Clause: "scheduled",
			ResourceKeys: append([]string(nil), chat.ResourceIDs...), TargetTier: chat.DesiredState.TargetTier, ExecuteAt: chat.ScheduledAt})
	}
	return intent
}

func enforceChatIntentCapability(intent ChatIntent, capability string, arguments json.RawMessage, mutating bool) error {
	if intent.IntentType == "" || !mutating {
		return nil
	}
	if intent.ReadOnly || intent.IntentType == ChatIntentQuery || intent.IntentType == ChatIntentAnalysis || intent.IntentType == ChatIntentAmbiguous {
		return errors.New("当前聊天意图是只读或需要澄清，禁止写能力")
	}
	if intent.RequiresConfirmation && !intent.Confirmed {
		return errors.New("高风险聊天意图尚未完成一次性确认")
	}
	allowed := false
	for _, name := range intent.AllowedCapabilities {
		if name == capability {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("能力 %s 超出聊天意图授权范围", capability)
	}
	actual, err := capabilityResourceKeys(capability, arguments)
	if err != nil {
		return err
	}
	expected := make([]string, 0, len(intent.ResourceIDs))
	for _, resource := range intent.ResourceIDs {
		switch intent.ResourceType {
		case ChatResourceAccount:
			expected = append(expected, "account:"+resource)
		case ChatResourcePool:
			expected = append(expected, "policy_scope:pool:"+resource)
		default:
			expected = append(expected, resource)
		}
	}
	resourcesMatch := administratorResourcesMatch(expected, actual)
	if intent.ResourceType == ChatResourceAccount && len(actual) == 1 {
		resourcesMatch = false
		for _, resource := range expected {
			if actual[0] == resource {
				resourcesMatch = true
				break
			}
		}
	}
	if !resourcesMatch {
		return errors.New("能力资源与聊天意图不匹配")
	}
	var values map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(arguments)))
	decoder.UseNumber()
	if err := decoder.Decode(&values); err != nil {
		return errors.New("能力参数无法核对")
	}
	if intent.DesiredState.LoadFactor != nil {
		factor, ok := integerMapValue(values, "load_factor")
		if !ok || factor != *intent.DesiredState.LoadFactor {
			return errors.New("能力负载值与聊天意图不匹配")
		}
	}
	if intent.DesiredState.TargetTier != "" {
		if tier, _ := values["target_tier"].(string); tier != intent.DesiredState.TargetTier {
			return errors.New("能力目标层级与聊天意图不匹配")
		}
	}
	return nil
}

var (
	accountQueryPattern      = regexp.MustCompile(`^账号\s*(\d+)\s*(?:现在)?\s*(?:怎么样|什么状态|状态如何)[？?]?$`)
	accountAnalysisPattern   = regexp.MustCompile(`^分析账号\s*(\d+)\s*最近为什么抖动(?:，|,)?\s*(?:不要执行动作|不执行动作|只分析)[。.]?$`)
	overallAnalysisPattern   = regexp.MustCompile(`^分析最近整体调度效果[。.]?$`)
	pausePattern             = regexp.MustCompile(`^暂停账号\s*(\d+)\s*(.+?)[。.]?$`)
	manualHoldPattern        = regexp.MustCompile(`^暂停账号\s*(\d+)(?:，|,|\s)*(?:直到我手动解除|永久保持|永久暂停)[。.]?$`)
	resumePattern            = regexp.MustCompile(`^恢复账号\s*(\d+)[。.]?$`)
	loadPattern              = regexp.MustCompile(`^(?:把)?账号\s*(\d+)\s*的?负载(?:调到|设为|设置为)\s*(\d{1,3})%?\s*(.+?)[。.]?$`)
	policyPattern            = regexp.MustCompile(`^以后池\s*([^\s，,。]+)\s*连续失败\s*([一二三四五六七八九十百\d]+)次再暂停[。.]?$`)
	scheduledGroupPattern    = regexp.MustCompile(`^(?:今天|今晚|明天|凌晨|上午|下午|晚上)?[^，,。]*?将令牌\s*([^\s，,。]+)\s*切(?:换)?到\s*(主|备用|紧急)(?:分组|组)[。.]?$`)
	delegatedPattern         = regexp.MustCompile(`^你自己处理账号\s*(\d+)\s*的异常[。.]?$`)
	bulkPausePattern         = regexp.MustCompile(`^暂停所有账号[。.]?$`)
	readOnlyDirectivePattern = regexp.MustCompile(`(?:不要执行动作|不执行动作|只分析|仅分析|不要执行|请勿执行)`)
)

type chatResourceResolver interface {
	AccountExists(int64) bool
	AccountIDs() []int64
	ResolveToken(context.Context, string) (sourceID int64, keyID string, ok bool, err error)
}

type managerChatResolver struct{ manager *Manager }

func (r managerChatResolver) AccountExists(id int64) bool {
	if r.manager == nil || r.manager.engine == nil {
		return false
	}
	_, ok := findBinding(r.manager.engine.Snapshot(), id)
	return ok
}

func (r managerChatResolver) AccountIDs() []int64 {
	if r.manager == nil || r.manager.engine == nil {
		return nil
	}
	ids := make([]int64, 0, len(r.manager.engine.Snapshot().Bindings))
	for _, binding := range r.manager.engine.Snapshot().Bindings {
		ids = append(ids, binding.Account.ID)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	return ids
}

func (r managerChatResolver) ResolveToken(ctx context.Context, identity string) (int64, string, bool, error) {
	if r.manager == nil || r.manager.balances == nil {
		return 0, "", false, nil
	}
	sources, err := r.manager.balances.List(ctx)
	if err != nil {
		return 0, "", false, err
	}
	type match struct {
		sourceID int64
		keyID    string
	}
	matches := make([]match, 0, 1)
	for _, source := range sources {
		for _, policy := range source.FailoverPolicies {
			if !policy.Enabled || !policy.Confirmed {
				continue
			}
			for _, candidate := range []string{policy.KeyID, policy.KeyName, policy.KeyHint} {
				if strings.TrimSpace(candidate) == strings.TrimSpace(identity) {
					matches = append(matches, match{sourceID: source.ID, keyID: policy.KeyID})
					break
				}
			}
		}
	}
	if len(matches) != 1 {
		return 0, "", false, nil
	}
	return matches[0].sourceID, matches[0].keyID, true, nil
}

func (m *Manager) classifyChatIntent(ctx context.Context, message string, now time.Time) ChatIntent {
	return classifyChatIntent(ctx, strings.TrimSpace(message), now.UTC(), managerChatResolver{manager: m})
}

func classifyChatIntent(ctx context.Context, message string, now time.Time, resolver chatResourceResolver) ChatIntent {
	ambiguous := func(detail string) ChatIntent {
		return ChatIntent{IntentType: ChatIntentAmbiguous, ResourceType: ChatResourceSystem, Operation: "clarify",
			ReadOnly: true, RiskLevel: model.AgentRiskReadOnly, UserFacingSummary: "需要澄清后才能继续", Clarification: detail}
	}
	accountID := func(raw string) (int64, bool) {
		id, err := strconv.ParseInt(raw, 10, 64)
		return id, err == nil && id > 0 && resolver != nil && resolver.AccountExists(id)
	}
	if message == "" {
		return ambiguous("请提供明确的查询、资源和期望动作")
	}
	if match := accountQueryPattern.FindStringSubmatch(message); len(match) == 2 {
		id, ok := accountID(match[1])
		if !ok {
			return ambiguous("账号不存在或当前无法唯一识别")
		}
		return ChatIntent{IntentType: ChatIntentQuery, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "get_status", ReadOnly: true, RiskLevel: model.AgentRiskReadOnly, UserFacingSummary: fmt.Sprintf("查询账号 %d 当前状态", id)}
	}
	if match := accountAnalysisPattern.FindStringSubmatch(message); len(match) == 2 {
		id, ok := accountID(match[1])
		if !ok {
			return ambiguous("账号不存在或当前无法唯一识别")
		}
		return ChatIntent{IntentType: ChatIntentAnalysis, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "analyze_flapping", ReadOnly: true, RiskLevel: model.AgentRiskReadOnly, UserFacingSummary: fmt.Sprintf("只读分析账号 %d 的抖动原因", id)}
	}
	if overallAnalysisPattern.MatchString(message) || readOnlyDirectivePattern.MatchString(message) {
		return ChatIntent{IntentType: ChatIntentAnalysis, ResourceType: ChatResourceSystem, Operation: "analyze",
			ReadOnly: true, RiskLevel: model.AgentRiskReadOnly, UserFacingSummary: "只读分析调度运行情况"}
	}
	if match := manualHoldPattern.FindStringSubmatch(message); len(match) == 2 {
		id, ok := accountID(match[1])
		if !ok {
			return ambiguous("账号不存在或当前无法唯一识别")
		}
		paused := false
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "manual_hold", DesiredState: ChatDesiredState{Schedulable: &paused}, PermanentHold: true,
			RequiresConfirmation: true, RiskLevel: model.AgentRiskHigh, AllowedCapabilities: []string{"manual_hold_account"},
			UserFacingSummary: fmt.Sprintf("永久暂停账号 %d，直到管理员手动解除", id)}
	}
	if match := pausePattern.FindStringSubmatch(message); len(match) == 3 {
		id, ok := accountID(match[1])
		duration, durationOK := parseChatDuration(match[2])
		if !ok || !durationOK || duration <= 0 || duration > 24*time.Hour {
			return ambiguous("临时暂停必须包含存在的账号和 24 小时内的明确时长")
		}
		expiresAt, paused := now.Add(duration), false
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "pause", DesiredState: ChatDesiredState{Schedulable: &paused}, DurationSeconds: int64(duration / time.Second), ExpiresAt: &expiresAt,
			RiskLevel: model.AgentRiskHigh, AllowedCapabilities: []string{"pause_account"}, UserFacingSummary: fmt.Sprintf("暂停账号 %d，持续 %s", id, duration)}
	}
	if match := resumePattern.FindStringSubmatch(message); len(match) == 2 {
		id, ok := accountID(match[1])
		if !ok {
			return ambiguous("账号不存在或当前无法唯一识别")
		}
		duration, resumed := 30*time.Minute, true
		expiresAt := now.Add(duration)
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "resume", DesiredState: ChatDesiredState{Schedulable: &resumed}, DurationSeconds: int64(duration / time.Second), ExpiresAt: &expiresAt,
			RiskLevel: model.AgentRiskHigh, AllowedCapabilities: []string{"resume_account"}, UserFacingSummary: fmt.Sprintf("恢复账号 %d，临时覆盖 30 分钟", id)}
	}
	if match := loadPattern.FindStringSubmatch(message); len(match) == 4 {
		id, ok := accountID(match[1])
		factor, factorErr := strconv.Atoi(match[2])
		duration, durationOK := parseChatDuration(match[3])
		if !ok || factorErr != nil || factor < 1 || factor > 100 || !durationOK || duration <= 0 || duration > 24*time.Hour {
			return ambiguous("负载动作必须包含存在的账号、1 到 100 的负载和 24 小时内的明确时长")
		}
		expiresAt := now.Add(duration)
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "set_load_factor", DesiredState: ChatDesiredState{LoadFactor: &factor}, DurationSeconds: int64(duration / time.Second), ExpiresAt: &expiresAt,
			RiskLevel: model.AgentRiskHigh, AllowedCapabilities: []string{"pin_load_until"}, UserFacingSummary: fmt.Sprintf("将账号 %d 负载设为 %d%%，持续 %s", id, factor, duration)}
	}
	if match := policyPattern.FindStringSubmatch(message); len(match) == 3 {
		threshold, ok := parseChatInteger(match[2])
		if !ok || threshold < 1 || threshold > 20 {
			return ambiguous("连续失败次数必须是 1 到 20 的明确整数")
		}
		pool := strings.TrimSpace(match[1])
		return ChatIntent{IntentType: ChatIntentPolicyChange, ResourceType: ChatResourcePool, ResourceIDs: []string{pool},
			Operation: "propose_policy", DesiredState: ChatDesiredState{FailureThreshold: &threshold}, RiskLevel: model.AgentRiskMedium,
			AllowedCapabilities: []string{"propose_dispatch_policy"}, UserFacingSummary: fmt.Sprintf("提议将池 %s 的连续失败暂停阈值改为 %d", pool, threshold)}
	}
	if match := scheduledGroupPattern.FindStringSubmatch(message); len(match) == 3 {
		executeAt, clockOK := parseAdministratorClock(message, now)
		sourceID, keyID, tokenOK, err := resolver.ResolveToken(ctx, match[1])
		if err != nil || !clockOK || !tokenOK {
			return ambiguous("定时切组必须包含明确时间和唯一受控令牌")
		}
		tier := map[string]string{"主": model.GroupTierMain, "备用": model.GroupTierBackup, "紧急": model.GroupTierEmergency}[match[2]]
		return ChatIntent{IntentType: ChatIntentScheduledAction, ResourceType: ChatResourceGroup,
			ResourceIDs: []string{fmt.Sprintf("source:%d", sourceID), "key:" + keyID}, Operation: "transition_group_tier",
			DesiredState: ChatDesiredState{TargetTier: tier}, ScheduledAt: &executeAt, Timezone: model.AgentDefaultTimezone,
			RequiresConfirmation: true, RiskLevel: model.AgentRiskCritical, AllowedCapabilities: []string{"schedule_command"},
			UserFacingSummary: fmt.Sprintf("在 %s 将令牌 %s 切换到%s层级", executeAt.In(time.FixedZone(model.AgentDefaultTimezone, 8*60*60)).Format(time.RFC3339), keyID, tier)}
	}
	if match := delegatedPattern.FindStringSubmatch(message); len(match) == 2 {
		id, ok := accountID(match[1])
		if !ok {
			return ambiguous("账号不存在或当前无法唯一识别")
		}
		duration := 15 * time.Minute
		expiresAt := now.Add(duration)
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: []string{strconv.FormatInt(id, 10)},
			Operation: "analyze_and_act", DurationSeconds: int64(duration / time.Second), ExpiresAt: &expiresAt, Autonomous: true,
			RiskLevel: model.AgentRiskHigh, AllowedCapabilities: []string{"pause_account", "resume_account", "set_load_factor"},
			UserFacingSummary: fmt.Sprintf("授权智能体分析账号 %d，并在证据充分时执行最长 15 分钟的临时动作", id)}
	}
	if bulkPausePattern.MatchString(message) {
		ids := resolver.AccountIDs()
		if len(ids) == 0 {
			return ambiguous("当前没有可预览的账号")
		}
		resources := make([]string, len(ids))
		for index, id := range ids {
			resources[index] = strconv.FormatInt(id, 10)
		}
		paused, duration := false, 30*time.Minute
		expiresAt := now.Add(duration)
		return ChatIntent{IntentType: ChatIntentDirectAction, ResourceType: ChatResourceAccount, ResourceIDs: resources,
			Operation: "bulk_pause", DesiredState: ChatDesiredState{Schedulable: &paused}, DurationSeconds: int64(duration / time.Second),
			ExpiresAt: &expiresAt, RequiresConfirmation: true,
			RiskLevel: model.AgentRiskCritical, AllowedCapabilities: []string{"pause_account"},
			UserFacingSummary: fmt.Sprintf("暂停全部 %d 个账号", len(resources))}
	}
	return ambiguous("目标、动作、时长或时间不够明确；系统不会根据“处理一下”等宽泛表述执行动作")
}

func parseChatDuration(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	pattern := regexp.MustCompile(`([一二三四五六七八九十百半\d]+)\s*(分钟|小时)`)
	match := pattern.FindStringSubmatch(value)
	if len(match) != 3 {
		return 0, false
	}
	if match[1] == "半" && match[2] == "小时" {
		return 30 * time.Minute, true
	}
	number, ok := parseChatInteger(match[1])
	if !ok || number <= 0 {
		return 0, false
	}
	if match[2] == "小时" {
		return time.Duration(number) * time.Hour, true
	}
	return time.Duration(number) * time.Minute, true
}

func parseChatInteger(value string) (int, bool) {
	if parsed, err := strconv.Atoi(value); err == nil {
		return parsed, true
	}
	digits := map[rune]int{'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
	if value == "十" {
		return 10, true
	}
	if strings.ContainsRune(value, '十') {
		parts := strings.Split(value, "十")
		tens := 1
		if parts[0] != "" {
			v, ok := digits[[]rune(parts[0])[0]]
			if !ok {
				return 0, false
			}
			tens = v
		}
		ones := 0
		if len(parts) > 1 && parts[1] != "" {
			v, ok := digits[[]rune(parts[1])[0]]
			if !ok {
				return 0, false
			}
			ones = v
		}
		return tens*10 + ones, true
	}
	runes := []rune(value)
	if len(runes) == 1 {
		v, ok := digits[runes[0]]
		return v, ok
	}
	return 0, false
}
