package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

type administratorTestAPI struct {
	accounts        []model.Account
	monitors        []model.Monitor
	actions         []bool
	scheduleStarted chan struct{}
	scheduleRelease chan struct{}
}

func (api *administratorTestAPI) ListAccounts(context.Context) ([]model.Account, error) {
	return append([]model.Account(nil), api.accounts...), nil
}

func (api *administratorTestAPI) ListMonitors(context.Context) ([]model.Monitor, error) {
	return append([]model.Monitor(nil), api.monitors...), nil
}

func (api *administratorTestAPI) SetSchedulable(_ context.Context, accountID int64, value bool) (model.Account, error) {
	if api.scheduleStarted != nil {
		api.scheduleStarted <- struct{}{}
	}
	if api.scheduleRelease != nil {
		<-api.scheduleRelease
	}
	api.actions = append(api.actions, value)
	for index := range api.accounts {
		if api.accounts[index].ID == accountID {
			api.accounts[index].Schedulable = value
			return api.accounts[index], nil
		}
	}
	return model.Account{}, io.EOF
}

func (api *administratorTestAPI) UpdateLoadFactor(_ context.Context, accountID int64, value *int) (model.Account, error) {
	for index := range api.accounts {
		if api.accounts[index].ID == accountID {
			api.accounts[index].LoadFactor = value
			return api.accounts[index], nil
		}
	}
	return model.Account{}, io.EOF
}

func newAdministratorTestManager(t *testing.T) (*Manager, *store.Store, *administratorTestAPI) {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now().UTC()
	api := &administratorTestAPI{
		accounts: []model.Account{
			{ID: 225, Name: "备用账号", Platform: "openai", Type: "apikey", Status: "active", Schedulable: false,
				Credentials: map[string]any{"base_url": "https://handsome.example/v1"}, UpdatedAt: now},
			{ID: 298, Name: "account-example", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true,
				Credentials: map[string]any{"base_url": "https://account-example.example/v1"}, UpdatedAt: now},
		},
		monitors: []model.Monitor{
			{ID: 2, Name: "备用账号", Provider: "openai", Endpoint: "https://handsome.example", Enabled: true, IntervalSeconds: 60, LastCheckedAt: &now, PrimaryStatus: model.StatusOperational},
			{ID: 8, Name: "account-example", Provider: "openai", Endpoint: "https://account-example.example", Enabled: true, IntervalSeconds: 60, LastCheckedAt: &now, PrimaryStatus: model.StatusOperational},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := reconcile.NewEngine(api, database, time.Minute, logger)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	balances := balance.NewManager(database, api, engine, nil, nil, time.Hour, logger)
	manager := NewManager(database, engine, balances, nil, logger)
	return manager, database, api
}

func TestCharacterizationAdministratorIntentScopesCapabilityAndAccount(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	intent := manager.parseAdministratorIntent(context.Background(), "立即恢复备用账号")
	if !intent.Explicit || len(intent.Grants) != 1 || intent.Grants[0].Capability != "resume_account" {
		t.Fatalf("unexpected administrator intent: %+v", intent)
	}
	resume225 := json.RawMessage(`{"account_id":225,"reason":"管理员命令"}`)
	grant, err := manager.administratorGrantForInvocation(intent, "resume_account", resume225)
	if err != nil || grant == nil {
		t.Fatalf("intended resume did not receive an exact grant: grant=%+v err=%v", grant, err)
	}
	for name, arguments := range map[string]json.RawMessage{
		"pause_account":  json.RawMessage(`{"account_id":225,"reason":"sibling"}`),
		"resume_account": json.RawMessage(`{"account_id":298,"reason":"wrong target"}`),
	} {
		grant, err := manager.administratorGrantForInvocation(intent, name, arguments)
		if err != nil || grant != nil {
			t.Fatalf("sibling or wrong target was elevated: capability=%s grant=%+v err=%v", name, grant, err)
		}
	}
	question := manager.parseAdministratorIntent(context.Background(), "为什么现在还不恢复备用账号？")
	if question.Explicit || len(question.Grants) != 0 {
		t.Fatalf("question was treated as a privileged command: %+v", question)
	}
}

func TestAdministratorGrantIDIsBoundAndLegacyEnvelopeFailsClosed(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	intent := manager.parseAdministratorIntent(context.Background(), "立即恢复备用账号")
	arguments := json.RawMessage(`{"account_id":225,"reason":"管理员命令"}`)
	first, err := manager.administratorGrantForInvocation(intent, "resume_account", arguments)
	if err != nil || first == nil || first.GrantID == "" {
		t.Fatalf("exact grant id was not minted: grant=%+v err=%v", first, err)
	}
	second, err := manager.administratorGrantForInvocation(intent, "resume_account", arguments)
	if err != nil || second == nil || second.GrantID != first.GrantID {
		t.Fatalf("same intent did not derive a stable grant id: first=%+v second=%+v err=%v", first, second, err)
	}

	tampered := *first
	tampered.ResourceKeys = []string{"account:298"}
	if err := validateAdministratorGrant(&tampered, "resume_account", arguments); err == nil {
		t.Fatal("tampered administrator grant id was accepted")
	}
	legacy := *first
	legacy.GrantID = ""
	if err := validateAdministratorGrant(&legacy, "resume_account", arguments); err == nil {
		t.Fatal("legacy grant without grant_id was accepted")
	}

	reissued := manager.parseAdministratorIntent(context.Background(), "立即恢复备用账号")
	third, err := manager.administratorGrantForInvocation(reissued, "resume_account", arguments)
	if err != nil || third == nil || third.GrantID == first.GrantID {
		t.Fatalf("a newly issued identical command reused the old grant id: first=%+v third=%+v err=%v", first, third, err)
	}
}

func TestCharacterizationAdministratorGrantCannotBeConsumedByAnotherStep(t *testing.T) {
	manager, _, api := newAdministratorTestManager(t)
	arguments := json.RawMessage(`{"account_id":298,"reason":"administrator request"}`)
	grant := mintAdministratorGrant(administratorCommandHash("single-use-test-scope"),
		administratorCommandHash("立即暂停account-example"), "immediate", "pause_account", arguments,
		[]string{"account:298"}, "", nil, nil)
	invocation := CapabilityInvocation{Name: "pause_account", Arguments: arguments, GoalID: 301, StepID: 401,
		Actor: "administrator:agent", AdministratorGrant: grant}
	if _, err := manager.ExecuteCapability(context.Background(), invocation); err != nil {
		t.Fatalf("first exact administrator action failed: %v", err)
	}
	if len(api.actions) != 1 {
		t.Fatalf("first action was not sent exactly once: %v", api.actions)
	}
	invocation.StepID = 402
	if _, err := manager.ExecuteCapability(context.Background(), invocation); err == nil {
		t.Fatal("another step reused an already consumed administrator grant")
	}
	if len(api.actions) != 1 {
		t.Fatalf("rejected grant reuse reached Sub2API: %v", api.actions)
	}
}

func TestAdministratorIntentRejectsNegatedConditionalAndAmbiguousCommands(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	for _, message := range []string{
		"不要恢复账号225",
		"不要把账号225自动恢复",
		"先别暂停账号225",
		"暂不恢复备用账号",
		"如果监控正常就恢复账号225",
		"当监控正常时恢复账号225",
		"仅当可用时恢复账号225",
		"等余额恢复后再恢复账号225",
		"视情况暂停账号225",
		"恢复账号225或者账号298",
		"恢复其中一个账号",
	} {
		message := message
		t.Run(message, func(t *testing.T) {
			t.Parallel()
			intent := manager.parseAdministratorIntent(context.Background(), message)
			if !intent.Explicit || len(intent.Grants) != 0 || len(intent.Issues) == 0 {
				t.Fatalf("unsafe command was not rejected closed: %+v", intent)
			}
			arguments := json.RawMessage(`{"account_id":225,"reason":"must not execute"}`)
			for _, capability := range []string{"pause_account", "resume_account"} {
				grant, err := manager.administratorGrantForInvocation(intent, capability, arguments)
				if err != nil || grant != nil {
					t.Fatalf("unsafe command minted a grant: capability=%s grant=%+v err=%v", capability, grant, err)
				}
			}
		})
	}
}

func TestAdministratorIntentStillAcceptsUnambiguousPositiveCommands(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	for _, test := range []struct {
		message    string
		capability string
	}{
		{message: "立即恢复账号225", capability: "resume_account"},
		{message: "立即暂停账号225", capability: "pause_account"},
		{message: "把账号298负载设为25并保持到早上6点", capability: "pin_load_until"},
		{message: "在早上6点恢复账号225", capability: "resume_account"},
	} {
		test := test
		t.Run(test.message, func(t *testing.T) {
			t.Parallel()
			intent := manager.parseAdministratorIntent(context.Background(), test.message)
			if !intent.Explicit || len(intent.Grants) != 1 || intent.Grants[0].Capability != test.capability {
				t.Fatalf("positive command regressed: %+v", intent)
			}
		})
	}
}

func TestAdministratorIntentDoesNotTreatLoadOrClockAsAccountID(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	intent := manager.parseAdministratorIntent(context.Background(), "把负载设为25并保持到早上6点")
	if len(intent.Grants) != 0 {
		t.Fatalf("load value or clock became an account selector: %+v", intent)
	}
	explicit := manager.parseAdministratorIntent(context.Background(), "把账号298负载设为25并保持到早上6点")
	if len(explicit.Grants) != 1 || explicit.Grants[0].ResourceKeys[0] != "account:298" {
		t.Fatalf("prefixed account id was not resolved: %+v", explicit)
	}
}

func TestCompoundAdministratorCommandCannotCrossAuthorizeTargets(t *testing.T) {
	t.Parallel()
	manager, _, _ := newAdministratorTestManager(t)
	intent := manager.parseAdministratorIntent(context.Background(), "把 account-example 负载设为25保持到早上6点，并在早上6点恢复备用账号")
	var pinUntil, resumeAt time.Time
	for _, item := range intent.Grants {
		if item.Capability == "pin_load_until" && item.ExecuteAt != nil {
			pinUntil = *item.ExecuteAt
		}
		if item.Capability == "resume_account" && item.ExecuteAt != nil {
			resumeAt = *item.ExecuteAt
		}
	}
	if pinUntil.IsZero() || resumeAt.IsZero() {
		t.Fatalf("compound command was not split into exact clauses: %+v", intent)
	}
	pin298, _ := json.Marshal(map[string]any{"account_id": 298, "load_factor": 25, "until": pinUntil, "reason": "night pin"})
	if grant, err := manager.administratorGrantForInvocation(intent, "pin_load_until", pin298); err != nil || grant == nil {
		t.Fatalf("intended load pin was not authorized: grant=%+v err=%v", grant, err)
	}
	pin225, _ := json.Marshal(map[string]any{"account_id": 225, "load_factor": 25, "until": pinUntil, "reason": "cross target"})
	if grant, _ := manager.administratorGrantForInvocation(intent, "pin_load_until", pin225); grant != nil {
		t.Fatalf("load clause crossed into the resume target: %+v", grant)
	}
	resume225 := json.RawMessage(`{"account_id":225,"reason":"scheduled resume"}`)
	schedule225, _ := json.Marshal(map[string]any{"capability": "resume_account", "arguments": resume225, "execute_at": resumeAt,
		"timezone": model.AgentDefaultTimezone, "reason": "scheduled"})
	if grant, err := manager.administratorGrantForInvocation(intent, "schedule_command", schedule225); err != nil || grant == nil {
		t.Fatalf("intended scheduled resume was not authorized: grant=%+v err=%v", grant, err)
	}
	resume298 := json.RawMessage(`{"account_id":298,"reason":"cross target"}`)
	schedule298, _ := json.Marshal(map[string]any{"capability": "resume_account", "arguments": resume298, "execute_at": resumeAt,
		"timezone": model.AgentDefaultTimezone, "reason": "scheduled"})
	if grant, _ := manager.administratorGrantForInvocation(intent, "schedule_command", schedule298); grant != nil {
		t.Fatalf("scheduled resume crossed into the load target: %+v", grant)
	}
}

func TestAdministratorGroupGrantBindsExactControlledToken(t *testing.T) {
	t.Parallel()
	manager, database, _ := newAdministratorTestManager(t)
	source, err := database.CreateUpstreamSource(context.Background(), model.UpstreamSource{Name: "示例站", Provider: "newapi",
		BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{1},
		PauseBelow: 1, ResumeAt: 2, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []model.GroupFailoverPolicy{
		{SourceID: source.ID, KeyID: "key-a", KeyName: "令牌甲", Enabled: true, MainGroupID: "main-a", BackupGroupID: "backup-a", EmergencyGroupID: "emergency-a", Pool: "gpt"},
		{SourceID: source.ID, KeyID: "key-b", KeyName: "令牌乙", Enabled: true, MainGroupID: "main-b", BackupGroupID: "backup-b", EmergencyGroupID: "emergency-b", Pool: "gpt"},
	} {
		saved, err := database.SaveGroupFailoverPolicy(context.Background(), policy)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.ConfirmGroupFailoverPolicy(context.Background(), saved.SourceID, saved.KeyID, saved.Version, "web"); err != nil {
			t.Fatal(err)
		}
	}
	intent := manager.parseAdministratorIntent(context.Background(), "把示例站的令牌甲切换到备用分组")
	if len(intent.Grants) != 1 {
		t.Fatalf("exact token name did not produce one grant: %+v", intent)
	}
	argsA, _ := json.Marshal(map[string]any{"source_id": source.ID, "key_id": "key-a", "target_tier": "backup", "confidence": 0.5, "reason": "admin"})
	if grant, err := manager.administratorGrantForInvocation(intent, "transition_token_group_tier", argsA); err != nil || grant == nil {
		t.Fatalf("named token did not receive grant: grant=%+v err=%v", grant, err)
	}
	argsB, _ := json.Marshal(map[string]any{"source_id": source.ID, "key_id": "key-b", "target_tier": "backup", "confidence": 0.5, "reason": "cross"})
	if grant, _ := manager.administratorGrantForInvocation(intent, "transition_token_group_tier", argsB); grant != nil {
		t.Fatalf("another token on the same source inherited the grant: %+v", grant)
	}
}

func TestLegacyAdministratorScheduledCommandFailsClosed(t *testing.T) {
	t.Parallel()
	manager, database, api := newAdministratorTestManager(t)
	manager.workerID = "legacy-grant-worker"
	now := time.Now().UTC()
	command := model.ScheduledCommand{Capability: "resume_account", Arguments: json.RawMessage(`{"account_id":225,"reason":"legacy"}`),
		Conditions: json.RawMessage(`{"administrator_direct":true,"administrator_command_hash":"legacy"}`), Status: model.AgentCommandStatusPending,
		Timezone: model.AgentDefaultTimezone, ExecuteAt: now.Add(-time.Second), IdempotencyKey: "legacy-admin-command", MaxAttempts: 3, CreatedBy: "administrator:agent"}
	if err := database.CreateScheduledCommand(context.Background(), &command); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(context.Background(), manager.workerID, now, time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("command was not claimed: items=%+v err=%v", claimed, err)
	}
	manager.executeScheduledCommand(context.Background(), claimed[0])
	loaded, err := database.GetScheduledCommand(context.Background(), command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != model.AgentCommandStatusFailed || len(api.actions) != 0 {
		t.Fatalf("legacy boolean authorization reached external write: command=%+v actions=%v", loaded, api.actions)
	}
}
