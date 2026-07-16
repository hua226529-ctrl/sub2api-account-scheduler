package testsupport

import (
	"fmt"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type FixtureConfig struct {
	Accounts                   int
	Monitors                   int
	UnhealthyEvery             int
	InfrastructureFailureEvery int
	CredentialFailureEvery     int
	RateLimitedEvery           int
	PoolCount                  int
	LockedEvery                int
	BalanceLockedEvery         int
	CostLockedEvery            int
	PolicyEvery                int
	Seed                       int64
	Now                        time.Time
}

type Fixture struct {
	Accounts  []model.Account
	Monitors  []model.Monitor
	Policies  []model.Policy
	Controls  []model.AccountControl
	Successes []model.TrafficSuccess
	Failures  []model.TrafficError
	History   map[int64][]model.MonitorHistoryRecord
	Pools     map[int64]string
}

// GenerateFixture returns stable account, monitor, policy, lock, pool and
// telemetry inputs for the 10/100/500-account benchmark scenarios.
func GenerateFixture(config FixtureConfig) Fixture {
	if config.Accounts < 1 {
		config.Accounts = 1
	}
	if config.Monitors < 1 {
		config.Monitors = config.Accounts
	}
	if config.Now.IsZero() {
		config.Now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	config.Now = config.Now.UTC()
	fixture := Fixture{History: make(map[int64][]model.MonitorHistoryRecord), Pools: make(map[int64]string)}
	infrastructureEvery := config.InfrastructureFailureEvery
	if infrastructureEvery == 0 {
		infrastructureEvery = config.UnhealthyEvery
	}
	availability := 100.0
	for i := range config.Monitors {
		id := int64(10_000 + i)
		status := model.StatusOperational
		if matchesEvery(i, infrastructureEvery, config.Seed) {
			status = model.StatusFailed
		}
		monitor := model.Monitor{
			ID: id, Name: fmt.Sprintf("monitor-%04d", i+1), Provider: "openai",
			Endpoint: fmt.Sprintf("https://upstream-%04d.example", i+1), PrimaryModel: "gpt-baseline",
			Enabled: true, IntervalSeconds: 60, LastCheckedAt: timePointer(config.Now),
			PrimaryStatus: status, PrimaryLatencyMS: int64(50 + i%20), Availability7D: &availability,
		}
		fixture.Monitors = append(fixture.Monitors, monitor)
		errorClass := ""
		if status == model.StatusFailed {
			errorClass = model.ErrorClassInfrastructure
		}
		fixture.History[id] = []model.MonitorHistoryRecord{{
			SourceID: int64(i + 1), MonitorID: id, Model: monitor.PrimaryModel, Status: status,
			LatencyMS: monitor.PrimaryLatencyMS, ErrorClass: errorClass, CheckedAt: config.Now, IngestedAt: config.Now,
		}}
	}
	for i := range config.Accounts {
		id := int64(i + 1)
		monitor := fixture.Monitors[i%len(fixture.Monitors)]
		load := 100
		account := model.Account{
			ID: id, Name: fmt.Sprintf("account-%04d", i+1), Platform: "openai", Type: "apikey",
			Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": monitor.Endpoint + "/v1"},
			Concurrency: 10, LoadFactor: &load, Priority: i % 5, UpdatedAt: config.Now,
		}
		if matchesEvery(i, config.CredentialFailureEvery, config.Seed) {
			account.CredentialStatus = "invalid"
			account.ErrorMessage = "credential rejected"
		}
		if matchesEvery(i, config.RateLimitedEvery, config.Seed) {
			resetAt := config.Now.Add(10 * time.Minute)
			account.RateLimitResetAt = &resetAt
			account.TempUnschedulableUntil = &resetAt
		}
		fixture.Accounts = append(fixture.Accounts, account)
		if config.PolicyEvery > 0 && (i+1)%config.PolicyEvery == 0 {
			monitorID := monitor.ID
			fixture.Policies = append(fixture.Policies, model.Policy{AccountID: id, MonitorID: &monitorID, Enabled: true})
		}
		manualLocked := matchesEvery(i, config.LockedEvery, config.Seed)
		balanceLocked := matchesEvery(i, config.BalanceLockedEvery, config.Seed)
		costLocked := matchesEvery(i, config.CostLockedEvery, config.Seed)
		if manualLocked || balanceLocked || costLocked {
			control := model.AccountControl{AccountID: id, ManualLocked: manualLocked, BalanceLocked: balanceLocked, CostLocked: costLocked, UpdatedAt: config.Now}
			if manualLocked {
				control.Owner = "operator"
			}
			if balanceLocked {
				sourceID := int64(1)
				control.BalanceSourceID = &sourceID
			}
			if costLocked {
				sourceID := int64(2)
				control.CostSourceID = &sourceID
				control.CostPool = "cost-standby"
			}
			fixture.Controls = append(fixture.Controls, control)
		}
		pool := "default"
		if config.PoolCount > 0 {
			pool = fmt.Sprintf("pool-%02d", i%config.PoolCount)
		}
		fixture.Pools[id] = pool
		fixture.Successes = append(fixture.Successes, model.TrafficSuccess{
			EventKey: fmt.Sprintf("success-%d", id), AccountID: id, Model: "gpt-baseline",
			UpstreamModel: pool, DurationMS: int64(100 + i%30), Kind: "chat", CreatedAt: config.Now,
		})
		if matchesEvery(i, infrastructureEvery, config.Seed) {
			fixture.Failures = append(fixture.Failures, model.TrafficError{
				EventKey: fmt.Sprintf("failure-%d", id), AccountID: id, Model: "gpt-baseline",
				StatusCode: 503, ErrorClass: model.ErrorClassInfrastructure, ReasonCode: "upstream_unavailable",
				ReasonFingerprint: fmt.Sprintf("failure-%d", id), CreatedAt: config.Now,
			})
		}
		if matchesEvery(i, config.CredentialFailureEvery, config.Seed) {
			fixture.Failures = append(fixture.Failures, model.TrafficError{
				EventKey: fmt.Sprintf("credential-failure-%d", id), AccountID: id, Model: "gpt-baseline",
				StatusCode: 401, ErrorClass: model.ErrorClassCredential, ReasonCode: "credential_rejected",
				ReasonFingerprint: fmt.Sprintf("credential-failure-%d", id), CreatedAt: config.Now,
			})
		}
		if matchesEvery(i, config.RateLimitedEvery, config.Seed) {
			fixture.Failures = append(fixture.Failures, model.TrafficError{
				EventKey: fmt.Sprintf("rate-limit-%d", id), AccountID: id, Model: "gpt-baseline",
				StatusCode: 429, ErrorClass: model.ErrorClassCapacity, ReasonCode: "rate_limited",
				ReasonFingerprint: fmt.Sprintf("rate-limit-%d", id), CreatedAt: config.Now,
			})
		}
	}
	return fixture
}

func matchesEvery(index, every int, seed int64) bool {
	if every <= 0 {
		return false
	}
	offset := int(seed % int64(every))
	if offset < 0 {
		offset += every
	}
	return (index+1+offset)%every == 0
}

func DefaultSettings() model.Settings {
	return model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
		HealthMode: model.HealthModeLegacy,
	}
}

func timePointer(value time.Time) *time.Time { return &value }
