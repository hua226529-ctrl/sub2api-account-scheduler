package balance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/automation"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

type AccountAPI interface {
	ListAccounts(context.Context) ([]model.Account, error)
}

type Trigger interface {
	Trigger()
}

type accountRequester interface {
	RequestAccounts(...int64)
}

type sourcedAccountRequester interface {
	RequestAccountsFrom(string, ...int64)
}

type validationSnapshotProvider interface {
	Snapshot() model.Snapshot
}

type automationBoundary interface {
	AutomationBarrier() *automation.Barrier
	FreezeState(context.Context) (model.FreezeState, error)
}

type SourceInput struct {
	Name           string  `json:"name"`
	Provider       string  `json:"provider"`
	BaseURL        string  `json:"base_url"`
	Username       string  `json:"username"`
	Password       string  `json:"password"`
	PauseBelow     float64 `json:"pause_below"`
	ResumeAt       float64 `json:"resume_at"`
	Enabled        bool    `json:"enabled"`
	SelectedKeyID  string  `json:"selected_key_id"`
	RoutingEnabled bool    `json:"routing_enabled"`
	RoutingPool    string  `json:"routing_pool"`
}

type Manager struct {
	store      *store.Store
	api        AccountAPI
	trigger    Trigger
	fetcher    *Fetcher
	box        *SecretBox
	interval   time.Duration
	logger     *slog.Logger
	runMu      sync.Mutex
	groupLocks *groupKeyLocks
	barrier    *automation.Barrier
	freeze     interface {
		FreezeState(context.Context) (model.FreezeState, error)
	}
	routeMu    sync.Mutex
	sourceMu   sync.Mutex
	sourceRuns map[int64]bool
	manualMu   sync.Mutex
	manualRuns map[int64]time.Time
	lastRunMu  sync.RWMutex
	lastRunAt  *time.Time
}

func NewManager(database *store.Store, api AccountAPI, trigger Trigger, fetcher *Fetcher, box *SecretBox, interval time.Duration, logger *slog.Logger) *Manager {
	manager := &Manager{store: database, api: api, trigger: trigger, fetcher: fetcher, box: box, interval: interval, logger: logger, groupLocks: newGroupKeyLocks(), sourceRuns: map[int64]bool{}, manualRuns: map[int64]time.Time{}}
	if boundary, ok := trigger.(automationBoundary); ok {
		manager.barrier = boundary.AutomationBarrier()
		manager.freeze = boundary
	}
	return manager
}

func (m *Manager) Start(ctx context.Context) {
	go func() {
		m.refreshAll(ctx)
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.refreshAll(ctx)
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(50 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.reconcileCostRouting(ctx); err != nil {
					m.logger.Warn("cost_routing_failed", "error", err)
				}
			}
		}
	}()
}

func (m *Manager) LastRunAt() *time.Time {
	m.lastRunMu.RLock()
	defer m.lastRunMu.RUnlock()
	if m.lastRunAt == nil {
		return nil
	}
	value := *m.lastRunAt
	return &value
}

func (m *Manager) Validate(ctx context.Context, input SourceInput) (model.UpstreamResult, error) {
	if err := m.validateInput(input, true); err != nil {
		return model.UpstreamResult{}, err
	}
	credentials := credentialsFromInput(input)
	m.fetcher.Invalidate(input.Provider, input.BaseURL, credentials)
	return m.fetcher.Fetch(ctx, input.Provider, input.BaseURL, credentials)
}

func (m *Manager) Create(ctx context.Context, input SourceInput, actor string) (model.UpstreamSource, error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	result, err := m.Validate(ctx, input)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	normalized, _ := NormalizeURL(input.BaseURL, m.fetcher.allowInsecure)
	credentials := credentialsFromInput(input)
	nonce, ciphertext, err := m.encrypt(credentials)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	source, err := m.store.CreateUpstreamSource(ctx, model.UpstreamSource{
		Name: strings.TrimSpace(input.Name), Provider: normalizeProvider(input.Provider), BaseURL: strings.TrimRight(strings.TrimSpace(input.BaseURL), "/"), NormalizedURL: normalized,
		CredentialNonce: nonce, CredentialCiphertext: ciphertext, PauseBelow: input.PauseBelow, ResumeAt: input.ResumeAt, Enabled: input.Enabled,
		SelectedKeyID: strings.TrimSpace(input.SelectedKeyID), RoutingEnabled: input.RoutingEnabled, RoutingPool: strings.TrimSpace(input.RoutingPool),
		CredentialMode: "password", MigrationRequired: false,
	})
	if err != nil {
		return model.UpstreamSource{}, err
	}
	if err := m.applySuccess(ctx, &source, result); err != nil {
		return model.UpstreamSource{}, err
	}
	m.record(ctx, model.Event{Type: "upstream_created", Severity: "info", Message: "已添加上游余额账户 " + source.Name, Actor: actor, Details: fmt.Sprintf(`{"source_id":%d}`, source.ID)})
	m.trigger.Trigger()
	if err := m.reconcileCostRouting(ctx); err != nil {
		return model.UpstreamSource{}, err
	}
	return m.Get(ctx, source.ID)
}

func (m *Manager) Update(ctx context.Context, id int64, input SourceInput, actor string) (model.UpstreamSource, error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	current, err := m.store.GetUpstreamSource(ctx, id)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	credentials, err := m.decrypt(current)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	if strings.TrimSpace(input.Username) == "" {
		input.Username = credentials.Username
	}
	identityChanged := normalizeProvider(input.Provider) != normalizeProvider(current.Provider) || strings.TrimRight(strings.TrimSpace(input.BaseURL), "/") != strings.TrimRight(strings.TrimSpace(current.BaseURL), "/") || strings.TrimSpace(input.Username) != strings.TrimSpace(credentials.Username)
	if input.Password == "" {
		if credentials.AuthMode == "access_key" || credentials.AccessKey != "" || identityChanged {
			return model.UpstreamSource{}, errors.New("旧访问密钥配置或登录身份发生变化，必须重新填写账号密码并测试连接")
		}
		input.Password = credentials.Password
	}
	if err := m.validateInput(input, true); err != nil {
		return model.UpstreamSource{}, err
	}
	var result model.UpstreamResult
	if input.Enabled {
		updatedCredentials := credentialsFromInput(input)
		m.fetcher.Invalidate(input.Provider, input.BaseURL, updatedCredentials)
		result, err = m.fetcher.Fetch(ctx, input.Provider, input.BaseURL, updatedCredentials)
		if err != nil {
			return model.UpstreamSource{}, err
		}
	}
	normalized, _ := NormalizeURL(input.BaseURL, m.fetcher.allowInsecure)
	nonce, ciphertext, err := m.encrypt(credentialsFromInput(input))
	if err != nil {
		return model.UpstreamSource{}, err
	}
	current.Name = strings.TrimSpace(input.Name)
	current.Provider = normalizeProvider(input.Provider)
	current.BaseURL = strings.TrimRight(strings.TrimSpace(input.BaseURL), "/")
	current.NormalizedURL = normalized
	current.CredentialNonce = nonce
	current.CredentialCiphertext = ciphertext
	current.PauseBelow = input.PauseBelow
	current.ResumeAt = input.ResumeAt
	current.Enabled = input.Enabled
	current.SelectedKeyID = strings.TrimSpace(input.SelectedKeyID)
	current.RoutingEnabled = input.RoutingEnabled
	current.RoutingPool = strings.TrimSpace(input.RoutingPool)
	current.CredentialMode = "password"
	current.MigrationRequired = false
	if !input.Enabled {
		current.BalanceLocked = false
		current.LowStreak = 0
		current.RecoveryStreak = 0
		_ = m.store.SyncBalanceLocks(ctx, id, nil, false)
	}
	if err := m.store.UpdateUpstreamSourceWithPolicyInvalidation(ctx, current, identityChanged); err != nil {
		return model.UpstreamSource{}, err
	}
	if input.Enabled {
		if err := m.applySuccess(ctx, &current, result); err != nil {
			return model.UpstreamSource{}, err
		}
	} else if err := m.store.ResetUpstreamControl(ctx, id); err != nil {
		return model.UpstreamSource{}, err
	}
	m.record(ctx, model.Event{Type: "upstream_updated", Severity: "info", Message: "已更新上游余额账户 " + current.Name, Actor: actor, Details: fmt.Sprintf(`{"source_id":%d}`, current.ID)})
	if identityChanged {
		m.record(ctx, model.Event{Type: "group_failover_policy_invalidated", Severity: "warning", Message: "上游登录身份已变化，三级分组策略确认已撤销", Actor: actor, Details: fmt.Sprintf(`{"source_id":%d}`, current.ID)})
	}
	m.trigger.Trigger()
	if err := m.reconcileCostRouting(ctx); err != nil {
		return model.UpstreamSource{}, err
	}
	return m.Get(ctx, id)
}

func (m *Manager) Delete(ctx context.Context, id int64, actor string) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	source, err := m.store.GetUpstreamSource(ctx, id)
	if err != nil {
		return err
	}
	if err := m.store.SyncBalanceLocks(ctx, id, nil, false); err != nil {
		return err
	}
	if err := m.store.DeleteUpstreamSource(ctx, id); err != nil {
		return err
	}
	m.manualMu.Lock()
	delete(m.manualRuns, id)
	m.manualMu.Unlock()
	m.record(ctx, model.Event{Type: "upstream_deleted", Severity: "warning", Message: "已删除上游余额账户 " + source.Name, Actor: actor, Details: fmt.Sprintf(`{"source_id":%d}`, id)})
	m.trigger.Trigger()
	return m.reconcileCostRouting(ctx)
}

func (m *Manager) Refresh(ctx context.Context, id int64) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	if err := m.refreshLocked(ctx, id); err != nil {
		return err
	}
	if err := m.reconcileGroupFailoverStatesForSourcesLocked(ctx, map[int64]bool{id: true}); err != nil {
		return err
	}
	return nil
}

func (m *Manager) refreshLocked(ctx context.Context, id int64) error {
	m.sourceMu.Lock()
	if m.sourceRuns[id] {
		m.sourceMu.Unlock()
		return errors.New("该上游正在刷新，请稍后重试")
	}
	m.sourceRuns[id] = true
	m.sourceMu.Unlock()
	defer func() {
		m.sourceMu.Lock()
		delete(m.sourceRuns, id)
		m.sourceMu.Unlock()
	}()

	source, err := m.store.GetUpstreamSource(ctx, id)
	if err != nil {
		return err
	}
	if !source.Enabled {
		return errors.New("该上游余额规则已关闭")
	}
	credentials, err := m.decrypt(source)
	if err != nil {
		return err
	}
	if credentials.AuthMode == "access_key" || strings.TrimSpace(credentials.AccessKey) != "" || strings.TrimSpace(credentials.Username) == "" || credentials.Password == "" {
		_ = m.store.MarkUpstreamCredentialMigrationRequired(ctx, source.ID)
		m.trigger.Trigger()
		return errors.New("该上游仍使用旧访问密钥，必须改用账号密码并重新测试连接")
	}
	attemptedAt := time.Now().UTC()
	result, err := m.fetcher.Fetch(ctx, source.Provider, source.BaseURL, credentials)
	if err != nil {
		message := err.Error()
		_ = m.store.SaveUpstreamFailure(ctx, source.ID, attemptedAt, message)
		m.record(ctx, model.Event{Type: "balance_query_failed", Severity: "error", Message: source.Name + " 余额查询失败: " + message, Actor: "system", Details: fmt.Sprintf(`{"source_id":%d}`, source.ID)})
		return err
	}
	if err := m.applySuccess(ctx, &source, result); err != nil {
		return err
	}
	return m.reconcileCostRouting(ctx)
}

func (m *Manager) RefreshManual(ctx context.Context, id int64) error {
	now := time.Now().UTC()
	m.manualMu.Lock()
	if last := m.manualRuns[id]; !last.IsZero() && now.Sub(last) < 30*time.Second {
		m.manualMu.Unlock()
		return errors.New("该上游刚刚手动刷新过，请 30 秒后再试")
	}
	m.manualRuns[id] = now
	m.manualMu.Unlock()
	return m.Refresh(ctx, id)
}

func (m *Manager) Get(ctx context.Context, id int64) (model.UpstreamSource, error) {
	items, err := m.List(ctx)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return model.UpstreamSource{}, errors.New("上游配置不存在")
}

func (m *Manager) List(ctx context.Context) ([]model.UpstreamSource, error) {
	items, err := m.store.ListUpstreamSources(ctx)
	if err != nil {
		return nil, err
	}
	accounts, accountErr := m.api.ListAccounts(ctx)
	now := time.Now().UTC()
	for i := range items {
		credentials, err := m.decrypt(items[i])
		if err == nil {
			items[i].UsernameHint = maskUsername(credentials.Username)
			items[i].CredentialHint = credentialHint(credentials)
			if credentials.AuthMode == "access_key" || credentials.AccessKey != "" || credentials.Username == "" || credentials.Password == "" {
				items[i].CredentialMode = "access_key"
				items[i].MigrationRequired = true
			} else {
				items[i].CredentialMode = "password"
			}
		}
		items[i].CredentialNonce = nil
		items[i].CredentialCiphertext = nil
		items[i].KeyRates, _ = m.store.ListUpstreamKeyRates(ctx, items[i].ID)
		items[i].Groups, _ = m.store.ListUpstreamGroups(ctx, items[i].ID)
		items[i].FailoverPolicies, _ = m.store.ListGroupFailoverPolicies(ctx, items[i].ID)
		items[i].MatchedAccounts = []model.AccountRef{}
		if accountErr == nil {
			for _, account := range accounts {
				key, err := NormalizeURL(account.BaseURL(), true)
				if err == nil && key == items[i].NormalizedURL {
					items[i].MatchedAccounts = append(items[i].MatchedAccounts, model.AccountRef{ID: account.ID, Name: account.Name, Schedulable: account.Schedulable})
				}
			}
		}
		items[i].Stale = items[i].LastSuccessAt == nil || now.Sub(*items[i].LastSuccessAt) > 3*m.interval
	}
	return items, nil
}

func (m *Manager) refreshAll(ctx context.Context) {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	sources, err := m.store.ListUpstreamSources(ctx)
	if err != nil {
		m.logger.Error("balance_sources_read_failed", "error", err)
		return
	}
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	var successMu sync.Mutex
	refreshed := make(map[int64]bool)
	for _, source := range sources {
		if !source.Enabled {
			continue
		}
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := m.refreshLocked(ctx, id); err != nil {
				m.logger.Warn("balance_refresh_failed", "source_id", id, "error", err)
				return
			}
			successMu.Lock()
			refreshed[id] = true
			successMu.Unlock()
		}(source.ID)
	}
	wg.Wait()
	if err := m.reconcileGroupFailoverStatesForSourcesLocked(ctx, refreshed); err != nil {
		m.logger.Warn("group_failover_state_reconcile_failed", "error", err)
	}
	if err := m.reconcileCostRouting(ctx); err != nil {
		m.logger.Warn("cost_routing_failed", "error", err)
	}
	now := time.Now().UTC()
	m.lastRunMu.Lock()
	m.lastRunAt = &now
	m.lastRunMu.Unlock()
}

func (m *Manager) applySuccess(ctx context.Context, source *model.UpstreamSource, result model.UpstreamResult) error {
	wasLocked := source.BalanceLocked
	value := result.Balance
	now := result.FetchedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	source.Balance = &value
	source.Unit = result.Unit
	source.LastAttemptAt = &now
	source.LastSuccessAt = &now
	source.LastError = ""
	if !source.Enabled {
		source.LowStreak = 0
		source.RecoveryStreak = 0
		source.BalanceLocked = false
	} else if value < source.PauseBelow {
		source.LowStreak++
		source.RecoveryStreak = 0
		if source.LowStreak >= 2 {
			source.BalanceLocked = true
		}
	} else if value >= source.ResumeAt {
		source.RecoveryStreak++
		source.LowStreak = 0
		if source.RecoveryStreak >= 2 {
			source.BalanceLocked = false
		}
	} else {
		source.LowStreak = 0
		source.RecoveryStreak = 0
	}
	if result.RotatedAccessKey != "" {
		credentials, err := m.decrypt(*source)
		if err != nil {
			return err
		}
		// Password sessions rotate their own refresh token in the fetcher cache;
		// never replace the durable account password with that short-lived token.
		if credentials.AuthMode == "access_key" || credentials.AccessKey != "" {
			credentials.AccessKey = result.RotatedAccessKey
			nonce, ciphertext, err := m.encrypt(credentials)
			if err != nil {
				return err
			}
			source.CredentialNonce = nonce
			source.CredentialCiphertext = ciphertext
			if err := m.store.UpdateUpstreamSource(ctx, *source); err != nil {
				return err
			}
		}
	}
	if err := m.store.SaveUpstreamSuccess(ctx, *source, result.KeyRates, result.Groups); err != nil {
		return err
	}
	accounts, err := m.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("读取 Sub2API 账号失败: %w", err)
	}
	accountIDs := make([]int64, 0)
	for _, account := range accounts {
		key, err := NormalizeURL(account.BaseURL(), true)
		if err == nil && key == source.NormalizedURL {
			accountIDs = append(accountIDs, account.ID)
		}
	}
	changedAccounts, err := m.store.SyncBalanceLocksChanged(ctx, source.ID, accountIDs, source.Enabled && source.BalanceLocked)
	if err != nil {
		return err
	}
	if !wasLocked && source.BalanceLocked {
		m.record(ctx, model.Event{Type: "balance_low_detected", Severity: "warning", Message: fmt.Sprintf("%s 余额连续低于阈值，已建立余额锁", source.Name), BeforeState: "normal", AfterState: "balance_locked", Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"balance":%g,"unit":%q}`, source.ID, value, source.Unit)})
	}
	if wasLocked && !source.BalanceLocked {
		m.record(ctx, model.Event{Type: "balance_recovered", Severity: "info", Message: fmt.Sprintf("%s 余额连续达到恢复阈值，已解除余额锁", source.Name), BeforeState: "balance_locked", AfterState: "normal", Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"balance":%g,"unit":%q}`, source.ID, value, source.Unit)})
	}
	m.requestAccounts("balance_lock", changedAccounts)
	return nil
}

func (m *Manager) requestAccounts(source string, accountIDs []int64) {
	if len(accountIDs) == 0 {
		return
	}
	if requester, ok := m.trigger.(sourcedAccountRequester); ok {
		requester.RequestAccountsFrom(source, accountIDs...)
		return
	}
	if requester, ok := m.trigger.(accountRequester); ok {
		requester.RequestAccounts(accountIDs...)
		return
	}
	m.trigger.Trigger()
}

type routingSource struct {
	source          model.UpstreamSource
	rate            float64
	accounts        []model.Account
	available       bool
	activationReady bool
	serving         bool
}

func (m *Manager) reconcileCostRouting(ctx context.Context) error {
	m.routeMu.Lock()
	defer m.routeMu.Unlock()

	sources, err := m.store.ListUpstreamSources(ctx)
	if err != nil {
		return err
	}
	accounts, err := m.api.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("读取 Sub2API 账号失败: %w", err)
	}
	currentLocks, err := m.store.ListCostLocks(ctx)
	if err != nil {
		return err
	}
	lockByAccount := make(map[int64]model.CostLock, len(currentLocks))
	for _, item := range currentLocks {
		lockByAccount[item.AccountID] = item
	}

	pools := map[string][]routingSource{}
	poolSourceIDs := map[string]map[int64]bool{}
	now := time.Now().UTC()
	for _, source := range sources {
		pool := strings.TrimSpace(source.RoutingPool)
		if !source.Enabled || !source.RoutingEnabled || pool == "" {
			continue
		}
		if poolSourceIDs[pool] == nil {
			poolSourceIDs[pool] = map[int64]bool{}
		}
		poolSourceIDs[pool][source.ID] = true
		item := routingSource{source: source}
		rates, rateErr := m.store.ListUpstreamKeyRates(ctx, source.ID)
		if rateErr == nil {
			for _, rate := range rates {
				if rate.ExternalID == source.SelectedKeyID && !rate.Dynamic && rate.RateMultiplier != nil && keyRateActive(rate.Status) {
					item.rate = *rate.RateMultiplier
					break
				}
			}
		}
		for _, account := range accounts {
			normalized, normalizeErr := NormalizeURL(account.BaseURL(), true)
			if normalizeErr != nil || normalized != source.NormalizedURL {
				continue
			}
			item.accounts = append(item.accounts, account)
			control, controlErr := m.store.GetControl(ctx, account.ID)
			if controlErr != nil || account.Status != "active" || account.ErrorMessage != "" || control.HealthLocked || control.BalanceLocked || control.ManualLocked {
				continue
			}
			_, costLocked := lockByAccount[account.ID]
			if account.Schedulable && !costLocked {
				item.serving = true
			}
		}
		fresh := source.LastSuccessAt != nil && now.Sub(*source.LastSuccessAt) <= 3*m.interval
		item.available = item.rate > 0 && len(item.accounts) > 0 && source.Balance != nil && fresh && !source.BalanceLocked
		if item.available {
			usableAccount := false
			for _, account := range item.accounts {
				control, controlErr := m.store.GetControl(ctx, account.ID)
				if controlErr != nil || account.Status != "active" || account.ErrorMessage != "" || control.HealthLocked || control.BalanceLocked || control.ManualLocked {
					continue
				}
				_, costLocked := lockByAccount[account.ID]
				if account.Schedulable || (control.OwnsPause && costLocked) {
					usableAccount = true
					break
				}
			}
			item.available = usableAccount
		}
		if item.available && source.Balance != nil {
			item.activationReady = *source.Balance >= source.ResumeAt && source.RecoveryStreak >= 2
		}
		pools[pool] = append(pools[pool], item)
	}

	desired := make(map[int64]model.CostLock)
	for pool, items := range pools {
		currentRate := math.MaxFloat64
		currentHealthy := false
		for _, item := range items {
			if item.serving && item.available && item.rate < currentRate {
				currentRate = item.rate
				currentHealthy = true
			}
		}
		targetRate := math.MaxFloat64
		if currentHealthy {
			targetRate = currentRate
			for _, item := range items {
				if item.available && item.activationReady && item.rate < targetRate {
					targetRate = item.rate
				}
			}
		} else {
			for _, item := range items {
				if item.available && item.source.Balance != nil && *item.source.Balance >= item.source.PauseBelow && item.rate < targetRate {
					targetRate = item.rate
				}
			}
		}
		if targetRate == math.MaxFloat64 {
			for _, lock := range currentLocks {
				if lock.Pool == pool && poolSourceIDs[pool][lock.SourceID] {
					desired[lock.AccountID] = lock
				}
			}
			continue
		}
		targetServing := false
		for _, item := range items {
			if sameRate(item.rate, targetRate) && item.serving {
				targetServing = true
				break
			}
		}
		if !targetServing {
			for _, lock := range currentLocks {
				if lock.Pool != pool || !poolSourceIDs[pool][lock.SourceID] {
					continue
				}
				remove := false
				for _, item := range items {
					if sameRate(item.rate, targetRate) {
						for _, account := range item.accounts {
							if account.ID == lock.AccountID {
								remove = true
							}
						}
					}
				}
				if !remove {
					desired[lock.AccountID] = lock
				}
			}
			continue
		}
		for _, item := range items {
			if sameRate(item.rate, targetRate) {
				continue
			}
			for _, account := range item.accounts {
				lock := model.CostLock{SourceID: item.source.ID, AccountID: account.ID, Pool: pool, RateMultiplier: item.rate, CreatedAt: now}
				if previous, ok := lockByAccount[account.ID]; ok {
					lock.CreatedAt = previous.CreatedAt
				}
				desired[account.ID] = lock
			}
		}
	}

	desiredItems := make([]model.CostLock, 0, len(desired))
	for _, item := range desired {
		desiredItems = append(desiredItems, item)
	}
	if costLocksEqual(lockByAccount, desired) {
		return nil
	}
	changedAccounts, err := m.store.SyncCostLocksChanged(ctx, desiredItems)
	if err != nil {
		return err
	}
	for accountID, item := range desired {
		if _, exists := lockByAccount[accountID]; !exists {
			id := accountID
			m.record(ctx, model.Event{Type: "cost_tier_disabled", Severity: "info", AccountID: &id, Message: fmt.Sprintf("倍率池 %s 已优先使用更低倍率来源，高倍率账号进入待命", item.Pool), BeforeState: "schedulable", AfterState: "cost_locked", Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"pool":%q,"rate":%g}`, item.SourceID, item.Pool, item.RateMultiplier)})
		}
	}
	for accountID, item := range lockByAccount {
		if _, exists := desired[accountID]; !exists {
			id := accountID
			m.record(ctx, model.Event{Type: "cost_tier_enabled", Severity: "warning", AccountID: &id, Message: fmt.Sprintf("倍率池 %s 的低倍率来源不可用，已启用备用倍率账号", item.Pool), BeforeState: "cost_locked", AfterState: "schedulable", Actor: "system", Details: fmt.Sprintf(`{"source_id":%d,"pool":%q,"rate":%g}`, item.SourceID, item.Pool, item.RateMultiplier)})
		}
	}
	m.requestAccounts("cost_lock", changedAccounts)
	return nil
}

func keyRateActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "1", "active", "enabled", "normal":
		return true
	default:
		return false
	}
}

func sameRate(left, right float64) bool {
	return math.Abs(left-right) < 0.000001
}

func costLocksEqual(current, desired map[int64]model.CostLock) bool {
	if len(current) != len(desired) {
		return false
	}
	for accountID, left := range current {
		right, ok := desired[accountID]
		if !ok || left.SourceID != right.SourceID || left.Pool != right.Pool || !sameRate(left.RateMultiplier, right.RateMultiplier) {
			return false
		}
	}
	return true
}

func (m *Manager) validateInput(input SourceInput, requirePassword bool) error {
	if strings.TrimSpace(input.Name) == "" {
		return errors.New("站点名称不能为空")
	}
	provider := normalizeProvider(input.Provider)
	if provider != "newapi" && provider != "sub2" {
		return errors.New("上游类型只能是 New API 或 Sub2")
	}
	if _, err := NormalizeURL(input.BaseURL, m.fetcher.allowInsecure); err != nil {
		return err
	}
	if strings.TrimSpace(input.Username) == "" || (requirePassword && input.Password == "") {
		return errors.New("登录账号和密码不能为空")
	}
	if input.RoutingEnabled && (strings.TrimSpace(input.SelectedKeyID) == "" || strings.TrimSpace(input.RoutingPool) == "") {
		return errors.New("启用倍率调度时必须选择调度令牌并填写倍率池")
	}
	if input.PauseBelow < 0 || input.ResumeAt <= input.PauseBelow {
		return errors.New("恢复阈值必须大于停用阈值，且停用阈值不能为负数")
	}
	return nil
}

func (m *Manager) encrypt(credentials model.UpstreamCredentials) ([]byte, []byte, error) {
	if m.box == nil {
		return nil, nil, errors.New("服务器尚未配置上游凭据加密密钥")
	}
	payload, _ := json.Marshal(credentials)
	return m.box.Encrypt(payload)
}

func (m *Manager) decrypt(source model.UpstreamSource) (model.UpstreamCredentials, error) {
	if m.box == nil {
		return model.UpstreamCredentials{}, errors.New("服务器尚未配置上游凭据加密密钥")
	}
	payload, err := m.box.Decrypt(source.CredentialNonce, source.CredentialCiphertext)
	if err != nil {
		return model.UpstreamCredentials{}, err
	}
	var credentials model.UpstreamCredentials
	if err := json.Unmarshal(payload, &credentials); err != nil {
		return model.UpstreamCredentials{}, errors.New("上游凭据解密失败")
	}
	return credentials, nil
}

func (m *Manager) record(ctx context.Context, event model.Event) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if err := m.store.AddEvent(ctx, event); err != nil {
		m.logger.Error("balance_event_write_failed", "type", event.Type, "error", err)
	}
}

func normalizeProvider(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "new-api" {
		return "newapi"
	}
	if value == "sub2api" {
		return "sub2"
	}
	return value
}

func maskUsername(value string) string {
	value = strings.TrimSpace(value)
	if at := strings.IndexByte(value, '@'); at > 1 {
		return value[:1] + "***" + value[at:]
	}
	if len(value) <= 2 {
		return "**"
	}
	return value[:1] + "***" + value[len(value)-1:]
}

func credentialsFromInput(input SourceInput) model.UpstreamCredentials {
	return model.UpstreamCredentials{AuthMode: "password", Username: strings.TrimSpace(input.Username), Password: input.Password}
}

func credentialHint(credentials model.UpstreamCredentials) string {
	if credentials.AccessKey != "" {
		return "旧访问密钥（需要迁移）"
	}
	if credentials.Username != "" {
		return "账号密码 " + maskUsername(credentials.Username)
	}
	return "未配置"
}

func maskSecret(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 8 {
		return "已配置"
	}
	return value[:3] + "..." + value[len(value)-3:]
}
