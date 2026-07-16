package balance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
)

const maxResponseBytes = 8 << 20

type Fetcher struct {
	timeout       time.Duration
	allowInsecure bool
	sessionMu     sync.Mutex
	newSessions   map[string]newAPISession
	sub2Sessions  map[string]sub2Session
}

type newAPISession struct {
	Jar    http.CookieJar
	UserID int
}

type sub2Session struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	Direct       bool
}

func NewFetcher(timeout time.Duration, allowInsecure bool) *Fetcher {
	return &Fetcher{timeout: timeout, allowInsecure: allowInsecure, newSessions: map[string]newAPISession{}, sub2Sessions: map[string]sub2Session{}}
}

func (f *Fetcher) Fetch(ctx context.Context, provider, baseURL string, credentials model.UpstreamCredentials) (model.UpstreamResult, error) {
	normalized, err := NormalizeURL(baseURL, f.allowInsecure)
	if err != nil {
		return model.UpstreamResult{}, err
	}
	if strings.TrimSpace(credentials.AccessKey) == "" && (strings.TrimSpace(credentials.Username) == "" || credentials.Password == "") {
		return model.UpstreamResult{}, errors.New("访问密钥不能为空")
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "newapi", "new-api":
		return f.fetchNewAPI(ctx, normalized, credentials)
	case "sub2", "sub2api":
		return f.fetchSub2(ctx, normalized, credentials)
	default:
		return model.UpstreamResult{}, fmt.Errorf("不支持的上游类型 %q", provider)
	}
}

func (f *Fetcher) SwitchGroup(ctx context.Context, provider, baseURL string, credentials model.UpstreamCredentials, keyID, groupID string) (model.UpstreamResult, error) {
	normalized, err := NormalizeURL(baseURL, f.allowInsecure)
	if err != nil {
		return model.UpstreamResult{}, err
	}
	keyID = strings.TrimSpace(keyID)
	groupID = strings.TrimSpace(groupID)
	if keyID == "" || groupID == "" {
		return model.UpstreamResult{}, errors.New("令牌和目标分组不能为空")
	}
	switch normalizeProvider(provider) {
	case "newapi":
		client, headers, _, err := f.newAPIManagementClient(ctx, normalized, credentials)
		if err != nil {
			return model.UpstreamResult{}, err
		}
		var detail newAPIEnvelope
		if err := requestJSON(ctx, client, http.MethodGet, normalized+"/api/token/"+url.PathEscape(keyID), nil, headers, &detail); err != nil || !detail.Success {
			return model.UpstreamResult{}, errors.New("无法读取 New API 令牌详情")
		}
		var payload map[string]any
		if err := json.Unmarshal(detail.Data, &payload); err != nil {
			return model.UpstreamResult{}, errors.New("New API 令牌详情格式不兼容")
		}
		if _, ok := payload["id"]; !ok {
			id, err := strconv.ParseInt(keyID, 10, 64)
			if err != nil {
				return model.UpstreamResult{}, errors.New("New API 令牌编号无效")
			}
			payload["id"] = id
		}
		payload["group"] = groupID
		var updated newAPIEnvelope
		if err := requestJSON(ctx, client, http.MethodPut, normalized+"/api/token/", payload, headers, &updated); err != nil {
			return model.UpstreamResult{}, mutation.Wrap("New API 切换令牌分组", err)
		}
		if !updated.Success {
			return model.UpstreamResult{}, fmt.Errorf("New API 切换令牌分组失败: %s", fallback(updated.Message, "上游拒绝修改"))
		}
	case "sub2":
		client := f.newClient(normalized, nil)
		sessionKey := normalized + "|" + credentialIdentity(credentials)
		session, err := f.getSub2Session(ctx, client, normalized, sessionKey, credentials)
		if err != nil {
			return model.UpstreamResult{}, err
		}
		keyNumber, keyErr := strconv.ParseInt(keyID, 10, 64)
		groupNumber, groupErr := strconv.ParseInt(groupID, 10, 64)
		if keyErr != nil || keyNumber <= 0 || groupErr != nil || groupNumber <= 0 {
			return model.UpstreamResult{}, errors.New("Sub2 令牌或分组编号无效")
		}
		headers := map[string]string{"Authorization": "Bearer " + session.AccessToken}
		var updated sub2Envelope
		endpoint := fmt.Sprintf("%s/api/v1/keys/%d", normalized, keyNumber)
		if err := requestJSON(ctx, client, http.MethodPut, endpoint, map[string]any{"group_id": groupNumber}, headers, &updated); err != nil {
			return model.UpstreamResult{}, mutation.Wrap("Sub2 切换令牌分组", err)
		}
		if updated.Code != 0 {
			return model.UpstreamResult{}, fmt.Errorf("Sub2 切换令牌分组失败: %s", fallback(updated.Message, "上游拒绝修改"))
		}
	default:
		return model.UpstreamResult{}, fmt.Errorf("不支持的上游类型 %q", provider)
	}
	result, err := f.Fetch(ctx, provider, normalized, credentials)
	if err != nil {
		return model.UpstreamResult{}, mutation.Wrap("令牌分组已提交但写后确认失败", err)
	}
	for _, item := range result.KeyRates {
		if item.ExternalID == keyID {
			if item.GroupID != groupID {
				return model.UpstreamResult{}, mutation.Wrap("令牌分组已提交但写后仍读取到旧分组", nil)
			}
			return result, nil
		}
	}
	return model.UpstreamResult{}, mutation.Wrap("令牌分组已提交但确认时未找到目标令牌", nil)
}

func (f *Fetcher) Invalidate(provider, baseURL string, credentials model.UpstreamCredentials) {
	normalized, err := NormalizeURL(baseURL, f.allowInsecure)
	if err != nil {
		return
	}
	key := normalized + "|" + credentialIdentity(credentials)
	f.sessionMu.Lock()
	defer f.sessionMu.Unlock()
	if normalizeProvider(provider) == "newapi" {
		delete(f.newSessions, key)
	} else {
		delete(f.sub2Sessions, key)
	}
}

func NormalizeURL(raw string, allowInsecure bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Hostname() == "" || parsed.Scheme == "" {
		return "", errors.New("上游地址无效")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "https" && !(allowInsecure && parsed.Scheme == "http") {
		return "", errors.New("上游地址必须使用 HTTPS")
	}
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if (parsed.Scheme == "https" && port == "443") || (parsed.Scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	}
	return parsed.Scheme + "://" + host, nil
}

func (f *Fetcher) newClient(baseURL string, jar http.CookieJar) *http.Client {
	base, _ := url.Parse(baseURL)
	return &http.Client{
		Timeout: f.timeout,
		Jar:     jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 || !strings.EqualFold(req.URL.Hostname(), base.Hostname()) {
				return errors.New("拒绝携带凭据跳转到其他主机")
			}
			return nil
		},
	}
}

type newAPIEnvelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (f *Fetcher) fetchNewAPI(ctx context.Context, baseURL string, credentials model.UpstreamCredentials) (model.UpstreamResult, error) {
	client, headers, cached, err := f.newAPIManagementClient(ctx, baseURL, credentials)
	if err != nil {
		return model.UpstreamResult{}, err
	}
	sessionKey := baseURL + "|" + credentialIdentity(credentials)

	var status newAPIEnvelope
	if err := requestJSON(ctx, client, http.MethodGet, baseURL+"/api/status", nil, nil, &status); err != nil || !status.Success {
		return model.UpstreamResult{}, errors.New("无法读取 New API 计价单位")
	}
	var statusData struct {
		QuotaPerUnit               float64 `json:"quota_per_unit"`
		QuotaDisplayType           string  `json:"quota_display_type"`
		USDExchangeRate            float64 `json:"usd_exchange_rate"`
		CustomCurrencySymbol       string  `json:"custom_currency_symbol"`
		CustomCurrencyExchangeRate float64 `json:"custom_currency_exchange_rate"`
	}
	_ = json.Unmarshal(status.Data, &statusData)

	var self newAPIEnvelope
	if err := requestJSON(ctx, client, http.MethodGet, baseURL+"/api/user/self", nil, headers, &self); err != nil || !self.Success {
		if cached && credentials.AccessKey == "" {
			f.sessionMu.Lock()
			delete(f.newSessions, sessionKey)
			f.sessionMu.Unlock()
			return f.fetchNewAPI(ctx, baseURL, credentials)
		}
		return model.UpstreamResult{}, errors.New("无法读取 New API 账户余额，请确认使用的是用户访问令牌而不是模型调用密钥")
	}
	var user struct {
		Username string  `json:"username"`
		Quota    float64 `json:"quota"`
	}
	if err := json.Unmarshal(self.Data, &user); err != nil {
		return model.UpstreamResult{}, errors.New("New API 余额响应格式不兼容")
	}
	balance, unit := convertNewAPIQuota(user.Quota, statusData.QuotaPerUnit, statusData.QuotaDisplayType, statusData.USDExchangeRate, statusData.CustomCurrencySymbol, statusData.CustomCurrencyExchangeRate)

	var groupsEnvelope newAPIEnvelope
	if err := requestJSON(ctx, client, http.MethodGet, baseURL+"/api/user/self/groups", nil, headers, &groupsEnvelope); err != nil || !groupsEnvelope.Success {
		return model.UpstreamResult{}, errors.New("无法读取 New API 分组倍率")
	}
	var groups map[string]struct {
		Ratio any    `json:"ratio"`
		Desc  string `json:"desc"`
	}
	_ = json.Unmarshal(groupsEnvelope.Data, &groups)
	groupItems := make([]model.UpstreamGroup, 0, len(groups))
	for id, group := range groups {
		if rate, ok := numberValue(group.Ratio); ok {
			groupItems = append(groupItems, model.UpstreamGroup{ExternalID: id, Name: id, RateMultiplier: rate})
		}
	}
	sort.Slice(groupItems, func(i, j int) bool {
		if groupItems[i].RateMultiplier == groupItems[j].RateMultiplier {
			return groupItems[i].Name < groupItems[j].Name
		}
		return groupItems[i].RateMultiplier < groupItems[j].RateMultiplier
	})

	rates := make([]model.KeyRate, 0)
	for page := 0; page < 100; page++ {
		var tokenEnvelope newAPIEnvelope
		path := fmt.Sprintf("%s/api/token/?p=%d&size=100", baseURL, page)
		if err := requestJSON(ctx, client, http.MethodGet, path, nil, headers, &tokenEnvelope); err != nil || !tokenEnvelope.Success {
			return model.UpstreamResult{}, errors.New("无法读取 New API 密钥列表")
		}
		var pageData struct {
			Items []struct {
				ID     int64  `json:"id"`
				Name   string `json:"name"`
				Key    string `json:"key"`
				Group  string `json:"group"`
				Status int    `json:"status"`
			} `json:"items"`
			Total int `json:"total"`
		}
		if err := json.Unmarshal(tokenEnvelope.Data, &pageData); err != nil {
			return model.UpstreamResult{}, errors.New("New API 密钥响应格式不兼容")
		}
		for _, token := range pageData.Items {
			rate := model.KeyRate{ExternalID: strconv.FormatInt(token.ID, 10), Name: token.Name, KeyHint: maskKey(token.Key), GroupID: token.Group, GroupName: token.Group, Status: strconv.Itoa(token.Status)}
			group, ok := groups[token.Group]
			if token.Group == "" || strings.EqualFold(token.Group, "auto") || !ok {
				rate.Dynamic = true
			} else if parsed, ok := numberValue(group.Ratio); ok {
				rate.RateMultiplier = &parsed
			} else {
				rate.Dynamic = true
			}
			rates = append(rates, rate)
		}
		if len(pageData.Items) == 0 || len(rates) >= pageData.Total {
			break
		}
	}
	return model.UpstreamResult{Balance: balance, Unit: unit, Username: fallback(user.Username, credentials.Username), KeyRates: rates, Groups: groupItems, FetchedAt: time.Now().UTC()}, nil
}

func (f *Fetcher) newAPIManagementClient(ctx context.Context, baseURL string, credentials model.UpstreamCredentials) (*http.Client, map[string]string, bool, error) {
	if strings.TrimSpace(credentials.AccessKey) != "" {
		userID, err := strconv.Atoi(strings.TrimSpace(credentials.UserID))
		if err != nil || userID <= 0 {
			return nil, nil, false, errors.New("New API 使用访问令牌时必须填写用户编号")
		}
		client := f.newClient(baseURL, nil)
		headers := map[string]string{
			"Authorization": "Bearer " + strings.TrimSpace(credentials.AccessKey),
			"New-Api-User":  strconv.Itoa(userID),
		}
		return client, headers, false, nil
	}

	sessionKey := baseURL + "|" + credentialIdentity(credentials)
	f.sessionMu.Lock()
	session, cached := f.newSessions[sessionKey]
	f.sessionMu.Unlock()
	if !cached {
		jar, _ := cookiejar.New(nil)
		session = newAPISession{Jar: jar}
	}
	client := f.newClient(baseURL, session.Jar)
	if !cached {
		loginBody := map[string]string{"username": credentials.Username, "password": credentials.Password}
		var login newAPIEnvelope
		if err := requestJSON(ctx, client, http.MethodPost, baseURL+"/api/user/login", loginBody, nil, &login); err != nil {
			return nil, nil, false, fmt.Errorf("New API 登录失败: %w", err)
		}
		if !login.Success {
			return nil, nil, false, fmt.Errorf("New API 登录失败: %s", fallback(login.Message, "账号或密码错误"))
		}
		var loginData struct {
			ID         int  `json:"id"`
			Require2FA bool `json:"require_2fa"`
		}
		if err := json.Unmarshal(login.Data, &loginData); err != nil {
			return nil, nil, false, errors.New("New API 登录响应格式不兼容")
		}
		if loginData.Require2FA {
			return nil, nil, false, errors.New("该 New API 账户启用了双重验证，第一版仅支持账号密码")
		}
		if loginData.ID <= 0 {
			return nil, nil, false, errors.New("New API 登录未返回用户编号，可能被验证码拦截")
		}
		session.UserID = loginData.ID
		f.sessionMu.Lock()
		f.newSessions[sessionKey] = session
		f.sessionMu.Unlock()
	}
	return client, map[string]string{"New-Api-User": strconv.Itoa(session.UserID)}, cached, nil
}

type sub2Envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (f *Fetcher) fetchSub2(ctx context.Context, baseURL string, credentials model.UpstreamCredentials) (model.UpstreamResult, error) {
	client := f.newClient(baseURL, nil)
	sessionKey := baseURL + "|" + credentialIdentity(credentials)
	session, err := f.getSub2Session(ctx, client, baseURL, sessionKey, credentials)
	if err != nil {
		return model.UpstreamResult{}, err
	}
	headers := map[string]string{"Authorization": "Bearer " + session.AccessToken}

	var profile sub2Envelope
	profileLoaded := false
	for _, endpoint := range []string{"/api/v1/auth/me", "/api/v1/user/profile"} {
		profile = sub2Envelope{}
		if err := requestJSON(ctx, client, http.MethodGet, baseURL+endpoint, nil, headers, &profile); err == nil && profile.Code == 0 {
			profileLoaded = true
			break
		}
	}
	if !profileLoaded {
		return model.UpstreamResult{}, errors.New("无法读取 Sub2 账户余额，请确认填写的是刷新密钥或有效用户访问令牌，而不是模型调用密钥")
	}
	var user struct {
		Username string  `json:"username"`
		Email    string  `json:"email"`
		Balance  float64 `json:"balance"`
	}
	if err := json.Unmarshal(profile.Data, &user); err != nil {
		return model.UpstreamResult{}, errors.New("Sub2 余额响应格式不兼容")
	}

	groups := map[int64]struct {
		Name string
		Rate float64
	}{}
	var groupEnvelope sub2Envelope
	if err := requestJSON(ctx, client, http.MethodGet, baseURL+"/api/v1/groups/available", nil, headers, &groupEnvelope); err != nil || groupEnvelope.Code != 0 {
		return model.UpstreamResult{}, errors.New("无法读取 Sub2 分组")
	}
	var groupItems []struct {
		ID             int64   `json:"id"`
		Name           string  `json:"name"`
		RateMultiplier float64 `json:"rate_multiplier"`
	}
	_ = json.Unmarshal(groupEnvelope.Data, &groupItems)
	for _, group := range groupItems {
		groups[group.ID] = struct {
			Name string
			Rate float64
		}{group.Name, group.RateMultiplier}
	}
	var customEnvelope sub2Envelope
	if err := requestJSON(ctx, client, http.MethodGet, baseURL+"/api/v1/groups/rates", nil, headers, &customEnvelope); err == nil && customEnvelope.Code == 0 {
		var custom map[string]float64
		_ = json.Unmarshal(customEnvelope.Data, &custom)
		for rawID, rate := range custom {
			if id, err := strconv.ParseInt(rawID, 10, 64); err == nil {
				group := groups[id]
				group.Rate = rate
				groups[id] = group
			}
		}
	}

	rates := make([]model.KeyRate, 0)
	keyEndpoint := ""
	for page := 1; page < 100; page++ {
		var keyEnvelope sub2Envelope
		endpoints := []string{keyEndpoint}
		if keyEndpoint == "" {
			endpoints = []string{"/api/v1/keys", "/api/v1/api-keys"}
		}
		keyPageLoaded := false
		for _, endpoint := range endpoints {
			keyEnvelope = sub2Envelope{}
			path := fmt.Sprintf("%s%s?page=%d&page_size=100", baseURL, endpoint, page)
			if err := requestJSON(ctx, client, http.MethodGet, path, nil, headers, &keyEnvelope); err == nil && keyEnvelope.Code == 0 {
				keyEndpoint = endpoint
				keyPageLoaded = true
				break
			}
		}
		if !keyPageLoaded {
			return model.UpstreamResult{}, errors.New("无法读取 Sub2 密钥列表")
		}
		var pageData struct {
			Items []struct {
				ID      int64  `json:"id"`
				Name    string `json:"name"`
				Key     string `json:"key"`
				GroupID *int64 `json:"group_id"`
				Status  string `json:"status"`
			} `json:"items"`
			Total int `json:"total"`
		}
		if err := json.Unmarshal(keyEnvelope.Data, &pageData); err != nil {
			return model.UpstreamResult{}, errors.New("Sub2 密钥响应格式不兼容")
		}
		for _, key := range pageData.Items {
			rate := model.KeyRate{ExternalID: strconv.FormatInt(key.ID, 10), Name: key.Name, KeyHint: maskKey(key.Key), Status: key.Status, Dynamic: key.GroupID == nil}
			if key.GroupID != nil {
				rate.GroupID = strconv.FormatInt(*key.GroupID, 10)
				if group, ok := groups[*key.GroupID]; ok {
					rate.GroupName = group.Name
					rate.RateMultiplier = &group.Rate
				} else {
					rate.Dynamic = true
				}
			}
			rates = append(rates, rate)
		}
		if len(pageData.Items) == 0 || len(rates) >= pageData.Total {
			break
		}
	}
	username := user.Username
	if username == "" {
		username = user.Email
	}
	availableGroups := make([]model.UpstreamGroup, 0, len(groups))
	for id, group := range groups {
		availableGroups = append(availableGroups, model.UpstreamGroup{ExternalID: strconv.FormatInt(id, 10), Name: group.Name, RateMultiplier: group.Rate})
	}
	sort.Slice(availableGroups, func(i, j int) bool {
		if availableGroups[i].RateMultiplier == availableGroups[j].RateMultiplier {
			return availableGroups[i].Name < availableGroups[j].Name
		}
		return availableGroups[i].RateMultiplier < availableGroups[j].RateMultiplier
	})
	result := model.UpstreamResult{Balance: user.Balance, Unit: "USD", Username: fallback(username, credentials.Username), KeyRates: rates, Groups: availableGroups, FetchedAt: time.Now().UTC()}
	if session.Direct {
		result.CredentialWarning = "当前使用短期访问令牌，令牌过期后需要重新配置；建议改用刷新密钥"
	} else if session.RefreshToken != "" && session.RefreshToken != credentials.AccessKey {
		result.RotatedAccessKey = session.RefreshToken
	}
	return result, nil
}

func (f *Fetcher) getSub2Session(ctx context.Context, client *http.Client, baseURL, sessionKey string, credentials model.UpstreamCredentials) (sub2Session, error) {
	f.sessionMu.Lock()
	session := f.sub2Sessions[sessionKey]
	f.sessionMu.Unlock()
	if session.AccessToken != "" && time.Now().Add(time.Minute).Before(session.ExpiresAt) {
		return session, nil
	}
	refreshToken := session.RefreshToken
	if refreshToken == "" && credentials.AccessKey != "" {
		refreshToken = strings.TrimSpace(credentials.AccessKey)
	}
	if refreshToken != "" {
		var refresh sub2Envelope
		err := requestJSON(ctx, client, http.MethodPost, baseURL+"/api/v1/auth/refresh", map[string]string{"refresh_token": refreshToken}, nil, &refresh)
		if err == nil && refresh.Code == 0 {
			var data struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				ExpiresIn    int    `json:"expires_in"`
			}
			if json.Unmarshal(refresh.Data, &data) == nil && data.AccessToken != "" {
				session = sub2Session{AccessToken: data.AccessToken, RefreshToken: fallback(data.RefreshToken, refreshToken), ExpiresAt: tokenExpiry(data.ExpiresIn)}
				f.sessionMu.Lock()
				f.sub2Sessions[sessionKey] = session
				f.sessionMu.Unlock()
				return session, nil
			}
		}
	}
	if strings.TrimSpace(credentials.AccessKey) != "" {
		session = sub2Session{AccessToken: strings.TrimSpace(credentials.AccessKey), ExpiresAt: time.Now().UTC().Add(10 * time.Minute), Direct: true}
		f.sessionMu.Lock()
		f.sub2Sessions[sessionKey] = session
		f.sessionMu.Unlock()
		return session, nil
	}

	var login sub2Envelope
	if err := requestJSON(ctx, client, http.MethodPost, baseURL+"/api/v1/auth/login", map[string]string{"email": credentials.Username, "password": credentials.Password}, nil, &login); err != nil {
		return sub2Session{}, fmt.Errorf("Sub2 登录失败: %w", err)
	}
	if login.Code != 0 {
		return sub2Session{}, fmt.Errorf("Sub2 登录失败: %s", fallback(login.Message, "账号或密码错误"))
	}
	var loginData struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Requires2FA  bool   `json:"requires_2fa"`
	}
	if err := json.Unmarshal(login.Data, &loginData); err != nil || loginData.AccessToken == "" {
		if loginData.Requires2FA {
			return sub2Session{}, errors.New("该 Sub2 账户启用了双重验证，第一版仅支持账号密码")
		}
		return sub2Session{}, errors.New("Sub2 登录未返回访问令牌，可能被验证码或双重验证拦截")
	}
	session = sub2Session{AccessToken: loginData.AccessToken, RefreshToken: loginData.RefreshToken, ExpiresAt: tokenExpiry(loginData.ExpiresIn)}
	f.sessionMu.Lock()
	f.sub2Sessions[sessionKey] = session
	f.sessionMu.Unlock()
	return session, nil
}

func tokenExpiry(expiresIn int) time.Time {
	if expiresIn <= 0 {
		expiresIn = 15 * 60
	}
	return time.Now().UTC().Add(time.Duration(expiresIn) * time.Second)
}

func requestJSON(ctx context.Context, client *http.Client, method, endpoint string, body any, headers map[string]string, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("上游返回 HTTP %d", resp.StatusCode)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return errors.New("上游返回了无法识别的数据")
	}
	return nil
}

func convertNewAPIQuota(quota, perUnit float64, displayType string, usdRate float64, customSymbol string, customRate float64) (float64, string) {
	if perUnit <= 0 {
		perUnit = 500000
	}
	usd := quota / perUnit
	switch strings.ToUpper(strings.TrimSpace(displayType)) {
	case "TOKENS":
		return quota, "TOKENS"
	case "CNY":
		if usdRate <= 0 {
			usdRate = 7
		}
		return usd * usdRate, "CNY"
	case "CUSTOM":
		if customRate <= 0 {
			customRate = 1
		}
		return usd * customRate, fallback(customSymbol, "CUSTOM")
	default:
		return usd, "USD"
	}
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case string:
		value, err := strconv.ParseFloat(typed, 64)
		return value, err == nil
	default:
		return 0, false
	}
}

func maskKey(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 4 {
		return "****"
	}
	if len(value) <= 8 {
		return value[:2] + "..." + value[len(value)-2:]
	}
	return value[:4] + "..." + value[len(value)-4:]
}

func fallback(value, fallbackValue string) string {
	if strings.TrimSpace(value) == "" {
		return fallbackValue
	}
	return value
}

func credentialIdentity(credentials model.UpstreamCredentials) string {
	if strings.TrimSpace(credentials.AccessKey) != "" {
		sum := sha256.Sum256([]byte(strings.TrimSpace(credentials.AccessKey)))
		return fmt.Sprintf("key:%x", sum[:8])
	}
	return "user:" + strings.TrimSpace(credentials.Username)
}
