package reconcile

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func NormalizeEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return "", fmt.Errorf("invalid endpoint %q", raw)
	}
	scheme := strings.ToLower(parsed.Scheme)
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	}
	return scheme + "://" + host, nil
}

func ResolveBindings(monitors []model.Monitor, accounts []model.Account, policies map[int64]model.Policy) ([]model.ResolvedBinding, []model.Monitor, []string) {
	monitorByID := make(map[int64]*model.Monitor, len(monitors))
	monitorsByKey := make(map[string][]*model.Monitor)
	for i := range monitors {
		monitor := &monitors[i]
		monitorByID[monitor.ID] = monitor
		key, err := NormalizeEndpoint(monitor.Endpoint)
		if err == nil {
			monitorsByKey[monitor.Provider+"|"+key] = append(monitorsByKey[monitor.Provider+"|"+key], monitor)
		}
	}

	resolved := make([]model.ResolvedBinding, 0, len(accounts))
	usedMonitors := make(map[int64]bool)
	conflicts := make([]string, 0)
	for _, account := range accounts {
		baseURL := strings.TrimSpace(account.BaseURL())
		if baseURL == "" {
			continue
		}
		policy, ok := policies[account.ID]
		if !ok {
			policy = model.Policy{AccountID: account.ID, Enabled: true}
		}
		binding := model.ResolvedBinding{Account: account, Policy: policy, Source: "auto", State: "unmatched"}
		key, err := NormalizeEndpoint(baseURL)
		if err != nil {
			binding.Reason = "账号上游地址无效"
			resolved = append(resolved, binding)
			continue
		}
		binding.NormalizedEndpoint = key
		if policy.Excluded || !policy.Enabled {
			binding.State = "excluded"
			binding.Reason = "已排除自动调度"
			resolved = append(resolved, binding)
			continue
		}
		if policy.MonitorID != nil {
			binding.Source = "manual"
			monitor := monitorByID[*policy.MonitorID]
			if monitor == nil {
				binding.State = "frozen"
				binding.Reason = "指定监控不存在"
			} else if !providerCompatible(monitor.Provider, account.Platform) {
				binding.State = "conflict"
				binding.Reason = "指定监控与账号平台不兼容"
				conflicts = append(conflicts, fmt.Sprintf("账号 %d 的指定监控平台不兼容", account.ID))
			} else {
				binding.Monitor = monitor
				binding.State = "bound"
				usedMonitors[monitor.ID] = true
			}
			resolved = append(resolved, binding)
			continue
		}
		candidates := monitorsByKey[account.Platform+"|"+key]
		switch len(candidates) {
		case 0:
			binding.Reason = "没有相同上游地址的监控"
		case 1:
			binding.Monitor = candidates[0]
			binding.State = "bound"
			usedMonitors[candidates[0].ID] = true
		default:
			binding.State = "conflict"
			binding.Reason = "同一上游地址存在多个监控"
			conflicts = append(conflicts, fmt.Sprintf("账号 %d 匹配到 %d 个监控", account.ID, len(candidates)))
		}
		resolved = append(resolved, binding)
	}

	unmatched := make([]model.Monitor, 0)
	for _, monitor := range monitors {
		if !usedMonitors[monitor.ID] {
			unmatched = append(unmatched, monitor)
		}
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Account.ID < resolved[j].Account.ID })
	sort.Slice(unmatched, func(i, j int) bool { return unmatched[i].ID < unmatched[j].ID })
	return resolved, unmatched, conflicts
}

func providerCompatible(provider, platform string) bool {
	return strings.EqualFold(strings.TrimSpace(provider), strings.TrimSpace(platform))
}
