package sub2api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const defaultTelemetryPageSize = 100

// TelemetryQuery limits the read-only operations endpoints. Times are sent as
// RFC3339 so the query is unambiguous across scheduler and Sub2API time zones.
type TelemetryQuery struct {
	AccountID int64
	Since     time.Time
	Until     time.Time
	PageSize  int
	Kind      string
}

type telemetryPage[T any] struct {
	Items    []T   `json:"items"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Pages    int   `json:"pages"`
}

func (p *telemetryPage[T]) UnmarshalJSON(payload []byte) error {
	if len(payload) > 0 && payload[0] == '[' {
		return json.Unmarshal(payload, &p.Items)
	}
	var object struct {
		Items    []T   `json:"items"`
		Records  []T   `json:"records"`
		List     []T   `json:"list"`
		Total    int64 `json:"total"`
		Page     int   `json:"page"`
		PageSize int   `json:"page_size"`
		Pages    int   `json:"pages"`
	}
	if err := json.Unmarshal(payload, &object); err != nil {
		return err
	}
	p.Items = object.Items
	if p.Items == nil {
		p.Items = object.Records
	}
	if p.Items == nil {
		p.Items = object.List
	}
	p.Total, p.Page, p.PageSize, p.Pages = object.Total, object.Page, object.PageSize, object.Pages
	return nil
}

type apiTime struct{ time.Time }

func (t *apiTime) UnmarshalJSON(payload []byte) error {
	if string(payload) == "null" || string(payload) == `""` {
		return nil
	}
	var value string
	if len(payload) > 0 && payload[0] == '"' {
		if err := json.Unmarshal(payload, &value); err != nil {
			return err
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, value); err == nil {
				t.Time = parsed.UTC()
				return nil
			}
		}
		return fmt.Errorf("unsupported Sub2API time %q", value)
	}
	var number json.Number
	if err := json.Unmarshal(payload, &number); err != nil {
		return err
	}
	value64, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return err
	}
	if value64 > 10_000_000_000 {
		t.Time = time.UnixMilli(value64).UTC()
	} else {
		t.Time = time.Unix(value64, 0).UTC()
	}
	return nil
}

type monitorHistoryWire struct {
	ID            int64   `json:"id"`
	MonitorID     int64   `json:"monitor_id"`
	Model         string  `json:"model"`
	Status        string  `json:"status"`
	LatencyMS     int64   `json:"latency_ms"`
	PingLatencyMS int64   `json:"ping_latency_ms"`
	StatusCode    int     `json:"status_code"`
	Message       string  `json:"message"`
	CheckedAt     apiTime `json:"checked_at"`
}

type successWire struct {
	AccountID     int64   `json:"account_id"`
	RequestID     string  `json:"request_id"`
	Model         string  `json:"model"`
	UpstreamModel string  `json:"upstream_model"`
	DurationMS    int64   `json:"duration_ms"`
	Kind          string  `json:"kind"`
	RequestKind   string  `json:"request_kind"`
	CreatedAt     apiTime `json:"created_at"`
}

type errorWire struct {
	AccountID  int64   `json:"account_id"`
	RequestID  string  `json:"request_id"`
	Model      string  `json:"model"`
	Requested  string  `json:"requested_model"`
	Upstream   string  `json:"upstream_model"`
	Phase      string  `json:"phase"`
	Type       string  `json:"type"`
	Severity   string  `json:"severity"`
	StatusCode int     `json:"status_code"`
	Message    string  `json:"message"`
	CreatedAt  apiTime `json:"created_at"`
}

func (c *Client) ListMonitorHistory(ctx context.Context, monitorID int64, query TelemetryQuery) ([]model.MonitorHistoryRecord, error) {
	if monitorID <= 0 {
		return nil, fmt.Errorf("monitor_id must be positive")
	}
	basePath := fmt.Sprintf("/api/v1/admin/channel-monitors/%d/history", monitorID)
	wires, err := listTelemetry[monitorHistoryWire](ctx, c, basePath, query)
	if err != nil {
		return nil, err
	}
	items := make([]model.MonitorHistoryRecord, 0, len(wires))
	seen := make(map[string]struct{}, len(wires))
	for _, wire := range wires {
		class, reason := "", ""
		if strings.EqualFold(wire.Status, model.StatusDegraded) {
			class, reason = "", "performance_degraded"
		} else if !strings.EqualFold(wire.Status, model.StatusOperational) {
			class, reason = ClassifyDiagnostic(wire.StatusCode, wire.TypeName(), "", wire.Message)
		}
		resolvedMonitorID := wire.MonitorID
		if resolvedMonitorID == 0 {
			resolvedMonitorID = monitorID
		}
		item := model.MonitorHistoryRecord{
			SourceID: wire.ID, MonitorID: resolvedMonitorID, Model: strings.TrimSpace(wire.Model),
			Status: strings.ToLower(strings.TrimSpace(wire.Status)), LatencyMS: wire.LatencyMS,
			PingLatencyMS: wire.PingLatencyMS, StatusCode: wire.StatusCode, ErrorClass: class,
			ReasonCode: reason, ReasonFingerprint: diagnosticFingerprint(wire.Message), CheckedAt: wire.CheckedAt.Time,
		}
		identity := fmt.Sprintf("%d\x00%s\x00%s", item.MonitorID, item.Model, item.CheckedAt.UTC().Format(time.RFC3339Nano))
		if _, exists := seen[identity]; exists {
			continue
		}
		seen[identity] = struct{}{}
		items = append(items, item)
	}
	return items, nil
}

func (w monitorHistoryWire) TypeName() string {
	if strings.EqualFold(w.Status, model.StatusOperational) || strings.EqualFold(w.Status, model.StatusDegraded) {
		return ""
	}
	return w.Status
}

func (c *Client) ListSuccessfulRequests(ctx context.Context, query TelemetryQuery) ([]model.TrafficSuccess, error) {
	// Newer Sub2API versions expose a unified success + error request list.
	// Filtering server-side reduces traffic, while the conversion below still
	// verifies every item because older versions may ignore this parameter.
	query.Kind = "success"
	if query.PageSize <= 0 || query.PageSize > 100 {
		query.PageSize = 100
	}
	wires, err := listTelemetry[successWire](ctx, c, "/api/v1/admin/ops/requests", query)
	if err != nil {
		return nil, err
	}
	items := make([]model.TrafficSuccess, 0, len(wires))
	for _, wire := range wires {
		recordKind := strings.ToLower(strings.TrimSpace(wire.Kind))
		if recordKind == "error" || wire.AccountID <= 0 || wire.CreatedAt.Time.IsZero() {
			// account_id is nullable in Sub2API operations records. An
			// unowned record cannot be used as account-level scheduler evidence.
			continue
		}
		requestKind := strings.TrimSpace(wire.RequestKind)
		if requestKind == "" && recordKind != "success" {
			// Legacy versions used kind for the request kind rather than for
			// the success/error discriminator.
			requestKind = strings.TrimSpace(wire.Kind)
		}
		items = append(items, model.TrafficSuccess{
			EventKey:  eventFingerprint("success", wire.RequestID, wire.AccountID, wire.Model, wire.CreatedAt.Time),
			AccountID: wire.AccountID, Model: strings.TrimSpace(wire.Model), UpstreamModel: strings.TrimSpace(wire.UpstreamModel),
			DurationMS: wire.DurationMS, Kind: requestKind, CreatedAt: wire.CreatedAt.Time,
		})
	}
	return items, nil
}

func (c *Client) ListRequestErrors(ctx context.Context, query TelemetryQuery) ([]model.TrafficError, error) {
	wires, err := listTelemetry[errorWire](ctx, c, "/api/v1/admin/ops/errors", query)
	if err != nil {
		return nil, err
	}
	items := make([]model.TrafficError, 0, len(wires))
	for _, wire := range wires {
		if wire.AccountID <= 0 || wire.CreatedAt.Time.IsZero() {
			// Errors raised before account selection legitimately have no
			// account_id. They are useful globally but cannot be attributed to
			// an account, so exclude them from account-level evidence.
			continue
		}
		class, reason := ClassifyDiagnostic(wire.StatusCode, wire.Type, wire.Phase, wire.Message)
		modelName := strings.TrimSpace(wire.Model)
		if modelName == "" {
			modelName = strings.TrimSpace(wire.Requested)
		}
		items = append(items, model.TrafficError{
			EventKey:  eventFingerprint("error", wire.RequestID, wire.AccountID, modelName, wire.CreatedAt.Time),
			AccountID: wire.AccountID, Model: modelName, RequestedModel: strings.TrimSpace(wire.Requested),
			UpstreamModel: strings.TrimSpace(wire.Upstream), Phase: safeToken(wire.Phase), Type: safeToken(wire.Type),
			Severity: safeToken(wire.Severity), StatusCode: wire.StatusCode, ErrorClass: class, ReasonCode: reason,
			ReasonFingerprint: diagnosticFingerprint(wire.Message), CreatedAt: wire.CreatedAt.Time,
		})
	}
	return items, nil
}

func listTelemetry[T any](ctx context.Context, client *Client, path string, query TelemetryQuery) ([]T, error) {
	pageSize := query.PageSize
	if pageSize <= 0 || pageSize > 500 {
		pageSize = defaultTelemetryPageSize
	}
	items := make([]T, 0)
	seenPages := map[string]struct{}{}
	for page := 1; page <= 10_000; page++ {
		values := url.Values{}
		values.Set("page", strconv.Itoa(page))
		values.Set("page_size", strconv.Itoa(pageSize))
		if query.AccountID > 0 {
			values.Set("account_id", strconv.FormatInt(query.AccountID, 10))
		}
		if kind := strings.TrimSpace(query.Kind); kind != "" {
			values.Set("kind", kind)
		}
		if !query.Since.IsZero() {
			values.Set("start_time", query.Since.UTC().Format(time.RFC3339Nano))
		}
		if !query.Until.IsZero() {
			values.Set("end_time", query.Until.UTC().Format(time.RFC3339Nano))
		}
		var envelope responseEnvelope[telemetryPage[T]]
		if err := client.get(ctx, path+"?"+values.Encode(), &envelope); err != nil {
			return nil, err
		}
		pagePayload, err := json.Marshal(envelope.Data.Items)
		if err != nil {
			return nil, err
		}
		pageDigest := sha256.Sum256(pagePayload)
		pageKey := hex.EncodeToString(pageDigest[:])
		if _, duplicatePage := seenPages[pageKey]; duplicatePage {
			return items, nil
		}
		seenPages[pageKey] = struct{}{}
		items = append(items, envelope.Data.Items...)
		currentPage := envelope.Data.Page
		if currentPage == 0 {
			currentPage = page
		}
		if len(envelope.Data.Items) == 0 || (envelope.Data.Pages > 0 && currentPage >= envelope.Data.Pages) ||
			(envelope.Data.Pages == 0 && len(envelope.Data.Items) < pageSize) {
			return items, nil
		}
	}
	return nil, fmt.Errorf("Sub2API pagination exceeded 10000 pages for %s", path)
}

// ClassifyDiagnostic converts free-form upstream errors into stable scheduler
// categories. The caller must discard the original message after this call.
func ClassifyDiagnostic(statusCode int, errorType, phase, message string) (string, string) {
	text := strings.ToLower(strings.Join([]string{errorType, phase, message}, " "))
	has := func(values ...string) bool {
		for _, value := range values {
			if strings.Contains(text, value) {
				return true
			}
		}
		return false
	}
	if statusCode == 401 || statusCode == 403 || has("decrypt", "credential", "unauthorized", "invalid api key", "invalid key", "token expired", "authentication", "密钥", "凭据", "认证失败", "解密失败") {
		return model.ErrorClassCredential, "credential_rejected"
	}
	if has("no available channel", "no available account", "no schedulable channel", "no schedulable account", "all channels unavailable", "all accounts unavailable", "无可用渠道", "无可用的渠道", "没有可用渠道", "没有可用的渠道", "无可用账号", "无可用的账号", "没有可调度渠道", "没有可调度的渠道", "没有可调度账号", "没有可调度的账号", "全部渠道不可用", "全部账号不可用") {
		return model.ErrorClassInfrastructure, "routing_pool_exhausted"
	}
	if has("model not found", "unsupported model", "does not support", "模型不存在", "不支持模型") {
		return model.ErrorClassModelCapability, "model_unsupported"
	}
	if has("context length", "context_length", "maximum context", "invalid argument", "invalid parameter", "bad request", "上下文过长", "参数错误") {
		return model.ErrorClassClient, "client_request_invalid"
	}
	if has("answer mismatch", "response mismatch", "expected answer", "semantic", "validation failed", "答案不匹配", "测试答案", "语义", "校验失败") {
		return model.ErrorClassSemantic, "semantic_validation_failed"
	}
	if statusCode == 429 || statusCode == 503 || has("rate limit", "too many requests", "overload", "overloaded", "capacity", "限流", "过载", "容量不足") {
		return model.ErrorClassCapacity, "upstream_capacity_limited"
	}
	if statusCode == 502 || statusCode == 504 || statusCode == 520 || statusCode == 522 ||
		has("timeout", "timed out", "connection refused", "connection reset", "dial tcp", "gateway", "network", "eof", "超时", "连接失败", "网关", "网络错误") {
		return model.ErrorClassInfrastructure, "upstream_transport_failed"
	}
	return model.ErrorClassUnknown, "unknown_error"
}

func safeToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 80 {
		value = value[:80]
	}
	for _, runeValue := range value {
		if !(runeValue == '_' || runeValue == '-' || runeValue == '.' || runeValue == ':' ||
			(runeValue >= 'a' && runeValue <= 'z') || (runeValue >= 'A' && runeValue <= 'Z') ||
			(runeValue >= '0' && runeValue <= '9')) {
			return "redacted"
		}
	}
	return value
}

func diagnosticFingerprint(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(message))
	return hex.EncodeToString(sum[:])
}

func eventFingerprint(kind, requestID string, accountID int64, modelName string, createdAt time.Time) string {
	identity := strings.Join([]string{kind, requestID, strconv.FormatInt(accountID, 10), modelName, createdAt.UTC().Format(time.RFC3339Nano)}, "\x00")
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}
