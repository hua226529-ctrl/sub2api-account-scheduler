package sub2api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestListMonitorHistoryStopsOnRepeatedMetadataFreePageAndRedactsMessage(t *testing.T) {
	now := time.Date(2026, 7, 14, 5, 0, 0, 0, time.UTC)
	pagesRead := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/channel-monitors/8/history" {
			http.NotFound(w, r)
			return
		}
		pagesRead++
		if r.URL.Query().Get("page_size") != "2" || r.URL.Query().Get("start_time") != now.Add(-time.Hour).Format(time.RFC3339Nano) {
			http.Error(w, "missing query", http.StatusBadRequest)
			return
		}
		// This endpoint has no pagination metadata in production. Simulate a
		// server that ignores page so the client must detect a repeated page.
		items := []map[string]any{
			{"id": 10, "monitor_id": 8, "model": "gpt-5.5", "status": "operational", "latency_ms": 1200, "ping_latency_ms": 80, "message": "secret success detail", "checked_at": now},
			{"id": 9, "monitor_id": 8, "model": "gpt-5.5", "status": "failed", "latency_ms": 2000, "message": "测试答案不匹配: private response", "checked_at": now.Add(-time.Minute)},
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{"items": items}})
	}))
	defer server.Close()

	client := New(server.URL, "key", time.Second)
	items, err := client.ListMonitorHistory(context.Background(), 8, TelemetryQuery{Since: now.Add(-time.Hour), PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if pagesRead != 2 || len(items) != 2 {
		t.Fatalf("pages=%d items=%+v", pagesRead, items)
	}
	if items[0].ErrorClass != "" || items[0].ReasonCode != "" {
		t.Fatalf("operational result should not be classified: %+v", items[0])
	}
	if items[1].ErrorClass != model.ErrorClassSemantic || items[1].ReasonCode != "semantic_validation_failed" || len(items[1].ReasonFingerprint) != 64 {
		t.Fatalf("semantic result not safely classified: %+v", items[1])
	}
	payload, _ := json.Marshal(items)
	if string(payload) == "" || containsString(string(payload), "private response") || containsString(string(payload), "secret success detail") {
		t.Fatalf("raw diagnostic leaked: %s", payload)
	}
}

func TestListOperationsPaginationQueryAndClassification(t *testing.T) {
	now := time.Date(2026, 7, 14, 5, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("account_id") != "225" || r.URL.Query().Get("end_time") != now.Format(time.RFC3339Nano) {
			http.Error(w, "missing filter", http.StatusBadRequest)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		var item map[string]any
		switch r.URL.Path {
		case "/api/v1/admin/ops/requests":
			item = map[string]any{"account_id": 225, "request_id": "request-secret-" + strconv.Itoa(page), "model": "gpt-5.5", "duration_ms": page * 100, "kind": "chat", "request_started_at": now.Add(-time.Duration(page)*time.Minute - time.Second), "created_at": now.Add(-time.Duration(page) * time.Minute)}
		case "/api/v1/admin/ops/errors":
			message := "dial tcp private-host: connection timeout"
			status := 502
			if page == 2 {
				message, status = "model not found: private-model", 404
			}
			item = map[string]any{"account_id": 225, "request_id": "error-secret-" + strconv.Itoa(page), "model": "gpt-5.5", "requested_model": "gpt-5.5", "phase": "upstream", "type": "request_error", "severity": "error", "status_code": status, "message": message, "request_started_at": now.Add(-time.Duration(page)*time.Minute - time.Second), "created_at": now.Add(-time.Duration(page) * time.Minute)}
		default:
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{item}, "page": page, "pages": 2, "page_size": 1, "total": 2}})
	}))
	defer server.Close()

	client := New(server.URL, "key", time.Second)
	query := TelemetryQuery{AccountID: 225, Since: now.Add(-time.Hour), Until: now, PageSize: 1}
	successes, err := client.ListSuccessfulRequests(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	failures, err := client.ListRequestErrors(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(successes) != 2 || len(successes[0].EventKey) != 64 || successes[0].DurationMS != 100 || successes[0].RequestStartedAt == nil {
		t.Fatalf("unexpected successes: %+v", successes)
	}
	if len(failures) != 2 || failures[0].ErrorClass != model.ErrorClassInfrastructure || failures[1].ErrorClass != model.ErrorClassModelCapability || failures[0].RequestStartedAt == nil {
		t.Fatalf("unexpected classified errors: %+v", failures)
	}
	encoded, _ := json.Marshal(failures)
	if containsString(string(encoded), "private-host") || containsString(string(encoded), "request-secret") || containsString(string(encoded), "error-secret") {
		t.Fatalf("sensitive operation fields leaked: %s", encoded)
	}
}

func TestListSuccessfulRequestsFiltersUnifiedErrorsAndUnattributedRows(t *testing.T) {
	now := time.Date(2026, 7, 14, 5, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/ops/requests" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("kind"); got != "success" {
			t.Fatalf("kind filter=%q, want success", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "100" {
			t.Fatalf("page_size=%q, want capped Sub2API request-list size 100", got)
		}
		items := []map[string]any{
			{"kind": "success", "account_id": 225, "request_id": "valid", "model": "gpt-5.5", "duration_ms": 1200, "created_at": now},
			{"kind": "error", "account_id": 225, "request_id": "mixed-error", "model": "gpt-5.5", "created_at": now},
			{"kind": "success", "account_id": nil, "request_id": "unattributed", "model": "gpt-5.5", "created_at": now},
			{"kind": "success", "account_id": 225, "request_id": "no-time", "model": "gpt-5.5"},
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items, "page": 1, "pages": 1, "page_size": 100, "total": len(items)}})
	}))
	defer server.Close()

	client := New(server.URL, "key", time.Second)
	items, err := client.ListSuccessfulRequests(context.Background(), TelemetryQuery{PageSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].AccountID != 225 || items[0].DurationMS != 1200 || items[0].Kind != "" {
		t.Fatalf("unexpected filtered successes: %+v", items)
	}
}

func TestListRequestErrorsSkipsRowsWithoutAccountOrTimestamp(t *testing.T) {
	now := time.Date(2026, 7, 14, 5, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		items := []map[string]any{
			{"account_id": 225, "request_id": "valid", "model": "gpt-5.5", "status_code": 502, "message": "timeout", "created_at": now},
			{"account_id": nil, "request_id": "unattributed", "model": "gpt-5.5", "created_at": now},
			{"account_id": 225, "request_id": "no-time", "model": "gpt-5.5"},
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items, "page": 1, "pages": 1, "page_size": 100, "total": len(items)}})
	}))
	defer server.Close()

	client := New(server.URL, "key", time.Second)
	items, err := client.ListRequestErrors(context.Background(), TelemetryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].AccountID != 225 || items[0].ErrorClass != model.ErrorClassInfrastructure {
		t.Fatalf("unexpected filtered errors: %+v", items)
	}
}

func TestClassifyDiagnosticFamilies(t *testing.T) {
	tests := []struct {
		status  int
		message string
		want    string
	}{
		{401, "anything", model.ErrorClassCredential},
		{429, "anything", model.ErrorClassCapacity},
		{0, "上下文过长", model.ErrorClassClient},
		{0, "答案不匹配", model.ErrorClassSemantic},
		{0, "没有可用渠道", model.ErrorClassInfrastructure},
		{0, "no available account for this pool", model.ErrorClassInfrastructure},
		{0, "no schedulable channel", model.ErrorClassInfrastructure},
		{0, "unsupported model gpt-x", model.ErrorClassModelCapability},
		{522, "anything", model.ErrorClassInfrastructure},
		{418, "anything", model.ErrorClassUnknown},
	}
	for _, test := range tests {
		got, _ := ClassifyDiagnostic(test.status, "", "", test.message)
		if got != test.want {
			t.Fatalf("status=%d message=%q class=%q want=%q", test.status, test.message, got, test.want)
		}
	}
}

func containsString(value, target string) bool {
	for index := 0; index+len(target) <= len(value); index++ {
		if value[index:index+len(target)] == target {
			return true
		}
	}
	return false
}
