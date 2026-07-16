package sub2api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestListMonitorsReadsAllPages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		items := []model.Monitor{{ID: int64(page), Name: "monitor"}}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{"items": items, "page": page, "page_size": 200, "pages": 2, "total": 2}})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	items, err := client.ListMonitors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ID != 1 || items[1].ID != 2 {
		t.Fatalf("unexpected monitors: %+v", items)
	}
}

func TestRequestRejectsNonzeroEnvelopeCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 403, "message": "denied"})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	if _, err := client.ListAccounts(context.Background()); err == nil {
		t.Fatal("expected envelope error")
	}
}

func TestListMonitorsDecodesHealthSignals(t *testing.T) {
	checkedAt := time.Date(2026, 7, 14, 4, 30, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"items": []map[string]any{{
					"id": 8, "name": "监控", "enabled": true, "last_checked_at": checkedAt,
					"primary_status": "degraded", "primary_latency_ms": 4321, "availability_7d": 97.25,
					"api_key_decrypt_failed": true,
					"extra_models_status":    []map[string]any{{"model": "gpt-5", "status": "operational", "latency_ms": 800}},
				}},
				"page": 1, "pages": 1, "page_size": 200, "total": 1,
			},
		})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	items, err := client.ListMonitors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].PrimaryLatencyMS != 4321 || items[0].Availability7D == nil ||
		*items[0].Availability7D != 97.25 || !items[0].DecryptFailed || len(items[0].ExtraModelsStatus) != 1 ||
		items[0].ExtraModelsStatus[0].LatencyMS != 800 {
		t.Fatalf("monitor health signals not decoded: %+v", items)
	}
}

func TestListMonitorsAcceptsStringExtraModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"items": []map[string]any{{
					"id":           8,
					"extra_models": "gpt-5.4, gpt-5.5",
					"extra_models_status": []map[string]any{{
						"model": "gpt-5.4", "status": "operational", "latency_ms": 800,
					}},
				}},
				"page": 1, "pages": 1, "page_size": 200, "total": 1,
			},
		})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	items, err := client.ListMonitors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].ExtraModels) != 2 || items[0].ExtraModels[0].Model != "gpt-5.4" ||
		items[0].ExtraModels[0].Status != "" || len(items[0].ExtraModelsStatus) != 1 ||
		items[0].ExtraModelsStatus[0].Status != model.StatusOperational {
		t.Fatalf("string extra_models not decoded compatibly: %+v", items)
	}
}

func TestUpdateLoadFactorUsesPutAndConfirmsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/admin/accounts/225" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		payload, _ := io.ReadAll(r.Body)
		var body map[string]int
		if err := json.Unmarshal(payload, &body); err != nil || body["load_factor"] != 5 {
			http.Error(w, "unexpected body", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"id": 225, "load_factor": 5}})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	target := 5
	account, err := client.UpdateLoadFactor(context.Background(), 225, &target)
	if err != nil {
		t.Fatal(err)
	}
	if account.LoadFactor == nil || *account.LoadFactor != target {
		t.Fatalf("unexpected account: %+v", account)
	}
}

func TestUpdateLoadFactorClearUsesZero(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]int
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["load_factor"] != 0 {
			http.Error(w, "unexpected body", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"id": 225, "load_factor": 0}})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	account, err := client.UpdateLoadFactor(context.Background(), 225, nil)
	if err != nil {
		t.Fatal(err)
	}
	if account.LoadFactor != nil {
		t.Fatalf("cleared load factor should be normalized to nil: %+v", account)
	}
}

func TestUpdateLoadFactorRejectsUnconfirmedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"id": 225, "load_factor": 3}})
	}))
	defer server.Close()

	client := New(server.URL, "test-key", time.Second)
	target := 5
	if _, err := client.UpdateLoadFactor(context.Background(), 225, &target); err == nil {
		t.Fatal("expected confirmation mismatch error")
	}
}
