package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type account struct {
	ID           int64          `json:"id"`
	Name         string         `json:"name"`
	Platform     string         `json:"platform"`
	Type         string         `json:"type"`
	Status       string         `json:"status"`
	Schedulable  bool           `json:"schedulable"`
	ErrorMessage string         `json:"error_message"`
	Credentials  map[string]any `json:"credentials"`
	UpdatedAt    time.Time      `json:"updated_at"`
	Concurrency  int            `json:"concurrency"`
	LoadFactor   *int           `json:"load_factor"`
}

func main() {
	var mu sync.Mutex
	now := time.Now().UTC()
	baseLoad := 100
	accounts := []account{
		{ID: 101, Name: "主上游", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://primary.example.com/v1"}, UpdatedAt: now, Concurrency: 100, LoadFactor: &baseLoad},
		{ID: 102, Name: "禁用渠道", Platform: "openai", Type: "apikey", Status: "active", Schedulable: false, Credentials: map[string]any{"base_url": "https://disabled.example.com/"}, UpdatedAt: now, Concurrency: 100, LoadFactor: &baseLoad},
		{ID: 103, Name: "备用渠道", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://secondary.example.com/"}, UpdatedAt: now, Concurrency: 100, LoadFactor: &baseLoad},
		{ID: 104, Name: "应急渠道", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://tertiary.example.com/"}, UpdatedAt: now, Concurrency: 100, LoadFactor: &baseLoad},
	}
	monitors := []map[string]any{
		{"id": 201, "name": "主监控", "provider": "openai", "endpoint": "https://primary.example.com", "primary_model": "gpt-5.5", "enabled": true, "interval_seconds": 60, "last_checked_at": now, "primary_status": "operational"},
		{"id": 202, "name": "禁用监控", "provider": "openai", "endpoint": "https://disabled.example.com", "primary_model": "gpt-5.5", "enabled": false, "interval_seconds": 60, "last_checked_at": now, "primary_status": "error"},
		{"id": 205, "name": "待绑定监控", "provider": "openai", "endpoint": "https://unmatched.example.com", "primary_model": "gpt-5.5", "enabled": true, "interval_seconds": 60, "last_checked_at": now, "primary_status": "failed"},
		{"id": 203, "name": "备用监控", "provider": "openai", "endpoint": "https://secondary.example.com", "primary_model": "gpt-5.5", "enabled": true, "interval_seconds": 60, "last_checked_at": now, "primary_status": "operational"},
		{"id": 204, "name": "应急监控", "provider": "openai", "endpoint": "https://tertiary.example.com", "primary_model": "gpt-5.5", "enabled": true, "interval_seconds": 60, "last_checked_at": now, "primary_status": "operational"},
	}

	wrap := func(data any) map[string]any { return map[string]any{"code": 0, "message": "success", "data": data} }
	http.HandleFunc("/api/v1/admin/channel-monitors", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "admin-local-test" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		current := time.Now().UTC()
		for i := range monitors {
			monitors[i]["last_checked_at"] = current
		}
		write(w, wrap(map[string]any{"items": monitors, "total": len(monitors), "page": 1, "page_size": 200, "pages": 1}))
	})
	http.HandleFunc("/api/v1/admin/channel-monitors/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "admin-local-test" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		monitorID, _ := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		current := time.Now().UTC()
		items := make([]map[string]any, 0, 30)
		for i := 0; i < 30; i++ {
			status, latency, message := "operational", 1800+(i%4)*300, ""
			if monitorID == 201 && i%4 == 0 {
				status, latency = "degraded", 7600
			}
			if monitorID == 203 && i%9 == 0 {
				status, message = "failed", "model not found"
			}
			items = append(items, map[string]any{
				"id": monitorID*1000 + int64(i), "monitor_id": monitorID, "model": "gpt-5.5",
				"status": status, "latency_ms": latency, "ping_latency_ms": 80, "message": message,
				"checked_at": current.Add(-time.Duration(i) * time.Minute),
			})
		}
		write(w, wrap(map[string]any{"items": items}))
	})
	http.HandleFunc("/api/v1/admin/ops/requests", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "admin-local-test" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		items := make([]map[string]any, 0, 80)
		current := time.Now().UTC()
		for _, accountID := range []int64{101, 103, 104} {
			for i := 0; i < 24; i++ {
				items = append(items, map[string]any{"account_id": accountID, "request_id": strconv.FormatInt(accountID, 10) + "-" + strconv.Itoa(i), "model": "gpt-5.5", "duration_ms": 2200 + i*40, "kind": "chat", "created_at": current.Add(-time.Duration(i) * time.Minute)})
			}
		}
		write(w, wrap(map[string]any{"items": items, "page": 1, "page_size": 200, "pages": 1, "total": len(items)}))
	})
	http.HandleFunc("/api/v1/admin/ops/errors", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "admin-local-test" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		items := []map[string]any{{"account_id": int64(103), "request_id": "capability-demo", "model": "gpt-5.4-mini", "requested_model": "gpt-5.4-mini", "type": "upstream", "status_code": 404, "message": "model not found", "created_at": time.Now().UTC().Add(-2 * time.Minute)}}
		write(w, wrap(map[string]any{"items": items, "page": 1, "page_size": 200, "pages": 1, "total": len(items)}))
	})
	accountHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "admin-local-test" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/schedulable") && r.Method == http.MethodPost {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			id, _ := strconv.ParseInt(parts[len(parts)-2], 10, 64)
			var body map[string]bool
			_ = json.NewDecoder(r.Body).Decode(&body)
			for i := range accounts {
				if accounts[i].ID == id {
					accounts[i].Schedulable = body["schedulable"]
					write(w, wrap(accounts[i]))
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPut {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			id, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
			var body map[string]int
			_ = json.NewDecoder(r.Body).Decode(&body)
			for i := range accounts {
				if accounts[i].ID == id {
					value := body["load_factor"]
					accounts[i].LoadFactor = &value
					write(w, wrap(accounts[i]))
					return
				}
			}
		}
		write(w, wrap(map[string]any{"items": accounts, "total": len(accounts), "page": 1, "page_size": 200, "pages": 1}))
	}
	http.HandleFunc("/api/v1/admin/accounts", accountHandler)
	http.HandleFunc("/api/v1/admin/accounts/", accountHandler)
	log.Println("mock Sub2API listening on 127.0.0.1:18080; key=admin-local-test")
	log.Fatal(http.ListenAndServe("127.0.0.1:18080", nil))
}

func write(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
