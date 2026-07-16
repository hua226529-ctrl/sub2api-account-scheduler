package balance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
)

func TestNewAPISwitchGroupMutationErrorClassification(t *testing.T) {
	tests := []struct {
		name              string
		writeStatus       int
		writeBody         string
		failReadback      bool
		wantUncertain     bool
		wantRejected      bool
		wantErrorContains string
	}{
		{
			name:              "HTTP 409 is a definite rejection",
			writeStatus:       http.StatusConflict,
			writeBody:         `{"success":false,"message":"group conflict"}`,
			wantRejected:      true,
			wantErrorContains: "HTTP 409",
		},
		{
			name:              "HTTP 500 is uncertain",
			writeStatus:       http.StatusInternalServerError,
			writeBody:         `{"success":false,"message":"temporary failure"}`,
			wantUncertain:     true,
			wantErrorContains: "HTTP 500",
		},
		{
			name:              "invalid JSON is uncertain",
			writeBody:         `not-json`,
			wantUncertain:     true,
			wantErrorContains: "无法识别",
		},
		{
			name:              "post-write readback failure is uncertain",
			writeBody:         `{"success":true,"data":true}`,
			failReadback:      true,
			wantUncertain:     true,
			wantErrorContains: "写后确认失败",
		},
		{
			name:              "explicit business failure is definite",
			writeBody:         `{"success":false,"message":"group is locked"}`,
			wantErrorContains: "group is locked",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var writes atomic.Int32
			var readbacks atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/token/1":
					writeTestJSON(w, map[string]any{
						"success": true,
						"data": map[string]any{
							"id": 1, "name": "router", "key": "sk-router1234", "group": "cheap", "status": 1,
						},
					})
				case "/api/token/":
					if r.Method != http.MethodPut {
						http.NotFound(w, r)
						return
					}
					writes.Add(1)
					if test.writeStatus != 0 {
						w.WriteHeader(test.writeStatus)
					}
					_, _ = w.Write([]byte(test.writeBody))
				case "/api/status":
					readbacks.Add(1)
					if test.failReadback {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`{"message":"readback unavailable"}`))
						return
					}
					http.NotFound(w, r)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			fetcher := NewFetcher(3*time.Second, true)
			credentials := model.UpstreamCredentials{AccessKey: "management-token", UserID: "17"}
			_, err := fetcher.SwitchGroup(context.Background(), "newapi", server.URL, credentials, "1", "backup")
			if err == nil {
				t.Fatal("expected group switch to fail")
			}
			if got := mutation.IsUncertain(err); got != test.wantUncertain {
				t.Fatalf("unexpected uncertainty classification: got %t want %t: %v", got, test.wantUncertain, err)
			}
			if got := mutation.DefinitelyRejected(err); got != test.wantRejected {
				t.Fatalf("unexpected rejection classification: got %t want %t: %v", got, test.wantRejected, err)
			}
			if !strings.Contains(err.Error(), test.wantErrorContains) {
				t.Fatalf("error %q does not contain %q", err, test.wantErrorContains)
			}
			if writes.Load() != 1 {
				t.Fatalf("expected one group write, got %d", writes.Load())
			}
			wantReadbacks := int32(0)
			if test.failReadback {
				wantReadbacks = 1
			}
			if readbacks.Load() != wantReadbacks {
				t.Fatalf("unexpected readback count: got %d want %d", readbacks.Load(), wantReadbacks)
			}
		})
	}
}

func TestSub2SwitchGroupExplicitBusinessFailureIsDefinite(t *testing.T) {
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			writeTestJSON(w, map[string]any{
				"code": 0,
				"data": map[string]any{"access_token": "access-token", "refresh_token": "refresh-token", "expires_in": 900},
			})
		case "/api/v1/keys/9":
			if r.Method != http.MethodPut {
				http.NotFound(w, r)
				return
			}
			writes.Add(1)
			writeTestJSON(w, map[string]any{"code": 3204, "message": "group is unavailable"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fetcher := NewFetcher(3*time.Second, true)
	_, err := fetcher.SwitchGroup(context.Background(), "sub2", server.URL, model.UpstreamCredentials{AccessKey: "refresh-token"}, "9", "2")
	if err == nil {
		t.Fatal("expected explicit Sub2 business failure")
	}
	if mutation.IsUncertain(err) || mutation.DefinitelyRejected(err) {
		t.Fatalf("explicit business failure must be definite without being classified as an HTTP rejection: %v", err)
	}
	if !strings.Contains(err.Error(), "group is unavailable") {
		t.Fatalf("unexpected error: %v", err)
	}
	if writes.Load() != 1 {
		t.Fatalf("expected one group write, got %d", writes.Load())
	}
}
