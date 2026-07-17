package httpserver

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/config"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestHandlerAcceptsBasePathAndStrippedRoutes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	server := &Server{cfg: config.Config{BasePath: "/scheduler/"}, mux: mux}

	for _, target := range []string{"http://scheduler.local/api/ping", "http://scheduler.local/scheduler/api/ping"} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusNoContent {
			t.Fatalf("%s returned %d", target, response.Code)
		}
	}

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://scheduler.local/scheduler", nil))
	if response.Code != http.StatusMovedPermanently || response.Header().Get("Location") != "/scheduler/" {
		t.Fatalf("base path redirect = %d %q", response.Code, response.Header().Get("Location"))
	}
}

func TestSameOriginRequestTrustsForwardedProtoOnlyFromConfiguredProxy(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://scheduler.local/api/session", nil)
	request.Host = "scheduler.local"
	request.RemoteAddr = "127.0.0.1:1234"
	request.Header.Set("Origin", "https://scheduler.local")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("Sec-Fetch-Site", "same-origin")

	untrusted := &Server{}
	if untrusted.sameOriginRequest(request) {
		t.Fatal("untrusted proxy changed the request scheme")
	}
	trusted := &Server{trustedProxies: parseTrustedProxyCIDRs([]string{"127.0.0.0/8"})}
	if !trusted.sameOriginRequest(request) {
		t.Fatal("trusted proxy scheme was ignored")
	}
	request.Header.Set("Origin", "https://evil.example")
	if trusted.sameOriginRequest(request) {
		t.Fatal("cross-origin request passed")
	}
}

func TestClientIPTrustedProxyChain(t *testing.T) {
	tests := []struct {
		name     string
		cidrs    []string
		remote   string
		header   string
		expected string
	}{
		{name: "unconfigured ignores spoof", remote: "192.0.2.8:443", header: "203.0.113.9", expected: "192.0.2.8"},
		{name: "untrusted ignores spoof", cidrs: []string{"10.0.0.0/8"}, remote: "192.0.2.8:443", header: "203.0.113.9", expected: "192.0.2.8"},
		{name: "trusted ipv4", cidrs: []string{"10.0.0.0/8"}, remote: "10.0.0.2:443", header: "203.0.113.9", expected: "203.0.113.9"},
		{name: "trusted ipv6", cidrs: []string{"2001:db8:1::/48"}, remote: "[2001:db8:1::2]:443", header: "2001:db8:2::9", expected: "2001:db8:2::9"},
		{name: "multiple proxies", cidrs: []string{"10.0.0.0/8"}, remote: "10.0.0.3:443", header: "198.51.100.7, 10.0.0.2", expected: "198.51.100.7"},
		{name: "invalid header falls back", cidrs: []string{"10.0.0.0/8"}, remote: "10.0.0.3:443", header: "not-an-ip", expected: "10.0.0.3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{trustedProxies: parseTrustedProxyCIDRs(test.cidrs)}
			request := httptest.NewRequest(http.MethodGet, "http://scheduler.local", nil)
			request.RemoteAddr = test.remote
			request.Header.Set("X-Forwarded-For", test.header)
			if actual := server.clientIP(request); actual != test.expected {
				t.Fatalf("client IP = %q, want %q", actual, test.expected)
			}
		})
	}
}

func TestRateLimiterBoundedAndExpires(t *testing.T) {
	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	limiter := newRateLimiter(time.Minute, 2)
	limiter.capacity = 3
	limiter.now = func() time.Time { return now }
	for _, key := range []string{"one", "two", "three"} {
		if !limiter.Allow(key) {
			t.Fatalf("initial key %q was rejected", key)
		}
	}
	if limiter.Allow("four") || len(limiter.entries) != 3 {
		t.Fatalf("capacity was not enforced: %d", len(limiter.entries))
	}
	if !limiter.Allow("one") || limiter.Allow("one") {
		t.Fatal("per-key limit was not enforced")
	}
	now = now.Add(2 * time.Minute)
	if !limiter.Allow("four") || len(limiter.entries) != 1 {
		t.Fatalf("expired entries were not cleaned: %d", len(limiter.entries))
	}
}

func TestLoginUsesSchedulerSecretAndHandlesRandomFailure(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &Server{cfg: config.Config{SchedulerAdminSecret: "scheduler-secret", AdminAPIKey: "upstream-secret", BasePath: "/", SessionIdleTimeout: time.Hour},
		store: database, loginLimiter: newRateLimiter(5*time.Minute, 5), randomReader: strings.NewReader(strings.Repeat("r", 128))}

	login := func(secret string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "http://scheduler.local/api/session", bytes.NewBufferString(`{"api_key":"`+secret+`"}`))
		request.RemoteAddr = "192.0.2.10:1234"
		server.login(response, request)
		return response
	}
	if response := login("upstream-secret"); response.Code != http.StatusUnauthorized {
		t.Fatalf("upstream key login status = %d", response.Code)
	}
	if response := login("scheduler-secret"); response.Code != http.StatusOK || response.Header().Get("Set-Cookie") == "" {
		t.Fatalf("scheduler secret login status = %d cookie=%q", response.Code, response.Header().Get("Set-Cookie"))
	}

	server.loginLimiter = newRateLimiter(5*time.Minute, 5)
	server.randomReader = errorReader{}
	if response := login("scheduler-secret"); response.Code != http.StatusInternalServerError || response.Header().Get("Set-Cookie") != "" {
		t.Fatalf("random failure status = %d cookie=%q", response.Code, response.Header().Get("Set-Cookie"))
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("random unavailable") }
