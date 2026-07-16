package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/config"
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

func TestSameOriginRequest(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://scheduler.local/api/session", nil)
	request.Host = "scheduler.local"
	request.Header.Set("Origin", "https://scheduler.local")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	if !sameOriginRequest(request) {
		t.Fatal("expected same-origin request to pass")
	}
	request.Header.Set("Origin", "https://evil.example")
	if sameOriginRequest(request) {
		t.Fatal("expected cross-origin request to fail")
	}
}

func TestWriteRateLimit(t *testing.T) {
	server := &Server{writeLog: map[string][]time.Time{}}
	for i := 0; i < 30; i++ {
		if !server.allowWrite("session") {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	if server.allowWrite("session") {
		t.Fatal("31st request should be rate limited")
	}
}
