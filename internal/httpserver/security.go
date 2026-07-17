package httpserver

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const rateLimiterCapacity = 4096

type rateLimiter struct {
	mu       sync.Mutex
	entries  map[string][]time.Time
	window   time.Duration
	limit    int
	capacity int
	now      func() time.Time
}

func newRateLimiter(window time.Duration, limit int) *rateLimiter {
	return &rateLimiter{entries: make(map[string][]time.Time), window: window, limit: limit,
		capacity: rateLimiterCapacity, now: time.Now}
}

func (l *rateLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-l.window)
	for entryKey, entries := range l.entries {
		entries = retainAfter(entries, cutoff)
		if len(entries) == 0 {
			delete(l.entries, entryKey)
		} else {
			l.entries[entryKey] = entries
		}
	}
	entries, exists := l.entries[key]
	if !exists && len(l.entries) >= l.capacity {
		return false
	}
	if len(entries) >= l.limit {
		return false
	}
	l.entries[key] = append(entries, now)
	return true
}

func retainAfter(entries []time.Time, cutoff time.Time) []time.Time {
	index := 0
	for index < len(entries) && !entries[index].After(cutoff) {
		index++
	}
	return append(entries[:0], entries[index:]...)
}

func randomToken(reader io.Reader, size int) (string, error) {
	if reader == nil {
		reader = rand.Reader
	}
	buffer := make([]byte, size)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func parseTrustedProxyCIDRs(values []string) []*net.IPNet {
	result := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		_, network, err := net.ParseCIDR(value)
		if err == nil {
			result = append(result, network)
		}
	}
	return result
}

func remoteIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(strings.TrimSpace(host))
}

func (s *Server) trustedProxy(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, network := range s.trustedProxies {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *Server) clientIP(r *http.Request) string {
	remote := remoteIP(r)
	if !s.trustedProxy(remote) {
		return remoteAddressValue(r, remote)
	}
	raw := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if raw == "" {
		return remoteAddressValue(r, remote)
	}
	parts := strings.Split(raw, ",")
	chain := make([]net.IP, 0, len(parts))
	for _, part := range parts {
		ip := net.ParseIP(strings.TrimSpace(part))
		if ip == nil {
			return remoteAddressValue(r, remote)
		}
		chain = append(chain, ip)
	}
	for index := len(chain) - 1; index >= 0; index-- {
		if !s.trustedProxy(chain[index]) {
			return chain[index].String()
		}
	}
	if len(chain) > 0 {
		return chain[0].String()
	}
	return remoteAddressValue(r, remote)
}

func remoteAddressValue(r *http.Request, ip net.IP) string {
	if ip != nil {
		return ip.String()
	}
	return r.RemoteAddr
}

func (s *Server) requestScheme(r *http.Request) string {
	if s.trustedProxy(remoteIP(r)) {
		value := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]))
		if value == "http" || value == "https" {
			return value
		}
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func (s *Server) sameOriginRequest(r *http.Request) bool {
	if site := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")); site != "" && site != "same-origin" {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Path != "" {
		return false
	}
	return strings.EqualFold(parsed.Scheme, s.requestScheme(r)) && strings.EqualFold(parsed.Host, r.Host)
}
