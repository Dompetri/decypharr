package request

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestRetryPolicyPreservesUnlistedProviderStatus(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider response", true: "configured retry"}[explicit], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(509)
				_, _ = io.WriteString(w, `{"error":"active_downloads_limit"}`)
			}))
			defer server.Close()
			opts := []ClientOption{WithMaxRetries(0)}
			if explicit {
				opts = append(opts, WithRetryableStatus(509))
			}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := New(opts...).Do(req)
			if calls.Load() != 1 {
				t.Fatalf("calls = %d", calls.Load())
			}
			if explicit {
				if err == nil {
					DrainAndClose(resp.Body)
					t.Fatal("configured retry did not reach its limit")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer DrainAndClose(resp.Body)
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 509 || string(body) != `{"error":"active_downloads_limit"}` {
				t.Fatalf("response = %d %q, error = %v", resp.StatusCode, body, err)
			}
		})
	}
}

func TestRetryPolicySkipsLongRetryAfter(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1376331")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error_code":"CONNECTED_APPS_QUOTA_EXCEEDED"}`)
	}))
	defer server.Close()

	opts := []ClientOption{
		WithMaxRetries(3),
		WithRetryableStatus(http.StatusTooManyRequests),
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := New(opts...).Do(req)
	if err != nil {
		t.Fatalf("expected immediate 429 response without error, got err=%v", err)
	}
	defer DrainAndClose(resp.Body)
	if calls.Load() != 1 {
		t.Fatalf("expected exactly 1 call (no retries), got %d", calls.Load())
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", resp.StatusCode)
	}
}
