package pikpak

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
)

func TestActionFor(t *testing.T) {
	tests := []struct {
		method   string
		rawurl   string
		expected string
	}{
		{http.MethodGet, "https://api-drive.mypikpak.com/drive/v1/about", "GET:/drive/v1/about"},
		{http.MethodGet, "https://api-drive.mypikpak.com/drive/v1/files/some-id", "GET:/drive/v1/about"},
		{http.MethodHead, "https://api-drive.mypikpak.com/drive/v1/files", "GET:/drive/v1/about"},
		{http.MethodPost, "https://api-drive.mypikpak.com/drive/v1/files", "POST:/drive/v1/files"},
		{http.MethodPost, "https://api-drive.mypikpak.com/drive/v1/files:batchDelete", "POST:/drive/v1/files:batchDelete"},
		{http.MethodDelete, "https://api-drive.mypikpak.com/drive/v1/tasks", "DELETE:/drive/v1/tasks"},
	}

	for _, tt := range tests {
		got := actionFor(tt.method, tt.rawurl)
		if got != tt.expected {
			t.Errorf("actionFor(%s, %s) = %q; want %q", tt.method, tt.rawurl, got, tt.expected)
		}
	}
}

func TestApiErrorFormat(t *testing.T) {
	err1 := &apiError{
		ErrorField:  "bad_request",
		Code:        4001,
		Description: "Please complete the safety verification",
		status:      400,
	}
	if got := err1.Error(); got != "pikpak: Please complete the safety verification (code 4001, status 400)" {
		t.Errorf("unexpected error format: %s", got)
	}

	err2 := &apiError{
		ErrorField: "unauthenticated",
		Code:       16,
		Message:    "token expired",
		status:     401,
	}
	if got := err2.Error(); got != "pikpak: token expired (code 16, status 401)" {
		t.Errorf("unexpected error format: %s", got)
	}

	err3 := &apiError{
		ErrorField: "invalid_argument",
		Code:       3,
		status:     400,
	}
	if got := err3.Error(); got != "pikpak: invalid_argument (code 3, status 400)" {
		t.Errorf("unexpected error format: %s", got)
	}

	// Connected Apps quota error with string error_code and user_message
	jsonPayload := []byte(`{"error_code":"CONNECTED_APPS_QUOTA_EXCEEDED","dimension":"cloud_download","user_message":"PikPak Connected Apps quota for Cloud Download has been reached. New offline download tasks will be blocked until Dec 1."}`)
	var err4 apiError
	if err := json.Unmarshal(jsonPayload, &err4); err != nil {
		t.Fatalf("failed to unmarshal apiError with string error_code: %v", err)
	}
	err4.status = 429
	expectedMsg := "pikpak: PikPak Connected Apps quota for Cloud Download has been reached. New offline download tasks will be blocked until Dec 1. (code CONNECTED_APPS_QUOTA_EXCEEDED, status 429)"
	if got := err4.Error(); got != expectedMsg {
		t.Errorf("unexpected error format for quota error:\ngot:  %s\nwant: %s", got, expectedMsg)
	}
}

func TestParseCredentials(t *testing.T) {
	// Username / password format
	c1, err := parseCredentials("myuser:mypassword")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c1.username != "myuser" || c1.password != "mypassword" || c1.token != "" {
		t.Errorf("expected username/pass, got %+v", c1)
	}

	// Bearer prefix
	c2, err := parseCredentials("Bearer eyJhbGciOi...")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c2.token != "eyJhbGciOi..." || c2.username != "" {
		t.Errorf("expected token, got %+v", c2)
	}

	// token: prefix
	c3, err := parseCredentials("token:my-token-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c3.token != "my-token-123" {
		t.Errorf("expected token, got %+v", c3)
	}

	// Raw JWT starting with eyJ
	c4, err := parseCredentials("eyJhbGciOi...")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c4.token != "eyJhbGciOi..." {
		t.Errorf("expected token, got %+v", c4)
	}

	// Opaque token without colon
	c5, err := parseCredentials("some-opaque-api-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c5.token != "some-opaque-api-token" {
		t.Errorf("expected token, got %+v", c5)
	}

	// Empty
	_, err = parseCredentials("")
	if err == nil {
		t.Errorf("expected error for empty api_key")
	}
}

func TestExtractJWTSub(t *testing.T) {
	// Dummy JWT with sub="test-user-123"
	// header: {"alg":"none"} -> eyJhbGciOiJub25lIn0
	// payload: {"sub":"test-user-123"} -> eyJzdWIiOiJ0ZXN0LXVzZXItMTIzIn0
	dummyJWT := "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0LXVzZXItMTIzIn0."
	if got := extractJWTSub(dummyJWT); got != "test-user-123" {
		t.Errorf("expected 'test-user-123', got %q", got)
	}

	if got := extractJWTSub("not-a-jwt"); got != "" {
		t.Errorf("expected empty for invalid jwt, got %q", got)
	}
}

func TestPikpakNewWithToken(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dummyJWT := "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0LXVzZXItNDU2In0."
	dc := config.Debrid{
		Name:   "pikpak",
		APIKey: dummyJWT,
	}
	p, err := New(dc, nil)
	if err != nil {
		t.Fatalf("unexpected error creating Pikpak provider with token: %v", err)
	}
	if !p.client.isTokenAuth {
		t.Errorf("expected client.isTokenAuth to be true")
	}
	if p.client.accessToken != dummyJWT {
		t.Errorf("expected client.accessToken to be %q, got %q", dummyJWT, p.client.accessToken)
	}
	if p.client.getUserID() != "test-user-456" {
		t.Errorf("expected userID to be 'test-user-456', got %q", p.client.getUserID())
	}
}

func TestLoginBackoffOnFailure(t *testing.T) {
	c := &apiClient{
		username: "user@example.com",
		password: "secretpassword",
	}
	rateLimitedErr := &apiError{
		Code:        4002,
		Description: "Aborted - Your operation is too frequent, please try again later",
		status:      400,
	}
	c.recordLoginFailureLocked(rateLimitedErr)
	if c.loginBackoff < 2*time.Minute {
		t.Fatalf("expected loginBackoff >= 2m for rate limited error, got %v", c.loginBackoff)
	}
	ctx := t.Context()
	// Next ensureLogin should return cached error without calling network
	_, err := c.ensureLogin(ctx)
	if err != rateLimitedErr {
		t.Fatalf("expected cached rateLimitedErr, got %v", err)
	}
}

func TestAddOfflineDownloadMCP(t *testing.T) {
	var initCalled bool
	var callReceived bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-mcp-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var req struct {
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch req.Method {
		case "initialize":
			initCalled = true
			w.Header().Set("mcp-session-id", "sess-xyz-987")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"result":  map[string]any{"serverInfo": map[string]string{"name": "mock"}},
			})
		case "tools/call":
			callReceived = true
			if r.Header.Get("mcp-session-id") != "sess-xyz-987" {
				http.Error(w, "missing or wrong session id", http.StatusBadRequest)
				return
			}
			if req.Params.Name != "add_link" {
				http.Error(w, "unexpected tool name: "+req.Params.Name, http.StatusBadRequest)
				return
			}
			if req.Params.Arguments["url"] != "magnet:?xt=urn:btih:12345" {
				http.Error(w, "unexpected url argument", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      2,
				"result": map[string]any{
					"content": []map[string]any{
						{
							"type": "text",
							"text": `{"task_id":"task-mcp-456","file_name":"TestMovie.2024.mkv"}`,
						},
					},
				},
			})
		default:
			http.Error(w, "unknown method", http.StatusBadRequest)
		}
	}))
	defer ts.Close()

	oldHost := mcpHost
	mcpHost = ts.URL
	defer func() { mcpHost = oldHost }()

	httpClient := request.New()
	client := newAPIClient(httpClient, zerolog.Nop(), "", "", "test-mcp-token")

	resp, err := client.addOfflineDownload(t.Context(), "magnet:?xt=urn:btih:12345", "TestMovie.2024.mkv")
	if err != nil {
		t.Fatalf("unexpected error adding offline download via MCP: %v", err)
	}

	if !initCalled {
		t.Errorf("expected MCP initialize to be called")
	}
	if !callReceived {
		t.Errorf("expected MCP tools/call to be called")
	}
	if resp.Task == nil {
		t.Fatalf("expected non-nil task in response")
	}
	if resp.Task.ID != "task-mcp-456" {
		t.Errorf("expected Task.ID = 'task-mcp-456', got %q", resp.Task.ID)
	}
	if resp.Task.FileName != "TestMovie.2024.mkv" {
		t.Errorf("expected Task.FileName = 'TestMovie.2024.mkv', got %q", resp.Task.FileName)
	}
}
