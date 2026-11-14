package pikpak

import (
	"net/http"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
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
