package pikpak

import (
	"net/http"
	"testing"
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
