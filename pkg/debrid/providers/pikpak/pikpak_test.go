package pikpak

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
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
		t.Errorf("expected token:my-token-123, got %+v", c3)
	}

	// Token with refresh token format
	cRef, err := parseCredentials("Bearer eyJhbGciOi...:os.refresh-token-456")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cRef.token != "eyJhbGciOi..." || cRef.refreshToken != "os.refresh-token-456" {
		t.Errorf("expected token and refresh token, got %+v", cRef)
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
	client := newAPIClient(httpClient, zerolog.Nop(), "", "", "test-mcp-token", "")

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

func TestRefreshWebToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/token" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("x-client-id") != "YUMx5nI8ZU8Ap8pm" {
			t.Errorf("expected x-client-id header 'YUMx5nI8ZU8Ap8pm', got %q", r.Header.Get("x-client-id"))
		}
		var req struct {
			ClientID     string `json:"client_id"`
			RefreshToken string `json:"refresh_token"`
			GrantType    string `json:"grant_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.ClientID != "YUMx5nI8ZU8Ap8pm" {
			t.Errorf("expected client_id in body 'YUMx5nI8ZU8Ap8pm', got %q", req.ClientID)
		}
		if req.RefreshToken != "os.initial-refresh-token" {
			t.Errorf("expected refresh_token 'os.initial-refresh-token', got %q", req.RefreshToken)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token_type":    "Bearer",
			"access_token":  "new-access-token-789",
			"refresh_token": "os.rotated-refresh-token-999",
			"expires_in":    7200,
			"sub":           "user-sub-123",
		})
	}))
	defer ts.Close()

	oldHost := userHost
	userHost = ts.URL
	defer func() { userHost = oldHost }()

	client := &apiClient{
		http:          request.New(),
		logger:        zerolog.Nop(),
		isTokenAuth:   true,
		tokenClientID: "YUMx5nI8ZU8Ap8pm",
		accessToken:   "initial-access-token",
		refreshToken:  "os.initial-refresh-token",
		tokenExpiry:   time.Now().Add(-10 * time.Minute), // expired
	}

	if err := client.refreshLocked(t.Context()); err != nil {
		t.Fatalf("unexpected error refreshing web token: %v", err)
	}

	if client.accessToken != "new-access-token-789" {
		t.Errorf("expected accessToken = 'new-access-token-789', got %q", client.accessToken)
	}
	if client.refreshToken != "os.rotated-refresh-token-999" {
		t.Errorf("expected refreshToken = 'os.rotated-refresh-token-999', got %q", client.refreshToken)
	}
	if client.getUserID() != "user-sub-123" {
		t.Errorf("expected userID = 'user-sub-123', got %q", client.getUserID())
	}
	if client.tokenExpiry.Before(time.Now().Add(7100 * time.Second)) {
		t.Errorf("expected tokenExpiry around 7200s in future, got %v", client.tokenExpiry)
	}
}

func TestExtensionFromMime(t *testing.T) {
	tests := []struct {
		mime string
		want string
	}{
		{"video/x-matroska", ".mkv"},
		{"video/x-matroska; codecs=hevc", ".mkv"},
		{"VIDEO/MP4", ".mp4"},
		{"video/quicktime", ".mov"},
		{"video/x-msvideo", ".avi"},
		{"video/webm", ".webm"},
		{"audio/mpeg", ".mp3"},
		{"audio/flac", ".flac"},
		{"application/x-subrip", ".srt"},
		{"application/octet-stream", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := extensionFromMime(tt.mime); got != tt.want {
			t.Errorf("extensionFromMime(%q) = %q; want %q", tt.mime, got, tt.want)
		}
	}
}

func TestFolderNameFor(t *testing.T) {
	tests := []struct {
		fileName    string
		releaseName string
		want        string
	}{
		{"Rover.2014.P.BDRemux(13).1080p", "Rover.2014.P.BDRemux.1080p", "Rover.2014.P.BDRemux.1080p"},
		{"Movie (1).2024", "", "Movie.2024"},
		{"Resident.Evil.2002.HYBRID(16).DoVi", "Resident.Evil.2002.HYBRID.DoVi", "Resident.Evil.2002.HYBRID.DoVi"},
	}
	for _, tt := range tests {
		if got := folderNameFor(tt.fileName, tt.releaseName); got != tt.want {
			t.Errorf("folderNameFor(%q, %q) = %q; want %q", tt.fileName, tt.releaseName, got, tt.want)
		}
	}
}

func TestSubmitMagnetReusesExistingTorrent(t *testing.T) {
	p := &Pikpak{
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "pikpak"},
		torrentsCache: map[string]*types.Torrent{
			"315e9949ae52315f1d65871ed1de70ddcec282ff": {
				Id:       "existing-rover-id",
				InfoHash: "315e9949ae52315f1d65871ed1de70ddcec282ff",
				Name:     "Rover.2014.P.BDRemux(13).1080p",
				Status:   types.TorrentStatusDownloaded,
				Files: map[string]types.File{
					"Rover.2014.P.BDRemux.1080p.mkv": {
						Id:   "existing-rover-id",
						Name: "Rover.2014.P.BDRemux.1080p.mkv",
						Size: 16616342032,
					},
				},
			},
		},
		cacheExpiry: time.Now().Add(10 * time.Minute),
	}

	roverMagnet, err := utils.GetMagnetInfo("magnet:?xt=urn:btih:315e9949ae52315f1d65871ed1de70ddcec282ff&dn=Rover.2014.P.BDRemux.1080p", false)
	if err != nil {
		t.Fatalf("GetMagnetInfo failed: %v", err)
	}

	tr, err := p.SubmitMagnet(&types.Torrent{
		InfoHash: "315e9949ae52315f1d65871ed1de70ddcec282ff",
		Name:     "Rover.2014.P.BDRemux.1080p",
		Magnet:   roverMagnet,
		Files:    make(map[string]types.File),
	})
	if err != nil {
		t.Fatalf("SubmitMagnet failed: %v", err)
	}

	if tr.Id != "existing-rover-id" {
		t.Errorf("expected reused Id 'existing-rover-id', got %q", tr.Id)
	}
	if tr.Name != "Rover.2014.P.BDRemux.1080p" {
		t.Errorf("expected clean folder name 'Rover.2014.P.BDRemux.1080p', got %q", tr.Name)
	}
	if tr.Status != types.TorrentStatusDownloaded {
		t.Errorf("expected status downloaded, got %s", tr.Status)
	}
	if len(tr.Files) != 1 {
		t.Errorf("expected 1 file, got %d", len(tr.Files))
	}
	if _, ok := tr.Files["Rover.2014.P.BDRemux.1080p.mkv"]; !ok {
		t.Errorf("missing expected file Rover.2014.P.BDRemux.1080p.mkv")
	}

	// Verify that an upgraded release with a DIFFERENT infohash is NOT reused
	upgradedHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	existingMatch := p.findExistingTorrent(t.Context(), upgradedHash)
	if existingMatch != nil {
		t.Errorf("expected upgraded release with different infohash to NOT match existing torrent, got %+v", existingMatch)
	}
}

func TestLiveAndroidLogin(t *testing.T) {
	username := os.Getenv("PIKPAK_USER")
	password := os.Getenv("PIKPAK_PASS")
	if username == "" || password == "" {
		t.Skip("skipping live test; set PIKPAK_USER and PIKPAK_PASS to run")
	}

	config.SetConfigPath(t.TempDir())
	client := newAPIClient(request.New(), zerolog.Nop(), username, password, "", "")
	if err := client.loginLocked(t.Context()); err != nil {
		t.Fatalf("live login failed: %v", err)
	}
	if client.accessToken == "" {
		t.Fatalf("expected non-empty access token after login")
	}
	t.Logf("Live login successful! AccessToken: %s...", client.accessToken[:20])

	res, err := client.addOfflineDownload(t.Context(), "magnet:?xt=urn:btih:08ada5a7a6183aae1e09d831df6748d566095a10&dn=Babygirl", "Babygirl")
	if err != nil {
		t.Fatalf("live addOfflineDownload failed: %v", err)
	}
	t.Logf("Live addOfflineDownload SUCCESS! TaskID: %s, FileName: %s", res.Task.ID, res.Task.FileName)
}
