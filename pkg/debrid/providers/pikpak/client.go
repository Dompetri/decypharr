package pikpak

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/request"
)

// PikPak Android client identifiers. These mirror the official mobile client so
// that the captcha-signature handshake is accepted by the API.
const (
	clientID      = "YNxT9w7GMdWvEOKa"
	clientSecret  = "dbw2OtmVEeuUvIptb1Coyg"
	clientVersion = "1.47.1"
	packageName   = "com.pikcloud.pikpak"
	sdkVersion    = "2.0.4.204000"
	appName       = packageName

	userHost     = "https://user.mypikpak.com"
	apiHost      = "https://api-drive.mypikpak.com"
	davHost      = "https://api-dav.mypikpak.com" // WebDAV management API
	davServeHost = "https://dav.mypikpak.com"     // WebDAV file-serving host
)

// davCreds holds Basic-auth credentials for the WebDAV serving host.
type davCreds struct {
	username string
	password string
}

type captchaItem struct {
	token  string
	expiry time.Time
}

// folderInfo caches a folder's name and parent for WebDAV path building.
type folderInfo struct {
	name     string
	parentID string
}

// md5Salts is the ordered salt chain used to compute the captcha signature.
// The list is taken from the official client and must stay in this exact order.
var md5Salts = []string{
	"Gez0T9ijiI9WCeTsKSg3SMlx",
	"zQdbalsolyb1R/",
	"ftOjr52zt51JD68C3s",
	"yeOBMH0JkbQdEFNNwQ0RI9T3wU/v",
	"BRJrQZiTQ65WtMvwO",
	"je8fqxKPdQVJiy1DM6Bc9Nb1",
	"niV",
	"9hFCW2R1",
	"sHKHpe2i96",
	"p7c5E6AcXQ/IJUuAEC9W6",
	"",
	"aRv9hjc9P+Pbn+u3krN6",
	"BzStcgE8qVdqjEH16l4",
	"SqgeZvL5j9zoHP95xWHt",
	"zVof5yaJkPe3VFpadPof",
}

var emailRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
var phoneRe = regexp.MustCompile(`^\d{11,18}$`)

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// captchaSign computes the "1.<md5chain>" signature required by the captcha
// endpoint for authenticated (non-signin) actions.
func captchaSign(deviceID, timestamp string) string {
	sign := clientID + clientVersion + packageName + deviceID + timestamp
	for _, salt := range md5Salts {
		sign = md5Hex(sign + salt)
	}
	return "1." + sign
}

// deviceSign mirrors the official client's device signature used in the user agent.
func deviceSign(deviceID string) string {
	base := fmt.Sprintf("%s%s1appkey", deviceID, packageName)
	return fmt.Sprintf("div101.%s%s", deviceID, md5Hex(sha1Hex(base)))
}

// apiClient handles authentication (access token + captcha) and signed requests.
type apiClient struct {
	http        *request.Client
	logger      zerolog.Logger
	deviceID    string
	username    string
	password    string
	isTokenAuth bool

	// mu serializes login/refresh (held across the network handshake), so any
	// helper invoked during login/refresh must NOT try to acquire it.
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	tokenExpiry  time.Time

	// captcha is a reusable captcha token attached to every authenticated request
	// (PikPak rejects calls without one). Tokens are cached per action.
	captchaMu     sync.Mutex
	captchaTokens map[string]captchaItem

	// userID is read while building request headers during login (under mu), so
	// it is stored atomically to stay lock-free and avoid a re-entrant deadlock.
	userID atomic.Pointer[string]

	// WebDAV routing: credentials + folder path cache + a non-redirect-following
	// client used to resolve the high-speed download URL.
	davClient   *http.Client
	davMu       sync.Mutex
	davCreds    *davCreds
	davErr      error
	davFetched  bool
	folderMu    sync.Mutex
	folderCache map[string]folderInfo
}

func (c *apiClient) getUserID() string {
	if p := c.userID.Load(); p != nil {
		return *p
	}
	return ""
}

func (c *apiClient) setUserID(id string) {
	c.userID.Store(&id)
}

func extractJWTSub(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(payload)
		if err != nil {
			return ""
		}
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(data, &claims); err == nil {
		return claims.Sub
	}
	return ""
}

func newAPIClient(httpClient *request.Client, logger zerolog.Logger, username, password, token string) *apiClient {
	var deviceID string
	if token != "" {
		deviceID = md5Hex(token)
	} else {
		// A stable device id per credential pair avoids re-triggering device
		// verification on every restart.
		deviceID = md5Hex(username + password)
	}

	c := &apiClient{
		http:          httpClient,
		logger:        logger,
		deviceID:      deviceID,
		username:      username,
		password:      password,
		isTokenAuth:   token != "",
		accessToken:   token,
		captchaTokens: make(map[string]captchaItem),
		davClient: &http.Client{
			Timeout: 30 * time.Second,
			// Don't follow the redirect; we want to capture the high-speed
			// Location URL it points to.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}

	if token != "" {
		if sub := extractJWTSub(token); sub != "" {
			c.setUserID(sub)
		}
		c.tokenExpiry = time.Now().Add(90 * 24 * time.Hour)
	}

	return c
}

// setDavProxy routes the WebDAV resolve (davClient) through proxyURL. PikPak
// decides a download link's tier and region cap at RESOLVE time — a dav->node
// redirect requested from a throttled egress yields a region-capped link even
// when the bytes are later fetched from a fast egress. Resolving through an
// Asia (e.g. Singapore) proxy makes PikPak issue the un-throttled "original"
// tier link, which then downloads at full speed even direct from home. No-op
// for an empty proxyURL. Preserves davClient's no-follow-redirect behaviour.
func (c *apiClient) setDavProxy(proxyURL string) {
	if proxyURL == "" {
		return
	}
	tr := &http.Transport{}
	request.SetProxy(tr, proxyURL)
	c.davClient.Transport = tr
}

func nowMillis() string {
	return fmt.Sprintf("%d", time.Now().UnixMilli())
}

func (c *apiClient) userAgent() string {
	userID := c.getUserID()
	parts := []string{
		fmt.Sprintf("ANDROID-%s/%s", appName, clientVersion),
		"protocolVersion/200",
		"accesstype/",
		"clientid/" + clientID,
		"clientversion/" + clientVersion,
		"action_type/",
		"networktype/WIFI",
		"sessionid/",
		"deviceid/" + c.deviceID,
		"providername/NONE",
		"devicesign/" + deviceSign(c.deviceID),
		"refresh_token/",
		"sdkversion/" + sdkVersion,
		"datetime/" + nowMillis(),
		"usrno/" + userID,
		"appname/" + appName,
		"session_origin/",
		"grant_type/",
		"appid/",
		"clientip/",
		"devicename/Xiaomi_M2004j7ac",
		"osversion/13",
		"platformversion/10",
		"accessmode/",
		"devicemodel/M2004J7AC",
	}
	return strings.Join(parts, " ")
}

func (c *apiClient) baseHeaders() map[string]string {
	return map[string]string{
		"User-Agent":   c.userAgent(),
		"Content-Type": "application/json; charset=utf-8",
		"X-Device-Id":  c.deviceID,
		"Referer":      "https://mypikpak.com/",
	}
}

// rawCall performs an HTTP request with the provided headers and decodes the
// JSON response into out. It surfaces PikPak's error envelope as *apiError.
func (c *apiClient) rawCall(ctx context.Context, method, rawurl string, params url.Values, body any, headers map[string]string, out any) error {
	u := rawurl
	if len(params) > 0 {
		if strings.Contains(u, "?") {
			u += "&" + params.Encode()
		} else {
			u += "?" + params.Encode()
		}
	}

	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var ae apiError
		if len(data) > 0 && json.Unmarshal(data, &ae) == nil && (ae.ErrorField != "" || ae.Code != 0 || ae.Description != "" || ae.Message != "") {
			ae.status = resp.StatusCode
			return &ae
		}
		return &apiError{status: resp.StatusCode, ErrorField: fmt.Sprintf("http %d", resp.StatusCode), Description: string(data)}
	}

	if len(data) > 0 {
		var ae apiError
		if json.Unmarshal(data, &ae) == nil && (ae.ErrorField != "" || (ae.Code != 0 && (ae.Description != "" || ae.Message != ""))) {
			ae.status = resp.StatusCode
			return &ae
		}
	}

	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

// requestCaptcha fetches a captcha token for the given action. When meta is nil a
// signed meta (captcha_sign) is generated for authenticated actions.
func (c *apiClient) requestCaptcha(ctx context.Context, action string, meta map[string]string) (string, int, error) {
	if meta == nil {
		ts := nowMillis()
		meta = map[string]string{
			"captcha_sign":   captchaSign(c.deviceID, ts),
			"client_version": clientVersion,
			"package_name":   packageName,
			"user_id":        c.getUserID(),
			"timestamp":      ts,
		}
	}

	reqBody := captchaInitRequest{
		ClientID: clientID,
		Action:   action,
		DeviceID: c.deviceID,
		Meta:     meta,
	}

	var resp captchaTokenResponse
	if err := c.rawCall(ctx, http.MethodPost, userHost+"/v1/shield/captcha/init", nil, reqBody, c.baseHeaders(), &resp); err != nil {
		return "", 0, err
	}
	if resp.CaptchaToken == "" {
		return "", 0, fmt.Errorf("pikpak: empty captcha token for action %q", action)
	}
	return resp.CaptchaToken, resp.ExpiresIn, nil
}

func actionFor(method, rawurl string) string {
	if method == http.MethodGet || method == http.MethodHead {
		return "GET:/drive/v1/about"
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return method + ":" + rawurl
	}
	return method + ":" + u.Path
}

// ensureCaptcha returns a valid cached captcha token for the requested action,
// refreshing it when missing or near expiry.
func (c *apiClient) ensureCaptcha(ctx context.Context, action string) (string, error) {
	c.captchaMu.Lock()
	defer c.captchaMu.Unlock()

	if item, ok := c.captchaTokens[action]; ok && item.token != "" && time.Now().Before(item.expiry.Add(-30*time.Second)) {
		return item.token, nil
	}

	tok, expiresIn, err := c.requestCaptcha(ctx, action, nil)
	if err != nil {
		return "", err
	}
	if expiresIn <= 0 {
		expiresIn = 300
	}
	c.captchaTokens[action] = captchaItem{
		token:  tok,
		expiry: time.Now().Add(time.Duration(expiresIn) * time.Second),
	}
	return tok, nil
}

// invalidateCaptcha clears cached captcha token(s) so the next call refreshes it.
// When action is empty all cached tokens are cleared.
func (c *apiClient) invalidateCaptcha(action string) {
	c.captchaMu.Lock()
	defer c.captchaMu.Unlock()
	if action == "" {
		c.captchaTokens = make(map[string]captchaItem)
	} else {
		delete(c.captchaTokens, action)
	}
}

// loginLocked performs a username/password sign-in. Caller must hold c.mu.
func (c *apiClient) loginLocked(ctx context.Context) error {
	if c.username == "" || c.password == "" {
		return fmt.Errorf("pikpak: username and password are required")
	}

	meta := map[string]string{}
	switch {
	case emailRe.MatchString(c.username):
		meta["email"] = c.username
	case phoneRe.MatchString(c.username):
		meta["phone_number"] = c.username
	default:
		meta["username"] = c.username
	}

	captcha, _, err := c.requestCaptcha(ctx, "POST:"+userHost+"/v1/auth/signin", meta)
	if err != nil {
		return fmt.Errorf("pikpak: failed to obtain signin captcha: %w", err)
	}

	body := map[string]string{
		"client_id":     clientID,
		"client_secret": clientSecret,
		"username":      c.username,
		"password":      c.password,
		"captcha_token": captcha,
	}
	headers := c.baseHeaders()
	headers["X-Captcha-Token"] = captcha

	var tok tokenResponse
	if err := c.rawCall(ctx, http.MethodPost, userHost+"/v1/auth/signin", nil, body, headers, &tok); err != nil {
		return fmt.Errorf("pikpak: login failed: %w", err)
	}
	if tok.AccessToken == "" {
		return fmt.Errorf("pikpak: login returned empty access token")
	}

	c.applyTokenLocked(&tok)
	return nil
}

// refreshLocked exchanges the refresh token for a new access token. Caller holds c.mu.
func (c *apiClient) refreshLocked(ctx context.Context) error {
	if c.refreshToken == "" {
		return fmt.Errorf("pikpak: no refresh token available")
	}
	body := map[string]string{
		"client_id":     clientID,
		"refresh_token": c.refreshToken,
		"grant_type":    "refresh_token",
	}
	var tok tokenResponse
	if err := c.rawCall(ctx, http.MethodPost, userHost+"/v1/auth/token", nil, body, c.baseHeaders(), &tok); err != nil {
		return fmt.Errorf("pikpak: token refresh failed: %w", err)
	}
	if tok.AccessToken == "" {
		return fmt.Errorf("pikpak: refresh returned empty access token")
	}
	c.applyTokenLocked(&tok)
	return nil
}

func (c *apiClient) applyTokenLocked(tok *tokenResponse) {
	c.accessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		c.refreshToken = tok.RefreshToken
	}
	if tok.Sub != "" {
		c.setUserID(tok.Sub)
	}
	expires := tok.ExpiresIn
	if expires <= 0 {
		expires = 7200
	}
	c.tokenExpiry = time.Now().Add(time.Duration(expires) * time.Second)
}

// ensureLogin guarantees a valid access token, logging in or refreshing as needed.
func (c *apiClient) ensureLogin(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isTokenAuth {
		if c.accessToken == "" {
			return "", fmt.Errorf("pikpak: token is empty")
		}
		return c.accessToken, nil
	}

	if c.accessToken == "" {
		if err := c.loginLocked(ctx); err != nil {
			return "", err
		}
		return c.accessToken, nil
	}

	if time.Now().After(c.tokenExpiry.Add(-60 * time.Second)) {
		if err := c.refreshLocked(ctx); err != nil {
			// Refresh failed; fall back to a fresh login.
			if err := c.loginLocked(ctx); err != nil {
				return "", err
			}
		}
	}
	return c.accessToken, nil
}

// forceRefresh refreshes (or re-logins) after an auth-expired response.
func (c *apiClient) forceRefresh(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isTokenAuth {
		return c.accessToken, fmt.Errorf("pikpak: token is expired or unauthorized")
	}

	if err := c.refreshLocked(ctx); err != nil {
		if err := c.loginLocked(ctx); err != nil {
			return "", err
		}
	}
	return c.accessToken, nil
}

// authCall performs an authenticated API request. It attaches a (cached) captcha
// token to every request and transparently retries once if the call is rejected
// for an expired access token (code 16) or a stale captcha (code 9).
func (c *apiClient) authCall(ctx context.Context, method, rawurl string, params url.Values, body any, out any) error {
	token, err := c.ensureLogin(ctx)
	if err != nil {
		return err
	}

	action := actionFor(method, rawurl)
	var captcha string
	if !c.isTokenAuth {
		captcha, err = c.ensureCaptcha(ctx, action)
		if err != nil {
			return err
		}
	}

	doRequest := func(tok, cap string) error {
		headers := c.baseHeaders()
		headers["Authorization"] = "Bearer " + tok
		if cap != "" {
			headers["X-Captcha-Token"] = cap
		}
		return c.rawCall(ctx, method, rawurl, params, body, headers, out)
	}

	var lastErr error
	// Up to two corrective retries: one for an expired token, one for a stale captcha.
	for attempt := 0; attempt < 3; attempt++ {
		err := doRequest(token, captcha)
		if err == nil {
			return nil
		}
		ae, ok := err.(*apiError)
		if !ok {
			return err
		}
		lastErr = ae
		switch {
		case ae.authExpired():
			if c.isTokenAuth {
				return fmt.Errorf("pikpak: token expired or invalid (HTTP %d)", ae.status)
			}
			token, err = c.forceRefresh(ctx)
			if err != nil {
				return err
			}
		case ae.captchaInvalid():
			c.invalidateCaptcha(action)
			captcha, err = c.ensureCaptcha(ctx, action)
			if err != nil {
				return err
			}
		default:
			return ae
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("pikpak: request failed after retries")
}

// ---------------------------------------------------------------------------
// High level operations
// ---------------------------------------------------------------------------

// addOfflineDownload submits a magnet/url for offline downloading to the user's
// special "DOWNLOAD" folder and returns the created task.
func (c *apiClient) addOfflineDownload(ctx context.Context, fileURL, name string) (*newTaskResponse, error) {
	body := map[string]any{
		"kind":        kindFile,
		"name":        name,
		"upload_type": "UPLOAD_TYPE_URL",
		"url":         map[string]string{"url": fileURL},
		"folder_type": "DOWNLOAD",
	}

	var resp newTaskResponse
	if err := c.authCall(ctx, http.MethodPost, apiHost+"/drive/v1/files", nil, body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// getFile returns rich metadata for a file id, including download links.
func (c *apiClient) getFile(ctx context.Context, fileID string) (*pikpakFile, error) {
	params := url.Values{}
	params.Set("thumbnail_size", "SIZE_LARGE")

	var f pikpakFile
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/files/"+fileID, params, nil, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// getTask fetches a single offline download task by id. Right after creation the
// task's file_id is empty ("Collecting" phase); it is populated once PikPak has
// resolved the magnet metadata and allocated the destination drive entry.
func (c *apiClient) getTask(ctx context.Context, taskID string) (*pikpakTask, error) {
	var t pikpakTask
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/tasks/"+taskID, nil, nil, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// listFiles lists the direct children of a folder (single page).
func (c *apiClient) listFiles(ctx context.Context, parentID, pageToken string) (*fileList, error) {
	params := url.Values{}
	params.Set("parent_id", parentID)
	params.Set("thumbnail_size", "SIZE_MEDIUM")
	params.Set("limit", "200")
	params.Set("with_audit", "true")
	if pageToken != "" {
		params.Set("page_token", pageToken)
	}
	params.Set("filters", `{"trashed":{"eq":false}}`)

	var fl fileList
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/files", params, nil, &fl); err != nil {
		return nil, err
	}
	return &fl, nil
}

// listOfflineTasks returns the running and errored offline download tasks.
func (c *apiClient) listOfflineTasks(ctx context.Context) ([]*pikpakTask, error) {
	params := url.Values{}
	params.Set("type", "offline")
	params.Set("thumbnail_size", "SIZE_SMALL")
	params.Set("limit", "10000")
	params.Set("filters", `{"phase":{"in":"PHASE_TYPE_RUNNING,PHASE_TYPE_PENDING,PHASE_TYPE_ERROR"}}`)
	params.Set("with", "reference_resource")

	var tl taskList
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/tasks", params, nil, &tl); err != nil {
		return nil, err
	}
	return tl.Tasks, nil
}

// deleteTask removes an offline download task (optionally its files).
func (c *apiClient) deleteTask(ctx context.Context, taskID string, deleteFiles bool) error {
	params := url.Values{}
	params.Set("task_ids", taskID)
	params.Set("delete_files", fmt.Sprintf("%t", deleteFiles))
	return c.authCall(ctx, http.MethodDelete, apiHost+"/drive/v1/tasks", params, nil, nil)
}

// batchDelete permanently deletes files/folders by id.
func (c *apiClient) batchDelete(ctx context.Context, ids []string) error {
	body := map[string]any{"ids": ids}
	return c.authCall(ctx, http.MethodPost, apiHost+"/drive/v1/files:batchDelete", nil, body, nil)
}

// getAbout returns drive quota information.
func (c *apiClient) getAbout(ctx context.Context) (*aboutResponse, error) {
	var about aboutResponse
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/about", nil, nil, &about); err != nil {
		return nil, err
	}
	return &about, nil
}

// getUserInfo returns the authenticated user's profile.
func (c *apiClient) getUserInfo(ctx context.Context) (*userInfo, error) {
	if c.isTokenAuth {
		return &userInfo{
			Sub:  c.getUserID(),
			Name: c.getUserID(),
		}, nil
	}
	var info userInfo
	if err := c.authCall(ctx, http.MethodGet, userHost+"/v1/user/me", nil, nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// getVIPInfo returns VIP/premium status for the account.
func (c *apiClient) getVIPInfo(ctx context.Context) (*vipResponse, error) {
	var vip vipResponse
	if err := c.authCall(ctx, http.MethodGet, apiHost+"/drive/v1/privilege/vip", nil, nil, &vip); err != nil {
		return nil, err
	}
	return &vip, nil
}

// webdavCreds returns the cached Basic-auth credentials for PikPak's WebDAV
// serving host, fetching them once. Returns nil (without error) when the account
// has no usable WebDAV application, so callers fall back to the direct link.
func (c *apiClient) webdavCreds(ctx context.Context) (*davCreds, error) {
	c.davMu.Lock()
	defer c.davMu.Unlock()
	if c.davFetched {
		return c.davCreds, c.davErr
	}
	c.davFetched = true

	var resp webdavAppsResponse
	if err := c.authCall(ctx, http.MethodGet, davHost+"/webdav/v1/applications", nil, nil, &resp); err != nil {
		c.davErr = err
		return nil, err
	}
	if !resp.WebDAVEnable {
		return nil, nil
	}
	for _, app := range resp.Applications {
		if app.Enable == 1 && app.Username != "" && app.Password != "" {
			c.davCreds = &davCreds{username: app.Username, password: app.Password}
			return c.davCreds, nil
		}
	}
	return nil, nil
}

// folderInfoByID returns a folder's name and parent id, cached to avoid repeated
// lookups while building WebDAV paths.
func (c *apiClient) folderInfoByID(ctx context.Context, id string) (folderInfo, error) {
	c.folderMu.Lock()
	if fi, ok := c.folderCache[id]; ok {
		c.folderMu.Unlock()
		return fi, nil
	}
	c.folderMu.Unlock()

	f, err := c.getFile(ctx, id)
	if err != nil {
		return folderInfo{}, err
	}
	fi := folderInfo{name: f.Name, parentID: f.ParentID}

	c.folderMu.Lock()
	if c.folderCache == nil {
		c.folderCache = make(map[string]folderInfo)
	}
	c.folderCache[id] = fi
	c.folderMu.Unlock()
	return fi, nil
}

// webdavPath builds the file's path within the WebDAV tree by walking parents.
func (c *apiClient) webdavPath(ctx context.Context, f *pikpakFile) (string, error) {
	segments := []string{f.Name}
	pid := f.ParentID
	for depth := 0; pid != "" && depth < 15; depth++ {
		fi, err := c.folderInfoByID(ctx, pid)
		if err != nil {
			return "", err
		}
		if fi.name == "" {
			break
		}
		segments = append([]string{fi.name}, segments...)
		pid = fi.parentID
	}

	var b strings.Builder
	for _, s := range segments {
		b.WriteString("/")
		b.WriteString(url.PathEscape(s))
	}
	return b.String(), nil
}

// resolveWebDAVURL returns a high-speed download URL for the file by following
// PikPak's WebDAV redirect (which routes through a faster node tier than the
// direct file link). Returns "" (without error) when WebDAV isn't available so
// the caller falls back to the direct link.
func (c *apiClient) resolveWebDAVURL(ctx context.Context, f *pikpakFile) (string, error) {
	creds, err := c.webdavCreds(ctx)
	if err != nil || creds == nil {
		return "", err
	}

	path, err := c.webdavPath(ctx, f)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, davServeHost+path, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(creds.username, creds.password)
	req.Header.Set("User-Agent", c.userAgent())

	resp, err := c.davClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusTemporaryRedirect {
		if loc := resp.Header.Get("Location"); loc != "" {
			return loc, nil
		}
	}
	return "", fmt.Errorf("pikpak: webdav resolve unexpected status %d for %s", resp.StatusCode, path)
}

// webdavURL returns the PikPak WebDAV serving URL for f with Basic-auth
// credentials embedded in the userinfo. Unlike resolveWebDAVURL it does NOT
// pre-resolve the node redirect: the stream client fetches dav.mypikpak.com
// directly and PikPak redirects each request to a node. That keeps the URL
// stable (no per-node signature/expiry) and lets concurrent range reads land on
// independent nodes rather than all sharing one cached node — which is how the
// WebDAV path reaches full speed. Returns "" when WebDAV isn't available so the
// caller fails the download (no direct-link fallback).
func (c *apiClient) webdavURL(ctx context.Context, f *pikpakFile) (string, error) {
	creds, err := c.webdavCreds(ctx)
	if err != nil || creds == nil {
		return "", err
	}
	path, err := c.webdavPath(ctx, f)
	if err != nil {
		return "", err
	}
	// path segments are already %-escaped by webdavPath; url.UserPassword handles
	// escaping the credentials. Go's http client sends these as Basic auth and
	// strips them on the cross-host redirect to the node.
	ui := url.UserPassword(creds.username, creds.password).String()
	return strings.Replace(davServeHost, "https://", "https://"+ui+"@", 1) + path, nil
}
