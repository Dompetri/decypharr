package pikpak

import "fmt"

// File kinds returned by the PikPak drive API.
const (
	kindFile   = "drive#file"
	kindFolder = "drive#folder"
)

// Offline download / file task phases.
const (
	phaseComplete = "PHASE_TYPE_COMPLETE"
	phaseRunning  = "PHASE_TYPE_RUNNING"
	phaseError    = "PHASE_TYPE_ERROR"
	phasePending  = "PHASE_TYPE_PENDING"
)

// apiError is the standard error envelope returned by PikPak endpoints.
type apiError struct {
	ErrorField  string `json:"error"`
	Code        int    `json:"error_code"`
	Description string `json:"error_description"`
	status      int
}

func (e *apiError) Error() string {
	if e.Description != "" {
		return fmt.Sprintf("pikpak: %s (code %d, status %d)", e.Description, e.Code, e.status)
	}
	return fmt.Sprintf("pikpak: %s (code %d, status %d)", e.ErrorField, e.Code, e.status)
}

// authExpired reports whether the error means the access token is no longer valid.
func (e *apiError) authExpired() bool {
	return e.Code == 16 || e.status == 401
}

// captchaInvalid reports whether the call was rejected for a stale/invalid captcha.
func (e *apiError) captchaInvalid() bool {
	return e.Code == 9 || e.Code == 4002 || e.ErrorField == "captcha_invalid"
}

// tokenResponse is returned by /v1/auth/signin and /v1/auth/token.
type tokenResponse struct {
	TokenType    string `json:"token_type"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Sub          string `json:"sub"`
}

// captchaInitRequest is the body sent to /v1/shield/captcha/init.
type captchaInitRequest struct {
	ClientID string            `json:"client_id"`
	Action   string            `json:"action"`
	DeviceID string            `json:"device_id"`
	Meta     map[string]string `json:"meta,omitempty"`
}

type captchaTokenResponse struct {
	CaptchaToken string `json:"captcha_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// link is a (possibly signed/expiring) download URL.
type link struct {
	URL    string `json:"url"`
	Token  string `json:"token"`
	Expire string `json:"expire"`
	Type   string `json:"type,omitempty"`
}

type fileLinks struct {
	ApplicationOctetStream *link `json:"application/octet-stream,omitempty"`
}

type media struct {
	MediaID   string `json:"media_id,omitempty"`
	MediaName string `json:"media_name,omitempty"`
	Link      *link  `json:"link,omitempty"`
	IsOrigin  bool   `json:"is_origin,omitempty"`
	IsDefault bool   `json:"is_default,omitempty"`
}

type fileAudit struct {
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

// fileParams holds extra file attributes. For offline downloads, url is the
// original source (magnet/http) the content was fetched from.
type fileParams struct {
	URL string `json:"url,omitempty"`
}

// pikpakFile represents a single drive file or folder.
type pikpakFile struct {
	ID             string      `json:"id,omitempty"`
	Kind           string      `json:"kind,omitempty"`
	Name           string      `json:"name,omitempty"`
	ParentID       string      `json:"parent_id,omitempty"`
	Size           int64       `json:"size,omitempty,string"`
	Phase          string      `json:"phase,omitempty"`
	MimeType       string      `json:"mime_type,omitempty"`
	Trashed        bool        `json:"trashed,omitempty"`
	WebContentLink string      `json:"web_content_link,omitempty"`
	Links          *fileLinks  `json:"links,omitempty"`
	Medias         []*media    `json:"medias,omitempty"`
	Audit          *fileAudit  `json:"audit,omitempty"`
	Params         *fileParams `json:"params,omitempty"`
	CreatedTime    string      `json:"created_time,omitempty"`
	ModifiedTime   string      `json:"modified_time,omitempty"`
}

// sourceInfohash extracts the BitTorrent infohash from the file's source magnet
// URL (params.url), if present. Returns "" when there is no magnet source.
func (f *pikpakFile) sourceInfohash() string {
	if f.Params == nil || f.Params.URL == "" {
		return ""
	}
	return infohashFromMagnet(f.Params.URL)
}

type fileList struct {
	Kind          string        `json:"kind,omitempty"`
	Files         []*pikpakFile `json:"files,omitempty"`
	NextPageToken string        `json:"next_page_token,omitempty"`
}

// taskParams carries PikPak's prediction hints. predict_type "3" indicates the
// content is already cached on PikPak (instant "Saving"), while "1" means it
// must actually be downloaded from the swarm ("Collecting").
type taskParams struct {
	PredictType  string `json:"predict_type,omitempty"`
	PredictSpeed string `json:"predict_speed,omitempty"`
}

// pikpakTask represents an offline download task.
type pikpakTask struct {
	Kind     string      `json:"kind,omitempty"`
	ID       string      `json:"id,omitempty"`
	Name     string      `json:"name,omitempty"`
	Type     string      `json:"type,omitempty"`
	FileID   string      `json:"file_id,omitempty"`
	FileName string      `json:"file_name,omitempty"`
	FileSize int64       `json:"file_size,omitempty,string"`
	Message  string      `json:"message,omitempty"`
	Phase    string      `json:"phase,omitempty"`
	Progress int         `json:"progress,omitempty"`
	Params   *taskParams `json:"params,omitempty"`
}

// cached reports whether PikPak considers this task's content already cached
// (predict_type "3"). The second return value is false when undetermined.
func (t *pikpakTask) cached() (isCached bool, known bool) {
	if t.Params == nil || t.Params.PredictType == "" {
		return false, false
	}
	return t.Params.PredictType == "3", true
}

type taskList struct {
	Tasks         []*pikpakTask `json:"tasks,omitempty"`
	NextPageToken string        `json:"next_page_token,omitempty"`
}

// newTaskResponse is returned when a new offline download task is created.
type newTaskResponse struct {
	UploadType string      `json:"upload_type,omitempty"`
	File       *pikpakFile `json:"file,omitempty"`
	Task       *pikpakTask `json:"task,omitempty"`
}

type aboutResponse struct {
	Quota struct {
		Limit        int64 `json:"limit,omitempty,string"`
		Usage        int64 `json:"usage,omitempty,string"`
		UsageInTrash int64 `json:"usage_in_trash,omitempty,string"`
	} `json:"quota"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type userInfo struct {
	Sub         string `json:"sub,omitempty"`
	Name        string `json:"name,omitempty"`
	Email       string `json:"email,omitempty"`
	PhoneNumber string `json:"phone_number,omitempty"`
	Status      string `json:"status,omitempty"`
}

// webdavApp is a single PikPak WebDAV application (Basic-auth credentials for
// the dav serving host).
type webdavApp struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Enable   int    `json:"enable,omitempty"`
}

type webdavAppsResponse struct {
	IsPremium    bool        `json:"is_premium,omitempty"`
	WebDAVEnable bool        `json:"webdav_enable,omitempty"`
	Applications []webdavApp `json:"applications,omitempty"`
}

type vipResponse struct {
	Result string `json:"result,omitempty"`
	Data   struct {
		Status string `json:"status,omitempty"`
		Type   string `json:"type,omitempty"`
		Expire string `json:"expire,omitempty"`
		UserID string `json:"user_id,omitempty"`
	} `json:"data"`
}
