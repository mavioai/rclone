// Package api defines types for Xunlei (Thunder) Drive backend
package api

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrResp represents Thunder API error response
type ErrResp struct {
	ErrorCode        int64  `json:"error_code"`
	ErrorMsg         string `json:"error"`
	ErrorDescription string `json:"error_description"`
	StatusCode       int    `json:"-"` // HTTP status code of the response
}

// IsError checks if the response is an error
func (e *ErrResp) IsError() bool {
	if e.ErrorMsg == "success" {
		return false
	}
	return e.ErrorCode != 0 || e.ErrorMsg != "" || e.ErrorDescription != ""
}

func (e *ErrResp) Error() string {
	return fmt.Sprintf("thunder error (%d): %s - %s", e.ErrorCode, e.ErrorMsg, e.ErrorDescription)
}

// IsTokenExpired reports whether the error means the access token
// must be refreshed.
func (e *ErrResp) IsTokenExpired() bool {
	switch e.ErrorCode {
	case 4122, 4121, 10, 16:
		return true
	}
	return false
}

// IsCaptchaInvalid reports whether the error means the captcha token
// for the action must be refreshed.
//
// Error code 9 is also used for some file errors such as
// "file_rename_uncompleted" which are not about the captcha.
func (e *ErrResp) IsCaptchaInvalid() bool {
	return e.ErrorCode == 9 && !strings.HasPrefix(e.ErrorMsg, "file_")
}

// CaptchaTokenRequest represents captcha token init request
type CaptchaTokenRequest struct {
	Action       string            `json:"action"`
	CaptchaToken string            `json:"captcha_token"`
	ClientID     string            `json:"client_id"`
	DeviceID     string            `json:"device_id"`
	Meta         map[string]string `json:"meta"`
	RedirectURI  string            `json:"redirect_uri"`
}

// CaptchaTokenResponse represents captcha token init response
type CaptchaTokenResponse struct {
	CaptchaToken string `json:"captcha_token"`
	ExpiresIn    int64  `json:"expires_in"`
	URL          string `json:"url"`
}

// TokenResp represents auth token response
type TokenResp struct {
	TokenType    string `json:"token_type"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Sub          string `json:"sub"`
	UserID       string `json:"user_id"`
}

// Token formats authorization header value
func (t *TokenResp) Token() string {
	return fmt.Sprintf("%s %s", t.TokenType, t.AccessToken)
}

// SignInRequest represents v1 sign in request
type SignInRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Provider     string `json:"provider"`
	SigninToken  string `json:"signin_token"`
}

// CoreLoginRequest represents v3 login request
type CoreLoginRequest struct {
	ProtocolVersion string `json:"protocolVersion"`
	SequenceNo      string `json:"sequenceNo"`
	PlatformVersion string `json:"platformVersion"`
	IsCompressed    string `json:"isCompressed"`
	Appid           string `json:"appid"`
	ClientVersion   string `json:"clientVersion"`
	PeerID          string `json:"peerID"`
	AppName         string `json:"appName"`
	SdkVersion      string `json:"sdkVersion"`
	Devicesign      string `json:"devicesign"`
	NetWorkType     string `json:"netWorkType"`
	ProviderName    string `json:"providerName"`
	DeviceModel     string `json:"deviceModel"`
	DeviceName      string `json:"deviceName"`
	OSVersion       string `json:"OSVersion"`
	Creditkey       string `json:"creditkey"`
	Hl              string `json:"hl"`
	UserName        string `json:"userName"`
	PassWord        string `json:"passWord"`
	VerifyKey       string `json:"verifyKey"`
	VerifyCode      string `json:"verifyCode"`
	IsMd5Pwd        string `json:"isMd5Pwd"`
}

// CoreLoginResp represents v3 login response
type CoreLoginResp struct {
	Account   string `json:"account"`
	Creditkey string `json:"creditkey"`
	SessionID string `json:"sessionID"`
	UserID    string `json:"userID"`
	UserName  string `json:"userName"`
	Error     string `json:"error"`
	ErrorCode string `json:"errorCode"`
	ErrorDesc string `json:"errorDesc"`
}

// FileItem represents a file or directory item
type FileItem struct {
	Kind           string    `json:"kind"`
	ID             string    `json:"id"`
	ParentID       string    `json:"parent_id"`
	Name           string    `json:"name"`
	Size           string    `json:"size"`
	WebContentLink string    `json:"web_content_link"`
	CreatedTime    time.Time `json:"created_time"`
	ModifiedTime   time.Time `json:"modified_time"`
	Hash           string    `json:"hash"`
	Phase          string    `json:"phase"`
	Trashed        bool      `json:"trashed"`
	Medias         []struct {
		Link struct {
			URL string `json:"url"`
		} `json:"link"`
	} `json:"medias"`
}

// GetSize returns numeric size
func (f *FileItem) GetSize() int64 {
	size, _ := strconv.ParseInt(f.Size, 10, 64)
	return size
}

// IsDir returns true if folder
func (f *FileItem) IsDir() bool {
	return f.Kind == "drive#folder"
}

// FileListResp represents file list response
type FileListResp struct {
	Kind          string     `json:"kind"`
	NextPageToken string     `json:"next_page_token"`
	Files         []FileItem `json:"files"`
}

// Phases of files and tasks
const (
	PhaseTypeComplete = "PHASE_TYPE_COMPLETE"
	PhaseTypeError    = "PHASE_TYPE_ERROR"
)

// Upload types
const (
	UploadTypeResumable = "UPLOAD_TYPE_RESUMABLE"
	UploadTypeURL       = "UPLOAD_TYPE_URL"
)

// Kinds of items
const (
	KindOfFile   = "drive#file"
	KindOfFolder = "drive#folder"
)

// UploadTaskResponse represents the response to a file creation
// request: a new folder, an upload or an offline download
type UploadTaskResponse struct {
	UploadType string   `json:"upload_type"`
	File       FileItem `json:"file"`
	Task       *Task    `json:"task"`
	Resumable  *struct {
		Kind   string          `json:"kind"`
		Params ResumableParams `json:"params"`
	} `json:"resumable"`
}

// ResumableParams are the credentials for a resumable (S3) upload
type ResumableParams struct {
	AccessKeyID     string    `json:"access_key_id"`
	AccessKeySecret string    `json:"access_key_secret"`
	Bucket          string    `json:"bucket"`
	Endpoint        string    `json:"endpoint"`
	Expiration      time.Time `json:"expiration"`
	Key             string    `json:"key"`
	SecurityToken   string    `json:"security_token"`
}

// Task represents a Thunder offline task
type Task struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	StatusText  string `json:"status_text"`
	Phase       string `json:"phase"`
	Progress    int64  `json:"progress"`
	Icon        string `json:"icon"`
	FileID      string `json:"file_id"`
	FileSize    string `json:"file_size"`
	Message     string `json:"message"`
	CreatedTime string `json:"created_time"`
	UpdatedTime string `json:"updated_time"`
}

// Created returns the creation time of the task, or the zero time if
// it can't be parsed
func (t *Task) Created() time.Time {
	created, _ := time.Parse(time.RFC3339, t.CreatedTime)
	return created
}

// TaskListResp represents task list response
type TaskListResp struct {
	Tasks         []Task `json:"tasks"`
	NextPageToken string `json:"next_page_token"`
}
