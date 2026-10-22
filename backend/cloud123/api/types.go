// Package api has type definitions for 123Pan API
package api

import (
	"fmt"
	"time"
)

// BaseResp is the standard API response structure
type BaseResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Base returns the common response fields
func (r *BaseResp) Base() *BaseResp { return r }

// Err returns the business error carried by the response, or nil if
// the call succeeded. 123Pan reports most failures with HTTP 200 and a
// non-zero code.
func (r *BaseResp) Err() error {
	if r.Code == 0 {
		return nil
	}
	return &Error{Code: r.Code, Message: r.Message}
}

// Response is implemented by every API response type
type Response interface {
	Base() *BaseResp
}

// Error is an error returned by the 123Pan API
type Error struct {
	Code    int
	Message string
}

// Error satisfies the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("123pan error %d: %s", e.Code, e.Message)
}

// CodeTokenExpired is the business code returned when the access token
// is missing, invalid or expired
const CodeTokenExpired = 401

// SignInResp is the response from /user/sign_in
type SignInResp struct {
	BaseResp
	Data struct {
		Token string `json:"token"`
	} `json:"data"`
}

// File represents a file or directory item in 123Pan
type File struct {
	FileID      int64     `json:"FileId"`
	FileName    string    `json:"FileName"`
	Size        int64     `json:"Size"`
	Type        int       `json:"Type"` // 1: folder, 0: file
	Etag        string    `json:"Etag"`
	S3KeyFlag   string    `json:"S3KeyFlag"`
	DownloadURL string    `json:"DownloadUrl"`
	UpdateAt    time.Time `json:"UpdateAt"`
}

// FileListResp is the response from /file/list/new
type FileListResp struct {
	BaseResp
	Data struct {
		Total    int    `json:"Total"`
		Next     string `json:"Next"`
		InfoList []File `json:"InfoList"`
	} `json:"data"`
}

// RedirectResp is the JSON body some download URLs answer with instead
// of redirecting
type RedirectResp struct {
	BaseResp
	Data struct {
		RedirectURL string `json:"redirect_url"`
	} `json:"data"`
}

// DownloadInfoResp is the response from /file/download_info
type DownloadInfoResp struct {
	BaseResp
	Data struct {
		DownloadURL string `json:"DownloadUrl"`
	} `json:"data"`
}

// UploadResp is the response from /file/upload_request
type UploadResp struct {
	BaseResp
	Data struct {
		AccessKeyID     string `json:"AccessKeyId"`
		Bucket          string `json:"Bucket"`
		Key             string `json:"Key"`
		SecretAccessKey string `json:"SecretAccessKey"`
		SessionToken    string `json:"SessionToken"`
		FileID          int64  `json:"FileId"`
		Reuse           bool   `json:"Reuse"`
		EndPoint        string `json:"EndPoint"`
		StorageNode     string `json:"StorageNode"`
		UploadID        string `json:"UploadId"`
	} `json:"data"`
}

// S3PreSignedURLs contains presigned chunk upload URLs
type S3PreSignedURLs struct {
	BaseResp
	Data struct {
		PreSignedUrls map[string]string `json:"presignedUrls"`
	} `json:"data"`
}

// Offline download task states reported by
// /offline_download/task/list
const (
	OfflineStatusRunning  = 0
	OfflineStatusFailed   = 1
	OfflineStatusComplete = 2
	OfflineStatusRetrying = 3
	OfflineStatusWaiting  = 4
)

// OfflineResource is a resource found by
// /v2/offline_download/task/resolve
type OfflineResource struct {
	Result  int    `json:"result"` // 0 if the URL was resolved
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ErrCode int    `json:"err_code"`
	ErrMsg  string `json:"err_msg"`
	Files   []struct {
		ID int64 `json:"id"`
	} `json:"files"`
}

// OfflineResolveResp is the response from
// /v2/offline_download/task/resolve
type OfflineResolveResp struct {
	BaseResp
	Data struct {
		List []OfflineResource `json:"list"`
	} `json:"data"`
}

// OfflineSubmitResp is the response from
// /v2/offline_download/task/submit
type OfflineSubmitResp struct {
	BaseResp
	Data struct {
		TaskList []struct {
			TaskID int64 `json:"task_id"`
			Result int   `json:"result"` // 0 if the task was created
		} `json:"task_list"`
	} `json:"data"`
}

// OfflineTask is an offline download task
type OfflineTask struct {
	TaskID     int64   `json:"task_id"`
	Name       string  `json:"name"`
	Status     int     `json:"status"`
	Size       int64   `json:"size"`
	Downloaded int64   `json:"downloaded"`
	Progress   float64 `json:"progress"`
	Speed      int64   `json:"speed"`
}

// OfflineTaskListResp is the response from /offline_download/task/list
type OfflineTaskListResp struct {
	BaseResp
	Data struct {
		List  []OfflineTask `json:"list"`
		Total int           `json:"total"`
	} `json:"data"`
}
