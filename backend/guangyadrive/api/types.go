// Package api has type definitions for GuangYaPan API
package api

import (
	"fmt"
	"strings"
	"time"
)

// BaseResp is the envelope common to the GuangYaPan API responses
type BaseResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Err returns an error if the response reports a failure.
//
// The API reports success with the message "success" (or an empty
// message); anything else is a failure.
func (r *BaseResp) Err() error {
	msg := strings.TrimSpace(r.Msg)
	if msg == "" || strings.EqualFold(msg, "success") {
		return nil
	}
	return &Error{Code: r.Code, Msg: msg}
}

// Error is an error returned by the GuangYaPan API
type Error struct {
	StatusCode int    `json:"-"`
	Code       int    `json:"code"`
	Msg        string `json:"msg"`
	ErrorName  string `json:"error"`
	ErrorDesc  string `json:"error_description"`
}

// Error satisfies the error interface
func (e *Error) Error() string {
	msg := e.Msg
	if msg == "" {
		msg = e.ErrorDesc
	}
	if msg == "" {
		msg = e.ErrorName
	}
	out := fmt.Sprintf("guangyadrive error: %s (code %d)", msg, e.Code)
	if e.StatusCode != 0 {
		out += fmt.Sprintf(" (HTTP status %d)", e.StatusCode)
	}
	return out
}

// TokenResp represents the OAuth token response
type TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Sub          string `json:"sub"`
	Error        string `json:"error"`
	ErrorCode    int    `json:"error_code"`
	ErrorDesc    string `json:"error_description"`
}

// VerificationResp represents the SMS verification response
type VerificationResp struct {
	VerificationID string `json:"verification_id"`
	Error          string `json:"error"`
	ErrorCode      int    `json:"error_code"`
	ErrorDesc      string `json:"error_description"`
}

// CaptchaInitResp represents captcha initialization response
type CaptchaInitResp struct {
	CaptchaToken string `json:"captcha_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Error        string `json:"error"`
	ErrorCode    int    `json:"error_code"`
	ErrorDesc    string `json:"error_description"`
}

// VerifyResp represents captcha verification response
type VerifyResp struct {
	VerificationToken string `json:"verification_token"`
	Error             string `json:"error"`
	ErrorCode         int    `json:"error_code"`
	ErrorDesc         string `json:"error_description"`
}

// UserMeResp represents user info response
type UserMeResp struct {
	Sub string `json:"sub"`
}

// ListResp represents file listing response
type ListResp struct {
	BaseResp
	Data struct {
		Total int        `json:"total"`
		List  []FileItem `json:"list"`
	} `json:"data"`
}

// FileItem represents a single file or directory item
type FileItem struct {
	FileID   string `json:"fileId"`
	ParentID string `json:"parentId"`
	FileName string `json:"fileName"`
	FileSize int64  `json:"fileSize"`
	ResType  int    `json:"resType"` // 2: folder, 1: file
	CTime    int64  `json:"ctime"`
	UTime    int64  `json:"utime"`
}

// DownloadResp represents download URL response
type DownloadResp struct {
	BaseResp
	Data struct {
		SignedURL   string `json:"signedURL"`
		DownloadURL string `json:"downloadUrl"`
	} `json:"data"`
}

// CreateDirResp represents directory creation response
type CreateDirResp struct {
	BaseResp
	Data struct {
		FileID   string `json:"fileId"`
		FileName string `json:"fileName"`
		ResType  int    `json:"resType"`
		CTime    int64  `json:"ctime"`
		UTime    int64  `json:"utime"`
	} `json:"data"`
}

// CommonResp represents generic API response
type CommonResp struct {
	BaseResp
}

// DeleteResp represents file deletion task response
type DeleteResp struct {
	BaseResp
	Data struct {
		TaskID string `json:"taskId"`
	} `json:"data"`
}

// TaskStatusResp represents background task status response
type TaskStatusResp struct {
	BaseResp
	Data struct {
		Status int `json:"status"`
	} `json:"data"`
}

// UploadTokenResp represents upload token response
type UploadTokenResp struct {
	BaseResp
	Data UploadTokenData `json:"data"`
}

// UploadTokenData represents credentials and endpoint for upload
type UploadTokenData struct {
	TaskID          string `json:"taskId"`
	ObjectPath      string `json:"objectPath"`
	Provider        any    `json:"provider"`
	Region          string `json:"region"`
	BucketName      string `json:"bucketName"`
	EndPoint        string `json:"endPoint"`
	FullEndPoint    string `json:"fullEndPoint"`
	CallbackVar     string `json:"callbackVar"`
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Creds           struct {
		AccessKeyID     string `json:"accessKeyID"`
		SecretAccessKey string `json:"secretAccessKey"`
		SessionToken    string `json:"sessionToken"`
	} `json:"creds"`
}

// TaskInfoResp represents task info response
//
// It does not embed BaseResp as in progress tasks are reported with
// non zero codes and messages which are not errors.
type TaskInfoResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		FileID string `json:"fileId"`
	} `json:"data"`
}

// UnixOrZero converts unix timestamp seconds to time.Time, returning zero time if non-positive
func UnixOrZero(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.Unix(v, 0)
}

// Offline download task states reported by /cloudcollection/v1/list_task
const (
	OfflineStatusQueued             = 0
	OfflineStatusRunning            = 1
	OfflineStatusCompleted          = 2
	OfflineStatusFailed             = 3
	OfflineStatusCanceled           = 4
	OfflineStatusPartiallyCompleted = 5
)

// OfflineResolveResp is the response from /cloudcollection/v1/resolve_res
type OfflineResolveResp struct {
	BaseResp
	Data struct {
		ResType   int    `json:"resType"`
		URL       string `json:"url"`
		BTResInfo *struct {
			InfoHash string `json:"infoHash"`
			FileName string `json:"fileName"`
			FileSize int64  `json:"fileSize"`
			Subfiles []struct {
				FileName  string `json:"fileName"`
				FileIndex *int   `json:"fileIndex"`
				FileSize  int64  `json:"fileSize"`
			} `json:"subfiles"`
		} `json:"btResInfo"`
	} `json:"data"`
}

// OfflineCreateResp is the response from /cloudcollection/v1/create_task
type OfflineCreateResp struct {
	BaseResp
	Data struct {
		TaskID string `json:"taskId"`
	} `json:"data"`
}

// OfflineTask is an offline download task
type OfflineTask struct {
	TaskID     string `json:"taskId"`
	FileName   string `json:"fileName"`
	TotalSize  int64  `json:"totalSize"`
	Status     int    `json:"status"`
	CreateTime int64  `json:"createTime"`
	Res        string `json:"res"`
	Progress   int    `json:"progress"`
}

// OfflineListResp is the response from /cloudcollection/v1/list_task
type OfflineListResp struct {
	BaseResp
	Data struct {
		Cursor string        `json:"cursor"`
		List   []OfflineTask `json:"list"`
	} `json:"data"`
}
