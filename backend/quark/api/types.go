// Package api defines types for Quark Drive backend
package api

import (
	"fmt"
	"time"
)

// Resp represents standard Quark JSON response envelope
type Resp struct {
	Status  int    `json:"status"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error formats error response
func (e *Resp) Error() string {
	return fmt.Sprintf("quark error: status=%d, code=%d, message=%s", e.Status, e.Code, e.Message)
}

// Err returns the response as an error if it reports a failure or
// nil otherwise
func (e *Resp) Err() error {
	if e.Status >= 400 || e.Code != 0 {
		return e
	}
	return nil
}

// CodeFileNotFound is the code returned by /file/info for a fid which
// doesn't exist
const CodeFileNotFound = 21001

// IsNotFound returns true if the response reports a missing file
func (e *Resp) IsNotFound() bool {
	return e.Status == 404 && e.Code == CodeFileNotFound
}

// File represents a file or directory in Quark
type File struct {
	Fid        string `json:"fid"`
	FileName   string `json:"file_name"`
	Category   int    `json:"category"`
	Size       int64  `json:"size"`
	File       bool   `json:"file"` // true = file, false = directory
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
	LCreatedAt int64  `json:"l_created_at"`
	LUpdatedAt int64  `json:"l_updated_at"`
}

// IsDir returns true if item is directory
func (f *File) IsDir() bool {
	return !f.File
}

// ModTime returns last modified time
func (f *File) ModTime() time.Time {
	if f.UpdatedAt > 0 {
		return time.UnixMilli(f.UpdatedAt)
	}
	return time.UnixMilli(f.CreatedAt)
}

// SortResp represents file list response
type SortResp struct {
	Resp
	Data struct {
		List []File `json:"list"`
	} `json:"data"`
	Metadata struct {
		Size  int    `json:"_size"`
		Page  int    `json:"_page"`
		Count int    `json:"_count"`
		Total int    `json:"_total"`
		Way   string `json:"way"`
	} `json:"metadata"`
}

// DownResp represents file download URL response
type DownResp struct {
	Resp
	Data []struct {
		DownloadURL string `json:"download_url"`
	} `json:"data"`
}

// UpPreResp represents upload pre-initialization response
type UpPreResp struct {
	Resp
	Data struct {
		TaskID    string `json:"task_id"`
		Finish    bool   `json:"finish"`
		UploadID  string `json:"upload_id"`
		ObjKey    string `json:"obj_key"`
		UploadURL string `json:"upload_url"`
		Fid       string `json:"fid"`
		Bucket    string `json:"bucket"`
		Callback  struct {
			CallbackURL  string `json:"callbackUrl"`
			CallbackBody string `json:"callbackBody"`
		} `json:"callback"`
		FormatType string `json:"format_type"`
		Size       int64  `json:"size"`
		AuthInfo   string `json:"auth_info"`
	} `json:"data"`
	Metadata struct {
		PartThread int `json:"part_thread"`
		PartSize   int `json:"part_size"`
	} `json:"metadata"`
}

// HashResp represents rapid-upload hash check response
type HashResp struct {
	Resp
	Data struct {
		Finish     bool   `json:"finish"`
		Fid        string `json:"fid"`
		Thumbnail  string `json:"thumbnail"`
		FormatType string `json:"format_type"`
	} `json:"data"`
}

// UpAuthResp represents part upload auth response
type UpAuthResp struct {
	Resp
	Data struct {
		AuthKey string `json:"auth_key"`
		Speed   int    `json:"speed"`
	} `json:"data"`
}

// FileInfoResp represents the /file/info response
type FileInfoResp struct {
	Resp
	Data File `json:"data"`
}

// CreateDirResp represents the create directory response
type CreateDirResp struct {
	Resp
	Data struct {
		Finish bool   `json:"finish"`
		Fid    string `json:"fid"`
	} `json:"data"`
}
