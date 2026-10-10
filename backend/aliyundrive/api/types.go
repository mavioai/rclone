// Package api has type definitions for Aliyun Drive Open API
package api

import (
	"fmt"
	"time"
)

// Error codes returned by the API
const (
	CodeAccessTokenInvalid = "AccessTokenInvalid"
	CodeAccessTokenExpired = "AccessTokenExpired"
	CodeI400JD             = "I400JD" // returned by some endpoints when the token is invalid
	CodePreHashMatched     = "PreHashMatched"
	CodeNotFound           = "NotFound.File"
)

// ErrResp represents error returned by Aliyun Drive Open API
type ErrResp struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	StatusCode int    `json:"-"`
}

// Error satisfies the error interface
func (e *ErrResp) Error() string {
	return fmt.Sprintf("aliyundrive: %s: %s (HTTP %d)", e.Code, e.Message, e.StatusCode)
}

// IsTokenError returns true if the error means the access token must be renewed
func (e *ErrResp) IsTokenError() bool {
	switch e.Code {
	case CodeAccessTokenInvalid, CodeAccessTokenExpired, CodeI400JD:
		return true
	}
	return false
}

// TokenResp is the response from a token refresh
type TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	// Text is the error message of the online API
	Text string `json:"text"`
}

// DriveInfoResp represents the user drive info response
type DriveInfoResp struct {
	UserID          string `json:"user_id"`
	Name            string `json:"name"`
	DefaultDriveID  string `json:"default_drive_id"`
	ResourceDriveID string `json:"resource_drive_id"`
	BackupDriveID   string `json:"backup_drive_id"`
}

// File represents a file or folder in Aliyun Drive
type File struct {
	DriveID         string    `json:"drive_id"`
	FileID          string    `json:"file_id"`
	ParentFileID    string    `json:"parent_file_id"`
	Name            string    `json:"name"`
	Size            int64     `json:"size"`
	FileExtension   string    `json:"file_extension"`
	ContentHash     string    `json:"content_hash"`
	Category        string    `json:"category"`
	Type            string    `json:"type"` // "file" or "folder"
	Thumbnail       string    `json:"thumbnail"`
	URL             string    `json:"url"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	LocalCreatedAt  time.Time `json:"local_created_at"`
	LocalModifiedAt time.Time `json:"local_modified_at"`
	FileName        string    `json:"file_name"`
}

// FileListResp represents a paginated list of files
type FileListResp struct {
	Items      []File `json:"items"`
	NextMarker string `json:"next_marker"`
}

// PartInfo describes a single multipart upload slice
type PartInfo struct {
	PartNumber int    `json:"part_number"`
	PartSize   int64  `json:"part_size,omitempty"`
	UploadURL  string `json:"upload_url,omitempty"`
	Etag       string `json:"etag,omitempty"`
}

// CreateFileRequest is the body for creating a file or folder
type CreateFileRequest struct {
	DriveID         string     `json:"drive_id"`
	ParentFileID    string     `json:"parent_file_id"`
	Name            string     `json:"name"`
	Type            string     `json:"type"` // "file" or "folder"
	CheckNameMode   string     `json:"check_name_mode"`
	Size            int64      `json:"size,omitempty"`
	PreHash         string     `json:"pre_hash,omitempty"`
	ContentHash     string     `json:"content_hash,omitempty"`
	ContentHashName string     `json:"content_hash_name,omitempty"`
	ProofCode       string     `json:"proof_code,omitempty"`
	ProofVersion    string     `json:"proof_version,omitempty"`
	LocalCreatedAt  string     `json:"local_created_at,omitempty"`
	LocalModifiedAt string     `json:"local_modified_at,omitempty"`
	PartInfoList    []PartInfo `json:"part_info_list,omitempty"`
}

// CreateFileResponse is the response from creating a file or folder
type CreateFileResponse struct {
	FileID       string     `json:"file_id"`
	UploadID     string     `json:"upload_id"`
	FileName     string     `json:"file_name"`
	RapidUpload  bool       `json:"rapid_upload"`
	PartInfoList []PartInfo `json:"part_info_list"`
	Type         string     `json:"type"`
	Exist        bool       `json:"exist"`
}

// GetUploadURLRequest gets presigned upload URLs for upload parts
type GetUploadURLRequest struct {
	DriveID      string     `json:"drive_id"`
	FileID       string     `json:"file_id"`
	UploadID     string     `json:"upload_id"`
	PartInfoList []PartInfo `json:"part_info_list"`
}

// GetUploadURLResponse receives presigned upload URLs
type GetUploadURLResponse struct {
	FileID       string     `json:"file_id"`
	UploadID     string     `json:"upload_id"`
	PartInfoList []PartInfo `json:"part_info_list"`
}

// CompleteUploadRequest finalizes a multipart upload
type CompleteUploadRequest struct {
	DriveID      string     `json:"drive_id"`
	FileID       string     `json:"file_id"`
	UploadID     string     `json:"upload_id"`
	PartInfoList []PartInfo `json:"part_info_list,omitempty"`
}

// DownloadURLResponse receives a presigned download URL
type DownloadURLResponse struct {
	URL        string `json:"url"`
	Expiration string `json:"expiration"`
	Size       int64  `json:"size"`
}

// MoveCopyRequest represents move or copy payload
type MoveCopyRequest struct {
	DriveID        string `json:"drive_id"`
	FileID         string `json:"file_id"`
	ToParentFileID string `json:"to_parent_file_id"`
	NewName        string `json:"new_name,omitempty"`
	CheckNameMode  string `json:"check_name_mode,omitempty"`
}

// MoveCopyResponse represents move or copy response
type MoveCopyResponse struct {
	DriveID string `json:"drive_id"`
	FileID  string `json:"file_id"`
	Exist   bool   `json:"exist"`
}

// UpdateFileRequest updates file metadata
type UpdateFileRequest struct {
	DriveID         string `json:"drive_id"`
	FileID          string `json:"file_id"`
	Name            string `json:"name,omitempty"`
	CheckNameMode   string `json:"check_name_mode,omitempty"`
	LocalModifiedAt string `json:"local_modified_at,omitempty"`
}
