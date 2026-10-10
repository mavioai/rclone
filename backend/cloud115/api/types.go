// Package api defines types for 115 Cloud backend
package api

import (
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
)

// FileInfoWithThumb wraps driver115.FileInfo with thumbnail
type FileInfoWithThumb struct {
	driver115.FileInfo
	ThumbURL string `json:"u"`
}

// FileListRespWithThumb is the response for list files with thumbnail
type FileListRespWithThumb struct {
	driver115.BasicResp
	CategoryID driver115.IntString `json:"cid"`
	Count      int                 `json:"count"`
	Offset     int                 `json:"offset"`
	Files      []FileInfoWithThumb `json:"data"`
}

// UnixTime formats unix timestamp into time.Time
func UnixTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts, 0)
}
