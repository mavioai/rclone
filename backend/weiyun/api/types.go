//go:build !plan9

// Package api defines types for Tencent Weiyun backend
package api

import (
	"time"

	weiyunsdkgo "github.com/foxxorcat/weiyun-sdk-go"
)

// FileItem wraps Weiyun SDK File
type FileItem struct {
	weiyunsdkgo.File
	PdirKey  string
	PPdirKey string
}

// ModTime returns modified time
func (f *FileItem) ModTime() time.Time {
	return time.Time(f.FileMtime)
}

// DirItem wraps Weiyun SDK Folder
type DirItem struct {
	weiyunsdkgo.Folder
	PdirKey  string
	PPdirKey string
}

// ModTime returns modified time
func (d *DirItem) ModTime() time.Time {
	return time.Time(d.DirMtime)
}
