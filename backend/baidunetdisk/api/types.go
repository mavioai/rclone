// Package api defines types for Baidu Netdisk backend
package api

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// TokenResp represents OAuth token response
type TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// TokenErrResp represents OAuth token error
type TokenErrResp struct {
	Err              string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// Error satisfies the error interface
func (e *TokenErrResp) Error() string {
	return fmt.Sprintf("token refresh error: %s: %s", e.Err, e.ErrorDescription)
}

// File represents a file or directory item in Baidu Netdisk
type File struct {
	Category       int    `json:"category"`
	FsID           int64  `json:"fs_id"`
	Size           int64  `json:"size"`
	Path           string `json:"path"`
	ServerFilename string `json:"server_filename"`
	Md5            string `json:"md5"`
	Isdir          int    `json:"isdir"`
	ServerCtime    int64  `json:"server_ctime"`
	ServerMtime    int64  `json:"server_mtime"`
	LocalMtime     int64  `json:"local_mtime"`
	LocalCtime     int64  `json:"local_ctime"`
	Ctime          int64  `json:"ctime"`
	Mtime          int64  `json:"mtime"`
}

// IsDir returns true if directory
func (f *File) IsDir() bool {
	return f.Isdir == 1
}

// ModTime returns modified time
func (f *File) ModTime() time.Time {
	if f.ServerMtime > 0 {
		return time.Unix(f.ServerMtime, 0)
	}
	if f.Mtime > 0 {
		return time.Unix(f.Mtime, 0)
	}
	return time.Now()
}

// DecryptMd5 reverses Baidu's obfuscated MD5 string
func DecryptMd5(encryptMd5 string) string {
	if _, err := hex.DecodeString(encryptMd5); err == nil {
		return encryptMd5
	}
	if len(encryptMd5) != 32 {
		return encryptMd5
	}

	var out strings.Builder
	out.Grow(len(encryptMd5))
	for i := 0; i < len(encryptMd5); i++ {
		var n int64
		if i == 9 {
			n = int64(unicode.ToLower(rune(encryptMd5[i])) - 'g')
		} else {
			n, _ = strconv.ParseInt(encryptMd5[i:i+1], 16, 64)
		}
		out.WriteString(strconv.FormatInt(n^int64(15&i), 16))
	}

	s := out.String()
	if len(s) == 32 {
		return s[8:16] + s[:8] + s[24:32] + s[16:24]
	}
	return encryptMd5
}

// ListResp represents file list response
type ListResp struct {
	Errno int    `json:"errno"`
	List  []File `json:"list"`
}

// DownloadResp represents download link response
type DownloadResp struct {
	Errno  int    `json:"errno"`
	Errmsg string `json:"errmsg"`
	List   []struct {
		Dlink string `json:"dlink"`
	} `json:"list"`
}

// CrackDownloadResp represents dlna crack download link response
type CrackDownloadResp struct {
	Errno  int    `json:"errno"`
	Errmsg string `json:"errmsg"`
	Info   []struct {
		Dlink string `json:"dlink"`
	} `json:"info"`
}

// PrecreateResp represents precreate upload response
type PrecreateResp struct {
	Errno      int    `json:"errno"`
	ReturnType int    `json:"return_type"` // 1 = need chunk upload, 2 = rapid upload matched
	Path       string `json:"path"`
	UploadID   string `json:"uploadid"`
	BlockList  []int  `json:"block_list"`
	File       File   `json:"info"`
}

// UserInfoResp represents user info response
type UserInfoResp struct {
	Errno       int    `json:"errno"`
	BaiduName   string `json:"baidu_name"`
	NetdiskName string `json:"netdisk_name"`
	VipType     int    `json:"vip_type"` // 0 normal, 1 vip, 2 svip
	Uk          int64  `json:"uk"`
}

// QuotaResp represents quota response
type QuotaResp struct {
	Errno int   `json:"errno"`
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

// Error is the common error part of every Baidu response.
//
// The xpan APIs report errors with errno/errmsg while the PCS upload
// APIs use error_code/error_msg, usually with an HTTP 200 status.
type Error struct {
	Errno     int    `json:"errno"`
	Errmsg    string `json:"errmsg"`
	ShowMsg   string `json:"show_msg"`
	ErrorCode int    `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}

// Failed returns true if the response reports an error
func (e *Error) Failed() bool {
	return e.Errno != 0 || e.ErrorCode != 0
}

// Code returns the error code, whichever field it was reported in
func (e *Error) Code() int {
	if e.Errno != 0 {
		return e.Errno
	}
	return e.ErrorCode
}

// IsTokenError returns true if the access token is invalid or expired
func (e *Error) IsTokenError() bool {
	switch e.Errno {
	case 111, -6, 20016:
		return true
	}
	switch e.ErrorCode {
	case 110, 111:
		return true
	}
	return false
}

// Error satisfies the error interface
func (e *Error) Error() string {
	msg := e.Errmsg
	if msg == "" {
		msg = e.ErrorMsg
	}
	if msg == "" {
		msg = e.ShowMsg
	}
	return fmt.Sprintf("baidu netdisk error %d: %s (see https://pan.baidu.com/union/doc/)", e.Code(), msg)
}

// Errors returned in errno
const (
	ErrnoNotFound      = -9    // file or directory does not exist
	ErrnoAlreadyExists = -8    // file or directory already exists
	ErrnoRateLimited   = 31034 // hit the API rate limit
)
