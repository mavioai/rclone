// Package guangyadrive provides an interface to GuangYa Drive.
package guangyadrive

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/guangyadrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/linkcache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	accountBaseURL   = "https://account.guangyapan.com"
	apiBaseURL       = "https://api.guangyapan.com"
	defaultClient    = "aMe-8VSlkrbQXpUR"
	minSleep         = 50 * time.Millisecond
	maxSleep         = 4 * time.Second
	decayConstant    = 2
	defaultChunkSize = 8 * fs.Mebi
	minChunkSize     = 100 * fs.Kibi
	maxChunkSize     = 5 * fs.Gibi
	maxParts         = 10000
	taskPollInterval = 500 * time.Millisecond
	taskPollTries    = 120
	uploadPollTries  = 600

	// linkLifetime is how long a download link is kept when its URL
	// states no expiry (signed OSS URLs do, which linkcache reads); a
	// conservative guess
	linkLifetime = 10 * time.Minute

	// resource types
	resTypeDir = 2

	// get_res_center_token code meaning the file was uploaded instantly
	codeInstantUpload = 156
)

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "guangyadrive",
		Description: "GuangYa Drive",
		NewFs:       NewFs,
		Aliases:     []string{"guangya", "guangyacloud"},
		CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name:      "access_token",
			Help:      "GuangYa Drive access token.",
			Sensitive: true,
		}, {
			Name:      "refresh_token",
			Help:      "GuangYaPan refresh token for automatic token renewal.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "client_id",
			Help:     "OAuth Client ID.",
			Default:  defaultClient,
			Advanced: true,
		}, {
			Name:     "device_id",
			Help:     "Custom device ID (32 hex characters, auto-generated if empty).",
			Advanced: true,
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID to start in (default empty for root).",
			Default:  "",
			Advanced: true,
		}, {
			Name:     "page_size",
			Help:     "Page size for file listings.",
			Default:  100,
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: `Chunk size for multipart uploads.

Files are uploaded to the storage in parts of this size. Each part is
buffered in memory while it is uploaded so it can be retried.

The chunk size is increased automatically if needed to keep the number
of parts below 10,000.`,
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     "Path encoding for GuangYaPan.",
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines configuration options for GuangYaPan
type Options struct {
	AccessToken  string               `config:"access_token"`
	RefreshToken string               `config:"refresh_token"`
	ClientID     string               `config:"client_id"`
	DeviceID     string               `config:"device_id"`
	RootFolderID string               `config:"root_folder_id"`
	PageSize     int                  `config:"page_size"`
	ChunkSize    fs.SizeSuffix        `config:"chunk_size"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote GuangYaPan filesystem
type Fs struct {
	name         string
	root         string
	opt          Options
	features     *fs.Features
	accountSrv   *rest.Client // for the account API
	srv          *rest.Client // for the drive API
	rawSrv       *rest.Client // for OSS uploads and downloads
	pacer        *fs.Pacer
	dirCache     *dircache.DirCache
	tokenMu      *sync.Mutex // protects accessToken and refreshToken
	accessToken  string
	refreshToken string
	m            configmap.Mapper
}

// Object describes a GuangYaPan file
type Object struct {
	fs      *Fs
	remote  string
	id      string
	size    int64
	modTime time.Time
}

func randomDeviceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// errorHandler parses a non 2xx error response from the API into an error
func errorHandler(resp *http.Response) error {
	errResponse := new(api.Error)
	err := rest.DecodeJSON(resp, errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	if errResponse.Msg == "" && errResponse.ErrorDesc == "" && errResponse.ErrorName == "" {
		errResponse.Msg = resp.Status
	}
	errResponse.StatusCode = resp.StatusCode
	return errResponse
}

// rawErrorHandler turns a non 2xx response from OSS or the download
// server into an error
func rawErrorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		body = nil
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 512 {
		msg = msg[:512]
	}
	return fmt.Errorf("HTTP error %s: %s", resp.Status, msg)
}

// NewFs constructs an Fs
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if opt.ClientID == "" {
		opt.ClientID = defaultClient
	}
	if opt.DeviceID == "" {
		opt.DeviceID = randomDeviceID()
	}
	if opt.PageSize <= 0 {
		opt.PageSize = 100
	}
	if opt.ChunkSize < minChunkSize {
		return nil, fmt.Errorf("chunk_size %v is less than the minimum %v", opt.ChunkSize, minChunkSize)
	}
	if opt.ChunkSize > maxChunkSize {
		return nil, fmt.Errorf("chunk_size %v is more than the maximum %v", opt.ChunkSize, maxChunkSize)
	}

	client := fshttp.NewClient(ctx)
	accountSrv := rest.NewClient(client).SetRoot(accountBaseURL).SetErrorHandler(errorHandler)
	accountSrv.SetHeader("X-Device-Id", opt.DeviceID)
	accountSrv.SetHeader("X-Client-Id", opt.ClientID)

	srv := rest.NewClient(client).SetRoot(apiBaseURL).SetErrorHandler(errorHandler)
	srv.SetHeader("Did", opt.DeviceID)
	srv.SetHeader("Dt", "4")

	f := &Fs{
		name:         name,
		root:         root,
		opt:          *opt,
		accountSrv:   accountSrv,
		srv:          srv,
		rawSrv:       rest.NewClient(client).SetErrorHandler(rawErrorHandler),
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		tokenMu:      new(sync.Mutex),
		accessToken:  opt.AccessToken,
		refreshToken: opt.RefreshToken,
		m:            m,
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            false,
		WriteMimeType:           false,
		FindByListing:           true,
		// Estimates, not measurements
		MaxFileStreams: 2,
		MaxStreams:     6,
	}).Fill(ctx, f)

	// Ensure token is valid
	if f.accessToken == "" {
		err = f.renewToken(ctx, "")
		if err != nil {
			return nil, fmt.Errorf("failed to authenticate with GuangYaPan: %w", err)
		}
	}

	f.dirCache = dircache.New(root, f.opt.RootFolderID, f)

	// Find the current root directory ID
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, f.opt.RootFolderID, &tempF)
		tempF.root = newRoot
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err = tempF.NewObject(ctx, remote)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// getAccessToken returns the current access token
func (f *Fs) getAccessToken() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	return f.accessToken
}

// renewToken refreshes the access token using refresh_token
//
// oldToken is the access token which was found to be invalid. If it
// has been replaced already by another goroutine then nothing is done.
func (f *Fs) renewToken(ctx context.Context, oldToken string) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()

	if oldToken != "" && f.accessToken != oldToken {
		// Already refreshed by somebody else
		return nil
	}
	if f.refreshToken == "" {
		return errors.New("refresh_token is empty")
	}

	body := map[string]any{
		"client_id":     f.opt.ClientID,
		"grant_type":    "refresh_token",
		"refresh_token": f.refreshToken,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/v1/auth/token",
	}
	var out api.TokenResp
	err := f.pacer.Call(func() (bool, error) {
		out = api.TokenResp{}
		resp, err := f.accountSrv.CallJSON(ctx, &opts, body, &out)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return fmt.Errorf("refresh token request failed: %w", err)
	}
	if out.Error != "" || out.AccessToken == "" {
		errMsg := out.ErrorDesc
		if errMsg == "" {
			errMsg = out.Error
		}
		if errMsg == "" {
			errMsg = "no access token returned"
		}
		return fmt.Errorf("refresh token error: %s", errMsg)
	}

	f.accessToken = out.AccessToken
	if out.RefreshToken != "" {
		f.refreshToken = out.RefreshToken
	}
	f.m.Set("access_token", f.accessToken)
	f.m.Set("refresh_token", f.refreshToken)

	fs.Debugf(f, "Successfully refreshed GuangYaPan access token")
	return nil
}

// errorer is satisfied by API responses which can carry an error
type errorer interface {
	Err() error
}

// postAPI executes a call to the drive API with automatic token
// refresh on 401/403 and retries. The response envelope is checked
// for errors if it supports it.
func (f *Fs) postAPI(ctx context.Context, apiPath string, reqBody any, out any) error {
	if out == nil {
		out = new(api.CommonResp)
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   apiPath,
	}
	renewed := false
	return f.pacer.Call(func() (bool, error) {
		token := f.getAccessToken()
		opts.ExtraHeaders = map[string]string{
			"Authorization": "Bearer " + token,
		}
		resp, err := f.srv.CallJSON(ctx, &opts, reqBody, out)
		if err != nil && resp != nil && !renewed &&
			(resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			renewed = true
			fs.Debugf(f, "Token expired during call to %s, renewing token", apiPath)
			if renewErr := f.renewToken(ctx, token); renewErr != nil {
				return false, fmt.Errorf("%w (token renewal failed: %v)", err, renewErr)
			}
			return true, err
		}
		if err == nil {
			if e, ok := out.(errorer); ok {
				err = e.Err()
			}
		}
		return f.shouldRetry(ctx, resp, err)
	})
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// Name returns the configured name of the remote
func (f *Fs) Name() string { return f.name }

// Root returns the root of the remote
func (f *Fs) Root() string { return f.root }

// String returns a description of the remote
func (f *Fs) String() string { return fmt.Sprintf("guangyadrive root %q", f.root) }

// Precision of the remote
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the supported hash types
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// listAllInDir fetches all items under parentID
func (f *Fs) listAllInDir(ctx context.Context, parentID string) ([]api.FileItem, error) {
	var allItems []api.FileItem
	for page := 0; ; page++ {
		body := map[string]any{
			"parentId":  parentID,
			"page":      page,
			"pageSize":  f.opt.PageSize,
			"orderBy":   3,
			"sortType":  1,
			"fileTypes": []int{},
		}

		var resp api.ListResp
		err := f.postAPI(ctx, "/userres/v1/file/get_file_list", body, &resp)
		if err != nil {
			return nil, err
		}

		allItems = append(allItems, resp.Data.List...)

		if len(resp.Data.List) < f.opt.PageSize || (resp.Data.Total > 0 && len(allItems) >= resp.Data.Total) {
			break
		}
	}
	return allItems, nil
}

// findItem finds the item called leaf in dirID which is a directory
// if isDir is set or a file otherwise. It returns nil if not found.
func (f *Fs) findItem(ctx context.Context, dirID, leaf string, isDir bool) (*api.FileItem, error) {
	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		item := &items[i]
		if (item.ResType == resTypeDir) != isDir {
			continue
		}
		if f.opt.Enc.ToStandardName(item.FileName) == leaf {
			return item, nil
		}
	}
	return nil, nil
}

// findItemByID finds the item with fileID in dirID. It returns nil
// if not found.
func (f *Fs) findItemByID(ctx context.Context, dirID, fileID string) (*api.FileItem, error) {
	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].FileID == fileID {
			return &items[i], nil
		}
	}
	return nil, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID dirID
func (f *Fs) FindLeaf(ctx context.Context, dirID, leaf string) (entryID string, found bool, err error) {
	item, err := f.findItem(ctx, dirID, leaf, true)
	if err != nil {
		return "", false, err
	}
	if item == nil {
		return "", false, nil
	}
	return item.FileID, true, nil
}

// CreateDir makes a directory with dirID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (createdID string, err error) {
	body := map[string]any{
		"parentId": dirID,
		"dirName":  f.opt.Enc.FromStandardName(leaf),
	}

	var resp api.CreateDirResp
	err = f.postAPI(ctx, "/nd.bizuserres.s/v1/file/create_dir", body, &resp)
	if err != nil {
		return "", err
	}
	if resp.Data.FileID != "" {
		return resp.Data.FileID, nil
	}

	// The create response didn't carry the ID so look it up
	createdID, found, err := f.FindLeaf(ctx, dirID, leaf)
	if err != nil {
		return "", fmt.Errorf("failed to find created directory %q: %w", leaf, err)
	}
	if !found {
		return "", fmt.Errorf("created directory %q but couldn't find its ID", leaf)
	}
	return createdID, nil
}

// itemToObject converts an api.FileItem into an Object
func (f *Fs) itemToObject(remote string, item *api.FileItem) *Object {
	return &Object{
		fs:      f,
		remote:  remote,
		id:      item.FileID,
		size:    item.FileSize,
		modTime: api.UnixOrZero(item.UTime),
	}
}

// List the objects and directories in dir into entries. The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return nil, err
	}

	var entries fs.DirEntries
	for i := range items {
		item := &items[i]
		name := f.opt.Enc.ToStandardName(item.FileName)
		remote := path.Join(dir, name)

		if item.ResType == resTypeDir {
			f.dirCache.Put(remote, item.FileID)
			d := fs.NewDir(remote, api.UnixOrZero(item.UTime)).SetID(item.FileID)
			entries = append(entries, d)
		} else {
			entries = append(entries, f.itemToObject(remote, item))
		}
	}

	return entries, nil
}

// NewObject finds the Object at remote. If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	item, err := f.findItem(ctx, dirID, leaf, false)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return f.itemToObject(remote, item), nil
}

// Put the object
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existingObj, err := f.NewObject(ctx, src.Remote())
	switch {
	case err == nil:
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case errors.Is(err, fs.ErrorObjectNotFound):
		return f.PutUnchecked(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// PutUnchecked uploads the object without checking whether it
// exists already
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	fileID, err := f.upload(ctx, in, src, dirID, leaf)
	if err != nil {
		return nil, err
	}
	item, err := f.finishUpload(ctx, dirID, leaf, fileID, src.Size())
	if err != nil {
		return nil, err
	}
	return f.itemToObject(remote, item), nil
}

// finishUpload looks up the uploaded file with fileID in dirID and
// makes sure it is called leaf, renaming it if the server chose a
// different name.
func (f *Fs) finishUpload(ctx context.Context, dirID, leaf, fileID string, size int64) (*api.FileItem, error) {
	item, err := f.findItemByID(ctx, dirID, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to read uploaded file info: %w", err)
	}
	if item == nil {
		// Listing not consistent yet - make up the info
		fs.Debugf(f, "Uploaded file %q (%s) not found in listing yet", leaf, fileID)
		return &api.FileItem{
			FileID:   fileID,
			FileName: f.opt.Enc.FromStandardName(leaf),
			FileSize: size,
			UTime:    time.Now().Unix(),
		}, nil
	}
	if f.opt.Enc.ToStandardName(item.FileName) != leaf {
		fs.Debugf(f, "Server named uploaded file %q, renaming to %q", item.FileName, leaf)
		if err := f.rename(ctx, fileID, leaf); err != nil {
			return nil, fmt.Errorf("failed to rename uploaded file: %w", err)
		}
		item.FileName = f.opt.Enc.FromStandardName(leaf)
	}
	return item, nil
}

// upload uploads the contents of in as leaf in dirID returning the
// new file ID.
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, dirID, leaf string) (fileID string, err error) {
	size := src.Size()
	if size < 0 {
		return "", errors.New("can't upload files of unknown size")
	}
	tokenReq := map[string]any{
		"capacity": 2,
		"name":     f.opt.Enc.FromStandardName(leaf),
		"parentId": dirID,
		"res": map[string]any{
			"fileSize": size,
		},
	}
	var tokenResp api.UploadTokenResp
	err = f.postAPI(ctx, "/nd.bizuserres.s/v1/get_res_center_token", tokenReq, &tokenResp)
	if err != nil {
		return "", fmt.Errorf("failed to get upload token: %w", err)
	}
	token := tokenResp.Data
	if token.TaskID == "" {
		return "", errors.New("failed to get upload token: empty task id")
	}

	if tokenResp.Code == codeInstantUpload {
		fs.Debugf(src, "Instant upload")
	} else {
		target, err := newOSSTarget(&token)
		if err != nil {
			return "", err
		}
		err = f.uploadToOSS(ctx, target, in, size)
		if err != nil {
			return "", err
		}
	}

	return f.waitUploadTask(ctx, token.TaskID)
}

// waitUploadTask polls until the upload task finishes returning the
// new file ID
func (f *Fs) waitUploadTask(ctx context.Context, taskID string) (string, error) {
	for range uploadPollTries {
		var out api.TaskInfoResp
		err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/get_info_by_task_id", map[string]any{
			"taskId": taskID,
		}, &out)
		if err != nil {
			return "", fmt.Errorf("waiting for upload task: %w", err)
		}
		if out.Data.FileID != "" {
			return out.Data.FileID, nil
		}
		switch out.Code {
		case 0, 145, 146, 147, 155, 163:
			// uploading/verifying/processing
		default:
			if msg := strings.TrimSpace(out.Msg); msg != "" {
				return "", fmt.Errorf("upload task failed: %w", &api.Error{Code: out.Code, Msg: msg})
			}
		}
		if err := sleepCtx(ctx, taskPollInterval); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("timeout waiting for upload task %s", taskID)
}

// waitTaskDone polls until the background task finishes
func (f *Fs) waitTaskDone(ctx context.Context, taskID string) error {
	if taskID == "" {
		// operation was synchronous
		return nil
	}
	for range taskPollTries {
		var out api.TaskStatusResp
		err := f.postAPI(ctx, "/nd.bizuserres.s/v1/get_task_status", map[string]any{
			"taskId": taskID,
		}, &out)
		if err != nil {
			return fmt.Errorf("get task status failed: %w", err)
		}
		switch out.Data.Status {
		case 2:
			return nil
		case -1, 3:
			return fmt.Errorf("task %s failed with status %d", taskID, out.Data.Status)
		}
		if err := sleepCtx(ctx, taskPollInterval); err != nil {
			return err
		}
	}
	return fmt.Errorf("timeout waiting for task %s", taskID)
}

// sleepCtx sleeps for d or until the context is cancelled
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ------------------------------------------------------------
// OSS upload
// ------------------------------------------------------------

// ossTarget describes where and how to upload an object to OSS
type ossTarget struct {
	baseURL         string // scheme://bucket.host
	bucket          string
	object          string // object key without leading /
	accessKeyID     string
	secretAccessKey string
	securityToken   string
}

// newOSSTarget makes an ossTarget from the upload token
func newOSSTarget(token *api.UploadTokenData) (*ossTarget, error) {
	t := &ossTarget{
		bucket:          strings.TrimSpace(token.BucketName),
		object:          strings.TrimLeft(token.ObjectPath, "/"),
		accessKeyID:     token.AccessKeyID,
		secretAccessKey: token.SecretAccessKey,
		securityToken:   token.SessionToken,
	}
	if t.accessKeyID == "" {
		t.accessKeyID = token.Creds.AccessKeyID
	}
	if t.secretAccessKey == "" {
		t.secretAccessKey = token.Creds.SecretAccessKey
	}
	if t.securityToken == "" {
		t.securityToken = token.Creds.SessionToken
	}
	endpoint := strings.TrimSpace(token.EndPoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(token.FullEndPoint)
	}
	if t.bucket == "" || t.object == "" || endpoint == "" || t.accessKeyID == "" || t.secretAccessKey == "" {
		return nil, errors.New("upload token is incomplete")
	}
	baseURL, err := ossBaseURL(endpoint, t.bucket)
	if err != nil {
		return nil, err
	}
	t.baseURL = baseURL
	return t, nil
}

// ossBaseURL returns the virtual hosted style URL for the bucket at endpoint
func ossBaseURL(endpoint, bucket string) (string, error) {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid upload endpoint %q", endpoint)
	}
	host := u.Host
	if !strings.HasPrefix(host, bucket+".") {
		host = bucket + "." + host
	}
	return u.Scheme + "://" + host, nil
}

// url returns the URL of the object with the sub resource query
func (t *ossTarget) url(subResource string) string {
	segments := strings.Split(t.object, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	u := t.baseURL + "/" + strings.Join(segments, "/")
	if subResource != "" {
		u += "?" + subResource
	}
	return u
}

// sign returns the OSS V1 Authorization header for the request
func (t *ossTarget) sign(method, contentMD5, contentType, date, subResource string) string {
	ossHeaders := ""
	if t.securityToken != "" {
		ossHeaders = "x-oss-security-token:" + t.securityToken + "\n"
	}
	resource := "/" + t.bucket + "/" + t.object
	if subResource != "" {
		resource += "?" + subResource
	}
	stringToSign := method + "\n" + contentMD5 + "\n" + contentType + "\n" + date + "\n" + ossHeaders + resource
	mac := hmac.New(sha1.New, []byte(t.secretAccessKey))
	_, _ = mac.Write([]byte(stringToSign))
	return "OSS " + t.accessKeyID + ":" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// call makes a signed call to OSS under the pacer
//
// body is the request body which is re-read on each retry.
func (f *Fs) ossCall(ctx context.Context, t *ossTarget, method, subResource, contentType string, body []byte, result any) (*http.Response, error) {
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		date := time.Now().UTC().Format(http.TimeFormat)
		size := int64(len(body))
		opts := rest.Opts{
			Method:        method,
			RootURL:       t.url(subResource),
			Body:          bytes.NewReader(body),
			ContentLength: &size,
			ContentType:   contentType,
			ExtraHeaders: map[string]string{
				"Date":          date,
				"Authorization": t.sign(method, "", contentType, date, subResource),
			},
		}
		if t.securityToken != "" {
			opts.ExtraHeaders["x-oss-security-token"] = t.securityToken
		}
		var err error
		resp, err = f.rawSrv.Call(ctx, &opts)
		if err == nil {
			if result != nil {
				err = rest.DecodeXML(resp, result)
			} else {
				err = resp.Body.Close()
			}
		}
		return f.shouldRetry(ctx, resp, err)
	})
	return resp, err
}

// ossInitiateResult is the response to InitiateMultipartUpload
type ossInitiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	UploadID string   `xml:"UploadId"`
}

// ossCompletePart is a part in CompleteMultipartUpload
type ossCompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// ossComplete is the body of CompleteMultipartUpload
type ossComplete struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []ossCompletePart `xml:"Part"`
}

// chunkSize returns the part size to use for size bytes
func (f *Fs) chunkSize(size int64) int64 {
	chunkSize := int64(f.opt.ChunkSize)
	for size > chunkSize*maxParts {
		chunkSize *= 2
	}
	return chunkSize
}

// uploadToOSS uploads size bytes from in to the OSS target
//
// Small files are uploaded with a single PUT, larger ones with a
// multipart upload. Only one part is held in memory at once.
func (f *Fs) uploadToOSS(ctx context.Context, t *ossTarget, in io.Reader, size int64) (err error) {
	chunkSize := f.chunkSize(size)
	const contentType = "application/octet-stream"

	if size <= chunkSize {
		buf := make([]byte, size)
		if _, err := io.ReadFull(in, buf); err != nil {
			return fmt.Errorf("failed to read data to upload: %w", err)
		}
		_, err = f.ossCall(ctx, t, "PUT", "", contentType, buf, nil)
		if err != nil {
			return fmt.Errorf("oss upload failed: %w", err)
		}
		return nil
	}

	var initResult ossInitiateResult
	_, err = f.ossCall(ctx, t, "POST", "uploads", contentType, nil, &initResult)
	if err != nil {
		return fmt.Errorf("oss initiate multipart upload failed: %w", err)
	}
	uploadID := initResult.UploadID
	if uploadID == "" {
		return errors.New("oss initiate multipart upload returned empty upload id")
	}
	uploadIDQuery := "uploadId=" + url.QueryEscape(uploadID)
	defer func() {
		if err != nil {
			fs.Debugf(f, "Aborting multipart upload: %v", err)
			_, abortErr := f.ossCall(context.WithoutCancel(ctx), t, "DELETE", uploadIDQuery, "", nil, nil)
			if abortErr != nil {
				fs.Debugf(f, "Failed to abort multipart upload: %v", abortErr)
			}
		}
	}()

	buf := make([]byte, chunkSize)
	var parts []ossCompletePart
	for partNumber, remaining := 1, size; remaining > 0; partNumber++ {
		n := min(chunkSize, remaining)
		chunk := buf[:n]
		if _, err = io.ReadFull(in, chunk); err != nil {
			return fmt.Errorf("failed to read part %d: %w", partNumber, err)
		}
		subResource := "partNumber=" + strconv.Itoa(partNumber) + "&" + uploadIDQuery
		resp, err := f.ossCall(ctx, t, "PUT", subResource, contentType, chunk, nil)
		if err != nil {
			return fmt.Errorf("oss upload part %d failed: %w", partNumber, err)
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			return fmt.Errorf("oss upload part %d returned no ETag", partNumber)
		}
		parts = append(parts, ossCompletePart{PartNumber: partNumber, ETag: etag})
		remaining -= n
	}

	completeBody, err := xml.Marshal(ossComplete{Parts: parts})
	if err != nil {
		return err
	}
	_, err = f.ossCall(ctx, t, "POST", uploadIDQuery, "application/xml", completeBody, nil)
	if err != nil {
		return fmt.Errorf("oss complete multipart upload failed: %w", err)
	}
	return nil
}

// ------------------------------------------------------------
// Directory operations
// ------------------------------------------------------------

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir deletes the directory if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return err
	}
	if len(items) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	err = f.deleteItem(ctx, dirID)
	if err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Purge deletes all the files and directories including the old versions.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	err = f.deleteItem(ctx, dirID)
	if err != nil {
		return err
	}

	f.dirCache.FlushDir(dir)
	return nil
}

// deleteItem deletes a file or folder by ID
func (f *Fs) deleteItem(ctx context.Context, fileID string) error {
	body := map[string]any{
		"fileIds": []string{fileID},
	}
	var out api.DeleteResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/delete_file", body, &out)
	if err != nil {
		return err
	}
	return f.waitTaskDone(ctx, out.Data.TaskID)
}

// rename renames the file or directory fileID to leaf
func (f *Fs) rename(ctx context.Context, fileID, leaf string) error {
	body := map[string]any{
		"fileId":  fileID,
		"newName": f.opt.Enc.FromStandardName(leaf),
	}
	return f.postAPI(ctx, "/nd.bizuserres.s/v1/file/rename", body, nil)
}

// moveItem moves the file or directory fileID into dirID
func (f *Fs) moveItem(ctx context.Context, fileID, dirID string) error {
	body := map[string]any{
		"fileIds":  []string{fileID},
		"parentId": dirID,
	}
	var out api.DeleteResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/move_file", body, &out)
	if err != nil {
		return err
	}
	return f.waitTaskDone(ctx, out.Data.TaskID)
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcLeaf, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}

	if srcDirID != dstDirID {
		err = f.moveItem(ctx, srcObj.id, dstDirID)
		if err != nil {
			return nil, fmt.Errorf("move failed: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		err = f.rename(ctx, srcObj.id, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("rename after move failed: %w", err)
		}
	}

	dstObj := *srcObj
	dstObj.fs = f
	dstObj.remote = remote
	return &dstObj, nil
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}

	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}

	// Snapshot the destination so we can find the new file
	before, err := f.listAllInDir(ctx, dstDirID)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, len(before))
	var oldDst *api.FileItem
	for i := range before {
		item := &before[i]
		existing[item.FileID] = struct{}{}
		if item.ResType != resTypeDir && f.opt.Enc.ToStandardName(item.FileName) == dstLeaf {
			oldDst = item
		}
	}

	body := map[string]any{
		"fileIds":  []string{srcObj.id},
		"parentId": dstDirID,
	}
	var out api.DeleteResp
	err = f.postAPI(ctx, "/nd.bizuserres.s/v1/file/copy_file", body, &out)
	if err != nil {
		return nil, fmt.Errorf("copy failed: %w", err)
	}
	err = f.waitTaskDone(ctx, out.Data.TaskID)
	if err != nil {
		return nil, fmt.Errorf("copy failed: %w", err)
	}

	after, err := f.listAllInDir(ctx, dstDirID)
	if err != nil {
		return nil, err
	}
	var newItem *api.FileItem
	srcName := f.opt.Enc.FromStandardName(path.Base(srcObj.remote))
	for i := range after {
		item := &after[i]
		if item.ResType == resTypeDir {
			continue
		}
		if _, found := existing[item.FileID]; found {
			continue
		}
		if newItem == nil || item.FileName == srcName {
			newItem = item
		}
	}
	if newItem == nil {
		return nil, errors.New("copy succeeded but couldn't find the copied file")
	}

	// Remove the file we are overwriting if any
	if oldDst != nil && oldDst.FileID != newItem.FileID {
		err = f.deleteItem(ctx, oldDst.FileID)
		if err != nil {
			return nil, fmt.Errorf("failed to remove old destination file: %w", err)
		}
	}
	if f.opt.Enc.ToStandardName(newItem.FileName) != dstLeaf {
		err = f.rename(ctx, newItem.FileID, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("rename after copy failed: %w", err)
		}
	}
	return f.itemToObject(remote, newItem), nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	if srcDirectoryID != dstDirectoryID {
		err = f.moveItem(ctx, srcID, dstDirectoryID)
		if err != nil {
			return fmt.Errorf("directory move failed: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		err = f.rename(ctx, srcID, dstLeaf)
		if err != nil {
			return fmt.Errorf("rename after directory move failed: %w", err)
		}
	}

	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// ------------------------------------------------------------
// Object methods
// ------------------------------------------------------------

// String returns remote name
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns remote path
func (o *Object) Remote() string { return o.remote }

// ModTime returns modification time
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// Size returns size of object
func (o *Object) Size() int64 { return o.size }

// Fs returns parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Hash returns unsupported hash
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable returns storable
func (o *Object) Storable() bool { return true }

// Open opens file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	key := linkcache.Key(o.fs.name, o.id, strconv.FormatInt(o.size, 10), strconv.FormatInt(o.modTime.UnixNano(), 10))
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the download link of the object
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	var resp api.DownloadResp
	err := o.fs.postAPI(ctx, "/nd.bizuserres.s/v1/get_res_download_url", map[string]any{
		"fileId": o.id,
	}, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to get download URL: %w", err)
	}

	downloadURL := strings.TrimSpace(resp.Data.SignedURL)
	if downloadURL == "" {
		downloadURL = strings.TrimSpace(resp.Data.DownloadURL)
	}
	if downloadURL == "" {
		return nil, errors.New("empty download URL returned")
	}
	return &linkcache.Link{URL: downloadURL}, nil
}

// downloadError is a download refused by the storage, carrying its
// HTTP status so that linkcache.Refused sees it.
type downloadError struct {
	status int
	err    error
}

// Error implements error
func (e *downloadError) Error() string { return e.err.Error() }

// Unwrap returns the underlying error
func (e *downloadError) Unwrap() error { return e.err }

// HTTPStatusCode returns the HTTP status of the download
func (e *downloadError) HTTPStatusCode() int { return e.status }

// openLink reads the object from link
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: link.URL,
		Options: options,
	}
	var httpResp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var err error
		httpResp, err = o.fs.rawSrv.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, httpResp, err)
	})
	if err != nil {
		if httpResp != nil {
			return nil, &downloadError{status: httpResp.StatusCode, err: err}
		}
		return nil, err
	}
	return httpResp.Body, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new file is uploaded first and the old one deleted only once
// the upload has succeeded.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	fileID, err := o.fs.upload(ctx, in, src, dirID, leaf)
	if err != nil {
		return err
	}
	if o.id != "" && o.id != fileID {
		err = o.fs.deleteItem(ctx, o.id)
		if err != nil {
			return fmt.Errorf("failed to remove old version: %w", err)
		}
	}
	item, err := o.fs.finishUpload(ctx, dirID, leaf, fileID, src.Size())
	if err != nil {
		return err
	}
	o.id = item.FileID
	o.size = item.FileSize
	o.modTime = api.UnixOrZero(item.UTime)
	return nil
}

// Remove deletes object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteItem(ctx, o.id)
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, mtime time.Time) error {
	return fs.ErrorCantSetModTime
}

// ID returns item ID
func (o *Object) ID() string { return o.id }

// offlineName returns the name GuangYaPan gives an offline download
// when none is set: the torrent's name or the last element of the URL.
func offlineName(resolved *api.OfflineResolveResp, rawURL string) string {
	if bt := resolved.Data.BTResInfo; bt != nil && strings.TrimSpace(bt.FileName) != "" {
		return strings.TrimSpace(bt.FileName)
	}
	if u, err := url.Parse(rawURL); err == nil {
		name := strings.TrimSpace(path.Base(u.Path))
		if name != "" && name != "." && name != "/" {
			if decoded, err := url.PathUnescape(name); err == nil {
				name = decoded
			}
			return name
		}
	}
	return "offline_download"
}

// AddURL submits a remote URL to be downloaded server-side.
//
// GuangYaPan first resolves the URL, then downloads it (all the files
// of a torrent) into the directory given by opt["path"].
func (f *Fs) AddURL(ctx context.Context, rawURL string, opt map[string]string) (*fs.OfflineTask, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("offline download URL is empty")
	}
	dirID, err := f.dirCache.FindDir(ctx, opt["path"], false)
	if err != nil {
		return nil, fmt.Errorf("find target dir failed: %w", err)
	}

	var resolved api.OfflineResolveResp
	if err := f.postAPI(ctx, "/cloudcollection/v1/resolve_res", map[string]any{"url": rawURL}, &resolved); err != nil {
		return nil, fmt.Errorf("resolve offline download failed: %w", err)
	}
	taskURL := strings.TrimSpace(resolved.Data.URL)
	if taskURL == "" {
		taskURL = rawURL
	}
	name := opt["name"]
	if name == "" {
		name = offlineName(&resolved, taskURL)
	}
	var size int64
	body := map[string]any{
		"url":      taskURL,
		"parentId": dirID,
		"newName":  f.opt.Enc.FromStandardName(name),
	}
	if bt := resolved.Data.BTResInfo; bt != nil && len(bt.Subfiles) > 0 {
		size = bt.FileSize
		indexes := make([]int, 0, len(bt.Subfiles))
		for i, file := range bt.Subfiles {
			if file.FileIndex != nil {
				i = *file.FileIndex
			}
			indexes = append(indexes, i)
		}
		body["fileIndexes"] = indexes
	}

	var created api.OfflineCreateResp
	if err := f.postAPI(ctx, "/cloudcollection/v1/create_task", body, &created); err != nil {
		return nil, fmt.Errorf("create offline download failed: %w", err)
	}
	taskID := strings.TrimSpace(created.Data.TaskID)
	if taskID == "" {
		return nil, errors.New("create offline download failed: no task ID in response")
	}
	return &fs.OfflineTask{
		ID:        taskID,
		Name:      name,
		URL:       rawURL,
		Size:      size,
		Status:    fs.OfflineTaskStatusInit,
		CreatedAt: time.Now(),
	}, nil
}

// offlineTask converts a GuangYaPan offline task
func (f *Fs) offlineTask(t *api.OfflineTask) *fs.OfflineTask {
	task := &fs.OfflineTask{
		ID:        t.TaskID,
		Name:      f.opt.Enc.ToStandardName(t.FileName),
		URL:       t.Res,
		Size:      t.TotalSize,
		Status:    fs.OfflineTaskStatusRunning,
		Progress:  float64(t.Progress),
		CreatedAt: api.UnixOrZero(t.CreateTime),
	}
	switch t.Status {
	case api.OfflineStatusQueued:
		task.Status = fs.OfflineTaskStatusInit
	case api.OfflineStatusCompleted:
		task.Status = fs.OfflineTaskStatusComplete
	case api.OfflineStatusPartiallyCompleted:
		task.Status = fs.OfflineTaskStatusComplete
		task.ErrorMsg = "partially completed"
	case api.OfflineStatusFailed:
		task.Status = fs.OfflineTaskStatusError
		task.ErrorMsg = "failed"
	case api.OfflineStatusCanceled:
		task.Status = fs.OfflineTaskStatusError
		task.ErrorMsg = "canceled"
	}
	return task
}

// ListOfflineTasks returns server-side offline download tasks, paging
// back to since.
func (f *Fs) ListOfflineTasks(ctx context.Context, since time.Time) ([]*fs.OfflineTask, error) {
	var tasks []*fs.OfflineTask
	cursor := ""
	for {
		body := map[string]any{"pageSize": f.opt.PageSize}
		if cursor != "" {
			body["cursor"] = cursor
		}
		var resp api.OfflineListResp
		if err := f.postAPI(ctx, "/cloudcollection/v1/list_task", body, &resp); err != nil {
			return nil, fmt.Errorf("list offline tasks failed: %w", err)
		}
		page := make([]*fs.OfflineTask, len(resp.Data.List))
		for i := range resp.Data.List {
			page[i] = f.offlineTask(&resp.Data.List[i])
		}
		tasks = append(tasks, page...)
		if len(resp.Data.List) == 0 || resp.Data.Cursor == "" || resp.Data.Cursor == cursor || fs.OfflineTasksBefore(page, since) {
			return tasks, nil
		}
		cursor = resp.Data.Cursor
	}
}

// DeleteOfflineTask deletes a server-side offline download task.
func (f *Fs) DeleteOfflineTask(ctx context.Context, taskID string) error {
	if err := f.postAPI(ctx, "/cloudcollection/v2/delete_task", map[string]any{"taskIds": []string{taskID}}, nil); err != nil {
		return fmt.Errorf("delete offline task failed: %w", err)
	}
	return nil
}

var commandHelp = []fs.CommandHelp{{
	Name:  "addurl",
	Short: "Add offline download task for url.",
	Long: `This command adds an offline (cloud-side) download task for a url
(http, magnet or ed2k). For a torrent all of its files are downloaded.
It is also available as "offline".

Usage examples:

` + "```console" + `
rclone backend addurl guangyadrive:dirpath url
rclone backend addurl guangyadrive:dirpath url -o name=custom_name
` + "```" + `

Downloads will be stored in 'dirpath', which must exist.

The result is the new task as JSON with the fields "id", "name",
"url", "size", "status" ("init") and "created_at".`,
	Opts: map[string]string{
		"name": "Custom name for the downloaded file or directory.",
	},
}, {
	Name:  "tasks",
	Short: "List offline download tasks.",
	Long: `This command lists the offline download tasks. It is also
available as "listoffline".

Usage example:

` + "```console" + `
rclone backend tasks guangyadrive:
` + "```" + `

The result is a JSON list of tasks with the fields "id", "name",
"url", "size", "status" ("init", "running", "complete" or "error"),
"progress" (percentage), "error_msg" and "created_at".`,
}, {
	Name:  "deltask",
	Short: "Delete an offline download task.",
	Long: `This command deletes the offline download task with the given ID.
It is also available as "deleteoffline".

Usage example:

` + "```console" + `
rclone backend deltask guangyadrive: task_id
` + "```",
}}

// Command runs named commands on the backend
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "addurl", "offline":
		if len(arg) != 1 {
			return nil, errors.New("need exactly 1 URL argument (magnet/ed2k/http)")
		}
		return f.AddURL(ctx, arg[0], opt)
	case "tasks", "listoffline":
		return f.ListOfflineTasks(ctx, time.Time{})
	case "deltask", "deleteoffline":
		if len(arg) != 1 {
			return nil, errors.New("need exactly 1 task ID argument")
		}
		if err := f.DeleteOfflineTask(ctx, arg[0]); err != nil {
			return nil, err
		}
		return map[string]any{"status": "success", "deleted": arg[0]}, nil
	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// Interface checks
var (
	_ fs.Commander         = (*Fs)(nil)
	_ fs.OfflineDownloader = (*Fs)(nil)
	_ fs.Fs                = (*Fs)(nil)
	_ fs.Purger            = (*Fs)(nil)
	_ fs.Copier            = (*Fs)(nil)
	_ fs.Mover             = (*Fs)(nil)
	_ fs.DirMover          = (*Fs)(nil)
	_ fs.PutUncheckeder    = (*Fs)(nil)
	_ fs.DirCacheFlusher   = (*Fs)(nil)
	_ dircache.DirCacher   = (*Fs)(nil)
	_ fs.Object            = (*Object)(nil)
	_ fs.IDer              = (*Object)(nil)
)
