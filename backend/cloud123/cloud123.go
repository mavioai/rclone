// Package cloud123 provides an interface to 123 Cloud.
package cloud123

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/cloud123/api"
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
	baseURL          = "https://yun.123pan.com/b/api"
	signPrefix       = "/b/api"
	loginURL         = "https://login.123pan.com/api/user/sign_in"
	minSleep         = 50 * time.Millisecond
	maxSleep         = 4 * time.Second
	decayConstant    = 2
	minChunkSize     = 5 * fs.Mebi
	maxChunkSize     = 16 * fs.Mebi
	defaultChunkSize = maxChunkSize
	// linkLifetime is how long a download link is kept when its URL
	// states no expiry (123's usually carry it in t or auth_key, which
	// linkcache reads); a conservative guess
	linkLifetime = 10 * time.Minute
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cloud123",
		Description: "123 Cloud / 123 Drive",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Aliases:     []string{"123", "123cloud", "123drive"},
		Options: []fs.Option{{
			Name: "user",
			Help: "123 Cloud login username (phone number or email).",
		}, {
			Name:       "pass",
			Help:       "123 Cloud login password.",
			IsPassword: true,
		}, {
			Name:      "token",
			Help:      "Existing authorization token (optional, auto-generated upon login).",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     "root_folder_id",
			Help:     "ID of the root folder (default '0' for root).",
			Default:  "0",
			Advanced: true,
		}, {
			Name:     "chunk_size",
			Help:     "Upload chunk size (5M to 16M).",
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     "Path encoding for 123Pan.",
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines configuration parameters for 123Pan
type Options struct {
	Username     string               `config:"user"`
	Password     string               `config:"pass"`
	Token        string               `config:"token"`
	RootFolderID string               `config:"root_folder_id"`
	ChunkSize    fs.SizeSuffix        `config:"chunk_size"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote 123Pan file system
type Fs struct {
	name      string
	root      string
	opt       Options
	features  *fs.Features
	srv       *rest.Client // API client
	uploadSrv *rest.Client // client for presigned upload and download URLs
	pacer     *fs.Pacer
	dirCache  *dircache.DirCache
	tokenMu   *sync.Mutex
	token     string
	m         configmap.Mapper
}

// Object describes a 123Pan file
type Object struct {
	fs        *Fs
	remote    string
	id        int64
	size      int64
	etag      string
	s3KeyFlag string
	modTime   time.Time
	listedURL string // download URL the listing gave, used once; may be empty
}

// NewFs creates a new 123Pan filesystem
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if opt.RootFolderID == "" {
		opt.RootFolderID = "0"
	}
	if opt.ChunkSize < minChunkSize || opt.ChunkSize > maxChunkSize {
		return nil, fmt.Errorf("chunk_size %v must be between %v and %v", opt.ChunkSize, minChunkSize, maxChunkSize)
	}
	if opt.Token == "" && (opt.Username == "" || opt.Password == "") {
		return nil, errors.New("cloud123: either token, or user and pass must be provided")
	}

	client := fshttp.NewClient(ctx)
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		srv:       rest.NewClient(client).SetRoot(baseURL).SetErrorHandler(errorHandler),
		uploadSrv: rest.NewClient(client),
		pacer:     fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		tokenMu:   new(sync.Mutex),
		token:     opt.Token,
		m:         m,
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            false,
		WriteMimeType:           false,
		FindByListing:           true,
		// Estimates, not measurements
		MaxFileStreams: 3,
		MaxStreams:     8,
	}).Fill(ctx, f)

	if f.token == "" {
		if err = f.login(ctx, ""); err != nil {
			return nil, fmt.Errorf("failed to login to 123 Cloud: %w", err)
		}
	}

	f.dirCache = dircache.New(root, f.opt.RootFolderID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		if !errors.Is(err, fs.ErrorDirNotFound) {
			return nil, err
		}
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, f.opt.RootFolderID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			if errors.Is(err, fs.ErrorDirNotFound) {
				// No root so return old f
				return f, nil
			}
			return nil, err
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		// Update the old f instead of returning tempF, since the
		// features were filled with methods having *f as receiver.
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// signPath calculates the CRC32 path signatures expected by 123Pan
func signPath(apiPath string, os string, version string) (k, v string) {
	table := []byte{'a', 'd', 'e', 'f', 'g', 'h', 'l', 'm', 'y', 'i', 'j', 'n', 'o', 'p', 'k', 'q', 'r', 's', 't', 'u', 'b', 'c', 'v', 'w', 's', 'z'}
	randVal := fmt.Sprintf("%.f", math.Round(1e7*rand.Float64()))
	now := time.Now().In(time.FixedZone("CST", 8*3600))
	timestamp := fmt.Sprint(now.Unix())
	nowStr := []byte(now.Format("200601021504"))
	for i := 0; i < len(nowStr); i++ {
		idx := int(nowStr[i]) - 48
		if idx >= 0 && idx < len(table) {
			nowStr[i] = table[idx]
		}
	}
	timeSign := fmt.Sprint(crc32.ChecksumIEEE(nowStr))
	data := strings.Join([]string{timestamp, randVal, apiPath, os, version, timeSign}, "|")
	dataSign := fmt.Sprint(crc32.ChecksumIEEE([]byte(data)))
	return timeSign, strings.Join([]string{timestamp, randVal, dataSign}, "-")
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	var base api.BaseResp
	if err := rest.DecodeJSON(resp, &base); err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	apiErr := &api.Error{Code: base.Code, Message: base.Message}
	if apiErr.Code == 0 {
		apiErr.Code = resp.StatusCode
	}
	if apiErr.Message == "" {
		apiErr.Message = resp.Status
	}
	return apiErr
}

// getToken returns the current access token
func (f *Fs) getToken() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	return f.token
}

// login logs in using username and password and stores the new token.
//
// staleToken is the token the caller found to be expired; if another
// goroutine has already replaced it, login does nothing.
func (f *Fs) login(ctx context.Context, staleToken string) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()

	if f.token != staleToken {
		return nil
	}
	if f.opt.Username == "" || f.opt.Password == "" {
		return errors.New("token expired and no user and pass configured to log in again")
	}

	var body map[string]any
	if strings.Contains(f.opt.Username, "@") {
		body = map[string]any{
			"mail":     f.opt.Username,
			"password": f.opt.Password,
			"type":     2,
		}
	} else {
		body = map[string]any{
			"passport": f.opt.Username,
			"password": f.opt.Password,
			"remember": true,
		}
	}

	opts := rest.Opts{
		Method:  "POST",
		RootURL: loginURL,
		ExtraHeaders: map[string]string{
			"origin":      "https://yun.123pan.com",
			"referer":     "https://yun.123pan.com/",
			"platform":    "web",
			"app-version": "3",
		},
	}

	var resp api.SignInResp
	_, err := f.srv.CallJSON(ctx, &opts, body, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 200 && resp.Code != 0 {
		return fmt.Errorf("login failed (%d): %s", resp.Code, resp.Message)
	}
	if resp.Data.Token == "" {
		return errors.New("empty token returned from login")
	}

	f.token = resp.Data.Token
	f.m.Set("token", f.token)

	fs.Debugf(f, "Successfully logged in to 123Pan")
	return nil
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if err == nil {
		return false, nil
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}
	return fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// isTokenExpired reports whether the call failed because the access
// token is invalid or expired
func isTokenExpired(resp *http.Response, err error) bool {
	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		return true
	}
	var apiErr *api.Error
	return errors.As(err, &apiErr) && apiErr.Code == api.CodeTokenExpired
}

// callAPI executes a signed API call to 123Pan through the pacer.
//
// It checks the business code of the response, and logs in again once
// if the token has expired. resp may be nil.
func (f *Fs) callAPI(ctx context.Context, method, apiPath string, params url.Values, reqBody any, resp api.Response) error {
	if resp == nil {
		resp = new(api.BaseResp)
	}
	relogged := false
	return f.pacer.Call(func() (bool, error) {
		token := f.getToken()
		query := url.Values{}
		for k, v := range params {
			query[k] = v
		}
		// The signature contains a timestamp so compute it per attempt
		sigK, sigV := signPath(signPrefix+apiPath, "web", "3")
		query.Set(sigK, sigV)
		opts := rest.Opts{
			Method:     method,
			Path:       apiPath,
			Parameters: query,
			ExtraHeaders: map[string]string{
				"origin":        "https://yun.123pan.com",
				"referer":       "https://yun.123pan.com/",
				"authorization": "Bearer " + token,
				"platform":      "web",
				"app-version":   "3",
			},
		}
		*resp.Base() = api.BaseResp{}
		httpResp, err := f.srv.CallJSON(ctx, &opts, reqBody, resp)
		if err == nil {
			err = resp.Base().Err()
		}
		if err != nil && !relogged && isTokenExpired(httpResp, err) {
			relogged = true
			fs.Debugf(f, "Token expired, logging in again")
			if loginErr := f.login(ctx, token); loginErr != nil {
				return false, fmt.Errorf("re-login failed: %w", loginErr)
			}
			return true, err
		}
		return f.shouldRetry(ctx, httpResp, err)
	})
}

// Name returns the configured name of the remote
func (f *Fs) Name() string { return f.name }

// Root returns the root of the remote
func (f *Fs) Root() string { return f.root }

// String returns a description of the remote
func (f *Fs) String() string { return fmt.Sprintf("123pan root %q", f.root) }

// Precision of the remote
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the supported hash types
func (f *Fs) Hashes() hash.Set { return hash.NewHashSet(hash.MD5) }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// FindLeaf finds a directory of name leaf in the folder with ID dirID
func (f *Fs) FindLeaf(ctx context.Context, dirID, leaf string) (pathIDOut string, found bool, err error) {
	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return "", false, err
	}
	for _, item := range items {
		if item.Type == 1 && f.opt.Enc.ToStandardName(item.FileName) == leaf {
			return strconv.FormatInt(item.FileID, 10), true, nil
		}
	}
	return "", false, nil
}

// CreateDir makes a directory with dirID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (newID string, err error) {
	req := map[string]any{
		"driveId":      0,
		"etag":         "",
		"fileName":     f.opt.Enc.FromStandardName(leaf),
		"parentFileId": dirID,
		"size":         0,
		"type":         1,
	}
	var resp api.UploadResp
	err = f.callAPI(ctx, "POST", "/file/upload_request", nil, req, &resp)
	if err != nil {
		return "", err
	}
	if resp.Data.FileID == 0 {
		return "", fmt.Errorf("no ID returned for created directory %q", leaf)
	}
	return strconv.FormatInt(resp.Data.FileID, 10), nil
}

// listAllInDir retrieves all files in a folder
func (f *Fs) listAllInDir(ctx context.Context, dirID string) ([]api.File, error) {
	var allItems []api.File
	for page := 1; ; page++ {
		params := url.Values{}
		params.Set("driveId", "0")
		params.Set("limit", "100")
		params.Set("next", "0")
		params.Set("orderBy", "file_id")
		params.Set("orderDirection", "desc")
		params.Set("parentFileId", dirID)
		params.Set("trashed", "false")
		params.Set("SearchData", "")
		params.Set("Page", strconv.Itoa(page))
		params.Set("OnlyLookAbnormalFile", "0")
		params.Set("event", "homeListFile")
		params.Set("operateType", "4")
		params.Set("inDirectSpace", "false")

		var resp api.FileListResp
		err := f.callAPI(ctx, "GET", "/file/list/new", params, nil, &resp)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, resp.Data.InfoList...)
		if len(resp.Data.InfoList) == 0 || resp.Data.Next == "-1" {
			break
		}
	}
	return allItems, nil
}

// newObjectWithInfo makes an Object from an API file item
func (f *Fs) newObjectWithInfo(remote string, item *api.File) *Object {
	return &Object{
		fs:        f,
		remote:    remote,
		id:        item.FileID,
		size:      item.Size,
		etag:      strings.ToLower(item.Etag),
		s3KeyFlag: item.S3KeyFlag,
		modTime:   item.UpdateAt,
		listedURL: item.DownloadURL,
	}
}

// List objects and directories in dir into entries
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
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.FileName))
		if item.Type == 1 {
			id := strconv.FormatInt(item.FileID, 10)
			f.dirCache.Put(remote, id)
			entries = append(entries, fs.NewDir(remote, item.UpdateAt).SetID(id))
		} else {
			entries = append(entries, f.newObjectWithInfo(remote, item))
		}
	}
	return entries, nil
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	items, err := f.listAllInDir(ctx, dirID)
	if err != nil {
		return nil, err
	}
	// Items are sorted newest first
	for i := range items {
		item := &items[i]
		if item.Type != 1 && f.opt.Enc.ToStandardName(item.FileName) == leaf {
			return f.newObjectWithInfo(remote, item), nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// Put uploads to remote path with ObjectInfo
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

// PutUnchecked uploads the object without checking for an existing one
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	return f.upload(ctx, in, src, remote, leaf, dirID)
}

// upload uploads in as leaf into the directory dirID.
//
// The API needs the MD5 of the whole file before the upload starts. If
// src can't supply it, in is spooled to a temporary file to compute it.
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote, leaf, dirID string) (*Object, error) {
	size := src.Size()
	md5sum := ""
	if size >= 0 {
		md5sum, _ = src.Hash(ctx, hash.MD5)
	}
	if md5sum == "" {
		tmp, n, sums, cleanup, err := spoolToTemp(in, hash.NewHashSet(hash.MD5))
		if err != nil {
			return nil, err
		}
		defer cleanup()
		in, size, md5sum = tmp, n, sums[hash.MD5]
	}
	md5sum = strings.ToLower(md5sum)

	uploadReq := map[string]any{
		"driveId":      0,
		"duplicate":    2, // 2: overwrite, 1: rename, 0: default
		"etag":         md5sum,
		"fileName":     f.opt.Enc.FromStandardName(leaf),
		"parentFileId": dirID,
		"size":         size,
		"type":         0,
	}
	var up api.UploadResp
	err := f.callAPI(ctx, "POST", "/file/upload_request", nil, uploadReq, &up)
	if err != nil {
		return nil, fmt.Errorf("upload request failed: %w", err)
	}

	if up.Data.Reuse {
		fs.Debugf(src, "Rapid upload succeeded")
	} else {
		if up.Data.Key == "" || up.Data.UploadID == "" {
			return nil, errors.New("upload request returned neither reuse nor an upload key")
		}
		if err = f.uploadChunks(ctx, in, size, &up); err != nil {
			return nil, err
		}
	}

	if up.Data.FileID == 0 {
		obj, err := f.NewObject(ctx, remote)
		if err != nil {
			return nil, err
		}
		return obj.(*Object), nil
	}
	return &Object{
		fs:      f,
		remote:  remote,
		id:      up.Data.FileID,
		size:    size,
		etag:    md5sum,
		modTime: time.Now(),
	}, nil
}

// uploadChunks streams in to the presigned S3 URLs in chunks and
// completes the upload
func (f *Fs) uploadChunks(ctx context.Context, in io.Reader, size int64, up *api.UploadResp) error {
	chunkSize := int64(f.opt.ChunkSize)
	numChunks := max(1, (size+chunkSize-1)/chunkSize)
	endpoint := "/file/s3_upload_object/auth"
	if numChunks > 1 {
		endpoint = "/file/s3_repare_upload_parts_batch"
	}

	buf := make([]byte, min(chunkSize, size))
	for part := int64(1); part <= numChunks; part++ {
		chunk := buf[:min(chunkSize, size-(part-1)*chunkSize)]
		if _, err := io.ReadFull(in, chunk); err != nil {
			return fmt.Errorf("failed to read chunk %d: %w", part, err)
		}
		if err := f.uploadPart(ctx, up, endpoint, int(part), chunk); err != nil {
			return fmt.Errorf("failed to upload chunk %d: %w", part, err)
		}
	}

	completeReq := map[string]any{
		"StorageNode": up.Data.StorageNode,
		"bucket":      up.Data.Bucket,
		"fileId":      up.Data.FileID,
		"fileSize":    size,
		"isMultipart": numChunks > 1,
		"key":         up.Data.Key,
		"uploadId":    up.Data.UploadID,
	}
	if err := f.callAPI(ctx, "POST", "/file/upload_complete/v2", nil, completeReq, nil); err != nil {
		return fmt.Errorf("complete upload failed: %w", err)
	}
	return nil
}

// partURL fetches the presigned URL for one part
func (f *Fs) partURL(ctx context.Context, up *api.UploadResp, endpoint string, part int) (string, error) {
	req := map[string]any{
		"StorageNode":     up.Data.StorageNode,
		"bucket":          up.Data.Bucket,
		"key":             up.Data.Key,
		"partNumberEnd":   part + 1,
		"partNumberStart": part,
		"uploadId":        up.Data.UploadID,
	}
	var resp api.S3PreSignedURLs
	if err := f.callAPI(ctx, "POST", endpoint, nil, req, &resp); err != nil {
		return "", fmt.Errorf("failed to get presigned upload URL: %w", err)
	}
	partURL := resp.Data.PreSignedUrls[strconv.Itoa(part)]
	if partURL == "" {
		return "", errors.New("no presigned upload URL returned")
	}
	return partURL, nil
}

// uploadPart uploads one chunk, fetching a fresh presigned URL once if
// the first one is rejected as expired
func (f *Fs) uploadPart(ctx context.Context, up *api.UploadResp, endpoint string, part int, chunk []byte) error {
	for attempt := 0; ; attempt++ {
		partURL, err := f.partURL(ctx, up, endpoint, part)
		if err != nil {
			return err
		}
		expired := false
		err = f.pacer.Call(func() (bool, error) {
			length := int64(len(chunk))
			opts := rest.Opts{
				Method:        "PUT",
				RootURL:       partURL,
				Body:          bytes.NewReader(chunk),
				ContentLength: &length,
				NoResponse:    true,
			}
			resp, err := f.uploadSrv.Call(ctx, &opts)
			if resp != nil && resp.StatusCode == http.StatusForbidden {
				expired = true
				return false, err
			}
			return f.shouldRetry(ctx, resp, err)
		})
		if expired && attempt == 0 {
			fs.Debugf(f, "Presigned URL for chunk %d rejected, fetching a new one", part)
			continue
		}
		return err
	}
}

// Mkdir creates the directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes the directory if it is empty
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
	if err = f.trashDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Purge removes the directory and all of its contents
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if err = f.trashDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// trashDir moves the directory with ID dirID and its contents to trash
func (f *Fs) trashDir(ctx context.Context, dirID string) error {
	if dirID == "0" {
		return errors.New("can't remove the root directory")
	}
	id, err := strconv.ParseInt(dirID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid directory ID %q: %w", dirID, err)
	}
	return f.deleteItem(ctx, id)
}

// deleteItem moves item to trash
func (f *Fs) deleteItem(ctx context.Context, fileID int64) error {
	req := map[string]any{
		"driveId":   0,
		"operation": true,
		"fileTrashInfoList": []map[string]any{{
			"FileId": fileID,
		}},
	}
	return f.callAPI(ctx, "POST", "/file/trash", nil, req, nil)
}

// rename renames the item with ID fileID to leaf
func (f *Fs) rename(ctx context.Context, fileID any, leaf string) error {
	req := map[string]any{
		"driveId":  0,
		"fileId":   fileID,
		"fileName": f.opt.Enc.FromStandardName(leaf),
	}
	return f.callAPI(ctx, "POST", "/file/rename", nil, req, nil)
}

// moveItem moves the item with ID fileID into the directory dirID
func (f *Fs) moveItem(ctx context.Context, fileID any, dirID string) error {
	req := map[string]any{
		"fileIdList":   []map[string]any{{"FileId": fileID}},
		"parentFileId": dirID,
	}
	return f.callAPI(ctx, "POST", "/file/mod_pid", nil, req, nil)
}

// Move moves a file on server side
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
		if err = f.moveItem(ctx, srcObj.id, dstDirID); err != nil {
			return nil, fmt.Errorf("move failed: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		if err = f.rename(ctx, srcObj.id, dstLeaf); err != nil {
			return nil, fmt.Errorf("rename failed: %w", err)
		}
	}

	dstObj := *srcObj
	dstObj.fs = f
	dstObj.remote = remote
	return &dstObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, srcParentID, srcLeaf, dstParentID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	if srcParentID != dstParentID {
		if err = f.moveItem(ctx, srcID, dstParentID); err != nil {
			return fmt.Errorf("couldn't dir move: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		if err = f.rename(ctx, srcID, dstLeaf); err != nil {
			return fmt.Errorf("dirmove: couldn't rename moved dir: %w", err)
		}
	}

	srcFs.dirCache.FlushDir(srcRemote)
	f.dirCache.FlushDir(dstRemote)
	return nil
}

// ------------------------------------------------------------
// Object methods
// ------------------------------------------------------------

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// ModTime returns the modification time of the object
//
// 123Pan doesn't store modification times so this is the time the
// file was last updated on the server.
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// Size returns the size of the object
func (o *Object) Size() int64 { return o.size }

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Hash returns MD5 hash
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.etag, nil
}

// Storable returns whether this object can be stored
func (o *Object) Storable() bool { return true }

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

// fetchLink returns the download link of the object: the URL the
// listing gave, the first time, else one requested from the API.
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	rawURL := o.listedURL
	// A listed URL is used once: when it is refused, a new link
	// comes from the API.
	o.listedURL = ""
	if rawURL == "" {
		var err error
		rawURL, err = o.downloadInfo(ctx)
		if err != nil {
			return nil, err
		}
	}
	rawURL, err := decodeDownloadURL(rawURL)
	if err != nil {
		return nil, err
	}
	return &linkcache.Link{URL: rawURL}, nil
}

// downloadInfo requests the download URL of the object from the API
func (o *Object) downloadInfo(ctx context.Context) (string, error) {
	req := map[string]any{
		"driveId":   0,
		"etag":      o.etag,
		"fileId":    o.id,
		"fileName":  o.fs.opt.Enc.FromStandardName(path.Base(o.remote)),
		"s3keyFlag": o.s3KeyFlag,
		"size":      o.size,
		"type":      0,
	}
	var resp api.DownloadInfoResp
	err := o.fs.callAPI(ctx, "POST", "/file/download_info", nil, req, &resp)
	if err != nil {
		return "", fmt.Errorf("failed to get download info: %w", err)
	}
	if resp.Data.DownloadURL == "" {
		return "", errors.New("empty download URL returned")
	}
	return resp.Data.DownloadURL, nil
}

// decodeDownloadURL returns the URL to fetch from a download URL of
// the API, which may carry the real one base64 encoded in its params
// parameter
func decodeDownloadURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid download URL: %w", err)
	}
	if params := u.Query().Get("params"); params != "" {
		decoded, err := base64.StdEncoding.DecodeString(params)
		if err != nil {
			return "", fmt.Errorf("invalid download URL params: %w", err)
		}
		if _, err = url.Parse(string(decoded)); err != nil {
			return "", fmt.Errorf("invalid decoded download URL: %w", err)
		}
		rawURL = string(decoded)
	}
	return rawURL, nil
}

// download fetches rawURL with the open options
func (o *Object) download(ctx context.Context, rawURL string, options []fs.OpenOption) (resp *http.Response, err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: rawURL,
		ExtraHeaders: map[string]string{
			"Referer": "https://yun.123pan.com/",
		},
		Options: options,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.uploadSrv.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil && resp != nil {
		return nil, &downloadError{status: resp.StatusCode, err: err}
	}
	return resp, err
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	key := linkcache.Key(o.fs.name, strconv.FormatInt(o.id, 10), strconv.FormatInt(o.size, 10), o.etag)
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// openLink reads the object from link
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	resp, err := o.download(ctx, link.URL, options)
	if err != nil {
		return nil, err
	}
	// The download URL may answer with a JSON document pointing to
	// the real location instead of redirecting
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		return resp.Body, nil
	}
	var redirect api.RedirectResp
	err = rest.DecodeJSON(resp, &redirect)
	if err != nil {
		return nil, fmt.Errorf("failed to decode download redirect: %w", err)
	}
	if err = redirect.Err(); err != nil {
		// The link was refused, as an expired one is: count it as a
		// 403 so that a new one is requested
		return nil, &downloadError{status: http.StatusForbidden, err: err}
	}
	if redirect.Data.RedirectURL == "" {
		return nil, errors.New("download returned JSON without a redirect URL")
	}
	resp, err = o.download(ctx, redirect.Data.RedirectURL, options)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Update the object with the contents of the io.Reader
//
// The upload overwrites the existing file; if the server still keeps
// the old version under another ID it is removed afterwards.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	newObj, err := o.fs.upload(ctx, in, src, o.remote, leaf, dirID)
	if err != nil {
		return err
	}
	if newObj.id != o.id {
		if err := o.fs.deleteItem(ctx, o.id); err != nil {
			// Overwriting normally removes the old version already
			fs.Debugf(o, "Failed to remove old version %d: %v", o.id, err)
		}
	}
	*o = *newObj
	return nil
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteItem(ctx, o.id)
}

// SetModTime sets modification time
func (o *Object) SetModTime(ctx context.Context, mtime time.Time) error {
	return fs.ErrorCantSetModTime
}

// ID returns the item ID
func (o *Object) ID() string { return strconv.FormatInt(o.id, 10) }

// AddURL submits a remote URL to be downloaded server-side.
//
// 123Pan first resolves the URL into a resource, then downloads all of
// its files into the directory given by opt["path"].
func (f *Fs) AddURL(ctx context.Context, rawURL string, opt map[string]string) (*fs.OfflineTask, error) {
	if opt["name"] != "" {
		return nil, errors.New("123pan offline downloads can't set the file name")
	}
	dirID, err := f.dirCache.FindDir(ctx, opt["path"], false)
	if err != nil {
		return nil, fmt.Errorf("find target dir failed: %w", err)
	}
	uploadDir, err := strconv.ParseInt(dirID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target dir ID %q: %w", dirID, err)
	}

	var resolved api.OfflineResolveResp
	if err := f.callAPI(ctx, "POST", "/v2/offline_download/task/resolve", nil, map[string]any{"urls": rawURL}, &resolved); err != nil {
		return nil, fmt.Errorf("resolve offline download failed: %w", err)
	}
	if len(resolved.Data.List) == 0 {
		return nil, errors.New("resolve offline download failed: no resource in response")
	}
	res := resolved.Data.List[0]
	if res.Result != 0 {
		return nil, fmt.Errorf("resolve offline download failed: %s (code %d)", res.ErrMsg, res.ErrCode)
	}
	// An empty selection downloads a single-file resource
	fileIDs := make([]int64, 0, len(res.Files))
	for _, file := range res.Files {
		if file.ID > 0 {
			fileIDs = append(fileIDs, file.ID)
		}
	}

	req := map[string]any{
		"resource_list": []map[string]any{{
			"resource_id":    res.ID,
			"select_file_id": fileIDs,
		}},
		"upload_dir": uploadDir,
	}
	var submitted api.OfflineSubmitResp
	if err := f.callAPI(ctx, "POST", "/v2/offline_download/task/submit", nil, req, &submitted); err != nil {
		return nil, fmt.Errorf("submit offline download failed: %w", err)
	}
	if len(submitted.Data.TaskList) == 0 || submitted.Data.TaskList[0].Result != 0 || submitted.Data.TaskList[0].TaskID == 0 {
		return nil, errors.New("submit offline download failed: no task created")
	}
	return &fs.OfflineTask{
		ID:        strconv.FormatInt(submitted.Data.TaskList[0].TaskID, 10),
		Name:      f.opt.Enc.ToStandardName(res.Name),
		URL:       rawURL,
		Size:      res.Size,
		Status:    fs.OfflineTaskStatusInit,
		CreatedAt: time.Now(),
	}, nil
}

// offlineStatus converts a 123Pan offline task state
func offlineStatus(status int) fs.OfflineTaskStatus {
	switch status {
	case api.OfflineStatusWaiting:
		return fs.OfflineTaskStatusInit
	case api.OfflineStatusFailed:
		return fs.OfflineTaskStatusError
	case api.OfflineStatusComplete:
		return fs.OfflineTaskStatusComplete
	}
	return fs.OfflineTaskStatusRunning
}

// ListOfflineTasks returns server-side offline download tasks.
func (f *Fs) ListOfflineTasks(ctx context.Context) ([]*fs.OfflineTask, error) {
	const pageSize = 100
	var tasks []*fs.OfflineTask
	for page := 1; ; page++ {
		req := map[string]any{
			"current_page": page,
			"page_size":    pageSize,
			"status_arr": []int{
				api.OfflineStatusRunning,
				api.OfflineStatusFailed,
				api.OfflineStatusComplete,
				api.OfflineStatusRetrying,
				api.OfflineStatusWaiting,
			},
		}
		var resp api.OfflineTaskListResp
		if err := f.callAPI(ctx, "POST", "/offline_download/task/list", nil, req, &resp); err != nil {
			return nil, fmt.Errorf("list offline tasks failed: %w", err)
		}
		for _, t := range resp.Data.List {
			tasks = append(tasks, &fs.OfflineTask{
				ID:       strconv.FormatInt(t.TaskID, 10),
				Name:     f.opt.Enc.ToStandardName(t.Name),
				Size:     t.Size,
				Status:   offlineStatus(t.Status),
				Progress: t.Progress,
				Speed:    t.Speed,
			})
		}
		if len(resp.Data.List) == 0 || page*pageSize >= resp.Data.Total {
			return tasks, nil
		}
	}
}

// DeleteOfflineTask deletes a server-side offline download task.
func (f *Fs) DeleteOfflineTask(ctx context.Context, taskID string) error {
	id, err := strconv.ParseInt(taskID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid task ID %q: %w", taskID, err)
	}
	req := map[string]any{"task_ids": []int64{id}}
	if err := f.callAPI(ctx, "POST", "/offline_download/task/delete", nil, req, nil); err != nil {
		return fmt.Errorf("delete offline task failed: %w", err)
	}
	return nil
}

// Command runs named commands on the backend
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "rapid-upload", "rapidupload":
		var fileName, md5Str string
		var fileSize int64

		if len(arg) >= 3 {
			// Args: [filename, size, md5]
			fileName = arg[0]
			fileSize, err = strconv.ParseInt(arg[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid size %q: %w", arg[1], err)
			}
			md5Str = arg[2]
		} else {
			fileName = opt["name"]
			if s, ok := opt["size"]; ok {
				fileSize, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid size %q: %w", s, err)
				}
			}
			if m, ok := opt["md5"]; ok {
				md5Str = m
			} else {
				md5Str = opt["etag"]
			}
		}

		if fileName == "" || fileSize < 0 || md5Str == "" {
			return nil, errors.New("rapid-upload requires filename, size, and md5 (e.g. 'rclone backend rapid-upload cloud123:path filename size md5')")
		}

		dirID, err := f.dirCache.FindDir(ctx, opt["path"], true)
		if err != nil {
			return nil, fmt.Errorf("find target dir failed: %w", err)
		}

		uploadReq := map[string]any{
			"driveId":      0,
			"duplicate":    2,
			"etag":         strings.ToLower(md5Str),
			"fileName":     f.opt.Enc.FromStandardName(fileName),
			"parentFileId": dirID,
			"size":         fileSize,
			"type":         0,
		}
		var uploadResp api.UploadResp
		if err := f.callAPI(ctx, "POST", "/file/upload_request", nil, uploadReq, &uploadResp); err != nil {
			return nil, fmt.Errorf("upload request failed: %w", err)
		}
		if !uploadResp.Data.Reuse {
			return map[string]any{
				"status":  "failed",
				"message": "file not found in 123pan server library (requires block upload)",
			}, nil
		}
		return map[string]any{
			"status":  "success",
			"file_id": uploadResp.Data.FileID,
			"name":    fileName,
			"size":    fileSize,
			"md5":     md5Str,
			"dir_id":  dirID,
		}, nil

	case "addurl", "offline":
		if len(arg) != 1 {
			return nil, errors.New("need exactly 1 URL argument (magnet/ed2k/thunder/http)")
		}
		return f.AddURL(ctx, arg[0], opt)
	case "tasks", "listoffline":
		return f.ListOfflineTasks(ctx)
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
	_ fs.Fs                = (*Fs)(nil)
	_ fs.Purger            = (*Fs)(nil)
	_ fs.PutUncheckeder    = (*Fs)(nil)
	_ fs.Mover             = (*Fs)(nil)
	_ fs.DirMover          = (*Fs)(nil)
	_ fs.Commander         = (*Fs)(nil)
	_ fs.OfflineDownloader = (*Fs)(nil)
	_ dircache.DirCacher   = (*Fs)(nil)
	_ fs.Object            = (*Object)(nil)
	_ fs.IDer              = (*Object)(nil)
	_ fs.SetModTimer       = (*Object)(nil)
)

var commandHelp = []fs.CommandHelp{{
	Name:  "rapid-upload",
	Short: "Create a file from its hashes without uploading data.",
	Long: `This command asks 123 Cloud to create a file from content it
already stores, identified by size and MD5. It is also available as
"rapidupload".

Usage examples:

` + "```console" + `
rclone backend rapid-upload cloud123:dir name size md5
rclone backend rapid-upload cloud123:dir -o name=file.zip -o size=52428800 -o md5=...
` + "```",
	Opts: map[string]string{
		"path": "Directory to create the file in, relative to the remote.",
		"name": "File name.",
		"size": "File size in bytes.",
		"md5":  "MD5 of the whole file.",
	},
}, {
	Name:  "addurl",
	Short: "Add offline download task for url.",
	Long: `This command adds an offline (cloud-side) download task for a url
(http, magnet, ed2k or thunder). 123 Cloud resolves the url and
downloads all of its files. It is also available as "offline".

Usage example:

` + "```console" + `
rclone backend addurl cloud123:dirpath url
` + "```" + `

Downloads will be stored in 'dirpath', which must exist. 123 Cloud
names the downloaded files itself.

The result is the new task as JSON with the fields "id", "name",
"url", "size", "status" ("init") and "created_at".`,
}, {
	Name:  "tasks",
	Short: "List offline download tasks.",
	Long: `This command lists the offline download tasks. It is also
available as "listoffline".

Usage example:

` + "```console" + `
rclone backend tasks cloud123:
` + "```" + `

The result is a JSON list of tasks with the fields "id", "name",
"size", "status" ("init", "running", "complete" or "error"),
"progress" and "speed".`,
}, {
	Name:  "deltask",
	Short: "Delete an offline download task.",
	Long: `This command deletes the offline download task with the given ID.
It is also available as "deleteoffline".

Usage example:

` + "```console" + `
rclone backend deltask cloud123: task_id
` + "```",
}}
