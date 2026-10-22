// Package quark provides a filesystem interface to Quark.
package quark

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/quark/api"
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
	baseURL          = "https://drive.quark.cn/1/clouddrive"
	referer          = "https://pan.quark.cn"
	userAgent        = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch"
	ossUserAgent     = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"
	pr               = "ucpro"
	fr               = "pc"
	minSleep         = 100 * time.Millisecond
	maxSleep         = 2 * time.Second
	decayConstant    = 2
	listPageSize     = 100
	mkdirConflictMsg = "file is doloading"
	dirVisibleTries  = 6

	// linkLifetime is how long a download link is kept when its URL
	// states no expiry (signed OSS URLs carry Expires, which linkcache
	// reads); a conservative guess
	linkLifetime = 10 * time.Minute
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
		Name:        "quark",
		Description: "Quark Drive / UC Drive",
		NewFs:       NewFs,
		Aliases:     []string{"quarkdrive", "ucdrive"},
		Options: []fs.Option{{
			Name: "cookie",
			Help: `Quark or UC Cookie (must include __puus)

The session cookie __puus is refreshed by the server from time to time
and the refreshed value is written back to the config file.`,
			Required:  true,
			Sensitive: true,
		}, {
			Name:    "mode",
			Help:    "Service mode: quark (Quark Drive) or uc (UC Drive)",
			Default: "quark",
			Examples: []fs.OptionExample{{
				Value: "quark",
				Help:  "Quark Drive",
			}, {
				Value: "uc",
				Help:  "UC Drive",
			}},
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID",
			Default:  "0",
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     "The encoding for the backend.",
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines configuration options for Quark
type Options struct {
	Cookie       string               `config:"cookie"`
	Mode         string               `config:"mode"`
	RootFolderID string               `config:"root_folder_id"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a Quark remote
type Fs struct {
	name      string
	root      string
	opt       Options
	features  *fs.Features
	srv       *rest.Client // the Quark API
	rawSrv    *rest.Client // for OSS uploads and downloads
	dirCache  *dircache.DirCache
	pacer     *fs.Pacer
	m         configmap.Mapper
	cookieMu  *sync.Mutex // protects cookie
	cookie    string
	referer   string
	userAgent string
	pr        string
}

// Object represents a file on Quark
type Object struct {
	fs      *Fs
	remote  string
	id      string
	size    int64
	modTime time.Time
}

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	errResponse := new(api.Resp)
	err := rest.DecodeJSON(resp, errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	if errResponse.Status == 0 {
		errResponse.Status = resp.StatusCode
	}
	if errResponse.Message == "" {
		errResponse.Message = resp.Status
	}
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

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	var apiErr *api.Resp
	if errors.As(err, &apiErr) {
		// Transient errors reported by the API
		msg := strings.ToLower(apiErr.Message)
		if strings.HasPrefix(msg, "complete_upload_lock_timeout") ||
			(strings.HasPrefix(msg, "inner error") && strings.Contains(msg, "requestid")) {
			return true, err
		}
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func (f *Fs) getCookie() string {
	f.cookieMu.Lock()
	defer f.cookieMu.Unlock()
	return f.cookie
}

// setCookieValue sets name=value in the cookie header string
func setCookieValue(cookie, name, value string) string {
	parts := strings.Split(cookie, ";")
	out := make([]string, 0, len(parts)+1)
	found := false
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, name+"=") {
			trimmed = name + "=" + value
			found = true
		}
		out = append(out, trimmed)
	}
	if !found {
		out = append(out, name+"="+value)
	}
	return strings.Join(out, "; ")
}

// updateCookies merges refreshed session cookies from resp into the
// cookie and saves it to the config if it changed
func (f *Fs) updateCookies(resp *http.Response) {
	if resp == nil {
		return
	}
	cookies := resp.Cookies()
	if len(cookies) == 0 {
		return
	}
	f.cookieMu.Lock()
	defer f.cookieMu.Unlock()
	newCookie := f.cookie
	for _, c := range cookies {
		if (c.Name == "__puus" || c.Name == "__pus") && c.Value != "" {
			newCookie = setCookieValue(newCookie, c.Name, c.Value)
		}
	}
	if newCookie != f.cookie {
		f.cookie = newCookie
		f.m.Set("cookie", newCookie)
		fs.Debugf(f, "Saved refreshed session cookie")
	}
}

// callOnce makes a single call to the Quark API
func (f *Fs) callOnce(ctx context.Context, opts *rest.Opts, reqBody, respObj any) (*http.Response, error) {
	if opts.ExtraHeaders == nil {
		opts.ExtraHeaders = make(map[string]string)
	}
	opts.ExtraHeaders["Cookie"] = f.getCookie()
	opts.ExtraHeaders["Referer"] = f.referer
	opts.ExtraHeaders["User-Agent"] = f.userAgent
	opts.ExtraHeaders["Accept"] = "application/json, text/plain, */*"
	if opts.Parameters == nil {
		opts.Parameters = url.Values{}
	}
	opts.Parameters.Set("pr", f.pr)
	opts.Parameters.Set("fr", fr)

	resp, err := f.srv.CallJSON(ctx, opts, reqBody, respObj)
	f.updateCookies(resp)
	if err == nil {
		if e, ok := respObj.(interface{ Err() error }); ok {
			err = e.Err()
		}
	}
	return resp, err
}

// request makes a call to the Quark API under the pacer retrying it
// if necessary. The response envelope is checked for errors.
func (f *Fs) request(ctx context.Context, opts *rest.Opts, reqBody, respObj any) error {
	if respObj == nil {
		respObj = new(api.Resp)
	}
	return f.pacer.Call(func() (bool, error) {
		resp, err := f.callOnce(ctx, opts, reqBody, respObj)
		return f.shouldRetry(ctx, resp, err)
	})
}

// requestNoRetry makes a call to the Quark API which must not be
// retried as it isn't idempotent
func (f *Fs) requestNoRetry(ctx context.Context, opts *rest.Opts, reqBody, respObj any) error {
	return f.pacer.CallNoRetry(func() (bool, error) {
		resp, err := f.callOnce(ctx, opts, reqBody, respObj)
		return f.shouldRetry(ctx, resp, err)
	})
}

// ------------------------------------------------------------
// Fs Implementation
// ------------------------------------------------------------

// NewFs constructs an Fs from path and config
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if opt.RootFolderID == "" {
		opt.RootFolderID = "0"
	}
	if opt.Cookie == "" {
		return nil, errors.New("quark: cookie is required")
	}

	apiEndpoint := baseURL
	ref := referer
	ua := userAgent
	prParam := pr

	if strings.EqualFold(opt.Mode, "uc") {
		apiEndpoint = "https://pc-api.uc.cn/1/clouddrive"
		ref = "https://drive.uc.cn"
		ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) uc-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch"
		prParam = "UCBrowser"
	}

	httpCl := fshttp.NewClient(ctx)
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		cookie:    opt.Cookie,
		referer:   ref,
		userAgent: ua,
		pr:        prParam,
		srv:       rest.NewClient(httpCl).SetRoot(apiEndpoint).SetErrorHandler(errorHandler),
		rawSrv:    rest.NewClient(httpCl).SetErrorHandler(rawErrorHandler),
		pacer:     fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		m:         m,
		cookieMu:  new(sync.Mutex),
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements, for both Quark and UC: both
		// throttle parallel downloads of accounts without membership.
		MaxFileStreams: 2,
		MaxStreams:     4,
	}).Fill(ctx, f)

	// Validate connection
	opts := rest.Opts{
		Method: http.MethodGet,
		Path:   "/config",
	}
	err = f.request(ctx, &opts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("quark: connection validation failed: %w", err)
	}

	f.dirCache = dircache.New(root, opt.RootFolderID, f)

	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, opt.RootFolderID, &tempF)
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

// Name returns remote name
func (f *Fs) Name() string { return f.name }

// Root returns root path
func (f *Fs) Root() string { return f.root }

// String returns remote string
func (f *Fs) String() string { return fmt.Sprintf("Quark Drive root '%s'", f.root) }

// Precision returns precision of modtime
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the supported hash types
//
// The listings don't return hashes so none are supported.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Features returns features
func (f *Fs) Features() *fs.Features { return f.features }

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// listFiles returns all the items in folderID
func (f *Fs) listFiles(ctx context.Context, folderID string) ([]api.File, error) {
	var files []api.File
	for page := 1; ; page++ {
		params := url.Values{}
		params.Set("pdir_fid", folderID)
		params.Set("_size", strconv.Itoa(listPageSize))
		params.Set("_page", strconv.Itoa(page))
		params.Set("_fetch_total", "1")
		params.Set("fetch_all_file", "1")
		params.Set("fetch_risk_file_name", "1")

		var resp api.SortResp
		opts := rest.Opts{
			Method:     http.MethodGet,
			Path:       "/file/sort",
			Parameters: params,
		}
		err := f.request(ctx, &opts, nil, &resp)
		if err != nil {
			return nil, err
		}

		for _, item := range resp.Data.List {
			item.FileName = f.opt.Enc.ToStandardName(html.UnescapeString(item.FileName))
			files = append(files, item)
		}

		if resp.Metadata.Total > 0 {
			if page*listPageSize >= resp.Metadata.Total {
				break
			}
		} else if len(resp.Data.List) < listPageSize {
			break
		}
	}
	return files, nil
}

// findItem returns the directory (if isDir) or file called leaf in
// folderID or nil if not found
func (f *Fs) findItem(ctx context.Context, folderID, leaf string, isDir bool) (*api.File, error) {
	files, err := f.listFiles(ctx, folderID)
	if err != nil {
		return nil, err
	}
	for i := range files {
		if files[i].IsDir() == isDir && files[i].FileName == leaf {
			return &files[i], nil
		}
	}
	return nil, nil
}

// fileInfo reads the info for fid. It returns nil if not found.
func (f *Fs) fileInfo(ctx context.Context, fid string) (*api.File, error) {
	var resp api.FileInfoResp
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/file/info",
		Parameters: url.Values{"fid": {fid}},
	}
	err := f.request(ctx, &opts, nil, &resp)
	if err != nil {
		var apiErr *api.Resp
		if errors.As(err, &apiErr) && apiErr.IsNotFound() {
			return nil, nil
		}
		return nil, err
	}
	if resp.Data.Fid != fid {
		return nil, fmt.Errorf("file info returned fid %q for %q", resp.Data.Fid, fid)
	}
	resp.Data.FileName = f.opt.Enc.ToStandardName(html.UnescapeString(resp.Data.FileName))
	return &resp.Data, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (dirID string, found bool, err error) {
	item, err := f.findItem(ctx, pathID, leaf, true)
	if err != nil {
		return "", false, err
	}
	if item == nil {
		return "", false, nil
	}
	return item.Fid, true, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (dirID string, err error) {
	body := map[string]any{
		"dir_init_lock": false,
		"dir_path":      "",
		"file_name":     f.opt.Enc.FromStandardName(leaf),
		"pdir_fid":      pathID,
	}
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file",
	}
	var resp api.CreateDirResp
	createErr := f.request(ctx, &opts, body, &resp)
	if createErr == nil && resp.Data.Fid != "" {
		return resp.Data.Fid, nil
	}
	if createErr != nil && !strings.Contains(createErr.Error(), mkdirConflictMsg) {
		return "", createErr
	}

	// The directory exists already or the create response didn't
	// carry the ID - wait for it to show up in the listing.
	delay := 200 * time.Millisecond
	for try := 0; ; try++ {
		dirID, found, err := f.FindLeaf(ctx, pathID, leaf)
		if err != nil {
			return "", err
		}
		if found {
			return dirID, nil
		}
		if try >= dirVisibleTries-1 {
			break
		}
		if err := sleepCtx(ctx, delay); err != nil {
			return "", err
		}
		delay = min(2*delay, 2*time.Second)
	}
	if createErr != nil {
		return "", createErr
	}
	return "", fmt.Errorf("created directory %q but couldn't find its ID", leaf)
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

// newObject makes an Object from an api.File
func (f *Fs) newObject(remote string, item *api.File) *Object {
	return &Object{
		fs:      f,
		remote:  remote,
		id:      item.Fid,
		size:    item.Size,
		modTime: item.ModTime(),
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
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	items, err := f.listFiles(ctx, dirID)
	if err != nil {
		return nil, err
	}

	for i := range items {
		item := &items[i]
		remote := path.Join(dir, item.FileName)
		if item.IsDir() {
			f.dirCache.Put(remote, item.Fid)
			d := fs.NewDir(remote, item.ModTime()).SetID(item.Fid)
			entries = append(entries, d)
		} else {
			entries = append(entries, f.newObject(remote, item))
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
	return f.newObject(remote, item), nil
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
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// deleteItem deletes the file or directory fid
//
// If the delete fails it checks whether the item has gone anyway in
// case a retried delete reports an error for an earlier success.
func (f *Fs) deleteItem(ctx context.Context, fid string) error {
	body := map[string]any{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{fid},
	}
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/delete",
	}
	err := f.request(ctx, &opts, body, nil)
	if err == nil {
		return nil
	}
	info, infoErr := f.fileInfo(ctx, fid)
	if infoErr == nil && info == nil {
		fs.Debugf(f, "Delete of %s returned error but it has gone: %v", fid, err)
		return nil
	}
	return err
}

// Rmdir deletes the directory if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	items, err := f.listFiles(ctx, dirID)
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

// moveItem moves fid into the directory dirID
func (f *Fs) moveItem(ctx context.Context, fid, dirID string) error {
	body := map[string]any{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{fid},
		"to_pdir_fid":  dirID,
	}
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/move",
	}
	return f.request(ctx, &opts, body, nil)
}

// rename renames fid to leaf
func (f *Fs) rename(ctx context.Context, fid, leaf string) error {
	body := map[string]any{
		"fid":       fid,
		"file_name": f.opt.Enc.FromStandardName(leaf),
	}
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/rename",
	}
	return f.request(ctx, &opts, body, nil)
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
// Object Implementation
// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// Size returns the size of the file
func (o *Object) Size() int64 { return o.size }

// ModTime returns the modification time
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// SetModTime sets the modification time
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns whether this object can be stored
func (o *Object) Storable() bool { return true }

// Hash returns the requested hash of a file if known
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string { return o.id }

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteItem(ctx, o.id)
}

// Open opens the file for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	key := linkcache.Key(o.fs.name, o.id, strconv.FormatInt(o.size, 10), strconv.FormatInt(o.modTime.UnixNano(), 10))
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the download link of the object
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	reqBody := map[string]any{
		"fids": []string{o.id},
	}
	var downResp api.DownResp
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/download",
	}

	// The download URL is signed for the cookie used to fetch it so
	// use the same cookie to download
	var cookieSnapshot string
	err := o.fs.pacer.Call(func() (bool, error) {
		cookieSnapshot = o.fs.getCookie()
		resp, err := o.fs.callOnce(ctx, &opts, reqBody, &downResp)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("quark: get download link failed: %w", err)
	}
	if len(downResp.Data) == 0 || downResp.Data[0].DownloadURL == "" {
		return nil, fmt.Errorf("quark: empty download URL for %s", o.remote)
	}
	return &linkcache.Link{
		URL: downResp.Data[0].DownloadURL,
		Header: http.Header{
			"User-Agent": {o.fs.userAgent},
			"Referer":    {o.fs.referer},
			"Cookie":     {cookieSnapshot},
		},
	}, nil
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

// openLink reads the object from link, sending the headers it is
// bound to
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	headers := make(map[string]string, len(link.Header))
	for k := range link.Header {
		headers[k] = link.Header.Get(k)
	}
	dlOpts := rest.Opts{
		Method:       http.MethodGet,
		RootURL:      link.URL,
		Options:      options,
		ExtraHeaders: headers,
	}
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var err error
		resp, err = o.fs.rawSrv.Call(ctx, &dlOpts)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		err = fmt.Errorf("quark: download failed: %w", err)
		if resp != nil {
			return nil, &downloadError{status: resp.StatusCode, err: err}
		}
		return nil, err
	}
	return resp.Body, nil
}

// spoolToTemp copies in to a temporary file calculating the MD5 and
// SHA1 hashes on the way. The returned file is positioned at the
// start and must be closed and removed with cleanup.
func spoolToTemp(in io.Reader) (file *os.File, size int64, md5sum, sha1sum string, cleanup func(), err error) {
	tmp, err := os.CreateTemp("", "rclone-quark-upload-*")
	if err != nil {
		return nil, 0, "", "", nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	cleanup = func() {
		if err := tmp.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			fs.Debugf(nil, "Failed to close temporary file: %v", err)
		}
		if err := os.Remove(tmp.Name()); err != nil {
			fs.Errorf(nil, "Failed to remove temporary file: %v", err)
		}
	}
	defer func() {
		if err != nil {
			cleanup()
			cleanup = nil
		}
	}()
	hasher, err := hash.NewMultiHasherTypes(hash.NewHashSet(hash.MD5, hash.SHA1))
	if err != nil {
		return nil, 0, "", "", nil, err
	}
	size, err = io.Copy(io.MultiWriter(tmp, hasher), in)
	if err != nil {
		return nil, 0, "", "", nil, fmt.Errorf("failed to write temporary file: %w", err)
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return nil, 0, "", "", nil, fmt.Errorf("failed to rewind temporary file: %w", err)
	}
	sums := hasher.Sums()
	return tmp, size, sums[hash.MD5], sums[hash.SHA1], cleanup, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If the source can supply MD5 and SHA1 hashes the data is streamed
// in parts, otherwise it is spooled to a temporary file first to
// calculate them as they are needed before the upload starts.
//
// The new file is uploaded first and the old one deleted only once
// the upload has succeeded.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	leaf, dirID, err := f.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}

	size := src.Size()
	md5sum, _ := src.Hash(ctx, hash.MD5)
	sha1sum, _ := src.Hash(ctx, hash.SHA1)
	if size < 0 || md5sum == "" || sha1sum == "" {
		var tmpFile *os.File
		var cleanup func()
		tmpFile, size, md5sum, sha1sum, cleanup, err = spoolToTemp(in)
		if err != nil {
			return err
		}
		defer cleanup()
		in = tmpFile
	}

	fid, err := f.upload(ctx, in, src, dirID, leaf, size, strings.ToLower(md5sum), strings.ToLower(sha1sum))
	if err != nil {
		return err
	}

	// Remove the old version
	if o.id != "" && o.id != fid {
		err = f.deleteItem(ctx, o.id)
		if err != nil {
			return fmt.Errorf("failed to remove old version: %w", err)
		}
	}

	// Read the info for the new file making sure it has the right name
	info, err := f.fileInfo(ctx, fid)
	if err != nil {
		return fmt.Errorf("failed to read uploaded file info: %w", err)
	}
	if info == nil {
		fs.Debugf(o, "Uploaded file %s not found yet", fid)
		info = &api.File{
			Fid:       fid,
			FileName:  leaf,
			Size:      size,
			File:      true,
			UpdatedAt: time.Now().UnixMilli(),
		}
	}
	if info.FileName != leaf {
		fs.Debugf(o, "Server named uploaded file %q, renaming", info.FileName)
		if err := f.rename(ctx, fid, leaf); err != nil {
			return fmt.Errorf("failed to rename uploaded file: %w", err)
		}
	}
	o.id = info.Fid
	o.size = info.Size
	o.modTime = info.ModTime()
	return nil
}

// upload uploads size bytes from in as leaf in dirID returning the fid
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, dirID, leaf string, size int64, md5sum, sha1sum string) (string, error) {
	mimeType := fs.MimeType(ctx, src)

	// 1. Upload pre - this allocates a task so must not be retried
	nowMilli := time.Now().UnixMilli()
	preBody := map[string]any{
		"ccp_hash_update": true,
		"dir_name":        "",
		"file_name":       f.opt.Enc.FromStandardName(leaf),
		"format_type":     mimeType,
		"l_created_at":    nowMilli,
		"l_updated_at":    nowMilli,
		"pdir_fid":        dirID,
		"size":            size,
	}
	var pre api.UpPreResp
	preOpts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/upload/pre",
	}
	err := f.requestNoRetry(ctx, &preOpts, preBody, &pre)
	if err != nil {
		return "", fmt.Errorf("quark: upload pre failed: %w", err)
	}
	if pre.Data.TaskID == "" || pre.Data.Fid == "" {
		return "", errors.New("quark: invalid upload pre response: missing task_id or fid")
	}
	if pre.Data.Finish {
		return pre.Data.Fid, nil
	}

	// 2. Hash check for rapid upload
	hashBody := map[string]any{
		"md5":     md5sum,
		"sha1":    sha1sum,
		"task_id": pre.Data.TaskID,
	}
	var hashResp api.HashResp
	hashOpts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/update/hash",
	}
	err = f.request(ctx, &hashOpts, hashBody, &hashResp)
	if err != nil {
		return "", fmt.Errorf("quark: hash update failed: %w", err)
	}
	if hashResp.Data.Finish {
		fs.Debugf(src, "Rapid upload succeeded")
		if hashResp.Data.Fid != "" {
			return hashResp.Data.Fid, nil
		}
		return pre.Data.Fid, nil
	}

	// 3. Multipart upload to OSS
	if pre.Data.UploadID == "" || pre.Data.ObjKey == "" || pre.Data.Bucket == "" || pre.Data.AuthInfo == "" {
		return "", errors.New("quark: invalid upload pre response: missing upload parameters")
	}
	partSize := int64(pre.Metadata.PartSize)
	if partSize <= 0 {
		return "", errors.New("quark: invalid upload pre response: part_size must be positive")
	}
	target, err := newUploadTarget(&pre)
	if err != nil {
		return "", err
	}

	buf := make([]byte, min(partSize, max(size, 1)))
	var etags []string
	// Always upload at least one part, even if empty
	for partNumber, remaining := 1, size; partNumber == 1 || remaining > 0; partNumber++ {
		n := min(partSize, remaining)
		chunk := buf[:n]
		if _, err := io.ReadFull(in, chunk); err != nil {
			return "", fmt.Errorf("quark: failed to read part %d: %w", partNumber, err)
		}
		etag, err := f.uploadPart(ctx, target, &pre, mimeType, partNumber, chunk)
		if err != nil {
			return "", fmt.Errorf("quark: upload part %d failed: %w", partNumber, err)
		}
		etags = append(etags, etag)
		remaining -= n
	}

	// 4. Commit multipart
	err = f.commitUpload(ctx, target, &pre, etags)
	if err != nil {
		return "", fmt.Errorf("quark: commit failed: %w", err)
	}

	// 5. Finish upload
	finishBody := map[string]any{
		"obj_key": pre.Data.ObjKey,
		"task_id": pre.Data.TaskID,
	}
	finishOpts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/upload/finish",
	}
	err = f.request(ctx, &finishOpts, finishBody, nil)
	if err != nil {
		return "", fmt.Errorf("quark: upload finish failed: %w", err)
	}
	return pre.Data.Fid, nil
}

// uploadTarget is where the parts are uploaded to
type uploadTarget struct {
	url string // URL of the object without query
}

// newUploadTarget works out the OSS URL from the pre response
func newUploadTarget(pre *api.UpPreResp) (*uploadTarget, error) {
	host := strings.TrimSpace(pre.Data.UploadURL)
	if strings.Contains(host, "://") {
		u, err := url.Parse(host)
		if err != nil {
			return nil, fmt.Errorf("quark: invalid upload_url %q: %w", pre.Data.UploadURL, err)
		}
		host = u.Host
	}
	host = strings.TrimRight(host, "/")
	if host == "" || strings.ContainsAny(host, "/?#") {
		return nil, fmt.Errorf("quark: invalid upload_url %q", pre.Data.UploadURL)
	}
	segments := strings.Split(pre.Data.ObjKey, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	return &uploadTarget{
		url: "https://" + pre.Data.Bucket + "." + host + "/" + strings.Join(segments, "/"),
	}, nil
}

// uploadAuth asks the API to sign authMeta for OSS
func (f *Fs) uploadAuth(ctx context.Context, pre *api.UpPreResp, authMeta string) (string, error) {
	authBody := map[string]any{
		"auth_info": pre.Data.AuthInfo,
		"auth_meta": authMeta,
		"task_id":   pre.Data.TaskID,
	}
	var authResp api.UpAuthResp
	authOpts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/file/upload/auth",
	}
	err := f.request(ctx, &authOpts, authBody, &authResp)
	if err != nil {
		return "", fmt.Errorf("upload auth failed: %w", err)
	}
	if authResp.Data.AuthKey == "" {
		return "", errors.New("upload auth returned empty auth key")
	}
	return authResp.Data.AuthKey, nil
}

// uploadPart uploads a single part retrying it if necessary
func (f *Fs) uploadPart(ctx context.Context, target *uploadTarget, pre *api.UpPreResp, mimeType string, partNumber int, chunk []byte) (etag string, err error) {
	subResource := fmt.Sprintf("partNumber=%d&uploadId=%s", partNumber, pre.Data.UploadID)
	err = f.pacer.Call(func() (bool, error) {
		timeStr := time.Now().UTC().Format(http.TimeFormat)
		authMeta := fmt.Sprintf("PUT\n\n%s\n%s\nx-oss-date:%s\nx-oss-user-agent:%s\n/%s/%s?%s",
			mimeType, timeStr, timeStr, ossUserAgent, pre.Data.Bucket, pre.Data.ObjKey, subResource)
		authKey, err := f.uploadAuth(ctx, pre, authMeta)
		if err != nil {
			return false, err
		}
		size := int64(len(chunk))
		opts := rest.Opts{
			Method:        http.MethodPut,
			RootURL:       target.url,
			Parameters:    url.Values{"partNumber": {strconv.Itoa(partNumber)}, "uploadId": {pre.Data.UploadID}},
			Body:          bytes.NewReader(chunk),
			ContentLength: &size,
			ContentType:   mimeType,
			ExtraHeaders: map[string]string{
				"Authorization":    authKey,
				"Referer":          f.referer + "/",
				"x-oss-date":       timeStr,
				"x-oss-user-agent": ossUserAgent,
			},
		}
		resp, err := f.rawSrv.Call(ctx, &opts)
		if err == nil {
			etag = resp.Header.Get("ETag")
			err = resp.Body.Close()
			if err == nil && etag == "" {
				err = errors.New("no ETag returned")
			}
		}
		return f.shouldRetry(ctx, resp, err)
	})
	return etag, err
}

// commitUpload completes the multipart upload
func (f *Fs) commitUpload(ctx context.Context, target *uploadTarget, pre *api.UpPreResp, etags []string) error {
	var xmlBuf bytes.Buffer
	xmlBuf.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUpload>\n")
	for i, etag := range etags {
		fmt.Fprintf(&xmlBuf, "<Part>\n<PartNumber>%d</PartNumber>\n<ETag>%s</ETag>\n</Part>\n", i+1, etag)
	}
	xmlBuf.WriteString("</CompleteMultipartUpload>")
	xmlBytes := xmlBuf.Bytes()

	m5 := md5.Sum(xmlBytes)
	contentMD5 := base64.StdEncoding.EncodeToString(m5[:])

	callbackBytes, err := json.Marshal(pre.Data.Callback)
	if err != nil {
		return err
	}
	callbackB64 := base64.StdEncoding.EncodeToString(callbackBytes)

	return f.pacer.Call(func() (bool, error) {
		timeStr := time.Now().UTC().Format(http.TimeFormat)
		commitMeta := fmt.Sprintf("POST\n%s\napplication/xml\n%s\nx-oss-callback:%s\nx-oss-date:%s\nx-oss-user-agent:%s\n/%s/%s?uploadId=%s",
			contentMD5, timeStr, callbackB64, timeStr, ossUserAgent, pre.Data.Bucket, pre.Data.ObjKey, pre.Data.UploadID)
		authKey, err := f.uploadAuth(ctx, pre, commitMeta)
		if err != nil {
			return false, err
		}
		size := int64(len(xmlBytes))
		opts := rest.Opts{
			Method:        http.MethodPost,
			RootURL:       target.url,
			Parameters:    url.Values{"uploadId": {pre.Data.UploadID}},
			Body:          bytes.NewReader(xmlBytes),
			ContentLength: &size,
			ContentType:   "application/xml",
			NoResponse:    true,
			ExtraHeaders: map[string]string{
				"Authorization":    authKey,
				"Content-MD5":      contentMD5,
				"Referer":          f.referer + "/",
				"x-oss-callback":   callbackB64,
				"x-oss-date":       timeStr,
				"x-oss-user-agent": ossUserAgent,
			},
		}
		resp, err := f.rawSrv.Call(ctx, &opts)
		return f.shouldRetry(ctx, resp, err)
	})
}

// Check interfaces
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
