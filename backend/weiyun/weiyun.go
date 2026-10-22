//go:build !plan9

// Package weiyun provides a filesystem interface to Tencent Weiyun.
package weiyun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	weiyunsdkgo "github.com/foxxorcat/weiyun-sdk-go"
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
	"resty.dev/v3"
)

const (
	minSleep      = 100 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2
	listPageSize  = 100

	// Files up to this size are buffered in memory for upload,
	// larger ones are spooled to a temporary file. This is the
	// block size the upload protocol uses.
	memoryUploadLimit = 1024 * 1024

	// Upload states returned by the upload API
	uploadStateMore = 1 // more pieces to upload on this channel
	uploadStateDone = 2 // upload complete

	// API return codes
	codeNameConflict  = 1051   // name already exists
	codeDirNotEmpty   = 1063   // can't delete non-empty directory
	codeDeleteRetry   = 1115   // concurrent delete conflict, retry
	codeStorageBusy   = 1013   // storage system busy
	codeServerError   = 190041 // server internal error
	codeServerTimeout = 190042 // backend server timeout
	codeRateLimited   = 190054 // too many requests
	codeUnavailable   = 190055 // server temporarily unavailable

	// linkLifetime is how long a download link is kept when its URL
	// states no expiry; Weiyun documents none, so a conservative guess
	linkLifetime = 10 * time.Minute
)

// retryErrorCodes is a slice of HTTP error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// retryAPICodes are API return codes that we will retry
var retryAPICodes = []int{
	codeDeleteRetry,
	codeStorageBusy,
	codeServerError,
	codeServerTimeout,
	codeRateLimited,
	codeUnavailable,
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "weiyun",
		Description: "Tencent Weiyun",
		NewFs:       NewFs,
		Aliases:     []string{"tencentweiyun"},
		Options: []fs.Option{{
			Name:      "cookies",
			Help:      "Weiyun Cookies (must include uin and skey or p_skey)",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID (defaults to user root directory)",
			Default:  "",
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     "The encoding for the backend.",
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines configuration options for Weiyun
type Options struct {
	Cookies      string               `config:"cookies"`
	RootFolderID string               `config:"root_folder_id"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a Weiyun remote
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	client   *weiyunsdkgo.WeiYunClient
	httpCl   *http.Client
	dirCache *dircache.DirCache
	pacer    *fs.Pacer
	dirInfo  *sync.Map // dir key => dirInfo
}

// dirInfo is the cached parent key and server side (encoded) name of a directory
type dirInfo struct {
	pdirKey string
	name    string
}

// Object represents a file on Weiyun
type Object struct {
	fs       *Fs
	remote   string
	id       string
	parentID string
	size     int64
	modTime  time.Time
	sha1     string
}

// ------------------------------------------------------------
// Fs Implementation
// ------------------------------------------------------------

// withContext returns a request option binding ctx to the request
func withContext(ctx context.Context) weiyunsdkgo.RestyOption {
	return func(r *resty.Request) {
		r.SetContext(ctx)
	}
}

// apiCode returns the API return code of err or 0 if none
func apiCode(err error) int {
	var resp *weiyunsdkgo.Resp
	if !errors.As(err, &resp) {
		return 0
	}
	if resp.Data != nil && resp.Data.RspHeader.Retcode != 0 {
		return resp.Data.RspHeader.Retcode
	}
	return resp.Code
}

// statusError is a download answered with an HTTP error status
type statusError struct {
	status int
}

// Error implements error
func (e *statusError) Error() string {
	return fmt.Sprintf("weiyun: download bad status code %d", e.status)
}

// HTTPStatusCode returns the HTTP status of the answer, which
// linkcache.Refused reads
func (e *statusError) HTTPStatusCode() int { return e.status }

// httpCode returns the HTTP status code of a download or SDK error or
// 0 if none
func httpCode(err error) int {
	var se *statusError
	if errors.As(err, &se) {
		return se.status
	}
	if errors.Is(err, weiyunsdkgo.ErrCode403) {
		return http.StatusForbidden
	}
	const prefix = "err http code: "
	msg := err.Error()
	if !strings.HasPrefix(msg, prefix) {
		return 0
	}
	code, convErr := strconv.Atoi(strings.TrimPrefix(msg, prefix))
	if convErr != nil {
		return 0
	}
	return code
}

// shouldRetry returns a boolean as to whether this err deserves to
// be retried. It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	for _, code := range retryAPICodes {
		if apiCode(err) == code {
			return true, err
		}
	}
	if code := httpCode(err); code != 0 {
		for _, retryCode := range retryErrorCodes {
			if code == retryCode {
				return true, err
			}
		}
	}
	return fserrors.ShouldRetry(err), err
}

// call runs fn under the pacer retrying it if necessary
func (f *Fs) call(ctx context.Context, fn func(opt weiyunsdkgo.RestyOption) error) error {
	opt := withContext(ctx)
	return f.pacer.Call(func() (bool, error) {
		return f.shouldRetry(ctx, fn(opt))
	})
}

// NewFs constructs an Fs from path and config
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if opt.Cookies == "" {
		return nil, errors.New("weiyun: cookies is required")
	}

	httpCl := fshttp.NewClient(ctx)
	// The SDK keeps the login cookies in the client's cookie jar so
	// give it one of its own.
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	sdkHTTPClient := *httpCl
	sdkHTTPClient.Jar = jar

	f := &Fs{
		name:    name,
		root:    root,
		opt:     *opt,
		client:  weiyunsdkgo.NewWeiYunClient().SetClient(&sdkHTTPClient),
		httpCl:  httpCl,
		pacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		dirInfo: new(sync.Map),
	}
	f.client.SetOnCookieExpired(func(err error) {
		fs.Errorf(f, "Login cookies have expired - please update the cookies in the config: %v", err)
	})

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements
		MaxFileStreams: 2,
		MaxStreams:     4,
	}).Fill(ctx, f)

	err = f.client.SetCookiesStr(opt.Cookies).RefreshCtoken()
	if err != nil {
		return nil, fmt.Errorf("weiyun: cookie validation failed: %w", err)
	}

	if f.opt.RootFolderID == "" {
		var userInfo *weiyunsdkgo.DiskUserInfoGetData
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) (err error) {
			userInfo, err = f.client.DiskUserInfoGet(o)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("weiyun: get user info failed: %w", err)
		}
		f.opt.RootFolderID = userInfo.MainDirKey
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = userInfo.RootDirKey
		}
	}

	f.dirCache = dircache.New(root, f.opt.RootFolderID, f)

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

// Name returns remote name
func (f *Fs) Name() string { return f.name }

// Root returns root path
func (f *Fs) Root() string { return f.root }

// String returns remote string
func (f *Fs) String() string { return fmt.Sprintf("Weiyun root '%s'", f.root) }

// Precision returns precision of modtime
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns supported hashes (SHA1)
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.SHA1) }

// Features returns features
func (f *Fs) Features() *fs.Features { return f.features }

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// getDirInfo returns the parent key and name of the directory dirKey
func (f *Fs) getDirInfo(ctx context.Context, dirKey string) (dirInfo, error) {
	if info, ok := f.dirInfo.Load(dirKey); ok {
		return info.(dirInfo), nil
	}
	var folders []weiyunsdkgo.FolderPath
	err := f.call(ctx, func(o weiyunsdkgo.RestyOption) (err error) {
		folders, err = f.client.LibDirPathGet(dirKey, o)
		return err
	})
	if err != nil {
		return dirInfo{}, fmt.Errorf("failed to read directory path: %w", err)
	}
	if len(folders) == 0 {
		return dirInfo{}, fmt.Errorf("directory %q not found", dirKey)
	}
	last := folders[len(folders)-1]
	info := dirInfo{pdirKey: last.PdirKey, name: last.DirName}
	f.dirInfo.Store(dirKey, info)
	return info, nil
}

// putDirInfo caches the parent and name of a directory
func (f *Fs) putDirInfo(dirKey, pdirKey, name string) {
	f.dirInfo.Store(dirKey, dirInfo{pdirKey: pdirKey, name: name})
}

// folderParam returns the parameters describing the directory dirKey
func (f *Fs) folderParam(ctx context.Context, dirKey string) (weiyunsdkgo.FolderParam, error) {
	info, err := f.getDirInfo(ctx, dirKey)
	if err != nil {
		return weiyunsdkgo.FolderParam{}, err
	}
	param := weiyunsdkgo.FolderParam{
		PdirKey: info.pdirKey,
		DirKey:  dirKey,
		DirName: info.name,
	}
	if info.pdirKey != "" {
		parentInfo, err := f.getDirInfo(ctx, info.pdirKey)
		if err == nil {
			param.PPdirKey = parentInfo.pdirKey
		}
	}
	return param, nil
}

// listAll lists all the directories and files in dirKey
func (f *Fs) listAll(ctx context.Context, dirKey string) (dirs []weiyunsdkgo.Folder, files []weiyunsdkgo.File, err error) {
	offset := int64(0)
	for {
		var data *weiyunsdkgo.DiskListData
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) (err error) {
			data, err = f.client.DiskDirFileList(dirKey, weiyunsdkgo.WarpParamOption(
				weiyunsdkgo.QueryFileOptionOffest(offset),
				weiyunsdkgo.QueryFileOptionGetType(weiyunsdkgo.FileAndDir),
				weiyunsdkgo.QueryFileOptionCount(listPageSize),
			), o)
			return err
		})
		if err != nil {
			return nil, nil, err
		}
		for i := range data.DirList {
			f.putDirInfo(data.DirList[i].DirKey, dirKey, data.DirList[i].DirName)
			data.DirList[i].DirName = f.opt.Enc.ToStandardName(data.DirList[i].DirName)
		}
		for i := range data.FileList {
			data.FileList[i].FileName = f.opt.Enc.ToStandardName(data.FileList[i].FileName)
		}
		dirs = append(dirs, data.DirList...)
		files = append(files, data.FileList...)

		n := len(data.DirList) + len(data.FileList)
		offset += int64(n)
		if data.FinishFlag || n == 0 {
			break
		}
	}
	return dirs, files, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (dirID string, found bool, err error) {
	dirs, _, err := f.listAll(ctx, pathID)
	if err != nil {
		return "", false, err
	}
	for _, d := range dirs {
		if d.DirName == leaf {
			return d.DirKey, true, nil
		}
	}
	return "", false, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (dirID string, err error) {
	parent, err := f.folderParam(ctx, pathID)
	if err != nil {
		return "", err
	}
	var folder *weiyunsdkgo.Folder
	tries := 0
	err = f.pacer.Call(func() (bool, error) {
		tries++
		if tries > 1 {
			// A previous attempt may have created the directory
			// so check before trying again as the server renames
			// rather than fails on conflicts.
			id, found, err := f.FindLeaf(ctx, pathID, leaf)
			if err != nil {
				return false, err
			}
			if found {
				folder = &weiyunsdkgo.Folder{DirKey: id}
				return false, nil
			}
		}
		var err error
		folder, err = f.client.DiskDirCreate(weiyunsdkgo.FolderParam{
			PPdirKey: parent.PdirKey,
			PdirKey:  pathID,
			DirName:  f.opt.Enc.FromStandardName(leaf),
		}, withContext(ctx))
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	if folder.DirKey == "" {
		return "", fmt.Errorf("created directory %q but no ID was returned", leaf)
	}
	f.putDirInfo(folder.DirKey, pathID, f.opt.Enc.FromStandardName(leaf))
	return folder.DirKey, nil
}

// newObject makes an Object from a weiyunsdkgo.File in dirKey
func (f *Fs) newObject(remote, dirKey string, file *weiyunsdkgo.File) *Object {
	return &Object{
		fs:       f,
		remote:   remote,
		id:       file.FileID,
		parentID: dirKey,
		size:     file.FileSize,
		modTime:  time.Time(file.FileMtime),
		sha1:     strings.ToLower(file.FileSha),
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

	dirs, files, err := f.listAll(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for _, folder := range dirs {
		remote := path.Join(dir, folder.DirName)
		f.dirCache.Put(remote, folder.DirKey)
		d := fs.NewDir(remote, time.Time(folder.DirMtime)).SetID(folder.DirKey)
		entries = append(entries, d)
	}
	for i := range files {
		remote := path.Join(dir, files[i].FileName)
		entries = append(entries, f.newObject(remote, dirID, &files[i]))
	}
	return entries, nil
}

// findFile finds the file called leaf in dirKey returning nil if not found
func (f *Fs) findFile(ctx context.Context, dirKey, leaf string) (*weiyunsdkgo.File, error) {
	_, files, err := f.listAll(ctx, dirKey)
	if err != nil {
		return nil, err
	}
	for i := range files {
		if files[i].FileName == leaf {
			return &files[i], nil
		}
	}
	return nil, nil
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
	file, err := f.findFile(ctx, dirID, leaf)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return f.newObject(remote, dirID, file), nil
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

// deleteDir deletes the directory dirID including its contents
func (f *Fs) deleteDir(ctx context.Context, dirID string) error {
	param, err := f.folderParam(ctx, dirID)
	if err != nil {
		return err
	}
	err = f.call(ctx, func(o weiyunsdkgo.RestyOption) error {
		return f.client.DiskDirDelete(param, o)
	})
	if err != nil {
		return err
	}
	f.dirInfo.Delete(dirID)
	return nil
}

// Rmdir deletes the directory if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	dirs, files, err := f.listAll(ctx, dirID)
	if err != nil {
		return err
	}
	if len(dirs) > 0 || len(files) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	err = f.deleteDir(ctx, dirID)
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
	err = f.deleteDir(ctx, dirID)
	if err != nil {
		if apiCode(err) == codeDirNotEmpty {
			// Server won't delete recursively so let rclone do it
			return fs.ErrorCantPurge
		}
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// fileParam returns the parameters describing the object
func (o *Object) fileParam(ctx context.Context) (weiyunsdkgo.FileParam, error) {
	parent, err := o.fs.getDirInfo(ctx, o.parentID)
	if err != nil {
		return weiyunsdkgo.FileParam{}, err
	}
	return weiyunsdkgo.FileParam{
		PPdirKey: parent.pdirKey,
		PdirKey:  o.parentID,
		FileID:   o.id,
		FileName: o.fs.opt.Enc.FromStandardName(path.Base(o.remote)),
	}, nil
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

	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcParam, err := srcObj.fileParam(ctx)
	if err != nil {
		return nil, err
	}

	if srcObj.parentID != dstDirID {
		dstParam, err := f.folderParam(ctx, dstDirID)
		if err != nil {
			return nil, err
		}
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) error {
			return f.client.DiskFileMove(srcParam, dstParam, o)
		})
		if err != nil {
			return nil, fmt.Errorf("move failed: %w", err)
		}
		srcParam.PPdirKey = dstParam.PdirKey
		srcParam.PdirKey = dstDirID
	}

	if srcParam.FileName != f.opt.Enc.FromStandardName(dstLeaf) {
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) error {
			return f.client.DiskFileRename(srcParam, f.opt.Enc.FromStandardName(dstLeaf), o)
		})
		if err != nil {
			return nil, fmt.Errorf("rename after move failed: %w", err)
		}
	}

	dstObj := *srcObj
	dstObj.fs = f
	dstObj.remote = remote
	dstObj.parentID = dstDirID
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

	srcID, srcDirectoryID, _, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	srcParam, err := f.folderParam(ctx, srcID)
	if err != nil {
		return err
	}
	if srcDirectoryID != dstDirectoryID {
		dstParam, err := f.folderParam(ctx, dstDirectoryID)
		if err != nil {
			return err
		}
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) error {
			return f.client.DiskDirMove(srcParam, dstParam, o)
		})
		if err != nil {
			return fmt.Errorf("directory move failed: %w", err)
		}
		srcParam.PPdirKey = dstParam.PdirKey
		srcParam.PdirKey = dstDirectoryID
		f.putDirInfo(srcID, dstDirectoryID, srcParam.DirName)
	}

	newName := f.opt.Enc.FromStandardName(dstLeaf)
	if srcParam.DirName != newName {
		err = f.call(ctx, func(o weiyunsdkgo.RestyOption) error {
			return f.client.DiskDirAttrModify(srcParam, newName, o)
		})
		if err != nil {
			return fmt.Errorf("rename after directory move failed: %w", err)
		}
		f.putDirInfo(srcID, srcParam.PdirKey, newName)
	}

	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// About gets quota and usage information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	var info *weiyunsdkgo.DiskUserInfoGetData
	err := f.call(ctx, func(o weiyunsdkgo.RestyOption) (err error) {
		info, err = f.client.DiskUserInfoGet(o)
		return err
	})
	if err != nil {
		return nil, err
	}

	usage := &fs.Usage{
		Total: fs.NewUsageValue(info.TotalSpace),
		Used:  fs.NewUsageValue(info.UsedSpace),
		Free:  fs.NewUsageValue(info.TotalSpace - info.UsedSpace),
	}
	return usage, nil
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
	if ty != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return o.sha1, nil
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
	param, err := o.fileParam(ctx)
	if err != nil {
		return err
	}
	return o.fs.call(ctx, func(opt weiyunsdkgo.RestyOption) error {
		return o.fs.client.DiskFileDelete(param, opt)
	})
}

// Open opens the file for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	key := linkcache.Key(o.fs.name, o.id, strconv.FormatInt(o.size, 10), o.sha1)
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the download link of the object
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	var data []weiyunsdkgo.DiskFileDownloadData
	err := o.fs.call(ctx, func(opt weiyunsdkgo.RestyOption) (err error) {
		data, err = o.fs.client.DiskFileBatchDownload([]weiyunsdkgo.FileParam{{
			PdirKey: o.parentID,
			FileID:  o.id,
		}}, opt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("weiyun: get download link failed: %w", err)
	}
	if len(data) == 0 || data[0].DownloadUrl == "" {
		return nil, fmt.Errorf("weiyun: no download link returned for %s", o.remote)
	}
	if data[0].Retcode != 0 {
		return nil, fmt.Errorf("weiyun: get download link failed: %s (code %d)", data[0].Retmsg, data[0].Retcode)
	}
	link := &linkcache.Link{URL: data[0].DownloadUrl}
	if data[0].CookieName != "" && data[0].CookieValue != "" {
		// The link is bound to this cookie
		link.Header = http.Header{"Cookie": {data[0].CookieName + "=" + data[0].CookieValue}}
	}
	return link, nil
}

// openLink reads the object from link, sending the headers it is
// bound to
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.URL, nil)
		if err != nil {
			return false, err
		}
		for k, v := range link.Header {
			req.Header[k] = v
		}
		fs.OpenOptionAddHTTPHeaders(req.Header, options)
		resp, err = o.fs.httpCl.Do(req)
		if err == nil && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			err = &statusError{status: resp.StatusCode}
			resp = nil
		}
		if err != nil {
			return o.fs.shouldRetry(ctx, err)
		}
		return false, nil
	})
	if err != nil {
		if slices.Contains(retryErrorCodes, httpCode(err)) {
			return nil, fserrors.RetryError(err)
		}
		return nil, err
	}
	return resp.Body, nil
}

// uploadSource is the data to upload with random access
type uploadSource interface {
	io.ReadSeeker
	io.ReaderAt
}

// bufferUpload makes in available for random access. Small files are
// kept in memory, larger ones are spooled to a temporary file which
// is removed by cleanup.
func bufferUpload(in io.Reader) (src uploadSource, size int64, cleanup func(), err error) {
	head := make([]byte, memoryUploadLimit+1)
	n, err := io.ReadFull(in, head)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return bytes.NewReader(head[:n]), int64(n), func() {}, nil
	}
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to read data to upload: %w", err)
	}

	tmp, err := os.CreateTemp("", "rclone-weiyun-upload-*")
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	cleanup = func() {
		if err := tmp.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			fs.Debugf(nil, "Failed to close temporary file: %v", err)
		}
		if err := os.Remove(tmp.Name()); err != nil {
			fs.Errorf(nil, "Failed to remove temporary file: %v", err)
		}
	}
	size, err = io.Copy(tmp, io.MultiReader(bytes.NewReader(head), in))
	if err != nil {
		cleanup()
		return nil, 0, nil, fmt.Errorf("failed to write temporary file: %w", err)
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, nil, fmt.Errorf("failed to rewind temporary file: %w", err)
	}
	return tmp, size, cleanup, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The upload protocol needs the SHA1 state of every 1 MiB block
// before the upload starts and the server then asks for pieces at
// arbitrary offsets, so the data must be available for random
// access. Small files are buffered in memory and larger ones are
// spooled to a temporary file.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	leaf, dirID, err := f.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	parent, err := f.getDirInfo(ctx, dirID)
	if err != nil {
		return err
	}

	data, size, cleanup, err := bufferUpload(in)
	if err != nil {
		return err
	}
	defer cleanup()
	if srcSize := src.Size(); srcSize >= 0 && srcSize != size {
		return fmt.Errorf("weiyun: upload size mismatch: read %d bytes, expected %d", size, srcSize)
	}

	// 1. Preupload - this calculates the block hashes and checks for
	// a rapid upload. The file is overwritten if it exists.
	var preData *weiyunsdkgo.PreUploadData
	err = f.pacer.Call(func() (bool, error) {
		if _, err := data.Seek(0, io.SeekStart); err != nil {
			return false, err
		}
		var err error
		preData, err = f.client.PreUpload(ctx, weiyunsdkgo.UpdloadFileParam{
			PdirKey:         parent.pdirKey,
			DirKey:          dirID,
			FileName:        f.opt.Enc.FromStandardName(leaf),
			FileSize:        size,
			File:            data,
			ChannelCount:    4,
			FileExistOption: 1, // Overwrite
		})
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("weiyun: preupload failed: %w", err)
	}

	// 2. Upload the pieces the server asks for unless it was a rapid upload
	if preData.FileExist {
		fs.Debugf(o, "Rapid upload succeeded")
	} else {
		err = f.uploadChannels(ctx, preData, data, size)
		if err != nil {
			return err
		}
	}

	// 3. Read the info for the new file
	_, files, err := f.listAll(ctx, dirID)
	if err != nil {
		return fmt.Errorf("weiyun: failed to read uploaded file info: %w", err)
	}
	newID := preData.File.FileID
	var newFile *weiyunsdkgo.File
	for i := range files {
		file := &files[i]
		if (newID != "" && file.FileID == newID) || (newID == "" && file.FileName == leaf) {
			newFile = file
			break
		}
	}
	if newFile == nil {
		fs.Debugf(o, "Uploaded file not found in listing yet")
		newFile = &preData.File
		newFile.FileName = leaf
		newFile.FileSize = size
		newFile.FileMtime = weiyunsdkgo.TimeStamp(time.Now())
	}

	// Remove the old version if the server didn't overwrite it
	if o.id != "" && o.id != newFile.FileID {
		for i := range files {
			if files[i].FileID == o.id {
				err = o.Remove(ctx)
				if err != nil {
					return fmt.Errorf("weiyun: failed to remove old version: %w", err)
				}
				break
			}
		}
	}

	*o = *f.newObject(o.remote, dirID, newFile)
	if o.sha1 == "" {
		o.sha1 = strings.ToLower(preData.File.FileSha)
	}
	return nil
}

// uploadChannels uploads the pieces requested on each upload channel
func (f *Fs) uploadChannels(ctx context.Context, preData *weiyunsdkgo.PreUploadData, data io.ReaderAt, size int64) error {
	for _, channel := range preData.ChannelList {
		for {
			channel.Len = int(min(size-channel.Offset, int64(channel.Len)))
			if channel.Len < 0 {
				return fmt.Errorf("weiyun: invalid upload channel offset %d", channel.Offset)
			}
			var upData *weiyunsdkgo.UploadPieceData
			err := f.pacer.Call(func() (bool, error) {
				section := io.NewSectionReader(data, channel.Offset, int64(channel.Len))
				var err error
				upData, err = f.client.UploadFile(ctx, channel, preData.UploadAuthData, section)
				return f.shouldRetry(ctx, err)
			})
			if err != nil {
				return fmt.Errorf("weiyun: upload piece at offset %d failed: %w", channel.Offset, err)
			}
			if upData.UploadState == uploadStateDone {
				return nil
			}
			if upData.UploadState != uploadStateMore {
				break
			}
			channel = upData.Channel
		}
	}
	return nil
}

// Check interfaces
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
