// Package baidunetdisk provides a filesystem interface to Baidu Netdisk.
package baidunetdisk

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	stdpath "path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/baidunetdisk/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
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
	defaultClientID     = "hq9yQ9w9kR4YHj1kyYafLygVocobh7Sf"
	defaultClientSecret = "YH2VpZcFJHYNnV6vLfHQXDBhcE7ZChyE"
	apiBaseURL          = "https://pan.baidu.com/rest/2.0"
	oauthTokenURL       = "https://openapi.baidu.com/oauth/2.0/token"
	uploadAPI           = "https://d.pcs.baidu.com"
	minSleep            = 100 * time.Millisecond
	maxSleep            = 2 * time.Second
	decayConstant       = 2

	// Upload chunk sizes by membership level. The block list sent to
	// precreate must use exactly this size for every block but the last.
	defaultChunkSize = 4 * fs.Mebi
	vipChunkSize     = 16 * fs.Mebi
	svipChunkSize    = 32 * fs.Mebi
	// maxChunks is the maximum number of blocks in an upload (the
	// documentation says 1024, tests show 2048 works).
	maxChunks = 2048
	// sliceMD5Size is how many leading bytes the slice-md5 covers
	sliceMD5Size = 256 * 1024

	// linkLifetime is how long a resolved download URL is kept when it
	// states no expiry. Baidu documents dlinks as valid for 8 hours but
	// not the storage URLs they redirect to, so this is conservative.
	linkLifetime = time.Hour
)

// retryErrorCodes are HTTP status codes worth retrying
var retryErrorCodes = []int{
	429, // Too Many Requests
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "baidunetdisk",
		Description: "Baidu Netdisk",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Aliases:     []string{"baidudrive"},
		Options: []fs.Option{{
			Name:      "refresh_token",
			Help:      "OAuth Refresh Token",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "client_id",
			Help:     "OAuth Client ID",
			Default:  defaultClientID,
			Advanced: true,
		}, {
			Name:      "client_secret",
			Help:      "OAuth Client Secret",
			Default:   defaultClientSecret,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:    "download_api",
			Help:    "Download API method to use (official or crack)",
			Default: "official",
			Examples: []fs.OptionExample{{
				Value: "official",
				Help:  "Official open API (uses dlink with pan.baidu.com UA)",
			}, {
				Value: "crack",
				Help:  "DLNA crack API (bypasses speed throttle on some accounts)",
			}},
		}, {
			Name:     "custom_crack_ua",
			Help:     "User-Agent used in crack download mode",
			Default:  "netdisk",
			Advanced: true,
		}, {
			Name:     "root_folder_path",
			Help:     "Root folder path",
			Default:  "/",
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeBackSlash |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodeColon |
				encoder.EncodeQuestion |
				encoder.EncodeAsterisk |
				encoder.EncodePipe |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines configuration options
type Options struct {
	RefreshToken   string               `config:"refresh_token"`
	ClientID       string               `config:"client_id"`
	ClientSecret   string               `config:"client_secret"`
	DownloadAPI    string               `config:"download_api"`
	CustomCrackUA  string               `config:"custom_crack_ua"`
	RootFolderPath string               `config:"root_folder_path"`
	Enc            encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a Baidu Netdisk remote
type Fs struct {
	name      string
	root      string
	opt       Options
	m         configmap.Mapper
	features  *fs.Features
	srv       *rest.Client
	oauthSrv  *rest.Client
	httpCl    *http.Client
	noRedirCl *http.Client
	pacer     *fs.Pacer
	vipType   int   // 0 normal user, 1 vip, 2 svip
	uk        int64 // user ID of the account

	tokenMu      sync.Mutex // protects accessToken and opt.RefreshToken
	accessToken  string
	refreshToken string
}

// Object represents a file on Baidu Netdisk
type Object struct {
	fs      *Fs
	remote  string
	fsID    int64
	size    int64
	modTime time.Time
	md5     string
}

// ------------------------------------------------------------
// Helpers and Token Refresh
// ------------------------------------------------------------

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("error reading error out of body: %w", err)
	}
	apiErr := new(api.Error)
	if json.Unmarshal(body, apiErr) == nil && apiErr.Failed() {
		return apiErr
	}
	tokenErr := new(api.TokenErrResp)
	if json.Unmarshal(body, tokenErr) == nil && tokenErr.Err != "" {
		return tokenErr
	}
	return fmt.Errorf("HTTP error %v (%v): %s", resp.StatusCode, resp.Status, strings.TrimSpace(string(body)))
}

// refreshAccessToken gets a new access token with the refresh token.
//
// If staleToken is not empty and the access token has already been
// changed from it by another goroutine, nothing is done.
func (f *Fs) refreshAccessToken(ctx context.Context, staleToken string) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if staleToken != "" && staleToken != f.accessToken {
		return nil
	}

	params := url.Values{}
	params.Set("grant_type", "refresh_token")
	params.Set("refresh_token", f.refreshToken)
	params.Set("client_id", f.opt.ClientID)
	params.Set("client_secret", f.opt.ClientSecret)

	opts := rest.Opts{
		Method:     http.MethodGet,
		RootURL:    oauthTokenURL,
		Parameters: params,
	}
	var tokenResp api.TokenResp
	_, err := f.oauthSrv.CallJSON(ctx, &opts, nil, &tokenResp)
	if err != nil {
		return fmt.Errorf("baidunetdisk: token refresh failed: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return errors.New("baidunetdisk: empty access token returned")
	}

	f.accessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" && tokenResp.RefreshToken != f.refreshToken {
		f.refreshToken = tokenResp.RefreshToken
		// m.Set persists the value to the config file
		f.m.Set("refresh_token", tokenResp.RefreshToken)
	}
	return nil
}

func (f *Fs) getAccessToken() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	return f.accessToken
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It refreshes the access token if it was
// rejected. token is the access token used for the request.
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error, token string) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		if apiErr.IsTokenError() {
			fs.Debugf(f, "Access token rejected (%v) - refreshing", apiErr.Code())
			if refreshErr := f.refreshAccessToken(ctx, token); refreshErr != nil {
				return false, refreshErr
			}
			return true, err
		}
		if apiErr.Code() == api.ErrnoRateLimited {
			return true, err
		}
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// callOnce makes a single API call with the given access token and
// decodes the response into result, returning an *api.Error if the
// response reports one.
func (f *Fs) callOnce(ctx context.Context, opts *rest.Opts, token string, result any) (*http.Response, error) {
	if opts.Parameters == nil {
		opts.Parameters = url.Values{}
	}
	opts.Parameters.Set("access_token", token)

	var raw json.RawMessage
	resp, err := f.srv.CallJSON(ctx, opts, nil, &raw)
	if err != nil {
		return resp, err
	}
	var apiErr api.Error
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &apiErr); err != nil {
			return resp, fmt.Errorf("baidunetdisk: failed to decode response: %w", err)
		}
	}
	if apiErr.Failed() {
		return resp, &apiErr
	}
	if result != nil && len(raw) > 0 {
		if err = json.Unmarshal(raw, result); err != nil {
			return resp, fmt.Errorf("baidunetdisk: failed to decode response: %w", err)
		}
	}
	return resp, nil
}

// call makes an API call under the pacer. If form is not nil it is
// sent as an application/x-www-form-urlencoded body.
func (f *Fs) call(ctx context.Context, opts *rest.Opts, form url.Values, result any) (resp *http.Response, err error) {
	err = f.pacer.Call(func() (bool, error) {
		if form != nil {
			opts.Body = strings.NewReader(form.Encode())
			opts.ContentType = "application/x-www-form-urlencoded"
		}
		token := f.getAccessToken()
		resp, err = f.callOnce(ctx, opts, token, result)
		return f.shouldRetry(ctx, resp, err, token)
	})
	return resp, err
}

// get makes a GET request to the API
func (f *Fs) get(ctx context.Context, path string, params url.Values, result any) (*http.Response, error) {
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       path,
		Parameters: params,
	}
	return f.call(ctx, &opts, nil, result)
}

// postForm makes a form POST request to the API
func (f *Fs) postForm(ctx context.Context, path string, params url.Values, form url.Values, result any) (*http.Response, error) {
	opts := rest.Opts{
		Method:     http.MethodPost,
		Path:       path,
		Parameters: params,
	}
	return f.call(ctx, &opts, form, result)
}

// isAPIError returns true if err is an API error with the code given
func isAPIError(err error, code int) bool {
	var apiErr *api.Error
	return errors.As(err, &apiErr) && apiErr.Code() == code
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

	if opt.ClientID == "" {
		opt.ClientID = defaultClientID
	}
	if opt.ClientSecret == "" {
		opt.ClientSecret = defaultClientSecret
	}
	if opt.RootFolderPath == "" {
		opt.RootFolderPath = "/"
	}

	httpCl := fshttp.NewClient(ctx)
	noRedirClient := fshttp.NewClient(ctx)
	noRedirClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	f := &Fs{
		name:         name,
		root:         strings.Trim(root, "/"),
		opt:          *opt,
		m:            m,
		srv:          rest.NewClient(httpCl).SetRoot(apiBaseURL).SetErrorHandler(errorHandler),
		oauthSrv:     rest.NewClient(httpCl).SetErrorHandler(errorHandler),
		httpCl:       httpCl,
		noRedirCl:    noRedirClient,
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		refreshToken: opt.RefreshToken,
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements: Baidu throttles parallel
		// downloads, harder for accounts without membership.
		MaxFileStreams: 2,
		MaxStreams:     4,
	}).Fill(ctx, f)

	// Initial token refresh
	err = f.pacer.Call(func() (bool, error) {
		err := f.refreshAccessToken(ctx, "")
		if fserrors.ContextError(ctx, &err) {
			return false, err
		}
		return fserrors.ShouldRetry(err), err
	})
	if err != nil {
		return nil, fmt.Errorf("baidunetdisk: initial authentication failed: %w", err)
	}

	// The membership level decides the upload chunk size
	var uinfo api.UserInfoResp
	_, err = f.get(ctx, "/xpan/nas", url.Values{"method": {"uinfo"}}, &uinfo)
	if err != nil {
		return nil, fmt.Errorf("baidunetdisk: failed to read user info: %w", err)
	}
	f.vipType = uinfo.VipType
	f.uk = uinfo.Uk

	// Check whether the root is a file
	if f.root != "" {
		oldRoot := f.root
		newRoot, leaf := dircache.SplitPath(f.root)
		f.root = newRoot
		_, err := f.NewObject(ctx, leaf)
		if err == nil {
			return f, fs.ErrorIsFile
		}
		f.root = oldRoot
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, err
		}
	}

	return f, nil
}

// fullPath returns the absolute server path of remote
func (f *Fs) fullPath(remote string) string {
	return stdpath.Join("/", f.opt.RootFolderPath, f.opt.Enc.FromStandardPath(stdpath.Join(f.root, remote)))
}

// Name returns remote name
func (f *Fs) Name() string { return f.name }

// Root returns root path
func (f *Fs) Root() string { return f.root }

// String returns remote string
func (f *Fs) String() string { return fmt.Sprintf("Baidu Netdisk root '%s'", f.root) }

// Precision returns precision of modtime
//
// The local_mtime sent on upload is stored, but the API can't change
// it afterwards, so modification times are not supported.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns supported hashes (MD5)
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

// Features returns features
func (f *Fs) Features() *fs.Features { return f.features }

// listPage lists one page of dirPath. A missing directory returns
// fs.ErrorDirNotFound.
func (f *Fs) listPage(ctx context.Context, dirPath string, start, limit int) ([]api.File, error) {
	params := url.Values{}
	params.Set("method", "list")
	params.Set("dir", dirPath)
	params.Set("web", "web")
	params.Set("start", strconv.Itoa(start))
	params.Set("limit", strconv.Itoa(limit))

	var resp api.ListResp
	_, err := f.get(ctx, "/xpan/file", params, &resp)
	if err != nil {
		if isAPIError(err, api.ErrnoNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	return resp.List, nil
}

func (f *Fs) listFiles(ctx context.Context, dirPath string) ([]api.File, error) {
	var files []api.File
	const limit = 1000
	for start := 0; ; start += limit {
		page, err := f.listPage(ctx, dirPath, start, limit)
		if err != nil {
			return nil, err
		}
		files = append(files, page...)
		if len(page) < limit {
			break
		}
	}
	return files, nil
}

func (f *Fs) newObjectWithInfo(remote string, item *api.File) *Object {
	return &Object{
		fs:      f,
		remote:  remote,
		fsID:    item.FsID,
		size:    item.Size,
		modTime: item.ModTime(),
		md5:     api.DecryptMd5(item.Md5),
	}
}

// List lists objects in dir
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirPath := f.fullPath(dir)
	items, err := f.listFiles(ctx, dirPath)
	if err != nil {
		return nil, err
	}

	for i := range items {
		item := &items[i]
		remote := stdpath.Join(dir, f.opt.Enc.ToStandardName(item.ServerFilename))
		if item.IsDir() {
			d := fs.NewDir(remote, item.ModTime()).SetID(strconv.FormatInt(item.FsID, 10))
			entries = append(entries, d)
		} else {
			entries = append(entries, f.newObjectWithInfo(remote, item))
		}
	}
	return entries, nil
}

// NewObject finds object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fullPath := f.fullPath(remote)
	parent := stdpath.Dir(fullPath)
	leaf := stdpath.Base(fullPath)

	items, err := f.listFiles(ctx, parent)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	for i := range items {
		item := &items[i]
		if !item.IsDir() && item.ServerFilename == leaf {
			return f.newObjectWithInfo(remote, item), nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// Put uploads an object
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: src.Remote(),
		size:   src.Size(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates dir
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dirPath := f.fullPath(dir)
	if dirPath == "/" {
		return nil
	}
	params := url.Values{"method": {"create"}}
	form := url.Values{
		"path":  {dirPath},
		"size":  {"0"},
		"isdir": {"1"},
		"rtype": {"0"}, // don't rename, report the conflict
	}
	_, err := f.postForm(ctx, "/xpan/file", params, form, nil)
	if isAPIError(err, api.ErrnoAlreadyExists) {
		return nil
	}
	return err
}

func (f *Fs) fileManager(ctx context.Context, opera, ondup string, fileList any) error {
	params := url.Values{
		"method": {"filemanager"},
		"opera":  {opera},
	}
	data, err := json.Marshal(fileList)
	if err != nil {
		return err
	}
	form := url.Values{
		"async":    {"0"},
		"filelist": {string(data)},
	}
	if ondup != "" {
		form.Set("ondup", ondup)
	}
	_, err = f.postForm(ctx, "/xpan/file", params, form, nil)
	return err
}

// Rmdir removes dir if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirPath := f.fullPath(dir)
	items, err := f.listPage(ctx, dirPath, 0, 1)
	if err != nil {
		return err
	}
	if len(items) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if dirPath == "/" {
		return nil
	}
	return f.fileManager(ctx, "delete", "", []string{dirPath})
}

// Purge removes dir and all its contents
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirPath := f.fullPath(dir)
	if dirPath == "/" {
		return errors.New("baidunetdisk: can't purge the root directory")
	}
	if _, err := f.listPage(ctx, dirPath, 0, 1); err != nil {
		return err
	}
	return f.fileManager(ctx, "delete", "", []string{dirPath})
}

// copyOrMove copies or moves srcObj to remote, overwriting any existing file
func (f *Fs) copyOrMove(ctx context.Context, opera string, srcObj *Object, remote string) (fs.Object, error) {
	srcPath := srcObj.fs.fullPath(srcObj.remote)
	dstPath := f.fullPath(remote)
	data := []map[string]string{{
		"path":    srcPath,
		"dest":    stdpath.Dir(dstPath),
		"newname": stdpath.Base(dstPath),
	}}
	if err := f.fileManager(ctx, opera, "overwrite", data); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

// Copy copies a file
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || srcObj.fs.uk != f.uk {
		return nil, fs.ErrorCantCopy
	}
	return f.copyOrMove(ctx, "copy", srcObj, remote)
}

// Move moves a file
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || srcObj.fs.uk != f.uk {
		return nil, fs.ErrorCantMove
	}
	return f.copyOrMove(ctx, "move", srcObj, remote)
}

// DirMove moves a directory
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || srcFs.uk != f.uk {
		return fs.ErrorCantDirMove
	}

	srcPath := srcFs.fullPath(srcRemote)
	dstPath := f.fullPath(dstRemote)

	// The destination must not exist
	if _, err := f.listPage(ctx, dstPath, 0, 1); err == nil {
		return fs.ErrorDirExists
	} else if !errors.Is(err, fs.ErrorDirNotFound) {
		return err
	}

	// Make sure the destination parent exists
	parent := stdpath.Dir(dstRemote)
	if parent == "." {
		parent = ""
	}
	if err := f.Mkdir(ctx, parent); err != nil {
		return err
	}

	data := []map[string]string{{
		"path":    srcPath,
		"dest":    stdpath.Dir(dstPath),
		"newname": stdpath.Base(dstPath),
	}}
	return f.fileManager(ctx, "move", "fail", data)
}

// About gets quota and usage information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	opts := rest.Opts{
		Method:  http.MethodGet,
		RootURL: "https://pan.baidu.com/api/quota",
	}
	var resp api.QuotaResp
	_, err := f.call(ctx, &opts, nil, &resp)
	if err != nil {
		return nil, err
	}

	usage := &fs.Usage{
		Total: fs.NewUsageValue(resp.Total),
		Used:  fs.NewUsageValue(resp.Used),
		Free:  fs.NewUsageValue(resp.Total - resp.Used),
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
	if ty != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5, nil
}

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.fileManager(ctx, "delete", "", []string{o.fs.fullPath(o.remote)})
}

// downloadLink returns the download link and User-Agent to use for the object
func (o *Object) downloadLink(ctx context.Context) (dlink, ua string, err error) {
	if o.fs.opt.DownloadAPI == "crack" {
		target, err := json.Marshal([]string{o.fs.fullPath(o.remote)})
		if err != nil {
			return "", "", err
		}
		params := url.Values{}
		params.Set("target", string(target))
		params.Set("dlink", "1")
		params.Set("web", "5")
		params.Set("origin", "dlna")

		opts := rest.Opts{
			Method:     http.MethodGet,
			RootURL:    "https://pan.baidu.com/api/filemetas",
			Parameters: params,
		}
		var crackResp api.CrackDownloadResp
		if _, err = o.fs.call(ctx, &opts, nil, &crackResp); err != nil {
			return "", "", fmt.Errorf("baidunetdisk: get crack dlink failed: %w", err)
		}
		if len(crackResp.Info) == 0 || crackResp.Info[0].Dlink == "" {
			return "", "", fmt.Errorf("baidunetdisk: empty crack dlink for %s", o.remote)
		}
		ua = o.fs.opt.CustomCrackUA
		if ua == "" {
			ua = "netdisk"
		}
		return crackResp.Info[0].Dlink, ua, nil
	}

	params := url.Values{}
	params.Set("method", "filemetas")
	params.Set("fsids", fmt.Sprintf("[%d]", o.fsID))
	params.Set("dlink", "1")
	var downResp api.DownloadResp
	if _, err = o.fs.get(ctx, "/xpan/multimedia", params, &downResp); err != nil {
		return "", "", fmt.Errorf("baidunetdisk: get dlink failed: %w", err)
	}
	if len(downResp.List) == 0 || downResp.List[0].Dlink == "" {
		return "", "", fmt.Errorf("baidunetdisk: empty dlink for %s", o.remote)
	}
	return downResp.List[0].Dlink, "pan.baidu.com", nil
}

// statusError is a download request answered with an HTTP error
// status. It carries the status so that the pacer and linkcache.Refused
// see it.
type statusError struct {
	op     string
	status int
}

// Error implements error
func (e *statusError) Error() string {
	return fmt.Sprintf("baidunetdisk: %s: bad status code %d", e.op, e.status)
}

// HTTPStatusCode returns the HTTP status of the answer
func (e *statusError) HTTPStatusCode() int { return e.status }

// Open opens the file for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	key := linkcache.Key(o.fs.name, strconv.FormatInt(o.fsID, 10), strconv.FormatInt(o.size, 10), o.md5)
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the dlink of the object and resolves the redirect
// it answers with to the storage's URL, which becomes the link.
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	dlink, ua, err := o.downloadLink(ctx)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(dlink)
	if err != nil {
		return nil, fmt.Errorf("baidunetdisk: bad dlink: %w", err)
	}
	q := u.Query()
	q.Set("access_token", o.fs.getAccessToken())
	u.RawQuery = q.Encode()
	dlinkWithToken := u.String()

	// Resolve the redirect location
	finalURL := dlinkWithToken
	err = o.fs.pacer.Call(func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, dlinkWithToken, nil)
		if err != nil {
			return false, err
		}
		req.Header.Set("User-Agent", ua)
		resp, err := o.fs.noRedirCl.Do(req)
		if err != nil {
			return o.fs.shouldRetry(ctx, nil, err, "")
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			return o.fs.shouldRetry(ctx, resp, &statusError{op: "resolve dlink", status: resp.StatusCode}, "")
		}
		if location, err := resp.Location(); err == nil {
			finalURL = location.String()
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return &linkcache.Link{URL: finalURL, Header: http.Header{"User-Agent": {ua}}}, nil
}

// openLink reads the object from link
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
		if err != nil {
			return o.fs.shouldRetry(ctx, nil, err, "")
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			failed := resp
			resp = nil
			return o.fs.shouldRetry(ctx, failed, &statusError{op: "download", status: failed.StatusCode}, "")
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// chunkSize returns the upload block size for the account's membership level
func (f *Fs) chunkSize() int64 {
	switch f.vipType {
	case 1:
		return int64(vipChunkSize)
	case 2:
		return int64(svipChunkSize)
	}
	return int64(defaultChunkSize)
}

// uploadHashes holds the hashes precreate needs
type uploadHashes struct {
	size       int64
	blockList  []string // MD5 of each chunkSize block
	contentMD5 string   // MD5 of the whole content
	sliceMD5   string   // MD5 of the first sliceMD5Size bytes
}

// limitedWriter writes at most n bytes to w and discards the rest
type limitedWriter struct {
	w io.Writer
	n int64
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n > 0 {
		q := p
		if int64(len(q)) > l.n {
			q = q[:l.n]
		}
		l.n -= int64(len(q))
		if _, err := l.w.Write(q); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// hashBlocks reads in to EOF and computes the upload hashes, copying
// the data to out if it is not nil.
func hashBlocks(in io.Reader, chunkSize int64, out io.Writer) (*uploadHashes, error) {
	var (
		h       = new(uploadHashes)
		content = md5.New()
		block   = md5.New()
		slice   = md5.New()
		writers = []io.Writer{content, block, &limitedWriter{w: slice, n: sliceMD5Size}}
	)
	if out != nil {
		writers = append(writers, out)
	}
	w := io.MultiWriter(writers...)
	for {
		block.Reset()
		n, err := io.CopyN(w, in, chunkSize)
		if n > 0 {
			h.size += n
			h.blockList = append(h.blockList, hex.EncodeToString(block.Sum(nil)))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	h.contentMD5 = hex.EncodeToString(content.Sum(nil))
	h.sliceMD5 = hex.EncodeToString(slice.Sum(nil))
	return h, nil
}

// uploadBlock uploads one block of an upload session
func (f *Fs) uploadBlock(ctx context.Context, fullPath, uploadID string, partseq int, chunk []byte) error {
	params := url.Values{}
	params.Set("method", "upload")
	params.Set("type", "tmpfile")
	params.Set("path", fullPath)
	params.Set("uploadid", uploadID)
	params.Set("partseq", strconv.Itoa(partseq))
	return f.pacer.Call(func() (bool, error) {
		opts := rest.Opts{
			Method:               http.MethodPost,
			RootURL:              uploadAPI,
			Path:                 "/rest/2.0/pcs/superfile2",
			Parameters:           params,
			Body:                 bytes.NewReader(chunk),
			MultipartParams:      url.Values{},
			MultipartContentName: "file",
			MultipartFileName:    stdpath.Base(fullPath),
			ExtraHeaders:         map[string]string{"User-Agent": "pan.baidu.com"},
		}
		token := f.getAccessToken()
		resp, err := f.callOnce(ctx, &opts, token, nil)
		return f.shouldRetry(ctx, resp, err, token)
	})
}

// precreate starts an upload. A ReturnType of 2 means the content was
// already on the server and the file has been created (rapid upload).
func (f *Fs) precreate(ctx context.Context, fullPath string, h *uploadHashes, blockListJSON string, mtime int64) (*api.PrecreateResp, error) {
	params := url.Values{"method": {"precreate"}}
	form := url.Values{
		"path":        {fullPath},
		"size":        {strconv.FormatInt(h.size, 10)},
		"isdir":       {"0"},
		"autoinit":    {"1"},
		"rtype":       {"3"}, // overwrite
		"block_list":  {blockListJSON},
		"content-md5": {h.contentMD5},
		"slice-md5":   {h.sliceMD5},
		"local_ctime": {strconv.FormatInt(mtime, 10)},
		"local_mtime": {strconv.FormatInt(mtime, 10)},
	}
	var resp api.PrecreateResp
	if _, err := f.postForm(ctx, "/xpan/file", params, form, &resp); err != nil {
		return nil, fmt.Errorf("baidunetdisk: precreate failed: %w", err)
	}
	return &resp, nil
}

// Update uploads new content
//
// Baidu needs the MD5 of every block before the upload starts, so the
// data is read twice: if the source is a local file it is opened again
// to compute the hashes, otherwise it is spooled to a temporary file.
// Only one block is held in memory at a time.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	f := o.fs
	size := src.Size()
	if size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}
	chunkSize := f.chunkSize()
	if size > 0 && size > maxChunks*chunkSize {
		return fmt.Errorf("baidunetdisk: file too large (%d bytes) for the account's upload chunk size %d", size, chunkSize)
	}

	var (
		hashes *uploadHashes
		data   io.Reader
	)
	if srcObj := fs.UnWrapObjectInfo(src); size > 0 && srcObj != nil && srcObj.Fs().Features().IsLocal {
		// Hash a second reader of the local file and upload from in
		rc, err := srcObj.Open(ctx)
		if err != nil {
			return fmt.Errorf("baidunetdisk: failed to open source for hashing: %w", err)
		}
		hashes, err = hashBlocks(rc, chunkSize, nil)
		closeErr := rc.Close()
		if err != nil {
			return fmt.Errorf("baidunetdisk: failed to hash source: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("baidunetdisk: failed to close source: %w", closeErr)
		}
		data = in
	} else {
		tmpFile, err := os.CreateTemp("", "rclone-baidunetdisk-*")
		if err != nil {
			return fmt.Errorf("baidunetdisk: failed to create temporary file: %w", err)
		}
		defer func() {
			_ = tmpFile.Close()
			if removeErr := os.Remove(tmpFile.Name()); removeErr != nil {
				fs.Errorf(o, "Failed to remove temporary file: %v", removeErr)
			}
		}()
		hashes, err = hashBlocks(in, chunkSize, tmpFile)
		if err != nil {
			return fmt.Errorf("baidunetdisk: failed to spool upload to temporary file: %w", err)
		}
		if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("baidunetdisk: failed to rewind temporary file: %w", err)
		}
		data = tmpFile
	}
	if size >= 0 && hashes.size != size {
		return fmt.Errorf("baidunetdisk: source size changed: read %d bytes, expected %d", hashes.size, size)
	}
	if hashes.size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}
	if len(hashes.blockList) > maxChunks {
		return fmt.Errorf("baidunetdisk: file too large (%d bytes) for the account's upload chunk size %d", hashes.size, chunkSize)
	}

	fullPath := f.fullPath(o.remote)
	mtime := src.ModTime(ctx).Unix()
	blockListJSONBytes, err := json.Marshal(hashes.blockList)
	if err != nil {
		return err
	}
	blockListJSON := string(blockListJSONBytes)

	// 1. Precreate - this does a rapid upload if the content is known
	pre, err := f.precreate(ctx, fullPath, hashes, blockListJSON, mtime)
	if err != nil {
		return err
	}

	if pre.ReturnType == 2 {
		fs.Debugf(o, "Rapid upload succeeded")
	} else {
		// 2. Upload the blocks the server asked for
		needed := make(map[int]bool, len(pre.BlockList))
		for _, partseq := range pre.BlockList {
			needed[partseq] = true
		}
		buf := make([]byte, min(chunkSize, hashes.size))
		for i, wantMD5 := range hashes.blockList {
			n := min(chunkSize, hashes.size-int64(i)*chunkSize)
			chunk := buf[:n]
			if _, err := io.ReadFull(data, chunk); err != nil {
				return fmt.Errorf("baidunetdisk: failed to read block %d: %w", i, err)
			}
			if !needed[i] {
				continue
			}
			sum := md5.Sum(chunk)
			if hex.EncodeToString(sum[:]) != wantMD5 {
				return fmt.Errorf("baidunetdisk: source changed while uploading block %d", i)
			}
			if err := f.uploadBlock(ctx, fullPath, pre.UploadID, i, chunk); err != nil {
				return fmt.Errorf("baidunetdisk: upload block %d failed: %w", i, err)
			}
		}

		// 3. Create the file from the blocks
		params := url.Values{"method": {"create"}}
		form := url.Values{
			"path":        {fullPath},
			"size":        {strconv.FormatInt(hashes.size, 10)},
			"isdir":       {"0"},
			"rtype":       {"3"}, // overwrite
			"uploadid":    {pre.UploadID},
			"block_list":  {blockListJSON},
			"local_ctime": {strconv.FormatInt(mtime, 10)},
			"local_mtime": {strconv.FormatInt(mtime, 10)},
		}
		var createResp api.File
		if _, err = f.postForm(ctx, "/xpan/file", params, form, &createResp); err != nil {
			return fmt.Errorf("baidunetdisk: create file failed: %w", err)
		}
	}

	fresh, err := f.NewObject(ctx, o.remote)
	if err != nil {
		return fmt.Errorf("baidunetdisk: failed to read uploaded file: %w", err)
	}
	*o = *fresh.(*Object)
	return nil
}

// Command runs named commands on the backend
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "rapid-upload", "rapidupload":
		var fileName, md5Str, sliceMD5 string
		var fileSize int64

		if len(arg) == 1 {
			// BaiduPCS format: md5#slicemd5#size#filename
			// Or bdlink: bdlink://...
			fileName, fileSize, md5Str, sliceMD5, err = parseBaiduLink(arg[0])
			if err != nil {
				return nil, err
			}
		} else if len(arg) >= 4 {
			// Args: [filename, size, md5, slice_md5]
			fileName = arg[0]
			fileSize, err = strconv.ParseInt(arg[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid size %q: %w", arg[1], err)
			}
			md5Str = arg[2]
			sliceMD5 = arg[3]
		} else {
			if n, ok := opt["name"]; ok {
				fileName = n
			}
			if s, ok := opt["size"]; ok {
				fileSize, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid size %q: %w", s, err)
				}
			}
			if m, ok := opt["md5"]; ok {
				md5Str = m
			}
			if sm, ok := opt["slice_md5"]; ok {
				sliceMD5 = sm
			}
		}

		if fileName == "" || fileSize <= 0 || md5Str == "" {
			return nil, errors.New("rapid-upload requires filename, size, and md5 (e.g. 'rclone backend rapid-upload baidunetdisk:path md5#slicemd5#size#filename')")
		}

		targetDir := ""
		if p, ok := opt["path"]; ok {
			targetDir = p
		}
		fullPath := f.fullPath(stdpath.Join(targetDir, fileName))

		preParams := url.Values{"method": []string{"precreate"}}
		blockListJSON, _ := json.Marshal([]string{md5Str})
		preForm := url.Values{
			"path":        []string{fullPath},
			"size":        []string{strconv.FormatInt(fileSize, 10)},
			"isdir":       []string{"0"},
			"autoinit":    []string{"1"},
			"rtype":       []string{"3"},
			"block_list":  []string{string(blockListJSON)},
			"content-md5": []string{md5Str},
		}
		if sliceMD5 != "" {
			preForm.Set("slice-md5", sliceMD5)
		}

		var preResp api.PrecreateResp
		if _, err := f.postForm(ctx, "/xpan/file", preParams, preForm, &preResp); err != nil {
			return nil, fmt.Errorf("baidunetdisk: precreate failed: %w", err)
		}

		if preResp.ReturnType != 2 {
			return map[string]any{
				"status":  "failed",
				"message": "file not found in Baidu Netdisk server library or requires block upload",
			}, nil
		}

		// Create file
		createParams := url.Values{"method": []string{"create"}}
		now := time.Now().Unix()
		createForm := url.Values{
			"path":        []string{fullPath},
			"size":        []string{strconv.FormatInt(fileSize, 10)},
			"isdir":       []string{"0"},
			"rtype":       []string{"3"},
			"uploadid":    []string{preResp.UploadID},
			"block_list":  []string{string(blockListJSON)},
			"local_ctime": []string{strconv.FormatInt(now, 10)},
			"local_mtime": []string{strconv.FormatInt(now, 10)},
		}
		var createResp api.File
		if _, err := f.postForm(ctx, "/xpan/file", createParams, createForm, &createResp); err != nil {
			return nil, fmt.Errorf("baidunetdisk: create file failed: %w", err)
		}

		return map[string]any{
			"status": "success",
			"fs_id":  createResp.FsID,
			"path":   fullPath,
			"size":   fileSize,
			"md5":    md5Str,
		}, nil

	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// parseBaiduLink parses standard Baidu rapid upload format (md5#slicemd5#size#filename)
func parseBaiduLink(raw string) (name string, size int64, md5Str string, sliceMD5 string, err error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "bdlink://")
	// If base64 encoded, decode it
	if strings.HasPrefix(raw, "BaiduPCS-Go") || (!strings.Contains(raw, "#") && len(raw) > 20) {
		decoded, errB64 := base64.StdEncoding.DecodeString(raw)
		if errB64 == nil && len(decoded) > 0 {
			raw = string(decoded)
		}
	}
	parts := strings.Split(raw, "#")
	if len(parts) >= 4 {
		md5Str = parts[0]
		sliceMD5 = parts[1]
		size, err = strconv.ParseInt(parts[2], 10, 64)
		name = parts[3]
		return name, size, md5Str, sliceMD5, err
	}
	return "", 0, "", "", errors.New("invalid Baidu rapid-upload link format: expected 'md5#slicemd5#size#filename'")
}

// Check interfaces
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.Purger    = (*Fs)(nil)
	_ fs.Copier    = (*Fs)(nil)
	_ fs.Mover     = (*Fs)(nil)
	_ fs.DirMover  = (*Fs)(nil)
	_ fs.Commander = (*Fs)(nil)
	_ fs.Abouter   = (*Fs)(nil)
	_ fs.Object    = (*Object)(nil)
)

var commandHelp = []fs.CommandHelp{{
	Name:  "rapid-upload",
	Short: "Create a file from its hashes without uploading data.",
	Long: `This command asks Baidu Netdisk to create a file from content it
already stores. It is also available as "rapidupload".

Usage examples:

` + "```console" + `
rclone backend rapid-upload baidunetdisk:dir "md5#slicemd5#size#name"
rclone backend rapid-upload baidunetdisk:dir name size md5 slice_md5
` + "```" + `

slice_md5 is the MD5 of the first 256 KiB.`,
	Opts: map[string]string{
		"path":      "Directory to create the file in, relative to the remote.",
		"name":      "File name.",
		"size":      "File size in bytes.",
		"md5":       "MD5 of the whole file.",
		"slice_md5": "MD5 of the first 256 KiB.",
	},
}}
