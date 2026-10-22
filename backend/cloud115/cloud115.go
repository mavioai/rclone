// Package cloud115 provides a filesystem interface to 115 Cloud (115 Drive).
package cloud115

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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
	qrcode "github.com/skip2/go-qrcode"
)

const (
	// minSleep keeps to two API calls a second, OpenList's default:
	// 115's firewall blocks clients that call more often for a while,
	// answering 405 with an HTML page.
	minSleep         = 500 * time.Millisecond
	maxSleep         = 2 * time.Second
	decayConstant    = 2
	listLimit        = driver115.MaxDirPageLimit
	minChunkSize     = 100 * fs.Kibi
	defaultChunkSize = 16 * fs.Mebi
	maxUploadParts   = 10000
	// blockedWait is how long a call blocked by the firewall waits
	// before it is retried.
	blockedWait = 10 * time.Second

	// linkLifetime is how long a download link is kept when its URL
	// states no expiry. 115's links carry it in their t parameter,
	// which linkcache reads; this is a conservative fallback.
	linkLifetime = 10 * time.Minute
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cloud115",
		Description: "115 Cloud / 115 Drive",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Config:      Config,
		Aliases:     []string{"115", "115drive"},
		Options: []fs.Option{{
			Name: "cookie",
			Help: `115 Cookie (UID=...; CID=...; SEID=...; KID=...).

Leave blank to log in by scanning a QR code with the 115 app
during "rclone config".`,
			Sensitive: true,
		}, {
			Name: "qrcode_token",
			Help: `UID of a QR code login session confirmed in the 115 app.

It is exchanged for a cookie on first use and then cleared.`,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name: "qrcode_source",
			Help: `Client device type to log in as with a QR code.

Logging in as a device type signs out other sessions of the same type.`,
			Default: "tv",
			Examples: []fs.OptionExample{{
				Value: "tv",
				Help:  "Smart TV client",
			}, {
				Value: "web",
				Help:  "Web browser client",
			}, {
				Value: "android",
				Help:  "Android app client",
			}, {
				Value: "ios",
				Help:  "iOS app client",
			}, {
				Value: "alipaymini",
				Help:  "Alipay Mini Program",
			}, {
				Value: "wechatmini",
				Help:  "WeChat Mini Program",
			}, {
				Value: "qandroid",
				Help:  "QAndroid client",
			}},
			Advanced: true,
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID",
			Default:  "0",
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: `Upload chunk size.

Files up to this size are uploaded in a single request, larger files
in parts of this size (raised automatically to stay within 10,000
parts). Each transfer buffers one chunk in memory.`,
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     "The encoding for the backend.\n\nSee the [encoding section in the overview](/overview/#encoding) for more info.",
			Advanced: true,
			Default: encoder.EncodeCtl |
				encoder.EncodeDot |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeSlash |
				encoder.EncodeBackSlash |
				encoder.EncodeDoubleQuote |
				encoder.EncodeAsterisk |
				encoder.EncodeColon |
				encoder.EncodeLtGt |
				encoder.EncodeQuestion |
				encoder.EncodePipe |
				encoder.EncodeInvalidUtf8,
		}},
	})
}

// Options defines configuration options for 115 Cloud
type Options struct {
	Cookie       string               `config:"cookie"`
	QRCodeToken  string               `config:"qrcode_token"`
	QRCodeSource string               `config:"qrcode_source"`
	RootFolderID string               `config:"root_folder_id"`
	ChunkSize    fs.SizeSuffix        `config:"chunk_size"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a 115 Cloud remote
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	client   *driver115.Pan115Client
	dirCache *dircache.DirCache
	pacer    *fs.Pacer
	srv      *rest.Client // client for download URLs
	version  string       // version of 115's browser the client claims
}

// Object represents a file on 115 Cloud
type Object struct {
	fs       *Fs
	remote   string
	id       string
	size     int64
	modTime  time.Time
	sha1     string
	pickCode string
}

// ------------------------------------------------------------
// Config and login
// ------------------------------------------------------------

// newClient makes a 115 API client using rclone's HTTP transport,
// claiming to be 115's browser of version
func newClient(ctx context.Context, version string) (*driver115.Pan115Client, *http.Client) {
	// The transport forces the user agent, so set it to the one 115
	// expects for the API and the download URLs it hands out.
	userAgent := userAgentOf(version)
	newCtx, ci := fs.AddConfig(ctx)
	ci.UserAgent = userAgent
	httpClient := fshttp.NewClient(newCtx)
	// The driver ignores the HTTP status of the API's answers, so the
	// API client's transport turns a status worth retrying into an
	// error carrying the response, which shouldRetry then sees.
	apiClient := *httpClient
	apiClient.Transport = &statusTransport{base: httpClient.Transport}
	return driver115.New(driver115.WithClient(&apiClient), driver115.UA(userAgent)), httpClient
}

// statusTransport fails the requests answered with a status in
// retryErrorCodes, or blocked by the firewall, with a *statusError.
type statusTransport struct {
	base http.RoundTripper
}

// RoundTrip implements http.RoundTripper
func (t *statusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil || !slices.Contains(retryErrorCodes, resp.StatusCode) && resp.StatusCode != http.StatusMethodNotAllowed {
		return resp, err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_ = resp.Body.Close()
	return nil, &statusError{resp: resp, body: strings.TrimSpace(string(body))}
}

// statusError is an answer of the 115 API with an HTTP error status.
type statusError struct {
	resp *http.Response // body closed
	body string         // the start of the body
}

// blocked reports whether 115's firewall blocked the request, as it
// does when the API is called too often: it answers 405 with an HTML page.
func (e *statusError) blocked() bool {
	return e.resp.StatusCode == http.StatusMethodNotAllowed
}

// Error implements error
func (e *statusError) Error() string {
	if e.blocked() {
		return fmt.Sprintf("115's firewall blocked the request as too frequent: HTTP error %s", e.resp.Status)
	}
	if e.body == "" {
		return fmt.Sprintf("HTTP error %s", e.resp.Status)
	}
	return fmt.Sprintf("HTTP error %s: %q", e.resp.Status, e.body)
}

// HTTPStatusCode returns the HTTP status of the answer
func (e *statusError) HTTPStatusCode() int { return e.resp.StatusCode }

// responseOf returns the HTTP response that err carries, or nil.
func responseOf(err error) *http.Response {
	var se *statusError
	if errors.As(err, &se) {
		return se.resp
	}
	return nil
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

// qrState is the QR code login session carried between config states
const qrStatePrefix = "qr_wait:"

// encodeQRState packs the login session into a config state
func encodeQRState(s *driver115.QRCodeSession) (string, error) {
	data, err := json.Marshal(driver115.QRCodeSession{UID: s.UID, Time: s.Time, Sign: s.Sign})
	if err != nil {
		return "", err
	}
	return qrStatePrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// decodeQRState unpacks the login session from a config state
func decodeQRState(state string) (*driver115.QRCodeSession, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(state, qrStatePrefix))
	if err != nil {
		return nil, fmt.Errorf("invalid QR code state: %w", err)
	}
	s := new(driver115.QRCodeSession)
	if err = json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("invalid QR code state: %w", err)
	}
	return s, nil
}

// Config runs the QR code login flow
func Config(ctx context.Context, name string, m configmap.Mapper, conf fs.ConfigIn) (*fs.ConfigOut, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	switch {
	case conf.State == "":
		return fs.ConfigConfirm("qr_start", opt.Cookie == "", "config_qr_login",
			"Log in by scanning a QR code with the 115 app?\n\nThis replaces any configured cookie.")
	case conf.State == "qr_start":
		if conf.Result == "false" {
			return nil, nil
		}
		client, _ := newClient(ctx, currentAppVersion(ctx))
		session, err := client.QRCodeStart()
		if err != nil {
			return nil, fmt.Errorf("failed to start QR code login: %w", err)
		}
		code, err := qrcode.New(session.QrcodeContent, qrcode.Low)
		if err != nil {
			return nil, fmt.Errorf("failed to render QR code: %w", err)
		}
		state, err := encodeQRState(session)
		if err != nil {
			return nil, err
		}
		return fs.ConfigConfirm(state, true, "config_qr_scanned",
			"Scan this QR code with the 115 app and confirm the login on the phone:\n\n"+
				code.ToSmallString(false)+"\nHave you confirmed the login?")
	case strings.HasPrefix(conf.State, qrStatePrefix):
		if conf.Result == "false" {
			return nil, errors.New("QR code login aborted")
		}
		session, err := decodeQRState(conf.State)
		if err != nil {
			return nil, err
		}
		client, _ := newClient(ctx, currentAppVersion(ctx))
		status, err := client.QRCodeStatus(session)
		if err != nil {
			return nil, fmt.Errorf("failed to check QR code status: %w", err)
		}
		switch {
		case status.IsAllowed():
		case status.IsExpired(), status.IsCanceled():
			return fs.ConfigError("qr_start", "The QR code expired or the login was cancelled, starting again.")
		default:
			return fs.ConfigConfirm(conf.State, true, "config_qr_scanned",
				"The login has not been confirmed in the 115 app yet. Have you confirmed it now?")
		}
		cr, err := client.QRCodeLoginWithApp(session, driver115.LoginApp(opt.QRCodeSource))
		if err != nil {
			return nil, fmt.Errorf("QR code login failed: %w", err)
		}
		client.ImportCredential(cr)
		if err = client.CookieCheck(); err != nil {
			return nil, fmt.Errorf("cookie check after QR code login failed: %w", err)
		}
		m.Set("cookie", cr.Cookie())
		m.Set("qrcode_token", "")
		return nil, nil
	}
	return nil, fmt.Errorf("unknown state %q", conf.State)
}

// login authenticates the client from the configured credentials
func (f *Fs) login(ctx context.Context, m configmap.Mapper) error {
	cr := &driver115.Credential{}
	switch {
	case f.opt.QRCodeToken != "":
		session := &driver115.QRCodeSession{UID: f.opt.QRCodeToken}
		err := f.call(ctx, func() (err error) {
			cr, err = f.client.QRCodeLoginWithApp(session, driver115.LoginApp(f.opt.QRCodeSource))
			return err
		})
		if err != nil {
			return fmt.Errorf("QR code login failed: %w", err)
		}
		f.opt.Cookie = cr.Cookie()
		f.opt.QRCodeToken = ""
		m.Set("cookie", f.opt.Cookie)
		m.Set("qrcode_token", "")
	case f.opt.Cookie != "":
		if err := cr.FromCookie(f.opt.Cookie); err != nil {
			return fmt.Errorf("invalid cookie: %w", err)
		}
	default:
		return errors.New("no cookie configured: run \"rclone config\" to log in with a QR code, or set cookie")
	}
	f.client.ImportCredential(cr)

	if err := f.call(ctx, f.client.CookieCheck); err != nil {
		return fmt.Errorf("cookie check failed: %w", err)
	}
	// Fetches the user ID and key that uploads need
	if err := f.call(ctx, f.client.GetUploadInfo); err != nil {
		return fmt.Errorf("failed to get upload info: %w", err)
	}
	return nil
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
	if opt.ChunkSize < minChunkSize {
		return nil, fmt.Errorf("chunk_size %v is less than %v", opt.ChunkSize, minChunkSize)
	}

	version := currentAppVersion(ctx)
	client, httpClient := newClient(ctx, version)
	f := &Fs{
		name:    name,
		root:    root,
		opt:     *opt,
		client:  client,
		pacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		srv:     rest.NewClient(httpClient),
		version: version,
	}
	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements: 115 throttles parallel
		// downloads of one file and of one account.
		MaxFileStreams: 2,
		MaxStreams:     4,
	}).Fill(ctx, f)

	if err = f.login(ctx, m); err != nil {
		return nil, fmt.Errorf("cloud115: %w", err)
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
	var se *statusError
	if errors.As(err, &se) && se.blocked() {
		return true, pacer.RetryAfterError(err, blockedWait)
	}
	var ossErr oss.ServiceError
	if errors.As(err, &ossErr) {
		return slices.Contains(retryErrorCodes, ossErr.StatusCode), err
	}
	return fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// call runs a 115 driver call through the pacer
func (f *Fs) call(ctx context.Context, fn func() error) error {
	return f.pacer.Call(func() (bool, error) {
		err := fn()
		return f.shouldRetry(ctx, responseOf(err), err)
	})
}

// Name returns the remote name
func (f *Fs) Name() string { return f.name }

// Root returns the root path
func (f *Fs) Root() string { return f.root }

// String returns the remote name and path
func (f *Fs) String() string { return fmt.Sprintf("115 Cloud root '%s'", f.root) }

// Precision returns the precision of modtime
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns supported hash algorithms
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.SHA1) }

// Features returns the optional features
func (f *Fs) Features() *fs.Features { return f.features }

// listAll lists every item in the directory with ID dirID
func (f *Fs) listAll(ctx context.Context, dirID string) (files []driver115.File, err error) {
	err = f.call(ctx, func() error {
		list, err := f.client.ListWithLimit(dirID, listLimit)
		if err != nil {
			return err
		}
		files = *list
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list directory %s: %w", dirID, err)
	}
	return files, nil
}

// newObjectWithInfo makes an Object from a listing item
func (f *Fs) newObjectWithInfo(remote string, file *driver115.File) *Object {
	return &Object{
		fs:       f,
		remote:   remote,
		id:       file.FileID,
		size:     file.Size,
		modTime:  file.UpdateTime,
		sha1:     strings.ToLower(file.Sha1),
		pickCode: file.PickCode,
	}
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	files, err := f.listAll(ctx, pathID)
	if err != nil {
		return "", false, err
	}
	for _, file := range files {
		if file.IsDirectory && f.opt.Enc.ToStandardName(file.Name) == leaf {
			return file.FileID, true, nil
		}
	}
	return "", false, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	err = f.call(ctx, func() (err error) {
		newID, err = f.client.Mkdir(pathID, f.opt.Enc.FromStandardName(leaf))
		return err
	})
	return newID, err
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	files, err := f.listAll(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for i := range files {
		file := &files[i]
		remote := path.Join(dir, f.opt.Enc.ToStandardName(file.Name))
		if file.IsDirectory {
			f.dirCache.Put(remote, file.FileID)
			entries = append(entries, fs.NewDir(remote, file.UpdateTime).SetID(file.FileID))
		} else {
			entries = append(entries, f.newObjectWithInfo(remote, file))
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
	files, err := f.listAll(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for i := range files {
		file := &files[i]
		if !file.IsDirectory && f.opt.Enc.ToStandardName(file.Name) == leaf {
			return f.newObjectWithInfo(remote, file), nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// Put uploads an object
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

// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// deleteDir deletes the directory with ID dirID and everything in it
func (f *Fs) deleteDir(ctx context.Context, dirID string) error {
	if dirID == "0" {
		return errors.New("can't remove the root directory")
	}
	return f.call(ctx, func() error { return f.client.Delete(dirID) })
}

// Rmdir removes the directory if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	files, err := f.listAll(ctx, dirID)
	if err != nil {
		return err
	}
	if len(files) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if err = f.deleteDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Purge deletes all files and directories under dir
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if err = f.deleteDir(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// rename renames the item with ID id to leaf
func (f *Fs) rename(ctx context.Context, id, leaf string) error {
	return f.call(ctx, func() error {
		return f.client.Rename(id, f.opt.Enc.FromStandardName(leaf))
	})
}

// Copy copies a file server-side
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

	// The copy keeps the source name, and 115 may rename it to avoid a
	// clash, so identify it as the item that wasn't there before.
	before, err := f.listAll(ctx, dstDirID)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]bool, len(before))
	var oldIDs []string
	for _, file := range before {
		existing[file.FileID] = true
		if !file.IsDirectory && f.opt.Enc.ToStandardName(file.Name) == dstLeaf {
			oldIDs = append(oldIDs, file.FileID)
		}
	}

	err = f.call(ctx, func() error { return f.client.Copy(dstDirID, srcObj.id) })
	if err != nil {
		return nil, fmt.Errorf("copy failed: %w", err)
	}

	after, err := f.listAll(ctx, dstDirID)
	if err != nil {
		return nil, err
	}
	var copied *driver115.File
	for i := range after {
		file := &after[i]
		if !file.IsDirectory && !existing[file.FileID] && strings.EqualFold(file.Sha1, srcObj.sha1) {
			copied = file
			break
		}
	}
	if copied == nil {
		return nil, errors.New("copy succeeded but the copied file was not found")
	}
	if f.opt.Enc.ToStandardName(copied.Name) != dstLeaf {
		if err = f.rename(ctx, copied.FileID, dstLeaf); err != nil {
			return nil, fmt.Errorf("copy: couldn't rename copied file: %w", err)
		}
	}
	// 115 allows duplicate names, so remove what the copy replaces
	for _, id := range oldIDs {
		if err = f.call(ctx, func() error { return f.client.Delete(id) }); err != nil {
			return nil, fmt.Errorf("copy: couldn't remove replaced file: %w", err)
		}
	}
	return f.newObjectWithInfo(remote, copied), nil
}

// Move moves a file server-side
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
		err = f.call(ctx, func() error { return f.client.Move(dstDirID, srcObj.id) })
		if err != nil {
			return nil, fmt.Errorf("move failed: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		if err = f.rename(ctx, srcObj.id, dstLeaf); err != nil {
			return nil, fmt.Errorf("move: couldn't rename: %w", err)
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
		err = f.call(ctx, func() error { return f.client.Move(dstParentID, srcID) })
		if err != nil {
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

// About gets quota and usage information
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	var info driver115.InfoData
	err := f.call(ctx, func() (err error) {
		info, err = f.client.GetInfo()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &fs.Usage{
		Total: fs.NewUsageValue(info.SpaceInfo.AllTotal.Size),
		Used:  fs.NewUsageValue(info.SpaceInfo.AllUse.Size),
		Free:  fs.NewUsageValue(info.SpaceInfo.AllRemain.Size),
	}, nil
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
//
// 115 doesn't store modification times so this is the time the file
// was last updated on the server.
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

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// ID returns the item ID
func (o *Object) ID() string { return o.id }

// Open opens the file for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.pickCode == "" {
		freshObj, err := o.fs.NewObject(ctx, o.remote)
		if err != nil {
			return nil, err
		}
		o.pickCode = freshObj.(*Object).pickCode
	}

	// The link is specific to the pick code; the hash and size make a
	// new version of the file get a new link.
	key := linkcache.Key(o.fs.name, o.pickCode, strconv.FormatInt(o.size, 10), o.sha1)
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the download link of the object
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	var downInfo *driver115.DownloadInfo
	err := o.fs.call(ctx, func() (err error) {
		downInfo, err = o.fs.client.DownloadWithUA(o.pickCode, userAgentOf(o.fs.version))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get download URL failed: %w", err)
	}
	if downInfo.Url.Url == "" {
		return nil, errors.New("get download URL failed: no URL returned")
	}
	// The URL is bound to the cookies and user agent it was issued for
	return &linkcache.Link{URL: downInfo.Url.Url, Header: downInfo.Header.Clone()}, nil
}

// openLink reads the object from link, sending the headers it is
// bound to
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	headers := make(map[string]string, len(link.Header))
	for k, v := range link.Header {
		if len(v) > 0 {
			headers[k] = strings.Join(v, "; ")
		}
	}
	opts := rest.Opts{
		Method:       "GET",
		RootURL:      link.URL,
		ExtraHeaders: headers,
		Options:      options,
	}
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var err error
		resp, err = o.fs.srv.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		if resp != nil {
			return nil, &downloadError{status: resp.StatusCode, err: err}
		}
		return nil, err
	}
	return resp.Body, nil
}

// Update replaces the object content.
//
// 115 allows duplicate names, so the old version is removed after a
// successful upload.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	newObj, err := o.fs.upload(ctx, in, src, o.remote, leaf, dirID)
	if err != nil {
		return err
	}
	if o.id != "" && newObj.id != o.id {
		oldID := o.id
		err = o.fs.call(ctx, func() error { return o.fs.client.Delete(oldID) })
		if err != nil {
			return fmt.Errorf("failed to remove old version: %w", err)
		}
	}
	*o = *newObj
	return nil
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.call(ctx, func() error { return o.fs.client.Delete(o.id) })
}

// ------------------------------------------------------------
// Commands and offline download
// ------------------------------------------------------------

// Command runs named commands on the backend
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "rapid-upload", "rapidupload":
		var fileName, sha1Str, blockSHA1 string
		var fileSize int64

		if len(arg) == 1 {
			// Format: 115://filename|filesize|sha1[|block_sha1]
			fileName, fileSize, sha1Str, blockSHA1, err = parse115Link(arg[0])
			if err != nil {
				return nil, err
			}
		} else if len(arg) >= 3 {
			// Args: [filename, size, sha1, (optional)block_sha1]
			fileName = arg[0]
			fileSize, err = strconv.ParseInt(arg[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid file size %q: %w", arg[1], err)
			}
			sha1Str = arg[2]
			if len(arg) >= 4 {
				blockSHA1 = arg[3]
			}
		} else {
			fileName = opt["name"]
			if s, ok := opt["size"]; ok {
				fileSize, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid file size %q: %w", s, err)
				}
			}
			sha1Str = opt["sha1"]
			blockSHA1 = opt["block_sha1"]
		}

		if fileName == "" || fileSize < 0 || sha1Str == "" {
			return nil, errors.New("rapid-upload requires filename, filesize, and sha1 (e.g. 'rclone backend rapid-upload cloud115:path 115://filename|size|sha1|block_sha1')")
		}

		dirID, err := f.dirCache.FindDir(ctx, opt["path"], true)
		if err != nil {
			return nil, fmt.Errorf("find target dir failed: %w", err)
		}

		// Without the content a range proof can't be answered
		fastInfo, err := f.rapidUpload(ctx, fileSize, f.opt.Enc.FromStandardName(fileName), dirID,
			strings.ToUpper(blockSHA1), strings.ToUpper(sha1Str), nil)
		if err != nil {
			return nil, fmt.Errorf("115 rapid upload failed: %w", err)
		}
		if matched, errOk := fastInfo.Ok(); errOk != nil || !matched {
			return map[string]any{
				"status":     "failed",
				"message":    "file not found in 115 server library or requires block verification",
				"sign_check": fastInfo.SignCheck,
			}, nil
		}

		return map[string]any{
			"status":    "success",
			"pick_code": fastInfo.PickCode,
			"name":      fileName,
			"size":      fileSize,
			"sha1":      sha1Str,
			"dir_id":    dirID,
		}, nil

	case "addurl", "offline":
		if len(arg) < 1 {
			return nil, errors.New("need at least 1 URL argument (magnet/ed2k/http)")
		}
		return f.AddURL(ctx, arg[0], opt)

	case "tasks", "listoffline":
		return f.ListOfflineTasks(ctx, time.Time{})

	case "deltask", "deleteoffline":
		if len(arg) < 1 {
			return nil, errors.New("need at least 1 task ID / hash argument")
		}
		err := f.DeleteOfflineTask(ctx, arg[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{"status": "success", "deleted": arg[0]}, nil

	case "cleartasks":
		err := f.call(ctx, func() error { return f.client.ClearOfflineTasks(0) })
		if err != nil {
			return nil, err
		}
		return map[string]any{"status": "success"}, nil

	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// AddURL submits a remote URL to be downloaded server-side.
func (f *Fs) AddURL(ctx context.Context, url string, opt map[string]string) (*fs.OfflineTask, error) {
	dirID, err := f.dirCache.FindDir(ctx, opt["path"], false)
	if err != nil {
		return nil, fmt.Errorf("find target dir failed: %w", err)
	}
	var hashes []string
	err = f.call(ctx, func() (err error) {
		hashes, err = f.client.AddOfflineTaskURIs([]string{url}, dirID, driver115.WithAppVer(f.version))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("115 add offline task failed: %w", err)
	}
	taskID := ""
	if len(hashes) > 0 {
		taskID = hashes[0]
	}
	return &fs.OfflineTask{
		ID:        taskID,
		Name:      opt["name"],
		URL:       url,
		Status:    fs.OfflineTaskStatusInit,
		CreatedAt: time.Now(),
	}, nil
}

// ListOfflineTasks returns server-side offline download tasks, paging
// back to since: listing a long history page by page gets the client
// blocked by 115's firewall.
func (f *Fs) ListOfflineTasks(ctx context.Context, since time.Time) ([]*fs.OfflineTask, error) {
	var tasks []*fs.OfflineTask
	for n := int64(1); ; n++ {
		var resp driver115.OfflineTaskResp
		err := f.call(ctx, func() (err error) {
			resp, err = f.client.ListOfflineTask(n)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("115 list offline tasks failed: %w", err)
		}
		var page []*fs.OfflineTask
		for _, t := range resp.Tasks {
			status := fs.OfflineTaskStatusRunning
			switch {
			case t.IsDone():
				status = fs.OfflineTaskStatusComplete
			case t.IsFailed():
				status = fs.OfflineTaskStatusError
			case t.IsTodo():
				status = fs.OfflineTaskStatusInit
			}
			page = append(page, &fs.OfflineTask{
				ID:        t.InfoHash,
				Name:      t.Name,
				URL:       t.Url,
				Size:      t.Size,
				Status:    status,
				Progress:  t.Percent,
				Speed:     int64(t.RateDownload),
				CreatedAt: time.Unix(t.AddTime, 0),
			})
		}
		tasks = append(tasks, page...)
		if n >= resp.PageCount || len(resp.Tasks) == 0 || fs.OfflineTasksBefore(page, since) {
			break
		}
	}
	return tasks, nil
}

// DeleteOfflineTask deletes a server-side offline download task.
func (f *Fs) DeleteOfflineTask(ctx context.Context, taskID string) error {
	return f.call(ctx, func() error {
		return f.client.DeleteOfflineTasks([]string{taskID}, false)
	})
}

// parse115Link parses standard 115 rapid upload link strings
func parse115Link(raw string) (name string, size int64, sha1Str string, blockSHA1 string, err error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "115://")
	parts := strings.Split(raw, "|")
	if len(parts) >= 3 {
		if len(parts[0]) == 40 && isHex(parts[0]) {
			sha1Str = parts[0]
			size, err = strconv.ParseInt(parts[1], 10, 64)
			name = parts[2]
			if len(parts) >= 4 {
				blockSHA1 = parts[3]
			}
			return name, size, sha1Str, blockSHA1, err
		}
		name = parts[0]
		size, err = strconv.ParseInt(parts[1], 10, 64)
		sha1Str = parts[2]
		if len(parts) >= 4 {
			blockSHA1 = parts[3]
		}
		return name, size, sha1Str, blockSHA1, err
	}
	return "", 0, "", "", errors.New("invalid 115 link format: expected 'filename|filesize|sha1[|block_sha1]'")
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// Check the interfaces are satisfied
var (
	_ fs.Fs                = (*Fs)(nil)
	_ fs.Purger            = (*Fs)(nil)
	_ fs.PutUncheckeder    = (*Fs)(nil)
	_ fs.Copier            = (*Fs)(nil)
	_ fs.Mover             = (*Fs)(nil)
	_ fs.DirMover          = (*Fs)(nil)
	_ fs.Commander         = (*Fs)(nil)
	_ fs.OfflineDownloader = (*Fs)(nil)
	_ fs.Abouter           = (*Fs)(nil)
	_ dircache.DirCacher   = (*Fs)(nil)
	_ fs.Object            = (*Object)(nil)
	_ fs.IDer              = (*Object)(nil)
	_ fs.SetModTimer       = (*Object)(nil)
)

var commandHelp = []fs.CommandHelp{{
	Name:  "rapid-upload",
	Short: "Create a file from its hashes without uploading data.",
	Long: `This command asks 115 to create a file from content it already
stores, identified by size and SHA1. It is also available as
"rapidupload".

Usage examples:

` + "```console" + `
rclone backend rapid-upload cloud115:dir "115://name|size|sha1|block_sha1"
rclone backend rapid-upload cloud115:dir name size sha1 [block_sha1]
rclone backend rapid-upload cloud115:dir -o name=file.mkv -o size=1048576 -o sha1=...
` + "```" + `

block_sha1 is the SHA1 of the first 128 KiB. If 115 asks for a proof of
a range of the content the command fails, since there is no data to
read it from.`,
	Opts: map[string]string{
		"path":       "Directory to create the file in, relative to the remote.",
		"name":       "File name.",
		"size":       "File size in bytes.",
		"sha1":       "SHA1 of the whole file.",
		"block_sha1": "SHA1 of the first 128 KiB.",
	},
}, {
	Name:  "addurl",
	Short: "Add an offline download task for a url.",
	Long: `This command adds an offline (cloud-side) download task for a url
(http, magnet or ed2k). It is also available as "offline".

Usage examples:

` + "```console" + `
rclone backend addurl cloud115:dirpath url
` + "```" + `

Downloads are stored in 'dirpath', which must exist.`,
	Opts: map[string]string{
		"path": "Directory to download into, relative to the remote.",
	},
}, {
	Name:  "tasks",
	Short: "List offline download tasks.",
	Long: `This command lists the offline download tasks. It is also
available as "listoffline".

` + "```console" + `
rclone backend tasks cloud115:
` + "```",
}, {
	Name:  "deltask",
	Short: "Delete an offline download task.",
	Long: `This command deletes the offline download task with the given
info hash. It is also available as "deleteoffline". Downloaded files
are kept.

` + "```console" + `
rclone backend deltask cloud115: hash
` + "```",
}, {
	Name:  "cleartasks",
	Short: "Clear finished offline download tasks.",
	Long: `This command clears the finished offline download tasks.

` + "```console" + `
rclone backend cleartasks cloud115:
` + "```",
}}
