// Package aliyundrive provides an interface to the Aliyun Drive Open API.
package aliyundrive

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/aliyundrive/api"
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
	apiBaseURL       = "https://openapi.alipan.com"
	defaultTokenURL  = "https://api.oplist.org/alicloud/renewapi"
	minSleep         = 50 * time.Millisecond
	maxSleep         = 4 * time.Second
	decayConstant    = 2
	defaultChunkSize = 20 * fs.Mebi
	maxParts         = 10000
	timeFormat       = "2006-01-02T15:04:05.000Z"
	// rapidUploadMinSize is the smallest file rapid upload is tried for
	rapidUploadMinSize = 100 * fs.Kibi
	// preHashSize is how many leading bytes the pre_hash covers
	preHashSize = 1024
	// uploadURLLifetime is how long part upload URLs are used before
	// they are refreshed (they expire after about an hour)
	uploadURLLifetime = 50 * time.Minute
	// downloadURLExpireSec is the life asked for download URLs, the
	// most the Open API grants
	downloadURLExpireSec = 14400
	// linkLifetime is how long a download link is kept when neither
	// the response's expiration nor the URL states its expiry: the
	// expire_sec asked for, 4 hours
	linkLifetime = downloadURLExpireSec * time.Second
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
		Name:        "aliyundrive",
		Description: "Aliyun Drive Open Platform API",
		NewFs:       NewFs,
		Aliases:     []string{"aliyun", "alibabadrive"},
		Options: []fs.Option{{
			Name:      "refresh_token",
			Help:      "Refresh Token obtained from Aliyun Drive Open Platform.",
			Required:  true,
			Sensitive: true,
		}, {
			Name: "use_online_api",
			Help: `Refresh the access token with the online API at oauth_token_url.

When this is set (the default) the refresh token is sent to
oauth_token_url, which by default is a third-party service run by the
OpenList community (api.oplist.org), not by Aliyun. That service
holds the Open Platform app credentials and returns new tokens.

If you have registered your own Aliyun Drive Open Platform app, set
client_id and client_secret and turn this off: rclone then refreshes
tokens directly with openapi.alipan.com and no token is sent to anyone
but Aliyun.`,
			Default: true,
		}, {
			Name:    "alipan_type",
			Help:    "Type of the refresh token when using the online API.",
			Default: "default",
			Examples: []fs.OptionExample{{
				Value: "default",
				Help:  "Token from the Aliyun Drive QR code login (driver_txt=alicloud_qr)",
			}, {
				Value: "alipanTV",
				Help:  "Token from the Aliyun Drive TV login (driver_txt=alicloud_tv)",
			}},
			Advanced: true,
		}, {
			Name: "oauth_token_url",
			Help: `URL of the online token refresh API.

Only used when use_online_api is set. The refresh token is sent to this
URL. The default is run by the OpenList community, not by Aliyun.`,
			Default:  defaultTokenURL,
			Advanced: true,
		}, {
			Name: "client_id",
			Help: `OAuth Client ID of your own Aliyun Drive Open Platform app.

Required when use_online_api is off. Leave empty to use the online API.`,
			Advanced: true,
		}, {
			Name: "client_secret",
			Help: `OAuth Client Secret of your own Aliyun Drive Open Platform app.

Required when use_online_api is off. Leave empty to use the online API.`,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:    "drive_type",
			Help:    "Drive type to access: resource, backup, or default.",
			Default: "resource",
			Examples: []fs.OptionExample{{
				Value: "resource",
				Help:  "Resource drive (Default / Recommended)",
			}, {
				Value: "backup",
				Help:  "Backup drive",
			}, {
				Value: "default",
				Help:  "Default drive",
			}},
		}, {
			Name:     "drive_id",
			Help:     "Specific drive ID (leave empty to discover automatically based on drive_type).",
			Advanced: true,
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID to start in (default is 'root').",
			Default:  "root",
			Advanced: true,
		}, {
			Name:    "remove_way",
			Help:    "Delete action behavior: trash (move to recycle bin) or delete (permanently delete).",
			Default: "trash",
			Examples: []fs.OptionExample{{
				Value: "trash",
				Help:  "Move file to recycle bin",
			}, {
				Value: "delete",
				Help:  "Delete file permanently",
			}},
		}, {
			Name: "rapid_upload",
			Help: `Attempt rapid upload before chunked uploading if content hash matches.

If the source has no SHA1 hash, the first 1 KiB is sent as a pre-hash
and, only if the server knows a matching file, the upload is spooled
to a temporary file to compute the full SHA1.`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     "chunk_size",
			Help:     "Chunk size for multipart upload (minimum 100k, maximum 5G).",
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Default:  encoder.Base | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines the configuration options for Aliyun Drive
type Options struct {
	RefreshToken  string               `config:"refresh_token"`
	UseOnlineAPI  bool                 `config:"use_online_api"`
	AlipanType    string               `config:"alipan_type"`
	OauthTokenURL string               `config:"oauth_token_url"`
	ClientID      string               `config:"client_id"`
	ClientSecret  string               `config:"client_secret"`
	DriveType     string               `config:"drive_type"`
	DriveID       string               `config:"drive_id"`
	RootFolderID  string               `config:"root_folder_id"`
	RemoveWay     string               `config:"remove_way"`
	RapidUpload   bool                 `config:"rapid_upload"`
	ChunkSize     fs.SizeSuffix        `config:"chunk_size"`
	Enc           encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote Aliyun Drive
type Fs struct {
	name         string
	root         string
	opt          Options
	features     *fs.Features
	srv          *rest.Client
	tokenSrv     *rest.Client
	uploadClient *http.Client
	pacer        *fs.Pacer
	dirCache     *dircache.DirCache
	driveID      string
	m            configmap.Mapper

	tokenMu      *sync.Mutex // protects the fields below
	accessToken  string
	refreshToken string
	tokenExpiry  time.Time
}

// Object describes a file on Aliyun Drive
type Object struct {
	fs          *Fs
	remote      string
	id          string
	size        int64
	contentHash string
	modTime     time.Time
}

// errorHandler parses a non 2xx error response into an *api.ErrResp
func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("error reading error out of body: %w", err)
	}
	errResp := &api.ErrResp{StatusCode: resp.StatusCode}
	if json.Unmarshal(body, errResp) != nil || errResp.Code == "" {
		// The online token API reports errors in "text"
		var tokenResp api.TokenResp
		if json.Unmarshal(body, &tokenResp) == nil && tokenResp.Text != "" {
			errResp.Message = tokenResp.Text
		} else {
			errResp.Message = strings.TrimSpace(string(body))
		}
		errResp.Code = resp.Status
	}
	return errResp
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if opt.RootFolderID == "" {
		opt.RootFolderID = "root"
	}
	if opt.DriveType == "" {
		opt.DriveType = "resource"
	}
	if opt.RemoveWay == "" {
		opt.RemoveWay = "trash"
	}
	if opt.ChunkSize < 100*fs.Kibi {
		opt.ChunkSize = defaultChunkSize
	}
	if !opt.UseOnlineAPI || opt.OauthTokenURL == "" {
		if opt.ClientID == "" || opt.ClientSecret == "" {
			return nil, errors.New("aliyundrive: client_id and client_secret are required when use_online_api is off or oauth_token_url is empty")
		}
	}

	root = strings.Trim(root, "/")
	client := fshttp.NewClient(ctx)
	f := &Fs{
		name:         name,
		root:         root,
		opt:          *opt,
		srv:          rest.NewClient(client).SetRoot(apiBaseURL).SetErrorHandler(errorHandler),
		tokenSrv:     rest.NewClient(client).SetErrorHandler(errorHandler),
		uploadClient: client,
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		tokenMu:      new(sync.Mutex),
		refreshToken: opt.RefreshToken,
		m:            m,
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements
		MaxFileStreams: 4,
		MaxStreams:     10,
	}).Fill(ctx, f)

	// Obtain initial access token
	if _, err = f.ensureToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to authenticate with Aliyun Drive: %w", err)
	}

	// Resolve DriveID if not explicitly configured
	if f.opt.DriveID == "" {
		err = f.discoverDriveID(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to discover drive ID: %w", err)
		}
	} else {
		f.driveID = f.opt.DriveID
	}

	f.dirCache = dircache.New(root, f.opt.RootFolderID, f)

	// Find the current root directory ID
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
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			if errors.Is(err, fs.ErrorDirNotFound) {
				// No root so return old f
				return f, nil
			}
			return nil, err
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
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// discoverDriveID queries the user's available drives and sets f.driveID
func (f *Fs) discoverDriveID(ctx context.Context) error {
	var resp api.DriveInfoResp
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/user/getDriveInfo",
	}
	if err := f.call(ctx, &opts, map[string]any{}, &resp); err != nil {
		return err
	}

	switch f.opt.DriveType {
	case "backup":
		f.driveID = resp.BackupDriveID
	case "default":
		f.driveID = resp.DefaultDriveID
	default: // "resource"
		f.driveID = resp.ResourceDriveID
		if f.driveID == "" {
			f.driveID = resp.DefaultDriveID
		}
	}

	if f.driveID == "" {
		return errors.New("no valid drive ID found for configured drive_type")
	}

	fs.Debugf(f, "Using drive ID %q (type: %s)", f.driveID, f.opt.DriveType)
	return nil
}

// ------------------------------------------------------------
// Token handling
// ------------------------------------------------------------

// ensureToken returns an unexpired access token, renewing it if needed
func (f *Fs) ensureToken(ctx context.Context) (string, error) {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()

	if f.accessToken != "" && time.Now().Before(f.tokenExpiry.Add(-2*time.Minute)) {
		return f.accessToken, nil
	}
	if err := f.renewToken(ctx); err != nil {
		return "", err
	}
	return f.accessToken, nil
}

// invalidateToken forces a renewal if the access token is still staleToken
func (f *Fs) invalidateToken(staleToken string) {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.accessToken == staleToken {
		f.accessToken = ""
	}
}

// jwtSub returns the "sub" claim of a JWT without verifying it
func jwtSub(token string) (string, error) {
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return "", errors.New("refresh token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		payload, err = base64.RawStdEncoding.DecodeString(segments[1])
		if err != nil {
			return "", errors.New("failed to decode refresh token")
		}
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", errors.New("failed to decode refresh token claims")
	}
	return claims.Sub, nil
}

// redactURLError removes the query, which holds the refresh token,
// from a *url.Error so it doesn't end up in logs.
func redactURLError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if u, parseErr := url.Parse(urlErr.URL); parseErr == nil {
			u.RawQuery = ""
			urlErr.URL = u.String()
		} else {
			urlErr.URL = "<redacted>"
		}
	}
	return err
}

// renewOnline refreshes the token with the online API
func (f *Fs) renewOnline(ctx context.Context) (*api.TokenResp, error) {
	driverTxt := "alicloud_qr"
	if f.opt.AlipanType == "alipanTV" {
		driverTxt = "alicloud_tv"
	}
	opts := rest.Opts{
		Method:  "GET",
		RootURL: f.opt.OauthTokenURL,
		Parameters: url.Values{
			"refresh_ui": {f.refreshToken},
			"server_use": {"true"},
			"driver_txt": {driverTxt},
		},
	}
	var resp api.TokenResp
	err := f.pacer.Call(func() (bool, error) {
		httpResp, err := f.tokenSrv.CallJSON(ctx, &opts, nil, &resp)
		return shouldRetryHTTP(ctx, httpResp, redactURLError(err))
	})
	if err != nil {
		return nil, fmt.Errorf("online token refresh failed: %w", err)
	}
	if resp.RefreshToken == "" || resp.AccessToken == "" {
		if resp.Text != "" {
			return nil, fmt.Errorf("online token refresh failed: %s", resp.Text)
		}
		return nil, errors.New("online token refresh returned an empty token, the refresh token or alipan_type may be wrong")
	}
	return &resp, nil
}

// renewLocal refreshes the token directly with Aliyun using the
// configured Open Platform app credentials.
func (f *Fs) renewLocal(ctx context.Context) (*api.TokenResp, error) {
	if f.opt.ClientID == "" || f.opt.ClientSecret == "" {
		return nil, errors.New("client_id and client_secret are required to refresh the token without the online API")
	}
	tokenReq := map[string]string{
		"client_id":     f.opt.ClientID,
		"client_secret": f.opt.ClientSecret,
		"grant_type":    "refresh_token",
		"refresh_token": f.refreshToken,
	}
	opts := rest.Opts{
		Method:  "POST",
		RootURL: apiBaseURL,
		Path:    "/oauth/access_token",
	}
	var resp api.TokenResp
	err := f.pacer.Call(func() (bool, error) {
		httpResp, err := f.tokenSrv.CallJSON(ctx, &opts, tokenReq, &resp)
		return shouldRetryHTTP(ctx, httpResp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("oauth token refresh failed: %w", err)
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("oauth token refresh failed: %s: %s", resp.Code, resp.Message)
	}
	if resp.RefreshToken == "" || resp.AccessToken == "" {
		return nil, errors.New("oauth token refresh returned an empty token")
	}
	// Make sure the new token belongs to the same user
	curSub, err := jwtSub(f.refreshToken)
	if err != nil {
		return nil, err
	}
	newSub, err := jwtSub(resp.RefreshToken)
	if err != nil {
		return nil, err
	}
	if curSub != newSub {
		return nil, errors.New("oauth token refresh failed: the new token is for a different user")
	}
	return &resp, nil
}

// renewToken fetches a new access token using the refresh token.
//
// Must be called with tokenMu held.
func (f *Fs) renewToken(ctx context.Context) error {
	var (
		resp *api.TokenResp
		err  error
	)
	if f.opt.UseOnlineAPI && f.opt.OauthTokenURL != "" {
		resp, err = f.renewOnline(ctx)
	} else {
		resp, err = f.renewLocal(ctx)
	}
	if err != nil {
		return err
	}

	f.accessToken = resp.AccessToken
	if resp.RefreshToken != f.refreshToken {
		f.refreshToken = resp.RefreshToken
		// m.Set persists the value to the config file
		f.m.Set("refresh_token", resp.RefreshToken)
	}
	expiresIn := resp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 7200
	}
	f.tokenExpiry = time.Now().Add(time.Duration(expiresIn) * time.Second)

	fs.Debugf(f, "Refreshed access token (expires in %d s)", expiresIn)
	return nil
}

// ------------------------------------------------------------
// API calls
// ------------------------------------------------------------

// shouldRetryHTTP returns a boolean as to whether this resp and err
// deserve to be retried, honouring Retry-After on 429.
func shouldRetryHTTP(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if err != nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if secs, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && secs > 0 {
			return true, pacer.RetryAfterError(err, time.Duration(secs)*time.Second)
		}
		return true, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. token is the access token used for the call:
// if it was rejected it is renewed before the retry.
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error, token string) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	var apiErr *api.ErrResp
	if errors.As(err, &apiErr) && apiErr.IsTokenError() {
		fs.Debugf(f, "Access token rejected (%s) - renewing", apiErr.Code)
		f.invalidateToken(token)
		return true, err
	}
	return shouldRetryHTTP(ctx, resp, err)
}

// call makes an authorized API call under the pacer
func (f *Fs) call(ctx context.Context, opts *rest.Opts, req, resp any) error {
	return f.pacer.Call(func() (bool, error) {
		token, err := f.ensureToken(ctx)
		if err != nil {
			return false, err
		}
		opts.ExtraHeaders = map[string]string{"Authorization": "Bearer " + token}
		httpResp, err := f.srv.CallJSON(ctx, opts, req, resp)
		return f.shouldRetry(ctx, httpResp, err, token)
	})
}

// Name returns the configured name of the remote
func (f *Fs) Name() string { return f.name }

// Root returns the root of the remote
func (f *Fs) Root() string { return f.root }

// String returns a description of the remote
func (f *Fs) String() string { return fmt.Sprintf("aliyun drive root %q", f.root) }

// Precision of the remote
func (f *Fs) Precision() time.Duration { return time.Millisecond }

// Hashes returns the supported hash types
func (f *Fs) Hashes() hash.Set { return hash.NewHashSet(hash.SHA1) }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// listAll calls fn for every item in the directory dirID until fn
// returns true.
func (f *Fs) listAll(ctx context.Context, dirID string, fn func(*api.File) bool) error {
	marker := ""
	for {
		req := map[string]any{
			"drive_id":        f.driveID,
			"parent_file_id":  dirID,
			"limit":           200,
			"order_by":        "name",
			"order_direction": "ASC",
		}
		if marker != "" {
			req["marker"] = marker
		}
		opts := rest.Opts{
			Method: "POST",
			Path:   "/adrive/v1.0/openFile/list",
		}
		var resp api.FileListResp
		if err := f.call(ctx, &opts, req, &resp); err != nil {
			return err
		}
		for i := range resp.Items {
			if fn(&resp.Items[i]) {
				return nil
			}
		}
		if resp.NextMarker == "" {
			return nil
		}
		marker = resp.NextMarker
	}
}

// FindLeaf finds a directory called leaf in the directory pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	err = f.listAll(ctx, pathID, func(item *api.File) bool {
		if item.Type == "folder" && f.opt.Enc.ToStandardName(item.Name) == leaf {
			pathIDOut = item.FileID
			found = true
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// CreateDir creates a directory in Aliyun Drive
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (createdID string, err error) {
	nowStr := time.Now().UTC().Format(timeFormat)
	req := api.CreateFileRequest{
		DriveID:         f.driveID,
		ParentFileID:    dirID,
		Name:            f.opt.Enc.FromStandardName(leaf),
		Type:            "folder",
		CheckNameMode:   "refuse",
		LocalCreatedAt:  nowStr,
		LocalModifiedAt: nowStr,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/create",
	}
	var resp api.CreateFileResponse
	if err = f.call(ctx, &opts, req, &resp); err != nil {
		return "", err
	}
	return resp.FileID, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	err = f.listAll(ctx, dirID, func(item *api.File) bool {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
		if item.Type == "folder" {
			f.dirCache.Put(remote, item.FileID)
			entries = append(entries, fs.NewDir(remote, item.UpdatedAt).SetID(item.FileID))
		} else {
			entries = append(entries, f.newObjectWithInfo(remote, item))
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// newObjectWithInfo creates an Object from api.File info
func (f *Fs) newObjectWithInfo(remote string, info *api.File) *Object {
	o := &Object{fs: f, remote: remote}
	o.setMetaData(info)
	return o
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.File) {
	o.id = info.FileID
	o.size = info.Size
	o.contentHash = strings.ToLower(info.ContentHash)
	switch {
	case !info.LocalModifiedAt.IsZero():
		o.modTime = info.LocalModifiedAt
	case !info.UpdatedAt.IsZero():
		o.modTime = info.UpdatedAt
	default:
		o.modTime = info.CreatedAt
	}
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

	var found *Object
	err = f.listAll(ctx, dirID, func(item *api.File) bool {
		if item.Type != "folder" && f.opt.Enc.ToStandardName(item.Name) == leaf {
			found = f.newObjectWithInfo(remote, item)
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return found, nil
}

// Put in to the remote path with the given ObjectInfo
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

// PutUnchecked uploads the object without checking if it already exists
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, err
	}
	info, err := f.upload(ctx, in, src, dirID, leaf)
	if err != nil {
		return nil, err
	}
	return f.newObjectWithInfo(src.Remote(), info), nil
}

// partSize returns the part size to use for size bytes
func (f *Fs) partSize(size int64) int64 {
	partSize := int64(f.opt.ChunkSize)
	if partSize <= 0 {
		partSize = int64(defaultChunkSize)
	}
	if size > partSize*maxParts {
		partSize = (size + maxParts - 1) / maxParts
	}
	return partSize
}

// tempFiles tracks temporary files used by an upload
type tempFiles []*os.File

// spool copies in to a new temporary file, returning it rewound with
// the size and SHA1 of the data.
func (t *tempFiles) spool(in io.Reader) (*os.File, int64, string, error) {
	tmp, err := os.CreateTemp("", "rclone-aliyundrive-*")
	if err != nil {
		return nil, 0, "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	*t = append(*t, tmp)
	h := sha1.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), in)
	if err != nil {
		return nil, 0, "", fmt.Errorf("failed to spool upload to temporary file: %w", err)
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return nil, 0, "", fmt.Errorf("failed to rewind temporary file: %w", err)
	}
	return tmp, n, hex.EncodeToString(h.Sum(nil)), nil
}

// cleanup closes and removes the temporary files
func (t tempFiles) cleanup() {
	for _, tmp := range t {
		_ = tmp.Close()
		if err := os.Remove(tmp.Name()); err != nil {
			fs.Errorf(nil, "aliyundrive: failed to remove temporary file: %v", err)
		}
	}
}

// proofRange returns the range of the content used for the rapid
// upload proof, derived from the access token.
func proofRange(token string, size int64) (start, length int64, err error) {
	sum := md5.Sum([]byte(token))
	seed, err := strconv.ParseUint(hex.EncodeToString(sum[:8]), 16, 64)
	if err != nil {
		return 0, 0, err
	}
	start = int64(seed % uint64(size))
	length = min(8, size-start)
	return start, length, nil
}

// calcProofCode calculates the proof code for rapid upload, reading
// the content from ra if set, otherwise from the source object.
func (f *Fs) calcProofCode(ctx context.Context, token string, src fs.ObjectInfo, ra io.ReaderAt, size int64) (proof string, err error) {
	if size <= 0 {
		return "", nil
	}
	start, length, err := proofRange(token, size)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if ra != nil {
		n, err := ra.ReadAt(buf, start)
		if int64(n) != length {
			return "", fmt.Errorf("failed to read proof range: %w", err)
		}
		return base64.StdEncoding.EncodeToString(buf), nil
	}
	srcObj := fs.UnWrapObjectInfo(src)
	if srcObj == nil {
		return "", errors.New("source can't be range read")
	}
	rc, err := srcObj.Open(ctx, &fs.RangeOption{Start: start, End: start + length - 1})
	if err != nil {
		return "", err
	}
	defer fs.CheckClose(rc, &err)
	if _, err = io.ReadFull(rc, buf); err != nil {
		return "", fmt.Errorf("failed to read proof range: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

// createFile calls openFile/create
func (f *Fs) createFile(ctx context.Context, req *api.CreateFileRequest) (*api.CreateFileResponse, error) {
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/create",
	}
	var resp api.CreateFileResponse
	if err := f.call(ctx, &opts, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// uploadSession holds the state of a multipart upload
type uploadSession struct {
	fileID   string
	uploadID string
	numParts int
	urls     map[int]string // part number to upload URL
	fetched  time.Time
	expired  bool
}

func (s *uploadSession) setParts(parts []api.PartInfo) {
	s.urls = make(map[int]string, len(parts))
	for _, part := range parts {
		s.urls[part.PartNumber] = part.UploadURL
	}
	s.fetched = time.Now()
	s.expired = false
}

func makePartInfos(n int) []api.PartInfo {
	parts := make([]api.PartInfo, n)
	for i := range parts {
		parts[i] = api.PartInfo{PartNumber: i + 1}
	}
	return parts
}

// refreshUploadURLs gets fresh upload URLs for all the parts
func (f *Fs) refreshUploadURLs(ctx context.Context, s *uploadSession) error {
	req := api.GetUploadURLRequest{
		DriveID:      f.driveID,
		FileID:       s.fileID,
		UploadID:     s.uploadID,
		PartInfoList: makePartInfos(s.numParts),
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/getUploadUrl",
	}
	var resp api.GetUploadURLResponse
	if err := f.call(ctx, &opts, req, &resp); err != nil {
		return fmt.Errorf("failed to refresh upload URLs: %w", err)
	}
	s.setParts(resp.PartInfoList)
	return nil
}

// uploadPart uploads one part, retrying and refreshing an expired
// upload URL as needed.
func (f *Fs) uploadPart(ctx context.Context, s *uploadSession, partNumber int, chunk []byte) error {
	return f.pacer.Call(func() (bool, error) {
		if s.expired || time.Since(s.fetched) > uploadURLLifetime {
			if err := f.refreshUploadURLs(ctx, s); err != nil {
				return false, err
			}
		}
		uploadURL := s.urls[partNumber]
		if uploadURL == "" {
			return false, fmt.Errorf("no upload URL for part %d", partNumber)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(chunk))
		if err != nil {
			return false, err
		}
		resp, err := f.uploadClient.Do(req)
		if err != nil {
			return shouldRetryHTTP(ctx, nil, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK, http.StatusConflict: // 409: part already uploaded
			return false, nil
		case http.StatusForbidden: // the upload URL has expired
			s.expired = true
			return true, fmt.Errorf("upload URL for part %d rejected: %s", partNumber, resp.Status)
		}
		return shouldRetryHTTP(ctx, resp, fmt.Errorf("uploading part %d failed: %s", partNumber, resp.Status))
	})
}

// upload uploads in as leaf in directory dirID and returns the new file.
//
// The data is streamed one part at a time. It is only spooled to a
// temporary file if the size is unknown, or if rapid upload is enabled,
// the source has no SHA1 and the server reports a pre-hash match.
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, dirID, leaf string) (*api.File, error) {
	var (
		tmps    tempFiles
		tmp     *os.File
		sha1Hex string
		size    = src.Size()
		err     error
	)
	defer tmps.cleanup()

	if size < 0 {
		tmp, size, sha1Hex, err = tmps.spool(in)
		if err != nil {
			return nil, err
		}
		in = tmp
	}

	partSize := f.partSize(size)
	numParts := int((size + partSize - 1) / partSize)
	mtime := src.ModTime(ctx).UTC().Format(timeFormat)
	createReq := api.CreateFileRequest{
		DriveID:         f.driveID,
		ParentFileID:    dirID,
		Name:            f.opt.Enc.FromStandardName(leaf),
		Type:            "file",
		CheckNameMode:   "ignore",
		Size:            size,
		LocalCreatedAt:  mtime,
		LocalModifiedAt: mtime,
		PartInfoList:    makePartInfos(numParts),
	}

	setContentHash := func(sha1Hex string, ra io.ReaderAt) error {
		token, err := f.ensureToken(ctx)
		if err != nil {
			return err
		}
		proof, err := f.calcProofCode(ctx, token, src, ra, size)
		if err != nil {
			return err
		}
		createReq.PreHash = ""
		createReq.ContentHash = strings.ToUpper(sha1Hex)
		createReq.ContentHashName = "sha1"
		createReq.ProofCode = proof
		createReq.ProofVersion = "v1"
		return nil
	}

	if f.opt.RapidUpload && size > int64(rapidUploadMinSize) {
		if sha1Hex == "" {
			sha1Hex, _ = src.Hash(ctx, hash.SHA1)
		}
		if sha1Hex != "" {
			var ra io.ReaderAt
			if tmp != nil {
				ra = tmp
			}
			if err := setContentHash(sha1Hex, ra); err != nil {
				fs.Debugf(src, "Skipping rapid upload: %v", err)
			}
		}
		if createReq.ContentHash == "" {
			// Send the SHA1 of the first KiB to find out whether a
			// rapid upload might work before hashing everything
			prefix := make([]byte, preHashSize)
			if _, err := io.ReadFull(in, prefix); err != nil {
				return nil, fmt.Errorf("failed to read pre-hash data: %w", err)
			}
			sum := sha1.Sum(prefix)
			createReq.PreHash = hex.EncodeToString(sum[:])
			in = io.MultiReader(bytes.NewReader(prefix), in)
		}
	}

	createResp, err := f.createFile(ctx, &createReq)
	var apiErr *api.ErrResp
	if err != nil && createReq.PreHash != "" && errors.As(err, &apiErr) && apiErr.Code == api.CodePreHashMatched {
		fs.Debugf(src, "Pre-hash matched, hashing the whole file for rapid upload")
		var n int64
		tmp, n, sha1Hex, err = tmps.spool(in)
		if err != nil {
			return nil, err
		}
		if n != size {
			return nil, fmt.Errorf("source size changed: read %d bytes, expected %d", n, size)
		}
		in = tmp
		if err = setContentHash(sha1Hex, tmp); err != nil {
			return nil, err
		}
		createResp, err = f.createFile(ctx, &createReq)
	}
	if err != nil {
		return nil, fmt.Errorf("create file request failed: %w", err)
	}

	if createResp.RapidUpload {
		fs.Debugf(src, "Rapid upload succeeded")
	} else {
		session := &uploadSession{
			fileID:   createResp.FileID,
			uploadID: createResp.UploadID,
			numParts: numParts,
		}
		session.setParts(createResp.PartInfoList)
		buf := make([]byte, min(partSize, size))
		for i := range numParts {
			chunk := buf[:min(partSize, size-int64(i)*partSize)]
			if _, err := io.ReadFull(in, chunk); err != nil {
				return nil, fmt.Errorf("reading part %d: %w", i+1, err)
			}
			if err := f.uploadPart(ctx, session, i+1, chunk); err != nil {
				return nil, err
			}
		}
	}

	// Complete upload
	completeReq := api.CompleteUploadRequest{
		DriveID:  f.driveID,
		FileID:   createResp.FileID,
		UploadID: createResp.UploadID,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/complete",
	}
	var info api.File
	if err = f.call(ctx, &opts, completeReq, &info); err != nil {
		return nil, fmt.Errorf("complete upload failed: %w", err)
	}
	return &info, nil
}

// Mkdir creates the directory if it doesn't already exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir deletes the directory if empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	empty := true
	err = f.listAll(ctx, dirID, func(*api.File) bool {
		empty = false
		return true
	})
	if err != nil {
		return err
	}
	if !empty {
		return fs.ErrorDirectoryNotEmpty
	}
	if err = f.deleteItem(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Purge deletes the directory and all its contents
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if err = f.deleteItem(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// deleteItem removes a file or folder by ID
func (f *Fs) deleteItem(ctx context.Context, fileID string) error {
	uri := "/adrive/v1.0/openFile/recyclebin/trash"
	if f.opt.RemoveWay == "delete" {
		uri = "/adrive/v1.0/openFile/delete"
	}
	req := map[string]string{
		"drive_id": f.driveID,
		"file_id":  fileID,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   uri,
	}
	return f.call(ctx, &opts, req, nil)
}

// removeExisting removes the file at remote, if any, unless its ID is keepID
func (f *Fs) removeExisting(ctx context.Context, remote, keepID string) error {
	existing, err := f.NewObject(ctx, remote)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	existingObj := existing.(*Object)
	if existingObj.id == keepID {
		return nil
	}
	return f.deleteItem(ctx, existingObj.id)
}

// moveOrCopy moves or copies the file with ID fileID into dstDirID as leaf
func (f *Fs) moveOrCopy(ctx context.Context, op, fileID, dstDirID, leaf string) error {
	req := api.MoveCopyRequest{
		DriveID:        f.driveID,
		FileID:         fileID,
		ToParentFileID: dstDirID,
		NewName:        f.opt.Enc.FromStandardName(leaf),
		CheckNameMode:  "refuse",
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/" + op,
	}
	var resp api.MoveCopyResponse
	if err := f.call(ctx, &opts, req, &resp); err != nil {
		return err
	}
	if resp.Exist {
		return fmt.Errorf("can't %s: %q already exists", op, leaf)
	}
	return nil
}

// Copy src to this remote using server-side copy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || srcObj.fs.driveID != f.driveID {
		return nil, fs.ErrorCantCopy
	}
	leaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	// Copy overwrites, so remove the destination first
	if err = f.removeExisting(ctx, remote, srcObj.id); err != nil {
		return nil, err
	}
	if err = f.moveOrCopy(ctx, "copy", srcObj.id, dstDirID, leaf); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

// Move src to this remote using server-side move
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || srcObj.fs.driveID != f.driveID {
		return nil, fs.ErrorCantMove
	}
	leaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcLeaf, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}
	if srcDirID == dstDirID && srcLeaf == leaf {
		return srcObj, nil
	}
	// Move overwrites, so remove the destination first
	if err = f.removeExisting(ctx, remote, srcObj.id); err != nil {
		return nil, err
	}

	if srcDirID == dstDirID {
		// Only renaming
		updateReq := api.UpdateFileRequest{
			DriveID:       f.driveID,
			FileID:        srcObj.id,
			Name:          f.opt.Enc.FromStandardName(leaf),
			CheckNameMode: "refuse",
		}
		opts := rest.Opts{
			Method: "POST",
			Path:   "/adrive/v1.0/openFile/update",
		}
		if err = f.call(ctx, &opts, updateReq, nil); err != nil {
			return nil, err
		}
	} else if err = f.moveOrCopy(ctx, "move", srcObj.id, dstDirID, leaf); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

// DirMove moves src directory to dst
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || srcFs.driveID != f.driveID {
		return fs.ErrorCantDirMove
	}
	srcID, _, _, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	if err = f.moveOrCopy(ctx, "move", srcID, dstDirectoryID, dstLeaf); err != nil {
		return err
	}
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// DirCacheFlush resets the directory cache
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
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
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// Size returns the size of the object
func (o *Object) Size() int64 { return o.size }

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Hash returns the SHA1 hash of the object
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return o.contentHash, nil
}

// Storable returns whether this object can be stored
func (o *Object) Storable() bool { return true }

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	key := linkcache.Key(o.fs.name, o.fs.driveID, o.id, strconv.FormatInt(o.size, 10), o.contentHash)
	return linkcache.Default.Open(ctx, key, linkLifetime, o.fetchLink, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.openLink(ctx, link, options)
	})
}

// fetchLink requests the download link of the object
func (o *Object) fetchLink(ctx context.Context) (*linkcache.Link, error) {
	req := map[string]any{
		"drive_id":   o.fs.driveID,
		"file_id":    o.id,
		"expire_sec": downloadURLExpireSec,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/getDownloadUrl",
	}
	var resp api.DownloadURLResponse
	if err := o.fs.call(ctx, &opts, req, &resp); err != nil {
		return nil, fmt.Errorf("failed to get download URL: %w", err)
	}
	if resp.URL == "" {
		return nil, errors.New("empty download URL returned")
	}
	link := &linkcache.Link{URL: resp.URL}
	if expires, err := time.Parse(time.RFC3339, resp.Expiration); err == nil {
		link.Expires = expires
	}
	return link, nil
}

// openLink reads the object from link
func (o *Object) openLink(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	downloadOpts := rest.Opts{
		Method:  "GET",
		RootURL: link.URL,
		Options: options,
	}
	var httpResp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var err error
		httpResp, err = o.fs.srv.Call(ctx, &downloadOpts)
		return shouldRetryHTTP(ctx, httpResp, err)
	})
	if err != nil {
		return nil, err
	}
	return httpResp.Body, nil
}

// Update the object with the contents of the io.Reader
//
// The new content is uploaded as a new file next to the old one, which
// is removed once the upload has completed.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	info, err := o.fs.upload(ctx, in, src, dirID, leaf)
	if err != nil {
		return err
	}
	oldID := o.id
	o.setMetaData(info)
	if oldID != "" && oldID != info.FileID {
		if err = o.fs.deleteItem(ctx, oldID); err != nil {
			return fmt.Errorf("failed to remove the old version: %w", err)
		}
	}
	return nil
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteItem(ctx, o.id)
}

// SetModTime sets the modification time of the local file
func (o *Object) SetModTime(ctx context.Context, mtime time.Time) error {
	req := api.UpdateFileRequest{
		DriveID:         o.fs.driveID,
		FileID:          o.id,
		CheckNameMode:   "ignore",
		LocalModifiedAt: mtime.UTC().Format(timeFormat),
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/adrive/v1.0/openFile/update",
	}
	if err := o.fs.call(ctx, &opts, req, nil); err != nil {
		return err
	}
	o.modTime = mtime
	return nil
}

// ID returns the ID of the Object
func (o *Object) ID() string { return o.id }

// Check interfaces
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.SetModTimer     = (*Object)(nil)
)
