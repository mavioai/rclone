// Package thunder provides a filesystem interface to Xunlei (Thunder) Drive.
package thunder

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/thunder/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/linkcache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/rest"
)

const (
	apiURL        = "https://api-pan.xunlei.com/drive/v1"
	xluserAPIBase = "https://xluser-ssl.xunlei.com"
	xluserAPIURL  = xluserAPIBase + "/v1"

	clientID      = "Xp6vsxz_7IYVw2BB"
	clientSecret  = "Xp6vsy4tN9toTVdMSpomVdXpRmES"
	clientVersion = "8.31.0.9726"
	packageName   = "com.xunlei.downloadprovider"
	appID         = "40"
	appKey        = "34a062aaa22f906fca4fefe9fb3a3021"
	userAgent     = "ANDROID-com.xunlei.downloadprovider/8.31.0.9726 netWorkType/5G appid/40 deviceName/Xiaomi_M2004j7ac deviceModel/M2004J7AC OSVersion/12 protocolVersion/301 platformVersion/10 sdkVersion/512000 Oauth2Client/0.9 (Linux 4_14_186-perf-gddfs8vbb238b) (JAVA 0)"
	downloadUA    = "Dalvik/2.1.0 (Linux; U; Android 12; M2004J7AC Build/SP1A.210812.016)"
	signProvider  = "access_end_point_token"

	minSleep      = 100 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2

	// linkExpiryMargin is how long before the expiry its URL states a
	// download link is renewed
	linkExpiryMargin = 30 * time.Second

	maxUploadParts      = 10000
	defaultChunkSize    = 5 * fs.Mebi
	minChunkSize        = 5 * fs.Mebi
	maxChunkSize        = 5 * fs.Gibi
	defaultUploadCutoff = 200 * fs.Mebi
	maxUploadCutoff     = 5 * fs.Gibi

	maxCaptchaTokens = 256 // max number of per-action captcha tokens kept
)

var defaultAlgorithms = []string{
	"9uJNVj/wLmdwKrJaVj/omlQ",
	"Oz64Lp0GigmChHMf/6TNfxx7O9PyopcczMsnf",
	"Eb+L7Ce+Ej48u",
	"jKY0",
	"ASr0zCl6v8W4aidjPK5KHd1Lq3t+vBFf41dqv5+fnOd",
	"wQlozdg6r1qxh0eRmt3QgNXOvSZO6q/GXK",
	"gmirk+ciAvIgA/cxUUCema47jr/YToixTT+Q6O",
	"5IiCoM9B1/788ntB",
	"P07JH0h6qoM6TSUAK2aL9T5s2QBVeY9JWvalf",
	"+oK0AN",
}

var emailRegexp = regexp.MustCompile(`\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*`)

// gcidType is the hash type of the GCID content hash
var gcidType hash.Type

func init() {
	gcidType = hash.RegisterHash("gcid", "GCID", 40, newGcidHasher)
	fs.Register(&fs.RegInfo{
		Name:        "thunder",
		Description: "Xunlei Drive",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Aliases:     []string{"xunlei", "thunderdrive"},
		Options: []fs.Option{{
			Name: "username",
			Help: "Account username or phone number",
		}, {
			Name:       "password",
			Help:       "Account password",
			IsPassword: true,
		}, {
			Name:      "refresh_token",
			Help:      "Refresh token",
			Sensitive: true,
		}, {
			Name:      "device_id",
			Help:      "32-character hexadecimal device ID (generated automatically if blank)",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:      "root_folder_id",
			Help:      "Root folder ID.\n\nLeave blank normally to use the root of the drive.",
			Default:   "",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name: "upload_cutoff",
			Help: `Cutoff for switching to chunked upload.

Any files larger than this will be uploaded in chunks of chunk_size.
The minimum is 0 and the maximum is 5 GiB.`,
			Default:  defaultUploadCutoff,
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: `Chunk size for multipart uploads.

Note that this is stored in memory and there may be up to
"--transfers" * "--thunder-upload-concurrency" chunks stored at once
in memory.

Rclone will automatically increase the chunk size when uploading a
large file of known size to stay below the 10,000 chunks limit.`,
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: `Concurrency for multipart uploads.

This is the number of chunks of the same file that are uploaded
concurrently for multipart uploads.`,
			Default:  4,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.EncodeCtl |
				encoder.EncodeDot |
				encoder.EncodeBackSlash |
				encoder.EncodeSlash |
				encoder.EncodeDoubleQuote |
				encoder.EncodeAsterisk |
				encoder.EncodeColon |
				encoder.EncodeLtGt |
				encoder.EncodeQuestion |
				encoder.EncodePipe |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeRightPeriod |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines configuration options
type Options struct {
	Username          string               `config:"username"`
	Password          string               `config:"password"`
	RefreshToken      string               `config:"refresh_token"`
	DeviceID          string               `config:"device_id"`
	RootFolderID      string               `config:"root_folder_id"`
	UploadCutoff      fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize         fs.SizeSuffix        `config:"chunk_size"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a Xunlei Drive remote
type Fs struct {
	name     string
	root     string
	opt      Options
	m        configmap.Mapper
	features *fs.Features
	srv      *rest.Client // drive API
	authSrv  *rest.Client // user (auth) API
	client   *http.Client // for downloads
	dirCache *dircache.DirCache
	pacer    *fs.Pacer

	tokenMu   *sync.Mutex
	token     *api.TokenResp
	captchaMu *sync.Mutex
	captcha   map[string]string // captcha token by action
	lastCap   string            // most recently issued captcha token
}

// Object represents a file on Xunlei Drive
type Object struct {
	fs       *Fs
	remote   string
	id       string
	parentID string
	size     int64
	modTime  time.Time
	gcid     string
	linkMu   *sync.Mutex
	link     string
	expires  time.Time // when link expires as its URL states; zero if unknown
}

// ------------------------------------------------------------
// Helpers and Authentication
// ------------------------------------------------------------

func generateDeviceSign(deviceID, pkgName string) string {
	base := fmt.Sprintf("%s%s%s%s", deviceID, pkgName, appID, appKey)
	s1 := sha1.Sum([]byte(base))
	s1Hex := hex.EncodeToString(s1[:])
	m := md5.Sum([]byte(s1Hex))
	return fmt.Sprintf("div101.%s%s", deviceID, hex.EncodeToString(m[:]))
}

func getCaptchaSign(deviceID string) (string, string) {
	ts := fmt.Sprint(time.Now().UnixMilli())
	str := fmt.Sprint(clientID, clientVersion, packageName, deviceID, ts)
	for _, alg := range defaultAlgorithms {
		m := md5.Sum([]byte(str + alg))
		str = hex.EncodeToString(m[:])
	}
	return ts, "1." + str
}

// getAction returns the captcha action of a request
func getAction(method, rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return method + ":" + rawURL
	}
	return method + ":" + parsed.Path
}

// genDeviceID makes a new random device ID
func genDeviceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// parentIDForRequest returns the parent_id to use in API requests
func parentIDForRequest(dirID string) string {
	if dirID == "root" {
		return ""
	}
	return dirID
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

// shouldRetryNoAuth returns whether the call should be retried
// without trying to fix the authentication
func shouldRetryNoAuth(ctx context.Context, resp *http.Response, err error) (bool, error) {
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

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It refreshes the access token or the captcha
// token used for the request if the API says they are invalid.
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error, action, usedToken, usedCaptcha string) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	var apiErr *api.ErrResp
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.IsTokenExpired():
			if authErr := f.reAuthorize(ctx, usedToken); authErr != nil {
				return false, fserrors.FatalError(fmt.Errorf("re-authorization failed: %w (after %v)", authErr, err))
			}
			return true, err
		case apiErr.IsCaptchaInvalid():
			if capErr := f.refreshCaptchaAtLogin(ctx, action, usedCaptcha); capErr != nil {
				return false, fmt.Errorf("captcha token refresh failed: %w (after %v)", capErr, err)
			}
			return true, err
		case apiErr.ErrorMsg == "file_rename_uncompleted":
			// renaming right after a server-side change
			return true, err
		case apiErr.ErrorMsg == "file_space_not_enough":
			return false, fserrors.FatalError(err)
		}
	}
	return shouldRetryNoAuth(ctx, resp, err)
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	errResponse := new(api.ErrResp)
	if err := rest.DecodeJSON(resp, errResponse); err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	errResponse.StatusCode = resp.StatusCode
	if !errResponse.IsError() {
		errResponse.ErrorMsg = resp.Status
	}
	return errResponse
}

// callAPI does a JSON call and decodes the response, checking
// for errors reported with a 2xx status
func callAPI(ctx context.Context, srv *rest.Client, opts *rest.Opts, request, response any) (*http.Response, error) {
	opts.NoResponse = false
	resp, err := srv.CallJSON(ctx, opts, request, nil)
	if err != nil {
		return resp, err
	}
	body, err := rest.ReadBody(resp)
	if err != nil {
		return resp, fmt.Errorf("failed to read response: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return resp, nil
	}
	var apiErr api.ErrResp
	if json.Unmarshal(body, &apiErr) == nil && apiErr.IsError() {
		apiErr.StatusCode = resp.StatusCode
		return resp, &apiErr
	}
	if response != nil {
		if err := json.Unmarshal(body, response); err != nil {
			return resp, fmt.Errorf("failed to decode response: %w", err)
		}
	}
	return resp, nil
}

// baseHeaders returns the headers sent with every API request
func (f *Fs) baseHeaders() map[string]string {
	return map[string]string{
		"User-Agent":       userAgent,
		"Accept":           "application/json;charset=UTF-8",
		"X-Device-Id":      f.opt.DeviceID,
		"X-Client-Id":      clientID,
		"X-Client-Version": clientVersion,
	}
}

// authCall calls the user API (no Authorization header)
func (f *Fs) authCall(ctx context.Context, opts *rest.Opts, request, response any) error {
	headers := f.baseHeaders()
	for k, v := range opts.ExtraHeaders {
		headers[k] = v
	}
	opts.ExtraHeaders = headers
	return f.pacer.Call(func() (bool, error) {
		resp, err := callAPI(ctx, f.authSrv, opts, request, response)
		return shouldRetryNoAuth(ctx, resp, err)
	})
}

// call calls the drive API with the access and captcha tokens
func (f *Fs) call(ctx context.Context, opts *rest.Opts, request, response any) (resp *http.Response, err error) {
	rootURL := opts.RootURL
	if rootURL == "" {
		rootURL = apiURL
	}
	action := getAction(opts.Method, rootURL+opts.Path)
	extra := opts.ExtraHeaders
	err = f.pacer.Call(func() (bool, error) {
		headers := f.baseHeaders()
		for k, v := range extra {
			headers[k] = v
		}
		tok := f.accessToken()
		if tok != "" {
			headers["Authorization"] = tok
		}
		capTok := f.captchaToken(action)
		if capTok != "" {
			headers["X-Captcha-Token"] = capTok
		}
		opts.ExtraHeaders = headers
		resp, err = callAPI(ctx, f.srv, opts, request, response)
		return f.shouldRetry(ctx, resp, err, action, tok, capTok)
	})
	return resp, err
}

// accessToken returns the Authorization header value
func (f *Fs) accessToken() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.token == nil {
		return ""
	}
	return f.token.Token()
}

// userID returns the ID of the logged in user
func (f *Fs) userID() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.token == nil {
		return ""
	}
	if f.token.UserID != "" {
		return f.token.UserID
	}
	return f.token.Sub
}

// captchaToken returns the captcha token to use for action
func (f *Fs) captchaToken(action string) string {
	f.captchaMu.Lock()
	defer f.captchaMu.Unlock()
	if tok, ok := f.captcha[action]; ok {
		return tok
	}
	return f.lastCap
}

// refreshCaptcha gets a new captcha token for action
//
// If the token for action is no longer used it has been refreshed
// by another goroutine already so nothing is done.
func (f *Fs) refreshCaptcha(ctx context.Context, action, used string, metas map[string]string) error {
	f.captchaMu.Lock()
	defer f.captchaMu.Unlock()

	current, ok := f.captcha[action]
	if !ok {
		current = f.lastCap
	}
	if used != "" && current != used {
		return nil
	}

	reqBody := api.CaptchaTokenRequest{
		Action:       action,
		CaptchaToken: current,
		ClientID:     clientID,
		DeviceID:     f.opt.DeviceID,
		Meta:         metas,
		RedirectURI:  "xlaccsdk01://xunlei.com/callback?state=harbor",
	}
	var resp api.CaptchaTokenResponse
	opts := rest.Opts{
		Method:  http.MethodPost,
		RootURL: xluserAPIURL,
		Path:    "/shield/captcha/init",
	}
	if err := f.authCall(ctx, &opts, &reqBody, &resp); err != nil {
		return err
	}
	if resp.URL != "" {
		return fmt.Errorf("verification required: %s", resp.URL)
	}
	if resp.CaptchaToken == "" {
		return errors.New("empty captcha token received")
	}
	if len(f.captcha) >= maxCaptchaTokens {
		clear(f.captcha)
	}
	f.captcha[action] = resp.CaptchaToken
	f.lastCap = resp.CaptchaToken
	return nil
}

// refreshCaptchaAtLogin refreshes the captcha token of action for the
// logged in user
func (f *Fs) refreshCaptchaAtLogin(ctx context.Context, action, used string) error {
	metas := map[string]string{
		"client_version": clientVersion,
		"package_name":   packageName,
		"user_id":        f.userID(),
	}
	metas["timestamp"], metas["captcha_sign"] = getCaptchaSign(f.opt.DeviceID)
	return f.refreshCaptcha(ctx, action, used, metas)
}

// refreshCaptchaInLogin refreshes the captcha token of action while
// logging in with the username
func (f *Fs) refreshCaptchaInLogin(ctx context.Context, action string) error {
	metas := map[string]string{}
	username := f.opt.Username
	if emailRegexp.MatchString(username) {
		metas["email"] = username
	} else if len(username) >= 11 && len(username) <= 18 {
		metas["phone_number"] = username
	} else {
		metas["username"] = username
	}
	return f.refreshCaptcha(ctx, action, "", metas)
}

// setToken stores a new token and persists the refresh token
//
// Call with tokenMu held
func (f *Fs) setToken(token *api.TokenResp) {
	if token.RefreshToken == "" && f.token != nil {
		// the server doesn't always rotate the refresh token
		token.RefreshToken = f.token.RefreshToken
	}
	f.token = token
	if token.RefreshToken != "" && token.RefreshToken != f.opt.RefreshToken {
		f.opt.RefreshToken = token.RefreshToken
		f.m.Set("refresh_token", token.RefreshToken)
	}
}

// refreshTokenLocked gets a new access token with the refresh token
//
// Call with tokenMu held
func (f *Fs) refreshTokenLocked(ctx context.Context) error {
	rToken := f.opt.RefreshToken
	if f.token != nil && f.token.RefreshToken != "" {
		rToken = f.token.RefreshToken
	}
	if rToken == "" {
		return errors.New("no refresh token available")
	}

	body := map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": rToken,
		"client_id":     clientID,
		"client_secret": clientSecret,
	}
	var resp api.TokenResp
	opts := rest.Opts{
		Method:  http.MethodPost,
		RootURL: xluserAPIURL,
		Path:    "/auth/token",
	}
	if err := f.authCall(ctx, &opts, &body, &resp); err != nil {
		return fmt.Errorf("refresh token: %w", err)
	}
	if resp.AccessToken == "" {
		return errors.New("refresh token: empty access token")
	}
	f.setToken(&resp)
	return nil
}

// loginLocked logs in with username and password
//
// Call with tokenMu held
func (f *Fs) loginLocked(ctx context.Context) error {
	if f.opt.Username == "" || f.opt.Password == "" {
		return errors.New("username and password or refresh_token required")
	}
	password, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return fmt.Errorf("failed to decode password - did you obscure it?: %w", err)
	}

	// Step 1: v3 core login to get a session ID
	coreReq := api.CoreLoginRequest{
		ProtocolVersion: "301",
		SequenceNo:      "1000012",
		PlatformVersion: "10",
		IsCompressed:    "0",
		Appid:           appID,
		ClientVersion:   clientVersion,
		PeerID:          "00000000000000000000000000000000",
		AppName:         "ANDROID-com.xunlei.downloadprovider",
		SdkVersion:      "512000",
		Devicesign:      generateDeviceSign(f.opt.DeviceID, packageName),
		NetWorkType:     "WIFI",
		ProviderName:    "NONE",
		DeviceModel:     "M2004J7AC",
		DeviceName:      "Xiaomi_M2004j7ac",
		OSVersion:       "12",
		Hl:              "zh-CN",
		UserName:        f.opt.Username,
		PassWord:        password,
		IsMd5Pwd:        "0",
	}
	var coreResp api.CoreLoginResp
	opts := rest.Opts{
		Method:       http.MethodPost,
		RootURL:      xluserAPIBase,
		Path:         "/xluser.core.login/v3/login",
		ExtraHeaders: map[string]string{"User-Agent": "android-ok-http-client/xl-acc-sdk/version-5.0.12.512000"},
	}
	if err := f.authCall(ctx, &opts, &coreReq, &coreResp); err != nil {
		return fmt.Errorf("core login failed: %w", err)
	}
	if coreResp.SessionID == "" {
		return fmt.Errorf("core login failed: %s %s (%s)", coreResp.Error, coreResp.ErrorDesc, coreResp.ErrorCode)
	}

	// Step 2: captcha token for the sign in
	signInURL := xluserAPIURL + "/auth/signin/token"
	if err := f.refreshCaptchaInLogin(ctx, getAction(http.MethodPost, signInURL)); err != nil {
		return fmt.Errorf("captcha token for sign in: %w", err)
	}

	// Step 3: v1 sign in to get the tokens
	signInReq := api.SignInRequest{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Provider:     signProvider,
		SigninToken:  coreResp.SessionID,
	}
	var tokenResp api.TokenResp
	signInOpts := rest.Opts{
		Method:  http.MethodPost,
		RootURL: xluserAPIURL,
		Path:    "/auth/signin/token",
	}
	if err := f.authCall(ctx, &signInOpts, &signInReq, &tokenResp); err != nil {
		return fmt.Errorf("sign in failed: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return errors.New("sign in failed: empty access token")
	}
	f.setToken(&tokenResp)
	return nil
}

// login logs in with the refresh token, falling back to the
// username and password
func (f *Fs) login(ctx context.Context) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.opt.RefreshToken != "" {
		err := f.refreshTokenLocked(ctx)
		if err == nil {
			return nil
		}
		fs.Logf(f, "refresh token login failed, falling back to credentials: %v", err)
	}
	return f.loginLocked(ctx)
}

// reAuthorize renews the access token which was rejected
//
// If the token in use is no longer used it has been renewed by
// another goroutine already so nothing is done.
func (f *Fs) reAuthorize(ctx context.Context, used string) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.token != nil && f.token.Token() != used {
		return nil
	}
	err := f.refreshTokenLocked(ctx)
	if err == nil {
		return nil
	}
	if f.opt.Username == "" || f.opt.Password == "" {
		return err
	}
	fs.Debugf(f, "refreshing token failed, logging in again: %v", err)
	return f.loginLocked(ctx)
}

// ------------------------------------------------------------
// Fs Implementation
// ------------------------------------------------------------

func checkUploadChunkSize(cs fs.SizeSuffix) error {
	if cs < minChunkSize {
		return fmt.Errorf("%s is less than %s", cs, minChunkSize)
	}
	if cs > maxChunkSize {
		return fmt.Errorf("%s is greater than %s", cs, maxChunkSize)
	}
	return nil
}

func (f *Fs) setUploadChunkSize(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	err = checkUploadChunkSize(cs)
	if err == nil {
		old, f.opt.ChunkSize = f.opt.ChunkSize, cs
	}
	return
}

func checkUploadCutoff(cs fs.SizeSuffix) error {
	if cs > maxUploadCutoff {
		return fmt.Errorf("%s is greater than %s", cs, maxUploadCutoff)
	}
	return nil
}

func (f *Fs) setUploadCutoff(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	err = checkUploadCutoff(cs)
	if err == nil {
		old, f.opt.UploadCutoff = f.opt.UploadCutoff, cs
	}
	return
}

// NewFs constructs an Fs from path and config
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if err = checkUploadChunkSize(opt.ChunkSize); err != nil {
		return nil, fmt.Errorf("thunder: chunk size: %w", err)
	}
	if err = checkUploadCutoff(opt.UploadCutoff); err != nil {
		return nil, fmt.Errorf("thunder: upload cutoff: %w", err)
	}

	if len(opt.DeviceID) != 32 {
		opt.DeviceID = genDeviceID()
		m.Set("device_id", opt.DeviceID)
		fs.Infof(nil, "Using new device id %q", opt.DeviceID)
	}

	client := fshttp.NewClient(ctx)
	f := &Fs{
		name:      name,
		root:      strings.Trim(root, "/"),
		opt:       *opt,
		m:         m,
		srv:       rest.NewClient(client).SetRoot(apiURL).SetErrorHandler(errorHandler),
		authSrv:   rest.NewClient(client).SetErrorHandler(errorHandler),
		client:    client,
		pacer:     fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		tokenMu:   new(sync.Mutex),
		captchaMu: new(sync.Mutex),
		captcha:   make(map[string]string),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		FindByListing:           true,
		// Estimates, not measurements
		MaxFileStreams: 3,
		MaxStreams:     8,
	}).Fill(ctx, f)

	if err = f.login(ctx); err != nil {
		return nil, fmt.Errorf("thunder: %w", err)
	}

	rootID := f.opt.RootFolderID
	if rootID == "" {
		rootID = "root"
	}
	f.dirCache = dircache.New(f.root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(f.root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
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
func (f *Fs) String() string { return fmt.Sprintf("Xunlei Drive root '%s'", f.root) }

// Precision returns precision of modtime
//
// Modification times can't be set, so they aren't used for syncing.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the supported hash types: the GCID
func (f *Fs) Hashes() hash.Set { return hash.NewHashSet(gcidType) }

// Features returns features
func (f *Fs) Features() *fs.Features { return f.features }

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// listAllFn is called for each item found by listAll
//
// Should return true to finish processing
type listAllFn func(*api.FileItem) bool

// listAll lists the folder with ID dirID calling fn on each item
//
// If fn ever returns true then it early exits with found = true
func (f *Fs) listAll(ctx context.Context, dirID string, fn listAllFn) (found bool, err error) {
	params := url.Values{}
	params.Set("parent_id", parentIDForRequest(dirID))
	params.Set("limit", "100")
	params.Set("with_audit", "true")
	params.Set("filters", `{"phase":{"eq":"PHASE_TYPE_COMPLETE"},"trashed":{"eq":false}}`)
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/files",
		Parameters: params,
	}
	pageToken := ""
	for {
		params.Set("page_token", pageToken)
		var resp api.FileListResp
		if _, err = f.call(ctx, &opts, nil, &resp); err != nil {
			return false, fmt.Errorf("couldn't list files: %w", err)
		}
		for i := range resp.Files {
			item := &resp.Files[i]
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				return true, nil
			}
		}
		if resp.NextPageToken == "" {
			return false, nil
		}
		pageToken = resp.NextPageToken
	}
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	found, err = f.listAll(ctx, pathID, func(item *api.FileItem) bool {
		if item.IsDir() && item.Name == leaf {
			pathIDOut = item.ID
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	body := map[string]any{
		"kind":      api.KindOfFolder,
		"name":      f.opt.Enc.FromStandardName(leaf),
		"parent_id": parentIDForRequest(pathID),
	}
	var resp api.UploadTaskResponse
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/files",
	}
	if _, err = f.call(ctx, &opts, &body, &resp); err != nil {
		return "", err
	}
	if resp.File.ID == "" {
		return "", fmt.Errorf("create directory %q: no ID returned", leaf)
	}
	return resp.File.ID, nil
}

// newObjectWithInfo makes an Object from an item in the folder dirID
func (f *Fs) newObjectWithInfo(remote, dirID string, item *api.FileItem) *Object {
	o := &Object{
		fs:     f,
		remote: remote,
		linkMu: new(sync.Mutex),
	}
	o.setMetaData(dirID, item)
	return o
}

// setMetaData sets the metadata of the object from item in the folder dirID
func (o *Object) setMetaData(dirID string, item *api.FileItem) {
	o.id = item.ID
	o.parentID = dirID
	o.size = item.GetSize()
	o.modTime = item.ModifiedTime
	o.gcid = item.Hash
	o.linkMu.Lock()
	o.setLink(item.WebContentLink)
	o.linkMu.Unlock()
}

// setLink keeps link as the download link with the expiry its URL
// states. linkMu must be held.
func (o *Object) setLink(link string) {
	o.link = link
	o.expires = time.Time{}
	if link != "" {
		o.expires = linkcache.ExpiresFromURL(link, time.Now())
	}
}

// linkExpired reports whether the download link is past, or close
// to, the expiry its URL states. linkMu must be held.
func (o *Object) linkExpired() bool {
	return !o.expires.IsZero() && time.Now().After(o.expires.Add(-linkExpiryMargin))
}

// List lists objects in dir
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	_, err = f.listAll(ctx, dirID, func(item *api.FileItem) bool {
		remote := path.Join(dir, item.Name)
		if item.IsDir() {
			f.dirCache.Put(remote, item.ID)
			entries = append(entries, fs.NewDir(remote, item.ModifiedTime).SetID(item.ID))
		} else {
			entries = append(entries, f.newObjectWithInfo(remote, dirID, item))
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// findFile finds the file called leaf in the folder dirID
func (f *Fs) findFile(ctx context.Context, dirID, leaf string) (info *api.FileItem, err error) {
	found, err := f.listAll(ctx, dirID, func(item *api.FileItem) bool {
		if !item.IsDir() && item.Name == leaf {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return info, nil
}

// NewObject finds object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	info, err := f.findFile(ctx, dirID, leaf)
	if err != nil {
		return nil, err
	}
	return f.newObjectWithInfo(remote, dirID, info), nil
}

// Put uploads an object
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existingObj, err := f.NewObject(ctx, src.Remote())
	switch err {
	case nil:
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		o := &Object{
			fs:     f,
			remote: src.Remote(),
			linkMu: new(sync.Mutex),
		}
		return o, o.upload(ctx, in, src, false, options...)
	default:
		return nil, err
	}
}

// Mkdir creates dir
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// trash moves the item with ID id to the trash
func (f *Fs) trash(ctx context.Context, id string) error {
	opts := rest.Opts{
		Method: http.MethodPatch,
		Path:   "/files/" + id + "/trash",
	}
	_, err := f.call(ctx, &opts, map[string]string{}, nil)
	return err
}

// purgeCheck removes the directory dir, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	if path.Join(f.root, dir) == "" {
		return errors.New("can't purge root directory")
	}
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if check {
		found, err := f.listAll(ctx, dirID, func(item *api.FileItem) bool {
			fs.Debugf(dir, "Rmdir: contains %q", item.Name)
			return true
		})
		if err != nil {
			return err
		}
		if found {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	// trashing a folder trashes everything in it
	if err = f.trash(ctx, dirID); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Rmdir removes the directory dir if it is empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, true)
}

// Purge moves the directory dir and all its contents to the trash
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

// batchAction runs a batch action (batchCopy, batchMove) on ids into dirID
func (f *Fs) batchAction(ctx context.Context, action string, ids []string, dirID string) error {
	body := map[string]any{
		"to":  map[string]string{"parent_id": parentIDForRequest(dirID)},
		"ids": ids,
	}
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/files:" + action,
	}
	_, err := f.call(ctx, &opts, &body, nil)
	return err
}

// rename renames the item with ID id to newName
func (f *Fs) rename(ctx context.Context, id, newName string) (*api.FileItem, error) {
	opts := rest.Opts{
		Method: http.MethodPatch,
		Path:   "/files/" + id,
	}
	var info api.FileItem
	_, err := f.call(ctx, &opts, map[string]string{"name": f.opt.Enc.FromStandardName(newName)}, &info)
	if err != nil {
		return nil, fmt.Errorf("rename failed: %w", err)
	}
	if info.ID == "" {
		return f.getFile(ctx, id)
	}
	info.Name = f.opt.Enc.ToStandardName(info.Name)
	return &info, nil
}

// getFile gets the item with ID id
func (f *Fs) getFile(ctx context.Context, id string) (*api.FileItem, error) {
	opts := rest.Opts{
		Method: http.MethodGet,
		Path:   "/files/" + id,
	}
	var info api.FileItem
	if _, err := f.call(ctx, &opts, nil, &info); err != nil {
		return nil, err
	}
	info.Name = f.opt.Enc.ToStandardName(info.Name)
	return &info, nil
}

var errNotVisible = errors.New("item not visible in the listing yet")

// waitForFile lists dirID until match returns true for one of its
// files, retrying as server-side operations can take a little while
// to show up. It returns the matching file and the file called
// leaf, if any, which isn't the matching file.
func (f *Fs) waitForFile(ctx context.Context, dirID, leaf string, match func(*api.FileItem) bool) (item, conflict *api.FileItem, err error) {
	err = f.pacer.Call(func() (bool, error) {
		item, conflict = nil, nil
		_, err := f.listAll(ctx, dirID, func(it *api.FileItem) bool {
			if it.IsDir() {
				return false
			}
			if item == nil && match(it) {
				item = it
			} else if it.Name == leaf {
				conflict = it
			}
			return false
		})
		if err != nil {
			return false, err
		}
		if item == nil {
			return true, errNotVisible
		}
		return false, nil
	})
	if item != nil && conflict != nil && conflict.ID == item.ID {
		conflict = nil
	}
	return item, conflict, err
}

// placeFile gives the file item in dirID the name dstLeaf, replacing
// the file conflict which already has that name, if any
func (f *Fs) placeFile(ctx context.Context, item, conflict *api.FileItem, dstLeaf string) (*api.FileItem, error) {
	if item.Name == dstLeaf {
		return item, nil
	}
	if conflict != nil {
		if err := f.trash(ctx, conflict.ID); err != nil {
			return nil, fmt.Errorf("couldn't remove existing file %q: %w", dstLeaf, err)
		}
	}
	return f.rename(ctx, item.ID, dstLeaf)
}

// Copy src to this remote using server-side copy operations.
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
	if srcObj.parentID == dstDirID {
		// The API can't copy into the source folder
		fs.Debugf(src, "Can't copy - same parent")
		return nil, fs.ErrorCantCopy
	}

	// Note the files already present so the copy can be identified by ID
	before := map[string]struct{}{}
	if _, err = f.listAll(ctx, dstDirID, func(item *api.FileItem) bool {
		before[item.ID] = struct{}{}
		return false
	}); err != nil {
		return nil, err
	}

	if err = f.batchAction(ctx, "batchCopy", []string{srcObj.id}, dstDirID); err != nil {
		return nil, fmt.Errorf("copy failed: %w", err)
	}

	// The copy is the new file with the source's content. It may
	// have a numbered name if the source name was taken.
	copied, conflict, err := f.waitForFile(ctx, dstDirID, dstLeaf, func(item *api.FileItem) bool {
		if _, ok := before[item.ID]; ok {
			return false
		}
		if item.GetSize() != srcObj.size {
			return false
		}
		return srcObj.gcid == "" || item.Hash == "" || strings.EqualFold(item.Hash, srcObj.gcid)
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't locate copied file: %w", err)
	}

	info, err := f.placeFile(ctx, copied, conflict, dstLeaf)
	if err != nil {
		if trashErr := f.trash(ctx, copied.ID); trashErr != nil {
			fs.Logf(f, "copy: couldn't remove copied file %q: %v", copied.Name, trashErr)
		}
		return nil, fmt.Errorf("copy: %w", err)
	}
	return f.newObjectWithInfo(remote, dstDirID, info), nil
}

// Move src to this remote using server-side move operations.
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

	if srcObj.parentID != dstDirID {
		// A numbered name might be given on name collision
		if err = f.batchAction(ctx, "batchMove", []string{srcObj.id}, dstDirID); err != nil {
			return nil, fmt.Errorf("move failed: %w", err)
		}
	}

	moved, conflict, err := f.waitForFile(ctx, dstDirID, dstLeaf, func(item *api.FileItem) bool {
		return item.ID == srcObj.id
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't locate moved file: %w", err)
	}
	info, err := f.placeFile(ctx, moved, conflict, dstLeaf)
	if err != nil {
		return nil, fmt.Errorf("move: %w", err)
	}
	return f.newObjectWithInfo(remote, dstDirID, info), nil
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
		if err = f.batchAction(ctx, "batchMove", []string{srcID}, dstParentID); err != nil {
			return fmt.Errorf("couldn't dir move: %w", err)
		}
	}
	if srcLeaf != dstLeaf {
		if _, err = f.rename(ctx, srcID, dstLeaf); err != nil {
			return fmt.Errorf("dirmove: couldn't rename moved dir: %w", err)
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

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string { return o.id }

// ParentID returns the ID of the Object parent if known, or "" if not
func (o *Object) ParentID() string { return o.parentID }

// Hash returns the GCID of the file as a lowercase hex string
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != gcidType {
		return "", hash.ErrUnsupported
	}
	return strings.ToLower(o.gcid), nil
}

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.trash(ctx, o.id)
}

// getLink returns the download link, fetching a fresh one if refresh
// is set, none is known or the one known has expired
func (o *Object) getLink(ctx context.Context, refresh bool) (string, error) {
	o.linkMu.Lock()
	defer o.linkMu.Unlock()
	if o.link != "" && !refresh {
		if !o.linkExpired() {
			return o.link, nil
		}
		fs.Debugf(o, "download link expired at %v, fetching a new one", o.expires)
	}
	info, err := o.fs.getFile(ctx, o.id)
	if err != nil {
		return "", err
	}
	if info.WebContentLink == "" {
		return "", fmt.Errorf("no download link available for %s", o.remote)
	}
	o.setLink(info.WebContentLink)
	return o.link, nil
}

// linkStatusError is returned when the download link gives an error
type linkStatusError struct {
	status int
}

func (e *linkStatusError) Error() string {
	return fmt.Sprintf("download failed: HTTP status %d (%s)", e.status, http.StatusText(e.status))
}

// expired reports whether the status means the link has expired
func (e *linkStatusError) expired() bool {
	switch e.status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusGone:
		return true
	}
	return false
}

// download opens link for reading
func (o *Object) download(ctx context.Context, link string, options []fs.OpenOption) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", downloadUA)
	fs.FixRangeOption(options, o.size)
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	var res *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		res, err = o.fs.client.Do(req)
		if err == nil && (res.StatusCode < 200 || res.StatusCode > 299) {
			_ = res.Body.Close()
			err = &linkStatusError{status: res.StatusCode}
		}
		return shouldRetryNoAuth(ctx, res, err)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Open opens the file for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.id == "" {
		return nil, errors.New("can't download: no id")
	}
	if o.size == 0 {
		// zero-byte objects may have no download link
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	link, err := o.getLink(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("can't download: %w", err)
	}
	res, err := o.download(ctx, link, options)
	var statusErr *linkStatusError
	if errors.As(err, &statusErr) && statusErr.expired() {
		fs.Debugf(o, "download link expired, fetching a new one: %v", err)
		if link, err = o.getLink(ctx, true); err != nil {
			return nil, fmt.Errorf("can't download: %w", err)
		}
		res, err = o.download(ctx, link, options)
	}
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// Update uploads new content, replacing the object
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return o.upload(ctx, in, src, true, options...)
}

// spoolToTemp copies in to a temporary file while calculating its
// GCID. It returns the temporary file positioned at the start, its
// size and a function to remove it.
func spoolToTemp(in io.Reader, size int64) (out *os.File, gcid string, n int64, cleanup func(), err error) {
	tmp, err := os.CreateTemp("", "rclone-thunder-upload-*")
	if err != nil {
		return nil, "", 0, func() {}, fmt.Errorf("failed to create temporary file: %w", err)
	}
	cleanup = func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}
	h := newGcidHasher()
	n, err = io.Copy(io.MultiWriter(tmp, h), in)
	if err != nil {
		return nil, "", 0, cleanup, fmt.Errorf("failed to write temporary file: %w", err)
	}
	if size >= 0 && n != size {
		return nil, "", 0, cleanup, fmt.Errorf("expected %d bytes in input, but got %d: %w", size, n, io.ErrUnexpectedEOF)
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return nil, "", 0, cleanup, fmt.Errorf("failed to rewind temporary file: %w", err)
	}
	return tmp, hex.EncodeToString(h.Sum(nil)), n, cleanup, nil
}

// upload uploads the object, to a temporary name first then replacing
// the existing object if withTemp is set
func (o *Object) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, withTemp bool, options ...fs.OpenOption) error {
	f := o.fs
	size := src.Size()
	leaf, dirID, err := f.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}

	// The upload needs the GCID up front: use the source's if it has
	// one (a local file computes it by reading the file), otherwise
	// spool the input to a temporary file while hashing it.
	gcid, err := src.Hash(ctx, gcidType)
	if err != nil || len(gcid) != 40 || size < 0 {
		fs.Debugf(o, "spooling to a temporary file to calculate gcid (source hash: %v)", err)
		tmp, tmpGcid, n, cleanup, err := spoolToTemp(in, size)
		defer cleanup()
		if err != nil {
			return err
		}
		in, gcid, size = tmp, tmpGcid, n
	}
	fs.Debugf(o, "gcid = %s", gcid)

	name := leaf
	if withTemp {
		name = "rcloneTemp" + random.String(8)
	}
	info, err := f.upload(ctx, in, o.remote, name, dirID, gcid, size, options...)
	if err != nil {
		return err
	}

	if withTemp {
		// upload was successful, need to delete old object before rename
		if err = o.Remove(ctx); err != nil {
			if trashErr := f.trash(ctx, info.ID); trashErr != nil {
				fs.Logf(o, "couldn't remove temporary upload %q: %v", name, trashErr)
			}
			return fmt.Errorf("failed to remove old object: %w", err)
		}
	}
	if info.Name != leaf {
		if info, err = f.rename(ctx, info.ID, leaf); err != nil {
			return fmt.Errorf("failed to rename uploaded file: %w", err)
		}
	}
	o.setMetaData(dirID, info)
	return nil
}

// upload uploads in as name to the folder dirID returning the new file
func (f *Fs) upload(ctx context.Context, in io.Reader, remote, name, dirID, gcid string, size int64, options ...fs.OpenOption) (info *api.FileItem, err error) {
	body := map[string]any{
		"kind":        api.KindOfFile,
		"parent_id":   parentIDForRequest(dirID),
		"name":        f.opt.Enc.FromStandardName(name),
		"size":        size,
		"hash":        strings.ToUpper(gcid),
		"upload_type": api.UploadTypeResumable,
	}
	var resp api.UploadTaskResponse
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/files",
	}
	if _, err = f.call(ctx, &opts, &body, &resp); err != nil {
		return nil, fmt.Errorf("init upload failed: %w", err)
	}
	file := resp.File
	if file.ID == "" {
		return nil, fmt.Errorf("init upload failed: no file ID in response: %+v", resp)
	}
	file.Name = f.opt.Enc.ToStandardName(file.Name)

	// Rapid upload: the server already has content with this GCID
	if file.Phase == api.PhaseTypeComplete || resp.UploadType != api.UploadTypeResumable {
		if acc, ok := in.(*accounting.Account); ok && acc != nil {
			// the input is unused so no data was transferred
			acc.ServerSideTransferStart()
			acc.ServerSideCopyEnd(size)
		}
		return &file, nil
	}
	if resp.Resumable == nil {
		return nil, fmt.Errorf("init upload failed: no upload parameters in response: %+v", resp)
	}

	if err = f.uploadToS3(ctx, in, remote, size, &resp.Resumable.Params, options...); err != nil {
		if trashErr := f.trash(ctx, file.ID); trashErr != nil {
			fs.Logf(remote, "failed to cancel upload: %v", trashErr)
		}
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	// Wait for the server to complete the file
	var fresh *api.FileItem
	err = f.pacer.Call(func() (bool, error) {
		fresh, err = f.getFile(ctx, file.ID)
		if err != nil {
			return false, err
		}
		if fresh.Phase != "" && fresh.Phase != api.PhaseTypeComplete {
			return true, fmt.Errorf("uploaded file is in phase %s", fresh.Phase)
		}
		return false, nil
	})
	if err != nil {
		fs.Debugf(remote, "couldn't read uploaded file info, using upload response: %v", err)
		file.Size = strconv.FormatInt(size, 10)
		file.Hash = gcid
		return &file, nil
	}
	return fresh, nil
}

// ------------------------------------------------------------
// Offline download
// ------------------------------------------------------------

// offlineStatus converts a task phase into an OfflineTaskStatus
func offlineStatus(phase string) fs.OfflineTaskStatus {
	switch phase {
	case api.PhaseTypeComplete:
		return fs.OfflineTaskStatusComplete
	case api.PhaseTypeError:
		return fs.OfflineTaskStatusError
	}
	return fs.OfflineTaskStatusRunning
}

// AddURL submits a remote URL to be downloaded server-side.
func (f *Fs) AddURL(ctx context.Context, rawURL string, opt map[string]string) (*fs.OfflineTask, error) {
	dirID, err := f.dirCache.FindDir(ctx, opt["path"], false)
	if err != nil {
		return nil, fmt.Errorf("find target dir failed: %w", err)
	}

	body := map[string]any{
		"kind":        api.KindOfFile,
		"name":        f.opt.Enc.FromStandardName(opt["name"]),
		"parent_id":   parentIDForRequest(dirID),
		"upload_type": api.UploadTypeURL,
		"url": map[string]string{
			"url": rawURL,
		},
	}
	var resp api.UploadTaskResponse
	opts := rest.Opts{
		Method: http.MethodPost,
		Path:   "/files",
	}
	if _, err = f.call(ctx, &opts, &body, &resp); err != nil {
		return nil, fmt.Errorf("add offline download failed: %w", err)
	}
	if resp.Task == nil {
		return nil, errors.New("add offline download failed: no task in response")
	}
	fileSize, _ := strconv.ParseInt(resp.Task.FileSize, 10, 64)
	return &fs.OfflineTask{
		ID:        resp.Task.ID,
		Name:      f.opt.Enc.ToStandardName(resp.Task.Name),
		URL:       rawURL,
		Size:      fileSize,
		Status:    fs.OfflineTaskStatusInit,
		CreatedAt: time.Now(),
	}, nil
}

// ListOfflineTasks returns server-side offline download tasks, paging
// back to since.
func (f *Fs) ListOfflineTasks(ctx context.Context, since time.Time) ([]*fs.OfflineTask, error) {
	params := url.Values{}
	params.Set("type", "offline")
	params.Set("limit", "100")
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/tasks",
		Parameters: params,
	}
	var tasks []*fs.OfflineTask
	pageToken := ""
	for {
		params.Set("page_token", pageToken)
		var resp api.TaskListResp
		if _, err := f.call(ctx, &opts, nil, &resp); err != nil {
			return nil, fmt.Errorf("list offline tasks failed: %w", err)
		}
		var page []*fs.OfflineTask
		for _, t := range resp.Tasks {
			fileSize, _ := strconv.ParseInt(t.FileSize, 10, 64)
			page = append(page, &fs.OfflineTask{
				ID:        t.ID,
				Name:      f.opt.Enc.ToStandardName(t.Name),
				Size:      fileSize,
				Status:    offlineStatus(t.Phase),
				Progress:  float64(t.Progress),
				ErrorMsg:  t.Message,
				CreatedAt: t.Created(),
			})
		}
		tasks = append(tasks, page...)
		if resp.NextPageToken == "" || len(resp.Tasks) == 0 || fs.OfflineTasksBefore(page, since) {
			return tasks, nil
		}
		pageToken = resp.NextPageToken
	}
}

// DeleteOfflineTask deletes a server-side offline download task.
func (f *Fs) DeleteOfflineTask(ctx context.Context, taskID string) error {
	params := url.Values{}
	params.Set("task_ids", taskID)
	params.Set("delete_files", "false")
	opts := rest.Opts{
		Method:     http.MethodDelete,
		Path:       "/tasks",
		Parameters: params,
	}
	_, err := f.call(ctx, &opts, nil, nil)
	return err
}

var commandHelp = []fs.CommandHelp{{
	Name:  "addurl",
	Short: "Add offline download task for url.",
	Long: `This command adds an offline (cloud-side) download task for a url
(http, magnet or ed2k). It is also available as "offline".

Usage examples:

` + "```console" + `
rclone backend addurl thunder:dirpath url
rclone backend addurl thunder:dirpath url -o name=custom_filename.zip
` + "```" + `

Downloads will be stored in 'dirpath', which must exist.

The result is the new task as JSON with the fields "id", "name",
"url", "size", "status" ("init"), "progress" and "created_at".`,
	Opts: map[string]string{
		"name": "Custom filename for the downloaded file.",
	},
}, {
	Name:  "tasks",
	Short: "List offline download tasks.",
	Long: `This command lists the offline download tasks. It is also
available as "listoffline".

Usage example:

` + "```console" + `
rclone backend tasks thunder:
` + "```" + `

The result is a JSON list of tasks with the fields "id", "name",
"size", "status" ("running", "complete" or "error"), "progress"
(percentage), "error_msg" and "created_at".`,
}, {
	Name:  "deltask",
	Short: "Delete an offline download task.",
	Long: `This command deletes the offline download task with the given ID.
Files already downloaded are kept. It is also available as
"deleteoffline".

Usage example:

` + "```console" + `
rclone backend deltask thunder: task_id
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

// Check interfaces
var (
	_ fs.Fs                = (*Fs)(nil)
	_ fs.Purger            = (*Fs)(nil)
	_ fs.Copier            = (*Fs)(nil)
	_ fs.Mover             = (*Fs)(nil)
	_ fs.DirMover          = (*Fs)(nil)
	_ fs.Commander         = (*Fs)(nil)
	_ fs.DirCacheFlusher   = (*Fs)(nil)
	_ fs.OfflineDownloader = (*Fs)(nil)
	_ fs.Object            = (*Object)(nil)
	_ fs.IDer              = (*Object)(nil)
	_ fs.ParentIDer        = (*Object)(nil)
)
