package rcserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/objcache"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServeObjectVersions(t *testing.T) {
	ctx := context.Background()
	configfile.Install()
	dir := t.TempDir()
	file := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(file, []byte("hello"), 0o600))

	opt := newTestOpt()
	opt.Serve = true
	opt.NoAuth = true
	opt.Template.Path = defaultTestTemplate
	s, err := newServer(ctx, &opt, http.NewServeMux())
	require.NoError(t, err)
	t.Cleanup(func() { objcache.Default.SetLimits(0, 0) })

	get := func(ifMatch string) (*http.Response, string) {
		req := httptest.NewRequest("GET", "http://1.2.3.4/["+dir+"]/a.txt", nil)
		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}
		w := httptest.NewRecorder()
		s.server.Router().ServeHTTP(w, req)
		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp, string(body)
	}

	resp, body := get("")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello", body)
	v1 := resp.Header.Get("ETag")
	assert.NotEmpty(t, v1)

	f, err := cache.Get(ctx, dir)
	require.NoError(t, err)
	assert.NotNil(t, objcache.Default.Get(f, "a.txt"), "the object is cached")

	resp, _ = get(v1)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, v1, resp.Header.Get("ETag"))

	// A version nobody has is refused
	resp, _ = get(`"other"`)
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)

	// Change the file and stat it, as Mavio does before reading
	require.NoError(t, os.WriteFile(file, []byte("hello, world"), 0o600))
	require.NoError(t, os.Chtimes(file, time.Now(), time.Now().Add(time.Hour)))
	_, err = operations.StatJSON(ctx, f, "a.txt", &operations.ListJSONOpt{})
	require.NoError(t, err)

	resp, body = get("")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello, world", body)
	v2 := resp.Header.Get("ETag")
	assert.NotEqual(t, v1, v2)

	// A reader pinned to the old version is told it changed
	resp, _ = get(v1)
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)

	// A removed file is not found, though it was cached
	require.NoError(t, os.Remove(file))
	resp, _ = get(v2)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// taggedObject is an object whose backend knows its version.
type taggedObject struct {
	fs.Object
	tag string
}

func (o taggedObject) VersionTag() string { return o.tag }

func TestObjectETagVersionTag(t *testing.T) {
	ctx := context.Background()
	f, err := mockfs.NewFs(ctx, "remote", "", nil)
	require.NoError(t, err)
	o := mockobject.New("a").WithContent([]byte("hello"), mockobject.SeekModeNone)
	o.SetFs(f)
	plain := objectETag(ctx, o)
	v1 := objectETag(ctx, taggedObject{o, `"v1"`})
	assert.NotEqual(t, plain, v1, "the version tag names the version")
	assert.NotEqual(t, v1, objectETag(ctx, taggedObject{o, `"v2"`}), "same size, another version")
	assert.Equal(t, v1, objectETag(ctx, taggedObject{o, "v1"}), "quotes are ignored")
}

// staleObject is a cached object whose storage replaced it: opening it
// fails.
type staleObject struct {
	taggedObject
}

func (staleObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, errors.New("precondition failed")
}

func TestServeObjectStale(t *testing.T) {
	ctx := context.Background()
	opt := newTestOpt()
	opt.Serve = true
	opt.NoAuth = true
	opt.Template.Path = defaultTestTemplate
	s, err := newServer(ctx, &opt, http.NewServeMux())
	require.NoError(t, err)
	objcache.Default.SetLimits(time.Minute, 100)
	t.Cleanup(func() { objcache.Default.SetLimits(0, 0) })

	f, err := mockfs.NewFs(ctx, "stale", "", nil)
	require.NoError(t, err)
	oldContent := mockobject.New("a.txt").WithContent([]byte("hello"), mockobject.SeekModeNone)
	oldContent.SetFs(f)
	stale := staleObject{taggedObject{oldContent, `"v1"`}}
	newContent := mockobject.New("a.txt").WithContent([]byte("hello, world"), mockobject.SeekModeNone)
	newContent.SetFs(f)
	fresh := taggedObject{newContent, `"v2"`}
	f.(*mockfs.Fs).AddObject(fresh)

	get := func(ifMatch string) (*http.Response, string) {
		objcache.Default.Put(f, stale)
		req := httptest.NewRequest("GET", "http://1.2.3.4/stale:a.txt", nil)
		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}
		w := httptest.NewRecorder()
		s.serveObject(w, req, f, "a.txt")
		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp, string(body)
	}

	// A read without a version gets the fresh object from the start
	resp, body := get("")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello, world", body)
	assert.Equal(t, objectETag(ctx, fresh), resp.Header.Get("ETag"))
	assert.Equal(t, "12", resp.Header.Get("Content-Length"))

	// A read pinned to the stale version is told it changed
	resp, _ = get(objectETag(ctx, stale))
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
}

func TestStreamLimit(t *testing.T) {
	for _, tc := range []struct{ option, backend, want int }{
		{0, 0, 0},
		{0, 4, 4},
		{2, 4, 2},
		{2, 0, 2},
		{-1, 4, 0},
	} {
		assert.Equal(t, tc.want, streamLimit(tc.option, tc.backend), "option %d, backend %d", tc.option, tc.backend)
	}
}
