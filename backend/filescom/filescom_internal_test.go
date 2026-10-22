package filescom

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Files-com/files-sdk-go/v3/file"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenCachesLink checks that two Opens of one object request one
// download link, and that a link the storage refuses is requested again.
func TestOpenCachesLink(t *testing.T) {
	ctx := context.Background()
	var links, downloads atomic.Int32
	var refuse atomic.Bool
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/api/rest/v1/files/dir/file.mkv", func(w http.ResponseWriter, r *http.Request) {
		n := links.Add(1)
		date := time.Now().UTC().Format("20060102T150405Z")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"path":"dir/file.mkv","type":"file","size":5,"download_uri":%q}`,
			fmt.Sprintf("%s/dl/%d?X-Files-Date=%s&X-Files-Expires=3600", srv.URL, n, date))
	})
	mux.HandleFunc("/dl/", func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if refuse.Swap(false) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "expired")
			return
		}
		assert.Equal(t, "bytes=1-4", r.Header.Get("Range"))
		w.Header().Set("Content-Range", "bytes 1-4/5")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "ello")
	})

	opt := &Options{Site: srv.URL, APIKey: "key", Enc: encoder.Base}
	config, err := newClientConfig(ctx, opt)
	require.NoError(t, err)
	config.DisableDirectTransfers = true
	f := &Fs{
		name:       t.Name(),
		root:       "dir",
		opt:        *opt,
		fileClient: &file.Client{Config: config},
		pacer:      fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	o := &Object{fs: f, remote: "file.mkv", size: 5, modTime: time.Unix(1, 0), md5: "x"}

	open := func() {
		in, err := o.Open(ctx, &fs.RangeOption{Start: 1, End: -1})
		require.NoError(t, err)
		data, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		assert.Equal(t, "ello", string(data))
	}
	open()
	open()
	assert.Equal(t, int32(1), links.Load(), "links")
	assert.Equal(t, int32(2), downloads.Load(), "downloads")

	refuse.Store(true)
	open()
	assert.Equal(t, int32(2), links.Load(), "links after refusal")
	assert.Equal(t, int32(4), downloads.Load(), "downloads after refusal")
}
