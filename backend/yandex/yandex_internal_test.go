package yandex

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
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
	mux.HandleFunc("/resources/download", func(w http.ResponseWriter, r *http.Request) {
		n := links.Add(1)
		_, _ = fmt.Fprintf(w, `{"href":%q,"method":"GET"}`, fmt.Sprintf("%s/dl/%d", srv.URL, n))
	})
	mux.HandleFunc("/dl/", func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if refuse.Swap(false) {
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		assert.Equal(t, "bytes=1-", r.Header.Get("Range"))
		_, _ = io.WriteString(w, "ello")
	})

	f := &Fs{
		name:  t.Name(),
		srv:   rest.NewClient(srv.Client()).SetRoot(srv.URL).SetErrorHandler(errorHandler),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	f.setRoot("")
	o := &Object{fs: f, remote: "file.mkv", size: 5, modTime: time.Unix(1, 0), md5sum: "x", hasMetaData: true}

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
