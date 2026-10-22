//go:build !plan9

package weiyun

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/linkcache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A download's 503 is retried by the pacer, and its 403 is recognised
// so that linkcache requests a new link; the link's cookie is sent
func TestOpenLink(t *testing.T) {
	var downloads, busy, refuse atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if r.Header.Get("Cookie") != "FTN5K=abc" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if busy.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if refuse.Add(-1) >= 0 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "content")
	}))
	defer ts.Close()
	f := &Fs{
		name:   t.Name(),
		httpCl: ts.Client(),
		pacer:  fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	o := &Object{fs: f, remote: "a.mkv", id: "id", size: 7}

	var fetches atomic.Int32
	fetch := func(context.Context) (*linkcache.Link, error) {
		fetches.Add(1)
		return &linkcache.Link{URL: ts.URL, Header: http.Header{"Cookie": {"FTN5K=abc"}}}, nil
	}
	cache := linkcache.New(10)
	open := func() (string, error) {
		in, err := cache.Open(context.Background(), "key", time.Minute, fetch, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
			return o.openLink(ctx, link, nil)
		})
		if err != nil {
			return "", err
		}
		data, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		return string(data), nil
	}
	busy.Store(1)
	refuse.Store(1)
	got, err := open()
	require.NoError(t, err)
	assert.Equal(t, "content", got)
	assert.Equal(t, int32(2), fetches.Load())
	assert.Equal(t, int32(3), downloads.Load())

	// A storage busy beyond the retries asks for a retry higher up
	busy.Store(100)
	_, err = open()
	require.Error(t, err)
	assert.True(t, fserrors.IsRetryError(err))
}
