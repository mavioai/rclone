package cloud115

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/linkcache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestFs(t *testing.T, client *http.Client) *Fs {
	return &Fs{
		name:  t.Name(),
		pacer: fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		srv:   rest.NewClient(client),
	}
}

// An API answer of 429 reaches shouldRetry with its response, so the
// call is retried
func TestCallRetriesTooManyRequests(t *testing.T) {
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"state":true}`)
	}))
	defer ts.Close()
	client := &http.Client{Transport: &statusTransport{base: ts.Client().Transport}}
	f := newTestFs(t, client)

	err := f.call(context.Background(), func() error {
		resp, err := client.Get(ts.URL)
		if err != nil {
			return err
		}
		return resp.Body.Close()
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), requests.Load())

	// The error carries the response and its status
	requests.Store(0)
	resp, err := client.Get(ts.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	resp = responseOf(err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	retry, _ := f.shouldRetry(context.Background(), resp, err)
	assert.True(t, retry)
}

// An API call blocked by 115's firewall, which answers 405 with an HTML
// page, is retried after blockedWait, with an error that names the block
func TestShouldRetryBlocked(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "<!doctypehtml><html><title>405</title></html>")
	}))
	defer ts.Close()
	client := &http.Client{Transport: &statusTransport{base: ts.Client().Transport}}
	f := newTestFs(t, client)

	resp, err := client.Get(ts.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "firewall blocked")
	assert.NotContains(t, err.Error(), "doctype")
	retry, err := f.shouldRetry(context.Background(), responseOf(err), err)
	assert.True(t, retry)
	after, ok := pacer.IsRetryAfter(err)
	assert.True(t, ok)
	assert.Equal(t, blockedWait, after)
}

// A download refused with a 403 is recognised, so linkcache requests
// a new link; the link's headers are sent
func TestOpenLinkRefused(t *testing.T) {
	var downloads atomic.Int32
	var refuse atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if r.Header.Get("Cookie") != "UID=1" || r.Header.Get("User-Agent") != userAgentOf(defaultAppVersion) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if refuse.Add(-1) >= 0 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "content")
	}))
	defer ts.Close()
	f := newTestFs(t, ts.Client())
	o := &Object{fs: f, remote: "a.mkv", pickCode: "pc", size: 7, sha1: "sha1"}

	var fetches atomic.Int32
	fetch := func(context.Context) (*linkcache.Link, error) {
		fetches.Add(1)
		return &linkcache.Link{URL: ts.URL, Header: http.Header{"Cookie": {"UID=1"}, "User-Agent": {userAgentOf(defaultAppVersion)}}}, nil
	}
	cache := linkcache.New(10)
	open := func() string {
		in, err := cache.Open(context.Background(), "key", time.Minute, fetch, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
			return o.openLink(ctx, link, nil)
		})
		require.NoError(t, err)
		data, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		return string(data)
	}
	assert.Equal(t, "content", open())
	assert.Equal(t, "content", open())
	assert.Equal(t, int32(1), fetches.Load())
	refuse.Store(1)
	assert.Equal(t, "content", open())
	assert.Equal(t, int32(2), fetches.Load())
	assert.Equal(t, int32(4), downloads.Load())
}
