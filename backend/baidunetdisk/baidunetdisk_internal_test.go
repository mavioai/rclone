package baidunetdisk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkServer serves the dlink API, the dlinks it hands out, which
// redirect to the storage, and the storage, which refuses the first
// refuse downloads with a 403 and answers the first busy with a 503.
type linkServer struct {
	*httptest.Server
	dlinks    atomic.Int32
	heads     atomic.Int32
	downloads atomic.Int32
	refuse    atomic.Int32
	busy      atomic.Int32
}

func newLinkServer(t *testing.T) *linkServer {
	s := &linkServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /xpan/multimedia", func(w http.ResponseWriter, r *http.Request) {
		n := s.dlinks.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errno": 0,
			"list":  []map[string]string{{"dlink": s.URL + "/dlink?n=" + strconv.Itoa(int(n))}},
		})
	})
	mux.HandleFunc("HEAD /dlink", func(w http.ResponseWriter, r *http.Request) {
		s.heads.Add(1)
		if r.Header.Get("User-Agent") != "pan.baidu.com" || r.URL.Query().Get("access_token") != "token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		http.Redirect(w, r, "/file?n="+r.URL.Query().Get("n"), http.StatusFound)
	})
	mux.HandleFunc("GET /file", func(w http.ResponseWriter, r *http.Request) {
		s.downloads.Add(1)
		if r.Header.Get("User-Agent") != "pan.baidu.com" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if s.busy.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if s.refuse.Add(-1) >= 0 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "content")
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func newTestFs(t *testing.T, s *linkServer) *Fs {
	ctx := context.Background()
	noRedir := *s.Client()
	noRedir.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Fs{
		name:        t.Name(),
		opt:         Options{DownloadAPI: "official"},
		srv:         rest.NewClient(s.Client()).SetRoot(s.URL).SetErrorHandler(errorHandler),
		httpCl:      s.Client(),
		noRedirCl:   &noRedir,
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		accessToken: "token",
	}
}

func readAll(t *testing.T, o *Object) string {
	in, err := o.Open(context.Background())
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	return string(data)
}

// Two opens of one object request one dlink and resolve it once
func TestOpenSharesLink(t *testing.T) {
	s := newLinkServer(t)
	o := &Object{fs: newTestFs(t, s), remote: "a.mkv", fsID: 1, size: 7, md5: "md5"}
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, int32(1), s.dlinks.Load(), "dlinks requested")
	assert.Equal(t, int32(1), s.heads.Load(), "redirects resolved")
	assert.Equal(t, int32(2), s.downloads.Load(), "downloads")
}

// A refused link is requested and resolved again once; a busy storage
// is retried by the pacer
func TestOpenRenewsRefusedLink(t *testing.T) {
	s := newLinkServer(t)
	o := &Object{fs: newTestFs(t, s), remote: "a.mkv", fsID: 1, size: 7, md5: "md5"}
	assert.Equal(t, "content", readAll(t, o))
	s.refuse.Store(1)
	s.busy.Store(1)
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, int32(2), s.dlinks.Load(), "dlinks requested")
	assert.Equal(t, int32(2), s.heads.Load(), "redirects resolved")
	assert.Equal(t, int32(4), s.downloads.Load(), "downloads")
}
