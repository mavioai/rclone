package aliyundrive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkServer serves the download URL API and the download URLs it
// hands out, refusing the first refuse downloads with a 403.
type linkServer struct {
	*httptest.Server
	links     atomic.Int32
	downloads atomic.Int32
	refuse    atomic.Int32
}

func newLinkServer(t *testing.T) *linkServer {
	s := &linkServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /adrive/v1.0/openFile/getDownloadUrl", func(w http.ResponseWriter, r *http.Request) {
		n := s.links.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"url":        s.URL + "/file?n=" + strconv.Itoa(int(n)),
			"expiration": time.Now().Add(4 * time.Hour).UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("GET /file", func(w http.ResponseWriter, r *http.Request) {
		s.downloads.Add(1)
		if s.refuse.Add(-1) >= 0 {
			http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)
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
	return &Fs{
		name:        t.Name(),
		srv:         rest.NewClient(s.Client()).SetRoot(s.URL).SetErrorHandler(errorHandler),
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		tokenMu:     new(sync.Mutex),
		accessToken: "token",
		tokenExpiry: time.Now().Add(time.Hour),
		driveID:     "drive",
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

// Two opens of one object request one link
func TestOpenSharesLink(t *testing.T) {
	s := newLinkServer(t)
	f := newTestFs(t, s)
	o := &Object{fs: f, remote: "a.mkv", id: "id", size: 7, contentHash: "hash"}
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, int32(1), s.links.Load(), "links requested")
	assert.Equal(t, int32(2), s.downloads.Load(), "downloads")
}

// A refused link is requested again once
func TestOpenRenewsRefusedLink(t *testing.T) {
	s := newLinkServer(t)
	f := newTestFs(t, s)
	o := &Object{fs: f, remote: "a.mkv", id: "id", size: 7, contentHash: "hash"}
	assert.Equal(t, "content", readAll(t, o))
	s.refuse.Store(1)
	assert.Equal(t, "content", readAll(t, o))
	assert.Equal(t, int32(2), s.links.Load(), "links requested")
	assert.Equal(t, int32(3), s.downloads.Load(), "downloads")
}
