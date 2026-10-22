package guangyadrive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

// linkServer serves the download URL API and the storage, which
// refuses the first refuse downloads with a 403.
type linkServer struct {
	*httptest.Server
	links     atomic.Int32
	downloads atomic.Int32
	refuse    atomic.Int32
}

func newLinkServer(t *testing.T) *linkServer {
	s := &linkServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nd.bizuserres.s/v1/get_res_download_url", func(w http.ResponseWriter, r *http.Request) {
		s.links.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"msg":  "success",
			"data": map[string]string{"signedURL": s.URL + "/file"},
		})
	})
	mux.HandleFunc("GET /file", func(w http.ResponseWriter, r *http.Request) {
		s.downloads.Add(1)
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

func newLinkTestFs(t *testing.T, s *linkServer) *Fs {
	ctx := context.Background()
	return &Fs{
		name:        t.Name(),
		srv:         rest.NewClient(s.Client()).SetRoot(s.URL).SetErrorHandler(errorHandler),
		rawSrv:      rest.NewClient(s.Client()).SetErrorHandler(rawErrorHandler),
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		tokenMu:     new(sync.Mutex),
		accessToken: "token",
	}
}

func readObject(t *testing.T, o *Object) string {
	in, err := o.Open(context.Background())
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	return string(data)
}

// Two opens of one object request one link, and a refused link is
// requested again once
func TestOpenLinkCache(t *testing.T) {
	s := newLinkServer(t)
	o := &Object{fs: newLinkTestFs(t, s), remote: "a.mkv", id: "id", size: 7, modTime: time.Unix(1, 0)}
	assert.Equal(t, "content", readObject(t, o))
	assert.Equal(t, "content", readObject(t, o))
	assert.Equal(t, int32(1), s.links.Load(), "links requested")
	s.refuse.Store(1)
	assert.Equal(t, "content", readObject(t, o))
	assert.Equal(t, int32(2), s.links.Load(), "links requested")
	assert.Equal(t, int32(4), s.downloads.Load(), "downloads")
}
