package thunder

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

// linkServer serves the file info API, which hands out links expiring
// in an hour, and the storage, which refuses links past their expiry.
type linkServer struct {
	*httptest.Server
	infos     atomic.Int32
	downloads atomic.Int32
}

func newLinkServer(t *testing.T) *linkServer {
	s := &linkServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /files/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.infos.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind":             "drive#file",
			"id":               r.PathValue("id"),
			"size":             "7",
			"web_content_link": s.URL + "/file?e=" + strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10),
		})
	})
	mux.HandleFunc("GET /file", func(w http.ResponseWriter, r *http.Request) {
		s.downloads.Add(1)
		e, _ := strconv.ParseInt(r.URL.Query().Get("e"), 10, 64)
		if time.Now().Unix() >= e {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "content")
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func newLinkTestFs(s *linkServer) *Fs {
	return &Fs{
		srv:       rest.NewClient(s.Client()).SetRoot(s.URL).SetErrorHandler(errorHandler),
		client:    s.Client(),
		pacer:     fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		tokenMu:   new(sync.Mutex),
		captchaMu: new(sync.Mutex),
		captcha:   make(map[string]string),
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

// A link close to the expiry its URL states is renewed before use
func TestOpenRenewsExpiringLink(t *testing.T) {
	s := newLinkServer(t)
	o := &Object{fs: newLinkTestFs(s), remote: "a.mkv", id: "id", size: 7, linkMu: new(sync.Mutex)}
	o.setLink(s.URL + "/file?e=" + strconv.FormatInt(time.Now().Add(linkExpiryMargin/2).Unix(), 10))
	assert.Equal(t, "content", readObject(t, o))
	assert.Equal(t, int32(1), s.infos.Load(), "links requested")
	assert.Equal(t, int32(1), s.downloads.Load(), "downloads")

	// The renewed link is kept
	assert.Equal(t, "content", readObject(t, o))
	assert.Equal(t, int32(1), s.infos.Load(), "links requested")
}

// A link whose URL states no expiry is used until refused
func TestLinkExpired(t *testing.T) {
	o := &Object{linkMu: new(sync.Mutex)}
	o.setLink("https://example.com/file")
	assert.False(t, o.linkExpired())
	o.setLink("https://example.com/file?e=" + strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	assert.False(t, o.linkExpired())
	o.setLink("https://example.com/file?e=" + strconv.FormatInt(time.Now().Add(linkExpiryMargin/2).Unix(), 10))
	assert.True(t, o.linkExpired())
}
