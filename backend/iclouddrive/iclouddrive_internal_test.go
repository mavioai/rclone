//go:build !plan9 && !solaris

package iclouddrive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/iclouddrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkServer serves download links and their downloads, counting both.
type linkServer struct {
	srv       *httptest.Server
	links     atomic.Int32
	downloads atomic.Int32
	refuse    atomic.Int32 // status to answer the next download with
}

func newLinkServer(t *testing.T) *linkServer {
	s := &linkServer{}
	mux := http.NewServeMux()
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	mux.HandleFunc("/ws/com.apple.CloudDocs/download/by_id", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DOC", r.URL.Query().Get("document_id"))
		n := s.links.Add(1)
		_, _ = fmt.Fprintf(w, `{"data_token":{"url":%q}}`, fmt.Sprintf("%s/dl/%d", s.srv.URL, n))
	})
	mux.HandleFunc("/dl/", func(w http.ResponseWriter, r *http.Request) {
		s.downloads.Add(1)
		if status := s.refuse.Swap(0); status != 0 {
			http.Error(w, "refused", int(status))
			return
		}
		assert.Equal(t, "bytes=1-4", r.Header.Get("Range"))
		_, _ = io.WriteString(w, "ello")
	})
	return s
}

func newTestObject(t *testing.T, s *linkServer, downloadURL string) *Object {
	ctx := context.Background()
	client, err := api.New("user", "pass", "", "", nil, nil, "remote", "")
	require.NoError(t, err)
	webservices := fmt.Sprintf(`{"webservices":{"docws":{"url":%q},"drivews":{"url":%q}}}`, s.srv.URL, s.srv.URL)
	require.NoError(t, json.Unmarshal([]byte(webservices), &client.Session.AccountInfo))
	service, err := api.NewDriveService(client)
	require.NoError(t, err)
	f := &Fs{
		name:    t.Name(),
		icloud:  client,
		service: service,
		pacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	return &Object{fs: f, remote: "file.mkv", size: 5, modTime: time.Unix(1, 0), driveID: "FILE::com.apple.CloudDocs::DOC", etag: "e", downloadURL: downloadURL}
}

func openObject(t *testing.T, o *Object) {
	in, err := o.Open(context.Background(), &fs.RangeOption{Start: 1, End: -1})
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	assert.Equal(t, "ello", string(data))
}

// TestOpenCachesLink checks that two Opens of one object request one
// download link, and that a link the storage refuses is requested again.
func TestOpenCachesLink(t *testing.T) {
	s := newLinkServer(t)
	o := newTestObject(t, s, "")
	openObject(t, o)
	openObject(t, o)
	assert.Equal(t, int32(1), s.links.Load(), "links")
	assert.Equal(t, int32(2), s.downloads.Load(), "downloads")

	s.refuse.Store(http.StatusForbidden)
	openObject(t, o)
	assert.Equal(t, int32(2), s.links.Load(), "links after refusal")
	assert.Equal(t, int32(4), s.downloads.Load(), "downloads after refusal")
}

// TestOpenListingLink checks that the listing's download link is used
// without requesting one, and that when it fails a link is requested
// and kept.
func TestOpenListingLink(t *testing.T) {
	s := newLinkServer(t)
	o := newTestObject(t, s, s.srv.URL+"/dl/listing")
	openObject(t, o)
	openObject(t, o)
	assert.Equal(t, int32(0), s.links.Load(), "links")
	assert.Equal(t, int32(2), s.downloads.Load(), "downloads")

	// Any failure of the listing's link, not only a refusal, makes a
	// link be requested
	s.refuse.Store(http.StatusMethodNotAllowed)
	openObject(t, o)
	assert.Equal(t, int32(1), s.links.Load(), "links after failure")
	assert.Equal(t, int32(4), s.downloads.Load(), "downloads after failure")

	openObject(t, o)
	assert.Equal(t, int32(1), s.links.Load(), "links after requested link")
	assert.Equal(t, int32(5), s.downloads.Load(), "downloads after requested link")
}
