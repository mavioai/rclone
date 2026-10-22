package internetarchive

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkServer is a frontend that redirects to a storage node.
type linkServer struct {
	*httptest.Server
	content   string
	links     atomic.Int32 // redirects given
	downloads atomic.Int32 // downloads served
	refuse    int32        // the link refused, 0 for none
	direct    bool         // answer with the content rather than a redirect
}

func newLinkServer(t *testing.T, content string) *linkServer {
	s := &linkServer{content: content}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /download/item/file", func(w http.ResponseWriter, r *http.Request) {
		if s.direct {
			s.downloads.Add(1)
			http.ServeContent(w, r, "file", time.Time{}, strings.NewReader(s.content))
			return
		}
		n := s.links.Add(1)
		http.Redirect(w, r, fmt.Sprintf("/%d/items/item/file", n), http.StatusFound)
	})
	mux.HandleFunc("GET /{n}/items/item/file", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("n") == fmt.Sprint(s.refuse) {
			http.Error(w, "gone", http.StatusNotFound)
			return
		}
		s.downloads.Add(1)
		http.ServeContent(w, r, "file", time.Time{}, strings.NewReader(s.content))
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func newLinkTestObject(t *testing.T, s *linkServer) *Object {
	ctx := context.Background()
	f := &Fs{
		name:  t.Name(),
		root:  "item",
		front: rest.NewClient(s.Client()).SetRoot(s.URL),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	return &Object{fs: f, remote: "file", size: int64(len(s.content)), modTime: time.Unix(1, 0), md5: "h"}
}

func readLink(t *testing.T, o *Object, options ...fs.OpenOption) string {
	in, err := o.Open(context.Background(), options...)
	require.NoError(t, err)
	b, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	return string(b)
}

func TestOpenSharesLink(t *testing.T) {
	s := newLinkServer(t, "0123456789")
	o := newLinkTestObject(t, s)
	assert.Equal(t, "0123456789", readLink(t, o))
	assert.Equal(t, "3456", readLink(t, o, &fs.RangeOption{Start: 3, End: 6}))
	assert.Equal(t, int32(1), s.links.Load(), "links")
	assert.Equal(t, int32(2), s.downloads.Load(), "downloads")
}

func TestOpenRenewsRefusedLink(t *testing.T) {
	s := newLinkServer(t, "0123456789")
	s.refuse = 1
	o := newLinkTestObject(t, s)
	assert.Equal(t, "789", readLink(t, o, &fs.SeekOption{Offset: 7}))
	assert.Equal(t, int32(2), s.links.Load(), "links")
	assert.Equal(t, int32(1), s.downloads.Load(), "downloads")
}

func TestOpenWithoutRedirect(t *testing.T) {
	s := newLinkServer(t, "0123456789")
	s.direct = true
	o := newLinkTestObject(t, s)
	assert.Equal(t, "2345", readLink(t, o, &fs.RangeOption{Start: 2, End: 5}))
}
