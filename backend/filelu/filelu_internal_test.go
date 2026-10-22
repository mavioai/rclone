package filelu

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenCachesLink checks that two Opens of one object request one
// direct link, that the range is requested from the storage or skipped
// to when the storage ignores it, and that a link the storage refuses
// is requested again.
func TestOpenCachesLink(t *testing.T) {
	for _, ignoreRange := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignoreRange=%v", ignoreRange), func(t *testing.T) {
			ctx := context.Background()
			var links, downloads atomic.Int32
			var refuse atomic.Bool
			mux := http.NewServeMux()
			srv := httptest.NewServer(mux)
			defer srv.Close()
			mux.HandleFunc("/file/direct_link", func(w http.ResponseWriter, r *http.Request) {
				n := links.Add(1)
				_, _ = fmt.Fprintf(w, `{"status":200,"msg":"OK","result":{"url":%q,"size":5}}`, fmt.Sprintf("%s/dl/%d", srv.URL, n))
			})
			mux.HandleFunc("/dl/", func(w http.ResponseWriter, r *http.Request) {
				downloads.Add(1)
				if refuse.Swap(false) {
					http.Error(w, "expired", http.StatusForbidden)
					return
				}
				assert.Equal(t, "bytes=1-3", r.Header.Get("Range"))
				if ignoreRange {
					_, _ = io.WriteString(w, "hello")
					return
				}
				w.Header().Set("Content-Range", "bytes 1-3/5")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, "ell")
			})

			f := &Fs{
				name:   t.Name(),
				client: srv.Client(),
				srv:    rest.NewClient(srv.Client()).SetRoot(srv.URL),
				pacer:  pacer.New(),
			}
			o := &Object{fs: f, remote: "file.mkv", size: 5, fileCode: "abcdefghijkl"}

			open := func() {
				in, err := o.Open(ctx, &fs.RangeOption{Start: 1, End: 3})
				require.NoError(t, err)
				data, err := io.ReadAll(in)
				require.NoError(t, err)
				require.NoError(t, in.Close())
				assert.Equal(t, "ell", string(data))
			}
			open()
			open()
			assert.Equal(t, int32(1), links.Load(), "links")
			assert.Equal(t, int32(2), downloads.Load(), "downloads")

			refuse.Store(true)
			open()
			assert.Equal(t, int32(2), links.Load(), "links after refusal")
			assert.Equal(t, int32(4), downloads.Load(), "downloads after refusal")
		})
	}
}
