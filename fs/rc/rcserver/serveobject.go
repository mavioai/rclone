package rcserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/objcache"
	"github.com/rclone/rclone/lib/http/serve"
)

// serveObject serves the object at remote in f.
//
// The object comes from the object cache when it is there. The response
// carries the object's version as its ETag; a request whose If-Match
// names another version looks the object up again, and gets 412 when
// the object still differs. An open of a cached object that fails looks
// the object up again and retries once: with the same version, or for a
// request without If-Match, serving the fresh object from the start.
func (s *Server) serveObject(w http.ResponseWriter, r *http.Request, f fs.Fs, remote string) {
	header := w.Header().Clone()
	rw := &restartWriter{ResponseWriter: w}
	if !s.serveObjectOnce(rw, r, f, remote) {
		return
	}
	// The cached object was stale and nothing was written: serve the
	// fresh one, now in the cache.
	for k := range w.Header() {
		delete(w.Header(), k)
	}
	for k, v := range header {
		w.Header()[k] = v
	}
	s.serveObjectOnce(w, r, f, remote)
}

// restartWriter discards the response once restart is set.
type restartWriter struct {
	http.ResponseWriter
	restart bool
}

func (w *restartWriter) WriteHeader(code int) {
	if !w.restart {
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *restartWriter) Write(p []byte) (int, error) {
	if w.restart {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

// serveObjectOnce serves the object at remote in f; true when the cached
// object turned out stale for a request without If-Match, which is then
// to be served again.
func (s *Server) serveObjectOnce(w http.ResponseWriter, r *http.Request, f fs.Fs, remote string) (restart bool) {
	ctx := r.Context()
	rw, _ := w.(*restartWriter)
	fail := func(err error) {
		status, retryAfter := serve.ErrorStatus(err)
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int((retryAfter+time.Second-1)/time.Second)))
		}
		writeError(remote, nil, w, err, status)
	}
	o, cached, err := s.findObject(ctx, f, remote)
	if err != nil {
		fail(err)
		return false
	}
	etag := objectETag(ctx, o)
	if want := r.Header.Get("If-Match"); want != "" && !etagMatches(want, etag) {
		if cached {
			objcache.Default.Remove(f, remote)
			o, err = objcache.Default.Find(ctx, f, remote)
			if err != nil {
				fail(fmt.Errorf("failed to find object: %w", err))
				return false
			}
			cached = false
			etag = objectETag(ctx, o)
		}
		if !etagMatches(want, etag) {
			fail(serve.ErrVersionChanged)
			return false
		}
	}
	w.Header().Set("ETag", etag)

	open := func(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
		in, err := s.open(ctx, o, options)
		if err == nil || !cached || ctx.Err() != nil {
			return in, err
		}
		fs.Debugf(o, "Open of cached object failed, looking it up again: %v", err)
		objcache.Default.Remove(f, remote)
		fresh, ferr := objcache.Default.Find(ctx, f, remote)
		if ferr != nil {
			return nil, ferr
		}
		if objectETag(ctx, fresh) != etag {
			if rw != nil && r.Header.Get("If-Match") == "" {
				rw.restart = true
			}
			return nil, serve.ErrVersionChanged
		}
		return s.open(ctx, fresh, options)
	}
	serve.ObjectWith(w, r, o, open)
	return rw != nil && rw.restart
}

// findObject returns the object at remote in f, and whether it came
// from the object cache.
func (s *Server) findObject(ctx context.Context, f fs.Fs, remote string) (o fs.Object, cached bool, err error) {
	if o := objcache.Default.Get(f, remote); o != nil {
		return o, true, nil
	}
	o, err = objcache.Default.Find(ctx, f, remote)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find object: %w", err)
	}
	return o, false, nil
}

// open opens o, holding a slot of its file and of its account. The
// limits are the options', else those the storage's backend declares.
func (s *Server) open(ctx context.Context, o fs.Object, options []fs.OpenOption) (io.ReadCloser, error) {
	file := fs.ConfigString(o.Fs()) + "\x00" + o.Remote()
	storage := storageOf(o.Fs())
	perFile := streamLimit(s.opt.ServeMaxFileStreams, storage.Features().MaxFileStreams)
	perAccount := streamLimit(s.opt.ServeMaxStreams, storage.Features().MaxStreams)
	return s.limits.OpenLimited(ctx, file, storage.Name(), perFile, perAccount, func() (io.ReadCloser, error) {
		return o.Open(ctx, options...)
	})
}

// streamLimit returns the limit set by an option, the backend's when
// the option is 0, and none when it is negative.
func streamLimit(option, backend int) int {
	switch {
	case option > 0:
		return option
	case option < 0:
		return 0
	}
	return backend
}

// storageOf returns the remote that serves f's bytes, the innermost
// remote beneath f's wrappers; its name names the account.
func storageOf(f fs.Info) fs.Info {
	for range 16 {
		unwrap := f.Features().UnWrap
		if unwrap == nil {
			break
		}
		inner := unwrap()
		if inner == nil {
			break
		}
		f = inner
	}
	return f
}

// accountOf names the account that serves f's bytes.
func accountOf(f fs.Info) string {
	return storageOf(f).Name()
}

// objectETag returns the entity tag of o's version: its ID where it has
// one, the storage's version tag where the backend knows it, size, and
// the modification time and hash where they are cheap.
func objectETag(ctx context.Context, o fs.Object) string {
	h := sha256.New()
	if ider, ok := o.(fs.IDer); ok {
		_, _ = io.WriteString(h, ider.ID())
	}
	if v, ok := fs.UnWrapObject(o).(fs.VersionTagger); ok {
		// Listings and reads may quote the same tag differently
		tag := strings.Trim(strings.TrimPrefix(v.VersionTag(), "W/"), `"`)
		_, _ = io.WriteString(h, "\x00"+tag)
	}
	_, _ = io.WriteString(h, "\x00"+fs.Fingerprint(ctx, o, true))
	return `"` + hex.EncodeToString(h.Sum(nil)[:16]) + `"`
}

// etagMatches reports whether the If-Match header value header names
// etag.
func etagMatches(header, etag string) bool {
	for tag := range strings.SplitSeq(header, ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" || strings.TrimPrefix(tag, "W/") == etag {
			return true
		}
	}
	return false
}
