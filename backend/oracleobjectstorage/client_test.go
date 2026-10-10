//go:build !plan9 && !solaris && !js

package oracleobjectstorage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/http/serve"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGzipEncoding checks the handling of objects stored with
// Content-Encoding: gzip.
//
// By default they should be downloaded as stored, not transparently
// decompressed by the Go runtime. With decompress set they should be
// decompressed on download with size and hash unknown.
func TestGzipEncoding(t *testing.T) {
	ctx := context.Background()

	// Gzip compressed test data served with Content-Encoding: gzip
	// as if it had been uploaded with that metadata set.
	plain := []byte("hello, world - some uncompressed data which is longer than the compressed version")
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	compressed := buf.Bytes()
	compressedMd5 := md5.Sum(compressed)

	var gotAcceptEncoding string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			gotAcceptEncoding = r.Header.Get("Accept-Encoding")
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(len(compressed)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(compressedMd5[:]))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_, _ = w.Write(compressed)
	}))
	defer srv.Close()

	newFs := func(t *testing.T, extraConfig configmap.Simple) *Fs {
		m := configmap.Simple{
			"provider":  noAuth,
			"namespace": "test",
			"endpoint":  srv.URL,
		}
		for k, v := range extraConfig {
			m[k] = v
		}
		fsInfo, err := NewFs(ctx, "TestOOSGzip", "bucket", m)
		require.NoError(t, err)
		return fsInfo.(*Fs)
	}

	readObject := func(t *testing.T, f *Fs) ([]byte, *Object) {
		obj, err := f.NewObject(ctx, "test.gz")
		require.NoError(t, err)
		rc, err := obj.Open(ctx)
		require.NoError(t, err)
		body, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		return body, obj.(*Object)
	}

	t.Run("Default", func(t *testing.T) {
		f := newFs(t, configmap.Simple{})
		body, obj := readObject(t, f)
		assert.Equal(t, "gzip", gotAcceptEncoding)
		assert.Equal(t, compressed, body)
		assert.Equal(t, int64(len(compressed)), obj.Size())
		md5sum, err := obj.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(compressedMd5[:]), md5sum)
	})

	t.Run("Decompress", func(t *testing.T) {
		f := newFs(t, configmap.Simple{"decompress": "true"})
		body, obj := readObject(t, f)
		assert.Equal(t, "gzip", gotAcceptEncoding)
		assert.Equal(t, plain, body)
		assert.Equal(t, int64(-1), obj.Size())
		md5sum, err := obj.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, "", md5sum)
	})
}

// TestOpenIfMatch checks that reads are conditional on the ETag the
// object was found with.
func TestOpenIfMatch(t *testing.T) {
	ctx := context.Background()
	var (
		mu         sync.Mutex
		current    = "etag-1"
		gotIfMatch []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == "GET" {
			ifMatch := r.Header.Get("If-Match")
			gotIfMatch = append(gotIfMatch, ifMatch)
			if ifMatch != "" && ifMatch != current {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = io.WriteString(w, `{"code":"IfMatchFailed","message":"The If-Match header did not match"}`)
				return
			}
		}
		w.Header().Set("ETag", current)
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", time.Unix(1000, 0).UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()

	f, err := NewFs(ctx, "TestOOSIfMatch", "bucket", configmap.Simple{
		"provider":  noAuth,
		"namespace": "test",
		"endpoint":  srv.URL,
	})
	require.NoError(t, err)
	read := func(o fs.Object) error {
		rc, err := o.Open(ctx)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, rc)
		return rc.Close()
	}
	o, err := f.NewObject(ctx, "file")
	require.NoError(t, err)
	require.NoError(t, read(o))

	mu.Lock()
	current = "etag-2"
	mu.Unlock()
	err = read(o)
	require.Error(t, err)
	status, _ := serve.ErrorStatus(err)
	assert.Equal(t, http.StatusPreconditionFailed, status)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"etag-1", "etag-1"}, gotIfMatch)
}
