package internxt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/buckets"
	config "github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testMnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
	testBucket   = "0123456789abcdef01234567"
	testIndex    = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
)

// shardServer is the network API of a file with one encrypted shard.
type shardServer struct {
	*httptest.Server
	plain     string
	encrypted []byte
	hash      string
	infos     atomic.Int32 // file information requests
	downloads atomic.Int32 // shard downloads served
	refuse    int32        // the shard URL refused, 0 for none
}

func newShardServer(t *testing.T, plain string) *shardServer {
	s := &shardServer{plain: plain}
	key, iv, err := buckets.GenerateFileKey(testMnemonic, testBucket, testIndex)
	require.NoError(t, err)
	enc, err := buckets.EncryptReader(strings.NewReader(plain), key, iv)
	require.NoError(t, err)
	s.encrypted, err = io.ReadAll(enc)
	require.NoError(t, err)
	sum := sha256.Sum256(s.encrypted)
	s.hash = buckets.ComputeFileHash(sum[:])

	mux := http.NewServeMux()
	mux.HandleFunc("GET /network/buckets/"+testBucket+"/files/ID/info", func(w http.ResponseWriter, r *http.Request) {
		n := s.infos.Add(1)
		_ = json.NewEncoder(w).Encode(buckets.BucketFileInfo{
			Index:  testIndex,
			Size:   int64(len(plain)),
			Shards: []buckets.ShardInfo{{URL: fmt.Sprintf("%s/shard/%d", s.URL, n), Hash: s.hash}},
		})
	})
	mux.HandleFunc("GET /shard/{n}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("n") == fmt.Sprint(s.refuse) {
			http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)
			return
		}
		s.downloads.Add(1)
		http.ServeContent(w, r, "shard", time.Time{}, bytes.NewReader(s.encrypted))
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func newShardTestObject(t *testing.T, s *shardServer) *Object {
	ctx := context.Background()
	f := &Fs{
		name: t.Name(),
		cfg: &config.Config{
			Bucket:     testBucket,
			Mnemonic:   testMnemonic,
			HTTPClient: s.Client(),
			Endpoints:  endpoints.NewConfig(s.URL),
		},
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	return &Object{f: f, remote: "file", id: "ID", size: int64(len(s.plain)), modTime: time.Unix(1, 0)}
}

func readShard(t *testing.T, o *Object, options ...fs.OpenOption) (string, error) {
	in, err := o.Open(context.Background(), options...)
	require.NoError(t, err)
	b, err := io.ReadAll(in)
	require.NoError(t, in.Close())
	return string(b), err
}

func TestOpenSharesShardLink(t *testing.T) {
	plain := strings.Repeat("0123456789", 10)
	s := newShardServer(t, plain)
	o := newShardTestObject(t, s)
	for _, test := range []struct {
		options []fs.OpenOption
		want    string
	}{
		{nil, plain},
		{[]fs.OpenOption{&fs.RangeOption{Start: 37, End: 56}}, plain[37:57]},
		{[]fs.OpenOption{&fs.SeekOption{Offset: 48}}, plain[48:]},
		{[]fs.OpenOption{&fs.RangeOption{Start: 0, End: 15}}, plain[:16]},
	} {
		got, err := readShard(t, o, test.options...)
		require.NoError(t, err)
		assert.Equal(t, test.want, got, "options %v", test.options)
	}
	assert.Equal(t, int32(1), s.infos.Load(), "infos")
	assert.Equal(t, int32(4), s.downloads.Load(), "downloads")
}

func TestOpenRenewsRefusedShardLink(t *testing.T) {
	plain := strings.Repeat("0123456789", 10)
	s := newShardServer(t, plain)
	s.refuse = 1
	o := newShardTestObject(t, s)
	got, err := readShard(t, o, &fs.SeekOption{Offset: 90})
	require.NoError(t, err)
	assert.Equal(t, plain[90:], got)
	assert.Equal(t, int32(2), s.infos.Load(), "infos")
	assert.Equal(t, int32(1), s.downloads.Load(), "downloads")
}

func TestOpenChecksShardHash(t *testing.T) {
	s := newShardServer(t, "0123456789")
	s.hash = "bad"
	o := newShardTestObject(t, s)
	_, err := readShard(t, o)
	assert.ErrorContains(t, err, "hash mismatch")
}
