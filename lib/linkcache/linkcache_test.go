package linkcache

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newCache(max int) (*Cache, *clock) {
	c := New(max)
	k := &clock{time.Unix(1_700_000_000, 0)}
	c.now = k.now
	return c, k
}

func fetcher(n *atomic.Int32, url string) func(context.Context) (*Link, error) {
	return func(context.Context) (*Link, error) {
		i := n.Add(1)
		return &Link{URL: url + "#" + string(rune('0'+i))}, nil
	}
}

func TestGetRenews(t *testing.T) {
	ctx := context.Background()
	c, clk := newCache(10)
	var n atomic.Int32
	fetch := fetcher(&n, "https://dl/a")

	l1, err := c.Get(ctx, "k", 10*time.Minute, fetch)
	require.NoError(t, err)
	l2, err := c.Get(ctx, "k", 10*time.Minute, fetch)
	require.NoError(t, err)
	assert.Same(t, l1, l2)
	assert.Equal(t, int32(1), n.Load())

	// Past four fifths of the life, a new link is requested
	clk.add(8*time.Minute + time.Second)
	l3, err := c.Get(ctx, "k", 10*time.Minute, fetch)
	require.NoError(t, err)
	assert.NotSame(t, l1, l3)
	assert.Equal(t, int32(2), n.Load())
}

func TestGetKeepsAliveLinkWhenRenewalFails(t *testing.T) {
	ctx := context.Background()
	c, clk := newCache(10)
	var n atomic.Int32
	l1, err := c.Get(ctx, "k", 10*time.Minute, fetcher(&n, "https://dl/a"))
	require.NoError(t, err)
	clk.add(9 * time.Minute)
	fail := func(context.Context) (*Link, error) { return nil, errors.New("rate limited") }
	l2, err := c.Get(ctx, "k", 10*time.Minute, fail)
	require.NoError(t, err)
	assert.Same(t, l1, l2)
	clk.add(2 * time.Minute)
	_, err = c.Get(ctx, "k", 10*time.Minute, fail)
	assert.Error(t, err)
}

func TestGetShared(t *testing.T) {
	ctx := context.Background()
	c, _ := newCache(10)
	var n atomic.Int32
	release := make(chan struct{})
	fetch := func(context.Context) (*Link, error) {
		n.Add(1)
		<-release
		return &Link{URL: "https://dl/a"}, nil
	}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			_, err := c.Get(ctx, "k", time.Minute, fetch)
			assert.NoError(t, err)
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	assert.Equal(t, int32(1), n.Load())
}

func TestExpiry(t *testing.T) {
	ctx := context.Background()
	c, clk := newCache(10)
	var n atomic.Int32
	// The URL's expiry wins over the declared lifetime
	exp := clk.t.Add(time.Hour).Unix()
	fetch := func(context.Context) (*Link, error) {
		n.Add(1)
		return &Link{URL: "https://cdn/f?t=" + itoa(exp)}, nil
	}
	_, err := c.Get(ctx, "k", time.Minute, fetch)
	require.NoError(t, err)
	clk.add(40 * time.Minute)
	_, err = c.Get(ctx, "k", time.Minute, fetch)
	require.NoError(t, err)
	assert.Equal(t, int32(1), n.Load())

	// A link about to expire is not kept
	short := func(context.Context) (*Link, error) {
		n.Add(1)
		return &Link{URL: "https://dl/b", Expires: clk.t.Add(time.Second)}, nil
	}
	_, err = c.Get(ctx, "s", time.Hour, short)
	require.NoError(t, err)
	assert.Equal(t, 1, c.Len())
}

func TestEvict(t *testing.T) {
	ctx := context.Background()
	c, clk := newCache(2)
	var n atomic.Int32
	for _, k := range []string{"a", "b"} {
		_, err := c.Get(ctx, k, time.Hour, fetcher(&n, "https://dl/"+k))
		require.NoError(t, err)
		clk.add(time.Second)
	}
	_, err := c.Get(ctx, "a", time.Hour, fetcher(&n, "https://dl/a"))
	require.NoError(t, err)
	_, err = c.Get(ctx, "c", time.Hour, fetcher(&n, "https://dl/c"))
	require.NoError(t, err)
	assert.Equal(t, 2, c.Len())
	assert.Contains(t, c.entries, "a")
	assert.NotContains(t, c.entries, "b", "the link used least recently goes")
}

type statusErr int

func (e statusErr) Error() string       { return "status " + itoa(int64(e)) }
func (e statusErr) HTTPStatusCode() int { return int(e) }

func TestOpenRetriesRefusedLink(t *testing.T) {
	ctx := context.Background()
	c, _ := newCache(10)
	var n atomic.Int32
	fetch := fetcher(&n, "https://dl/a")
	var opened []string
	open := func(_ context.Context, l *Link) (io.ReadCloser, error) {
		opened = append(opened, l.URL)
		if len(opened) == 1 {
			return nil, statusErr(403)
		}
		return io.NopCloser(strings.NewReader("x")), nil
	}
	in, err := c.Open(ctx, "k", time.Hour, fetch, open)
	require.NoError(t, err)
	_ = in.Close()
	assert.Equal(t, []string{"https://dl/a#1", "https://dl/a#2"}, opened)

	// Other errors are not retried
	opened = nil
	_, err = c.Open(ctx, "k", time.Hour, fetch, func(context.Context, *Link) (io.ReadCloser, error) {
		opened = append(opened, "")
		return nil, statusErr(500)
	})
	assert.Error(t, err)
	assert.Len(t, opened, 1)
}

func TestRefused(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{statusErr(401), true},
		{statusErr(403), true},
		{statusErr(404), true},
		{statusErr(410), true},
		{statusErr(429), false},
		{statusErr(500), false},
		{fs.ErrorObjectNotFound, true},
	} {
		assert.Equal(t, tc.want, Refused(tc.err), "%v", tc.err)
	}
}

func TestExpiresFromURL(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	in := func(d time.Duration) time.Time { return now.Add(d) }
	for _, tc := range []struct {
		url  string
		want time.Time
	}{
		{"https://b.s3.amazonaws.com/k?X-Amz-Date=20261013T120000Z&X-Amz-Expires=3600&X-Amz-Signature=x", in(time.Hour)},
		{"https://storage.googleapis.com/b/k?X-Goog-Date=20261013T113000Z&X-Goog-Expires=3600", in(30 * time.Minute)},
		{"https://storage.googleapis.com/b/k?X-Goog-Date=20261013T113000Z&X-Goog-Expires=900", time.Time{}},
		{"https://a.blob.core.windows.net/c/k?se=2026-10-13T14:00:00Z&sig=x", in(2 * time.Hour)},
		{"https://cdn.115.com/f?t=" + itoa(in(4*time.Hour).Unix()), in(4 * time.Hour)},
		{"https://oss/f?Expires=" + itoa(in(30*time.Minute).Unix()) + "&OSSAccessKeyId=x", in(30 * time.Minute)},
		{"https://cdn.123/f?auth_key=" + itoa(in(time.Hour).Unix()) + "-0-0-abc", in(time.Hour)},
		{"https://dl/f?t=123", time.Time{}},
		{"https://dl/f?expires=" + itoa(in(30*24*time.Hour).Unix()), time.Time{}},
		{"https://dl/f", time.Time{}},
	} {
		got := ExpiresFromURL(tc.url, now)
		assert.True(t, tc.want.Equal(got), "%s: got = %v, want = %v", tc.url, got, tc.want)
	}
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }
