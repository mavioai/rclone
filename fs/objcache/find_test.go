package objcache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingFs counts the lookups and listings of a mock Fs, and finds
// objects by listing when byListing is set.
type countingFs struct {
	*mockfs.Fs
	features   fs.Features
	lists      atomic.Int32
	newObjects atomic.Int32
	gate       chan struct{} // when set, listings wait for it to close
}

func newCountingFs(t *testing.T, byListing bool, names ...string) *countingFs {
	f, err := mockfs.NewFs(context.Background(), "drive", "", nil)
	require.NoError(t, err)
	m := f.(*mockfs.Fs)
	for _, name := range names {
		m.AddObject(mockobject.New(name))
	}
	cf := &countingFs{Fs: m}
	cf.features.FindByListing = byListing
	return cf
}

func (f *countingFs) Features() *fs.Features { return &f.features }

func (f *countingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	f.lists.Add(1)
	if f.gate != nil {
		<-f.gate
	}
	return f.Fs.List(ctx, dir)
}

func (f *countingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	f.newObjects.Add(1)
	return f.Fs.NewObject(ctx, remote)
}

func TestFindByListing(t *testing.T) {
	ctx := context.Background()
	f := newCountingFs(t, true, "a.mkv", "a.srt", "a.nfo")
	c := New(time.Minute, 100)

	o, err := c.Find(ctx, f, "a.mkv")
	require.NoError(t, err)
	assert.Equal(t, "a.mkv", o.Remote())
	assert.Equal(t, int32(1), f.lists.Load())
	assert.Equal(t, int32(0), f.newObjects.Load())
	assert.Equal(t, 3, c.Len(), "the neighbours are cached")
	assert.NotNil(t, c.Get(f, "a.srt"))

	_, err = c.Find(ctx, f, "missing.mkv")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	_, err = c.Find(ctx, f, "dir/missing.mkv")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestFindByListingRoom(t *testing.T) {
	ctx := context.Background()
	f := newCountingFs(t, true, "a", "b", "c", "d", "e", "f", "g", "h")
	c := New(time.Minute, 8)
	_, err := c.Find(ctx, f, "h")
	require.NoError(t, err)
	assert.Equal(t, 2, c.Len(), "a quarter of the cache")
	assert.NotNil(t, c.Get(f, "h"), "the object looked up is cached")
}

func TestFindShared(t *testing.T) {
	ctx := context.Background()
	f := newCountingFs(t, true, "a", "b", "c")
	f.gate = make(chan struct{})
	c := New(time.Minute, 100)
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b", "c", "a"} {
		wg.Go(func() {
			o, err := c.Find(ctx, f, name)
			if assert.NoError(t, err) {
				assert.Equal(t, name, o.Remote())
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(f.gate)
	wg.Wait()
	assert.Equal(t, int32(1), f.lists.Load())
}

func TestFindByNewObject(t *testing.T) {
	ctx := context.Background()
	f := newCountingFs(t, false, "a", "b")
	c := New(time.Minute, 100)
	_, err := c.Find(ctx, f, "a")
	require.NoError(t, err)
	assert.Equal(t, int32(0), f.lists.Load())
	assert.Equal(t, int32(1), f.newObjects.Load())
	assert.Equal(t, 1, c.Len())
}
