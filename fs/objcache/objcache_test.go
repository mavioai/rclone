package objcache

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFs(t *testing.T, name, root string) fs.Fs {
	f, err := mockfs.NewFs(context.Background(), name, root, nil)
	require.NoError(t, err)
	return f
}

func TestGetPut(t *testing.T) {
	f := newFs(t, "remote", "")
	c := New(time.Minute, 2)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }

	a, b, d := mockobject.New("a"), mockobject.New("dir/b"), mockobject.New("d")
	assert.Nil(t, c.Get(f, "a"))
	c.Put(f, a)
	c.Put(f, b)
	assert.Equal(t, a, c.Get(f, "a"))

	// b is the least recently used
	c.Put(f, d)
	assert.Nil(t, c.Get(f, "dir/b"))
	assert.Equal(t, 2, c.Len())

	now = now.Add(time.Minute)
	assert.Nil(t, c.Get(f, "a"))
	assert.Equal(t, 1, c.Len())
}

func TestDisabled(t *testing.T) {
	f := newFs(t, "remote", "")
	c := New(0, 10)
	c.Put(f, mockobject.New("a"))
	assert.Nil(t, c.Get(f, "a"))
	assert.Equal(t, 0, c.Len())
}

func TestRefresh(t *testing.T) {
	f := newFs(t, "remote", "")
	c := New(time.Minute, 10)
	for _, name := range []string{"dir/a", "dir/b", "dir/sub/c", "other"} {
		c.Put(f, mockobject.New(name))
	}
	fresh := mockobject.New("dir/a").WithContent([]byte("new"), mockobject.SeekModeNone)
	listing := fs.DirEntries{fresh, mockobject.New("dir/new")}

	c.Refresh(f, "dir", listing, false)
	assert.Equal(t, fresh, c.Get(f, "dir/a"))
	assert.NotNil(t, c.Get(f, "dir/b"))
	assert.Nil(t, c.Get(f, "dir/new"), "a listing adds no entries")

	c.Refresh(f, "dir", listing, true)
	assert.Nil(t, c.Get(f, "dir/b"))
	assert.NotNil(t, c.Get(f, "dir/sub/c"))
	assert.NotNil(t, c.Get(f, "other"))
}

func TestDrop(t *testing.T) {
	root := newFs(t, "remote", "")
	lib := newFs(t, "remote", "lib")
	other := newFs(t, "other", "")
	c := New(time.Minute, 10)
	c.Put(root, mockobject.New("lib/dir/a"))
	c.Put(lib, mockobject.New("dir/b"))
	c.Put(lib, mockobject.New("dirx/c"))
	c.Put(other, mockobject.New("lib/dir/a"))

	c.Drop(root, "lib/dir")
	assert.Nil(t, c.Get(root, "lib/dir/a"))
	assert.Nil(t, c.Get(lib, "dir/b"))
	assert.NotNil(t, c.Get(lib, "dirx/c"))
	assert.NotNil(t, c.Get(other, "lib/dir/a"))

	c.Drop(lib, "")
	assert.Nil(t, c.Get(lib, "dirx/c"))
	assert.Equal(t, 1, c.Len())
}

// notifyFs is an Fs that can notify changes.
type notifyFs struct {
	fs.Fs
	features *fs.Features
	mu       sync.Mutex
	starts   int
	notify   func(string, fs.EntryType)
	polling  bool // whether the notifications run
}

func newNotifyFs(t *testing.T, name, root string) *notifyFs {
	f := &notifyFs{Fs: newFs(t, name, root)}
	f.features = (&fs.Features{}).Fill(context.Background(), f)
	f.features.ChangeNotify = f.changeNotify
	return f
}

func (f *notifyFs) Features() *fs.Features { return f.features }

// changeNotify runs like the backends' ChangeNotify: it waits for the
// interval, then notifies until the channel is closed.
func (f *notifyFs) changeNotify(ctx context.Context, notify func(string, fs.EntryType), pollChan <-chan time.Duration) {
	f.mu.Lock()
	f.starts++
	f.notify = notify
	f.mu.Unlock()
	go func() {
		for interval := range pollChan {
			f.mu.Lock()
			f.polling = interval > 0
			f.mu.Unlock()
		}
		f.mu.Lock()
		f.polling = false
		f.mu.Unlock()
	}()
}

func (f *notifyFs) state() (starts int, polling bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.polling
}

func (f *notifyFs) changed(remote string, typ fs.EntryType) {
	f.mu.Lock()
	notify := f.notify
	f.mu.Unlock()
	notify(remote, typ)
}

func TestChangeNotify(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lib := newNotifyFs(t, "remote", "lib")
		root := newFs(t, "remote", "")
		c := New(time.Hour, 10)
		c.SetPollInterval(time.Minute)

		c.Put(lib, mockobject.New("dir/a"))
		c.Put(lib, mockobject.New("dir/b"))
		c.Put(lib, mockobject.New("other"))
		c.Put(root, mockobject.New("lib/dir/c"))
		synctest.Wait()
		starts, polling := lib.state()
		assert.Equal(t, 1, starts)
		assert.True(t, polling)

		lib.changed("dir/a", fs.EntryObject)
		assert.Nil(t, c.Get(lib, "dir/a"))
		assert.NotNil(t, c.Get(lib, "dir/b"))

		// a directory drops everything beneath it, in every root
		lib.changed("dir", fs.EntryDirectory)
		assert.Nil(t, c.Get(lib, "dir/b"))
		assert.Nil(t, c.Get(root, "lib/dir/c"))
		assert.NotNil(t, c.Get(lib, "other"))

		c.Remove(lib, "other")
		synctest.Wait()
		_, polling = lib.state()
		assert.False(t, polling, "notifications stop with the last entry")

		c.Put(lib, mockobject.New("again"))
		synctest.Wait()
		starts, polling = lib.state()
		assert.Equal(t, 2, starts)
		assert.True(t, polling)

		c.SetLimits(0, 0)
		synctest.Wait()
		_, polling = lib.state()
		assert.False(t, polling, "notifications stop when the cache is disabled")
	})
}

func TestChangeNotifyExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newNotifyFs(t, "remote", "")
		c := New(5*time.Minute, 10)
		c.SetPollInterval(time.Minute)
		c.Put(f, mockobject.New("a"))
		synctest.Wait()
		_, polling := f.state()
		assert.True(t, polling)

		time.Sleep(4*time.Minute + 30*time.Second)
		synctest.Wait()
		_, polling = f.state()
		assert.True(t, polling)
		assert.Equal(t, 1, c.Len())

		time.Sleep(time.Minute)
		synctest.Wait()
		_, polling = f.state()
		assert.False(t, polling, "expired entries are swept")
		assert.Equal(t, 0, c.Len())
	})
}

func TestChangeNotifyDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newNotifyFs(t, "remote", "")
		c := New(time.Minute, 10)
		c.Put(f, mockobject.New("a"))
		synctest.Wait()
		starts, _ := f.state()
		assert.Equal(t, 0, starts, "no notifications without a poll interval")

		c.SetPollInterval(time.Minute)
		c.Put(f, mockobject.New("b"))
		synctest.Wait()
		starts, _ = f.state()
		assert.Equal(t, 1, starts)

		c.SetPollInterval(0)
		synctest.Wait()
		_, polling := f.state()
		assert.False(t, polling)
		c.Put(f, mockobject.New("c"))
		synctest.Wait()
		starts, _ = f.state()
		assert.Equal(t, 1, starts)
	})
}

func TestNoChangeNotify(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFs(t, "remote", "")
		c := New(time.Minute, 10)
		c.SetPollInterval(time.Minute)
		c.Put(f, mockobject.New("a"))
		assert.Empty(t, c.notifiers)
	})
}
