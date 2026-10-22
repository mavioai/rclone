// Package objcache keeps the objects that serving files over the rc
// server finds, so that consecutive requests for one file skip the
// backend's lookup.
//
// Only the rc server's file serving reads the cache. Listings and stats
// always ask the backend and replace the entries of the objects they
// return, and writes through the rc drop the entries of the paths they
// touch.
//
// Backends with the FindByListing feature find an object by listing its
// directory. For them a lookup lists the directory once and caches all
// its objects, so that the next lookups of its neighbours hit, and
// concurrent lookups in one directory share the listing.
//
// While the cache holds objects of an Fs whose backend can notify
// changes, it polls the backend for changes every poll interval and
// drops the entries of the paths reported.
package objcache

import (
	"container/list"
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sync/singleflight"
)

// Cache is a size and time limited cache of objects.
//
// A Cache with a zero size or lifetime keeps nothing.
type Cache struct {
	mu        sync.Mutex
	ttl       time.Duration
	max       int
	poll      time.Duration // interval of change notifications, 0 for none
	ll        *list.List    // of *entry, most recently used first
	items     map[key]*list.Element
	counts    map[string]int           // number of entries of each Fs
	notifiers map[string]chan struct{} // closed to stop the change notifications of each Fs
	now       func() time.Time
	listings  singleflight.Group // listings of directories by lookups, by Fs and directory
}

// key identifies an object by its Fs and its path within the Fs.
type key struct {
	fs     string
	remote string
}

type entry struct {
	key     key
	name    string // the name of the Fs's remote
	abs     string // the object's path from the remote's root
	o       fs.Object
	expires time.Time
}

// Default is the cache of the rc server.
var Default = New(0, 0)

// New makes a cache keeping at most max objects for at most ttl each.
func New(ttl time.Duration, max int) *Cache {
	return &Cache{
		ttl:       ttl,
		max:       max,
		ll:        list.New(),
		items:     make(map[key]*list.Element),
		counts:    make(map[string]int),
		notifiers: make(map[string]chan struct{}),
		now:       time.Now,
	}
}

// SetLimits changes the cache's lifetime and size, dropping what no
// longer fits.
func (c *Cache) SetLimits(ttl time.Duration, max int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ttl, c.max = ttl, max
	c.trim()
}

// SetPollInterval sets the interval at which the cache polls the
// backends that can notify changes, 0 for not polling. It stops the
// running notifications; puts start them again at the new interval.
func (c *Cache) SetPollInterval(interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.poll = interval
	for cfg := range c.notifiers {
		c.stopNotifier(cfg)
	}
}

func (c *Cache) enabled() bool {
	return c.ttl > 0 && c.max > 0
}

func keyOf(f fs.Fs, remote string) key {
	return key{fs: fs.ConfigString(f), remote: remote}
}

func absPath(f fs.Fs, remote string) string {
	return strings.Trim(path.Join(f.Root(), remote), "/")
}

// Get returns the cached object at remote in f, or nil.
func (c *Cache) Get(f fs.Fs, remote string) fs.Object {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled() {
		return nil
	}
	el, ok := c.items[keyOf(f, remote)]
	if !ok {
		return nil
	}
	e := el.Value.(*entry)
	if !c.now().Before(e.expires) {
		c.remove(el)
		return nil
	}
	c.ll.MoveToFront(el)
	return e.o
}

// Put caches o, found in f.
func (c *Cache) Put(f fs.Fs, o fs.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled() {
		return
	}
	c.put(f, o)
}

func (c *Cache) put(f fs.Fs, o fs.Object) {
	k := keyOf(f, o.Remote())
	expires := c.now().Add(c.ttl)
	if el, ok := c.items[k]; ok {
		e := el.Value.(*entry)
		e.o, e.expires = o, expires
		c.ll.MoveToFront(el)
		return
	}
	e := &entry{key: k, name: f.Name(), abs: absPath(f, o.Remote()), o: o, expires: expires}
	c.items[k] = c.ll.PushFront(e)
	c.counts[k.fs]++
	c.trim()
	c.startNotifier(f, k.fs)
}

// Find looks remote up in f, asking the backend, and caches what it
// finds. A backend that finds objects by listing has the directory of
// remote listed, and its objects cached, at most a quarter of the
// cache's size of them.
func (c *Cache) Find(ctx context.Context, f fs.Fs, remote string) (fs.Object, error) {
	c.mu.Lock()
	enabled := c.enabled()
	c.mu.Unlock()
	if !enabled || !f.Features().FindByListing {
		o, err := f.NewObject(ctx, remote)
		if err != nil {
			return nil, err
		}
		c.Put(f, o)
		return o, nil
	}
	dir := parent(remote)
	// The listing is shared: it must not end with one caller's context
	v, err, _ := c.listings.Do(fs.ConfigString(f)+"\x00"+dir, func() (any, error) {
		return f.List(context.WithoutCancel(ctx), dir)
	})
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	entries := v.(fs.DirEntries)
	var found fs.Object
	for _, de := range entries {
		if o, ok := de.(fs.Object); ok && o.Remote() == remote {
			found = o
			break
		}
	}
	c.fill(f, dir, entries, found)
	if found == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return found, nil
}

// fill caches the objects of a complete listing of dir in f, found
// first, and drops the entries of dir's objects missing from it.
func (c *Cache) fill(f fs.Fs, dir string, entries fs.DirEntries, found fs.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled() {
		return
	}
	cfg := fs.ConfigString(f)
	listed := make(map[string]struct{}, len(entries))
	room := max(c.max/4, 1)
	if found != nil {
		c.put(f, found)
		room--
	}
	for _, de := range entries {
		o, ok := de.(fs.Object)
		if !ok {
			continue
		}
		listed[o.Remote()] = struct{}{}
		if o == found {
			continue
		}
		if _, ok := c.items[key{fs: cfg, remote: o.Remote()}]; ok || room > 0 {
			if !ok {
				room--
			}
			c.put(f, o)
		}
	}
	for k, el := range c.items {
		if k.fs != cfg || parent(k.remote) != dir {
			continue
		}
		if _, ok := listed[k.remote]; !ok {
			c.remove(el)
		}
	}
}

// Remove drops the entry of remote in f.
func (c *Cache) Remove(f fs.Fs, remote string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[keyOf(f, remote)]; ok {
		c.remove(el)
	}
}

// Refresh replaces the entries of the objects that a listing of dir in
// f returned. When complete is set, entries holds everything in dir,
// and the entries of dir's objects missing from it are dropped.
//
// A listing adds no entries; only serving files does.
func (c *Cache) Refresh(f fs.Fs, dir string, entries fs.DirEntries, complete bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled() || len(c.items) == 0 {
		return
	}
	cfg := fs.ConfigString(f)
	listed := make(map[string]struct{}, len(entries))
	for _, de := range entries {
		o, ok := de.(fs.Object)
		if !ok {
			continue
		}
		listed[o.Remote()] = struct{}{}
		if _, ok := c.items[key{fs: cfg, remote: o.Remote()}]; ok {
			c.put(f, o)
		}
	}
	if !complete {
		return
	}
	for k, el := range c.items {
		if k.fs != cfg || parent(k.remote) != dir {
			continue
		}
		if _, ok := listed[k.remote]; !ok {
			c.remove(el)
		}
	}
}

// Drop removes the entries of remote in f and of everything beneath it,
// in every Fs of the same remote whatever its root.
func (c *Cache) Drop(f fs.Fs, remote string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) == 0 {
		return
	}
	name, abs := f.Name(), absPath(f, remote)
	for _, el := range c.items {
		e := el.Value.(*entry)
		if e.name != name {
			continue
		}
		if abs == "" || e.abs == abs || strings.HasPrefix(e.abs, abs+"/") {
			c.remove(el)
		}
	}
}

// Len returns the number of cached objects.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *Cache) remove(el *list.Element) {
	c.ll.Remove(el)
	k := el.Value.(*entry).key
	delete(c.items, k)
	if c.counts[k.fs]--; c.counts[k.fs] <= 0 {
		delete(c.counts, k.fs)
		c.stopNotifier(k.fs)
	}
}

// startNotifier starts the change notifications of f, whose config
// string is cfg, unless they run already or f can't notify changes.
func (c *Cache) startNotifier(f fs.Fs, cfg string) {
	if c.poll <= 0 || c.notifiers[cfg] != nil {
		return
	}
	do := f.Features().ChangeNotify
	if do == nil {
		return
	}
	stop := make(chan struct{})
	c.notifiers[cfg] = stop
	go c.notify(f, cfg, do, c.poll, stop)
}

// stopNotifier stops the change notifications of the Fs with config
// string cfg.
func (c *Cache) stopNotifier(cfg string) {
	if stop, ok := c.notifiers[cfg]; ok {
		close(stop)
		delete(c.notifiers, cfg)
	}
}

// notify runs the change notifications of f until stop is closed,
// dropping the entries of the paths reported and sweeping the expired
// entries of f on each poll interval, so that the notifications stop
// once f's entries are gone.
func (c *Cache) notify(f fs.Fs, cfg string, do func(context.Context, func(string, fs.EntryType), <-chan time.Duration), interval time.Duration, stop <-chan struct{}) {
	pollChan := make(chan time.Duration)
	defer close(pollChan)
	do(context.Background(), func(remote string, _ fs.EntryType) {
		fs.Debugf(f, "Change notified, dropping cached objects of %q", remote)
		c.Drop(f, remote)
	}, pollChan)
	select {
	case pollChan <- interval:
	case <-stop:
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.sweep(cfg)
		case <-stop:
			return
		}
	}
}

// sweep drops the expired entries of the Fs with config string cfg.
func (c *Cache) sweep(cfg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for k, el := range c.items {
		if k.fs == cfg && !now.Before(el.Value.(*entry).expires) {
			c.remove(el)
		}
	}
}

// trim drops the least recently used entries over the size limit.
func (c *Cache) trim() {
	limit := c.max
	if c.ttl <= 0 {
		limit = 0
	}
	for c.ll.Len() > limit {
		c.remove(c.ll.Back())
	}
}

func parent(remote string) string {
	dir := path.Dir(remote)
	if dir == "." {
		return ""
	}
	return dir
}
