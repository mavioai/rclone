// Package linkcache keeps the download links that backends request
// before reading an object, so that the reads of one object, such as
// the seeks of a player, share a link instead of each requesting one.
//
// A link lives until the expiry the storage gives, in its response or
// in the link's URL, else for the lifetime the backend declares. It is
// renewed at four fifths of its life, and concurrent requests for the
// link of one object share a request. When the storage refuses a link,
// it is dropped and a new one requested.
package linkcache

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/http/serve"
	"golang.org/x/sync/singleflight"
)

// MaxLifetime caps the life of a link, whatever expiry it states.
const MaxLifetime = 24 * time.Hour

// minLife is the least life left for a link to be kept.
const minLife = 10 * time.Second

// Link is a download link of an object.
type Link struct {
	URL     string
	Header  http.Header // headers, cookies included, the link is bound to; may be nil
	Expires time.Time   // when the storage said the link expires; zero if it didn't
	Value   any         // what else the backend keeps with the link, such as a decryption key
}

// Cache keeps links by key.
type Cache struct {
	mu      sync.Mutex
	max     int
	entries map[string]*entry
	group   singleflight.Group
	now     func() time.Time
}

type entry struct {
	link    *Link
	renew   time.Time // when to request a new link
	expires time.Time
	used    time.Time
}

// Default is the cache the backends share.
var Default = New(10000)

// New makes a cache keeping at most max links.
func New(max int) *Cache {
	return &Cache{max: max, entries: make(map[string]*entry), now: time.Now}
}

// Key makes the key of an object's link from what names the object and
// its version: the remote's name, the object's ID or path, and its
// fingerprint, such as the size and modification time or hash.
func Key(parts ...string) string {
	return strings.Join(parts, "\x00")
}

// Get returns the link of key, calling fetch for a new one when there
// is none or the one there is past four fifths of its life. lifetime is
// how long a link lives when its storage states no expiry.
func (c *Cache) Get(ctx context.Context, key string, lifetime time.Duration, fetch func(context.Context) (*Link, error)) (*Link, error) {
	now := c.now()
	var stale *Link // a link due for renewal but still alive
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		if now.Before(e.renew) {
			e.used = now
			link := e.link
			c.mu.Unlock()
			return link, nil
		}
		if now.Before(e.expires) {
			stale = e.link
		} else {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
	ch := c.group.DoChan(key, func() (any, error) {
		// Shared: it must not end with one caller's context
		link, err := fetch(context.WithoutCancel(ctx))
		if err != nil {
			return nil, err
		}
		c.put(key, link, lifetime)
		return link, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			if stale != nil && ctx.Err() == nil {
				fs.Debugf(nil, "Renewing a link failed, using the one still alive: %v", res.Err)
				return stale, nil
			}
			return nil, res.Err
		}
		return res.Val.(*Link), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// put keeps link under key, unless it expires too soon.
func (c *Cache) put(key string, link *Link, lifetime time.Duration) {
	now := c.now()
	expires := link.Expires
	if expires.IsZero() {
		expires = ExpiresFromURL(link.URL, now)
	}
	if expires.IsZero() {
		expires = now.Add(lifetime)
	}
	expires = minTime(expires, now.Add(MaxLifetime))
	life := expires.Sub(now)
	if life < minLife {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max <= 0 {
		return
	}
	if _, ok := c.entries[key]; !ok && len(c.entries) >= c.max {
		c.evict(now)
	}
	c.entries[key] = &entry{link: link, renew: now.Add(life * 4 / 5), expires: expires, used: now}
}

// evict drops the expired links, or else the one used least recently.
func (c *Cache) evict(now time.Time) {
	var oldest string
	var oldestUsed time.Time
	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)
			continue
		}
		if oldest == "" || e.used.Before(oldestUsed) {
			oldest, oldestUsed = k, e.used
		}
	}
	if len(c.entries) >= c.max && oldest != "" {
		delete(c.entries, oldest)
	}
}

// Drop forgets the link of key if it is still link, or whatever link
// key has when link is nil.
func (c *Cache) Drop(key string, link *Link) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok && (link == nil || e.link == link) {
		delete(c.entries, key)
	}
}

// Len returns the number of links kept.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Open gets the link of key and opens it with open. When the storage
// refuses the link (Refused), it drops the link, gets a new one and
// opens that once more.
func (c *Cache) Open(ctx context.Context, key string, lifetime time.Duration, fetch func(context.Context) (*Link, error), open func(context.Context, *Link) (io.ReadCloser, error)) (io.ReadCloser, error) {
	link, err := c.Get(ctx, key, lifetime, fetch)
	if err != nil {
		return nil, err
	}
	in, err := open(ctx, link)
	if err == nil || !Refused(err) || ctx.Err() != nil {
		return in, err
	}
	fs.Debugf(nil, "Link refused, requesting another: %v", err)
	c.Drop(key, link)
	link, err = c.Get(ctx, key, lifetime, fetch)
	if err != nil {
		return nil, err
	}
	return open(ctx, link)
}

// Refused reports whether err says the storage refused a link: 401,
// 403, 404 or 410, which an expired or revoked link gets.
func Refused(err error) bool {
	if err == nil {
		return false
	}
	status, _ := serve.ErrorStatus(err)
	return status == http.StatusForbidden || status == http.StatusNotFound
}

// ExpiresFromURL returns the expiry a link states in its query, or the
// zero time. It knows the parameters of S3 and Google Cloud Storage
// signatures (X-Amz-Date and X-Amz-Expires, X-Goog-Date and
// X-Goog-Expires), Azure SAS (se), Unix times in Expires, expires,
// expire, x-oss-expires, e and t, and the timestamp that begins
// Alibaba Cloud CDN's auth_key. Times not within MaxLifetime from now
// are ignored.
func ExpiresFromURL(link string, now time.Time) time.Time {
	u, err := url.Parse(link)
	if err != nil {
		return time.Time{}
	}
	q := u.Query()
	plausible := func(t time.Time) time.Time {
		if t.After(now) && t.Before(now.Add(MaxLifetime+time.Hour)) {
			return t
		}
		return time.Time{}
	}
	signed := func(dateKey, expiresKey string) time.Time {
		date, err := time.Parse("20060102T150405Z", q.Get(dateKey))
		if err != nil {
			return time.Time{}
		}
		secs, err := strconv.ParseInt(q.Get(expiresKey), 10, 64)
		if err != nil {
			return time.Time{}
		}
		return plausible(date.Add(time.Duration(secs) * time.Second))
	}
	if t := signed("X-Amz-Date", "X-Amz-Expires"); !t.IsZero() {
		return t
	}
	if t := signed("X-Goog-Date", "X-Goog-Expires"); !t.IsZero() {
		return t
	}
	if se := q.Get("se"); se != "" {
		if t, err := time.Parse(time.RFC3339, se); err == nil {
			return plausible(t)
		}
	}
	unix := func(v string) time.Time {
		secs, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return time.Time{}
		}
		return plausible(time.Unix(secs, 0))
	}
	for _, k := range []string{"Expires", "expires", "expire", "x-oss-expires", "e", "t"} {
		if t := unix(q.Get(k)); !t.IsZero() {
			return t
		}
	}
	if auth := q.Get("auth_key"); auth != "" {
		ts, _, _ := strings.Cut(auth, "-")
		return unix(ts)
	}
	return time.Time{}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
