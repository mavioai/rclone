// Package connlimit caps the streams open to storage, per file and per
// account.
//
// When a file has as many streams as it may, opening another closes the
// stream of that file read least recently: players and browsers open
// the next stream before they drop the last. When an account has as
// many streams as it may, opening another waits for a slot.
package connlimit

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPreempted is returned by the reads of a stream closed to make room
// for a newer stream of its file.
var ErrPreempted = errors.New("stream closed for a newer stream of the file")

// Limiter caps open streams. A zero limit means no limit.
type Limiter struct {
	perFile    int
	perAccount int

	mu       sync.Mutex
	files    map[string]map[*Stream]struct{}
	accounts map[string]chan struct{}
}

// New makes a limiter allowing perFile streams of a file and perAccount
// streams of an account.
func New(perFile, perAccount int) *Limiter {
	return &Limiter{
		perFile:    perFile,
		perAccount: perAccount,
		files:      make(map[string]map[*Stream]struct{}),
		accounts:   make(map[string]chan struct{}),
	}
}

// Stream is an open stream holding its slots until it is closed.
type Stream struct {
	l        *Limiter
	file     string
	perFile  int
	account  chan struct{}
	lastRead atomic.Int64
	once     sync.Once
	preempt  atomic.Bool

	mu     sync.Mutex
	body   io.ReadCloser
	closed bool
}

// Open takes a slot for file and for account, then calls open. The
// slots are freed when the returned stream is closed or open fails.
func (l *Limiter) Open(ctx context.Context, file, account string, open func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if l == nil {
		return open()
	}
	return l.OpenLimited(ctx, file, account, l.perFile, l.perAccount, open)
}

// OpenLimited is Open with the limits of file and account given: those
// of the storage serving them. The limits of an account are those of
// its first stream.
func (l *Limiter) OpenLimited(ctx context.Context, file, account string, perFile, perAccount int, open func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if l == nil || (perFile <= 0 && perAccount <= 0) {
		return open()
	}
	s := &Stream{l: l, file: file, perFile: perFile}
	s.lastRead.Store(time.Now().UnixNano())

	if perFile > 0 {
		l.mu.Lock()
		streams := l.files[file]
		if streams == nil {
			streams = make(map[*Stream]struct{})
			l.files[file] = streams
		}
		var victims []*Stream
		for len(streams) >= perFile {
			victim := oldest(streams)
			delete(streams, victim)
			victims = append(victims, victim)
		}
		streams[s] = struct{}{}
		l.mu.Unlock()
		for _, victim := range victims {
			_ = victim.close(true)
		}
	}

	if perAccount > 0 {
		l.mu.Lock()
		slots := l.accounts[account]
		if slots == nil {
			slots = make(chan struct{}, perAccount)
			l.accounts[account] = slots
		}
		l.mu.Unlock()
		select {
		case slots <- struct{}{}:
			s.account = slots
		case <-ctx.Done():
			s.release()
			return nil, ctx.Err()
		}
	}

	body, err := open()
	if err != nil {
		s.release()
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		// Preempted while opening
		s.mu.Unlock()
		_ = body.Close()
		return nil, ErrPreempted
	}
	s.body = body
	s.mu.Unlock()
	return s, nil
}

// oldest returns the stream read least recently.
func oldest(streams map[*Stream]struct{}) *Stream {
	var out *Stream
	for s := range streams {
		if out == nil || s.lastRead.Load() < out.lastRead.Load() {
			out = s
		}
	}
	return out
}

// Read reads from the stream.
func (s *Stream) Read(p []byte) (int, error) {
	if s.preempt.Load() {
		return 0, ErrPreempted
	}
	s.lastRead.Store(time.Now().UnixNano())
	n, err := s.body.Read(p)
	if err != nil && s.preempt.Load() {
		err = ErrPreempted
	}
	return n, err
}

// Close closes the stream and frees its slots.
func (s *Stream) Close() error {
	return s.close(false)
}

// close frees the stream's slots and closes its body. A preempted
// stream's reads fail from then on.
func (s *Stream) close(preempted bool) error {
	if preempted {
		s.preempt.Store(true)
	}
	s.release()
	s.mu.Lock()
	body, closed := s.body, s.closed
	s.closed = true
	s.mu.Unlock()
	if closed || body == nil {
		return nil
	}
	return body.Close()
}

func (s *Stream) release() {
	s.once.Do(func() {
		l := s.l
		if s.perFile > 0 {
			l.mu.Lock()
			if streams := l.files[s.file]; streams != nil {
				delete(streams, s)
				if len(streams) == 0 {
					delete(l.files, s.file)
				}
			}
			l.mu.Unlock()
		}
		if s.account != nil {
			<-s.account
		}
	})
}

// Streams returns the number of open streams of file.
func (l *Limiter) Streams(file string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.files[file])
}
