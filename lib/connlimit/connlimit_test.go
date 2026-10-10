package connlimit

import (
	"context"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type body struct {
	io.Reader
	closed bool
}

func (b *body) Close() error {
	b.closed = true
	return nil
}

func opener(b *body) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) { return b, nil }
}

func newBody() *body {
	return &body{Reader: strings.NewReader("0123456789")}
}

func TestUnlimited(t *testing.T) {
	l := New(0, 0)
	b := newBody()
	rc, err := l.Open(context.Background(), "f", "a", opener(b))
	require.NoError(t, err)
	assert.Equal(t, b, rc)
}

func TestPreemptsLeastRecentlyRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		l := New(2, 0)
		b1, b2, b3 := newBody(), newBody(), newBody()
		s1, err := l.Open(ctx, "f", "a", opener(b1))
		require.NoError(t, err)
		time.Sleep(time.Second)
		s2, err := l.Open(ctx, "f", "a", opener(b2))
		require.NoError(t, err)
		time.Sleep(time.Second)
		// Reading s1 makes s2 the least recently read
		_, err = s1.Read(make([]byte, 1))
		require.NoError(t, err)
		time.Sleep(time.Second)

		s3, err := l.Open(ctx, "f", "a", opener(b3))
		require.NoError(t, err)
		assert.True(t, b2.closed)
		_, err = s2.Read(make([]byte, 1))
		assert.ErrorIs(t, err, ErrPreempted)
		assert.NoError(t, s2.Close())
		assert.Equal(t, 2, l.Streams("f"))

		// Another file has slots of its own
		s4, err := l.Open(ctx, "g", "a", opener(newBody()))
		require.NoError(t, err)
		assert.False(t, b1.closed)

		for _, s := range []io.Closer{s1, s3, s4} {
			require.NoError(t, s.Close())
		}
		assert.Equal(t, 0, l.Streams("f"))
	})
}

func TestAccountWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New(0, 1)
		s1, err := l.Open(context.Background(), "f", "a", opener(newBody()))
		require.NoError(t, err)

		// Another account is not held up
		s2, err := l.Open(context.Background(), "g", "b", opener(newBody()))
		require.NoError(t, err)
		require.NoError(t, s2.Close())

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err = l.Open(ctx, "g", "a", opener(newBody()))
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		opened := make(chan io.ReadCloser)
		go func() {
			s, err := l.Open(context.Background(), "g", "a", opener(newBody()))
			assert.NoError(t, err)
			opened <- s
		}()
		synctest.Wait()
		select {
		case <-opened:
			t.Fatal("opened while the account was full")
		default:
		}
		require.NoError(t, s1.Close())
		s3 := <-opened
		require.NoError(t, s3.Close())
	})
}

func TestFailedOpenFreesSlots(t *testing.T) {
	l := New(1, 1)
	_, err := l.Open(context.Background(), "f", "a", func() (io.ReadCloser, error) { return nil, io.ErrUnexpectedEOF })
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Equal(t, 0, l.Streams("f"))
	s, err := l.Open(context.Background(), "f", "a", opener(newBody()))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestOpenLimited(t *testing.T) {
	ctx := context.Background()
	l := New(0, 0)
	b1 := newBody()
	s1, err := l.OpenLimited(ctx, "f", "a", 1, 0, opener(b1))
	require.NoError(t, err)
	// Limits are given per open: the file's single slot is taken over
	s2, err := l.OpenLimited(ctx, "f", "a", 1, 0, opener(newBody()))
	require.NoError(t, err)
	assert.True(t, b1.closed)
	assert.Equal(t, 1, l.Streams("f"))
	// No limits, no slots
	s3, err := l.OpenLimited(ctx, "f", "a", 0, 0, opener(newBody()))
	require.NoError(t, err)
	assert.Equal(t, 1, l.Streams("f"))
	for _, s := range []io.Closer{s1, s2, s3} {
		require.NoError(t, s.Close())
	}
	assert.Equal(t, 0, l.Streams("f"))
}
