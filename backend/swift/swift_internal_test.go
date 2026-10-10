package swift

import (
	"context"
	"testing"
	"time"

	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
)

func TestInternalUrlEncode(t *testing.T) {
	for _, test := range []struct {
		in   string
		want string
	}{
		{"", ""},
		{"abcdefghijklmnopqrstuvwxyz", "abcdefghijklmnopqrstuvwxyz"},
		{"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
		{"0123456789", "0123456789"},
		{"abc/ABC/123", "abc/ABC/123"},
		{"   ", "%20%20%20"},
		{"&", "%26"},
		{"ß£", "%C3%9F%C2%A3"},
		{"Vidéo Potato Sausage?&£.mkv", "Vid%C3%A9o%20Potato%20Sausage%3F%26%C2%A3.mkv"},
	} {
		got := urlEncode(test.in)
		if got != test.want {
			t.Logf("%q: want %q got %q", test.in, test.want, got)
		}
	}
}

func TestInternalShouldRetryHeaders(t *testing.T) {
	ctx := context.Background()
	headers := swift.Headers{
		"Content-Length": "64",
		"Content-Type":   "text/html; charset=UTF-8",
		"Date":           "Mon: 18 Mar 2019 12:11:23 GMT",
		"Retry-After":    "1",
	}
	err := &swift.Error{
		StatusCode: 429,
		Text:       "Too Many Requests",
	}

	// Short sleep should just do the sleep
	start := time.Now()
	retry, gotErr := shouldRetryHeaders(ctx, headers, err)
	dt := time.Since(start)
	assert.True(t, retry)
	assert.Equal(t, err, gotErr)
	assert.True(t, dt > time.Second/2)

	// Long sleep should return RetryError
	headers["Retry-After"] = "3600"
	start = time.Now()
	retry, gotErr = shouldRetryHeaders(ctx, headers, err)
	dt = time.Since(start)
	assert.True(t, dt < time.Second)
	assert.False(t, retry)
	assert.Equal(t, true, fserrors.IsRetryAfterError(gotErr))
	after := gotErr.(fserrors.RetryAfter).RetryAfter()
	dt = after.Sub(start)
	assert.True(t, dt >= time.Hour-time.Second && dt <= time.Hour+time.Second)

}

func TestInternalIfMatch(t *testing.T) {
	const md5 = "0123456789abcdef0123456789abcdef"
	for _, test := range []struct {
		name           string
		md5            string
		headers        swift.Headers
		noLargeObjects bool
		want           string
	}{
		{"no hash", "", swift.Headers{}, false, ""},
		{"listed", md5, nil, false, ""},
		{"listed without large objects", md5, nil, true, md5},
		{"headed", md5, swift.Headers{"Etag": md5}, false, md5},
		{"dynamic large object", md5, swift.Headers{"X-Object-Manifest": "c/p"}, false, ""},
		{"static large object", md5, swift.Headers{"X-Static-Large-Object": "True"}, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := &Object{fs: &Fs{opt: Options{NoLargeObjects: test.noLargeObjects}}, md5: test.md5, headers: test.headers}
			assert.Equal(t, test.want, o.ifMatch())
		})
	}
}
