package quark

import (
	"html"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/quark/api"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestQuark:",
		NilObject:  (*Object)(nil),
	})
}

func TestFileNameUnescape(t *testing.T) {
	raw := "Tom &amp; Jerry &quot;Classic&quot; &lt;1080p&gt;.mp4"
	unescaped := html.UnescapeString(raw)
	assert.Equal(t, "Tom & Jerry \"Classic\" <1080p>.mp4", unescaped)
}

func TestFileMetadata(t *testing.T) {
	now := time.Now().UnixMilli()
	file := &api.File{
		Fid:       "fid123",
		FileName:  "test.txt",
		File:      true,
		Size:      1024,
		CreatedAt: now,
		UpdatedAt: now + 5000,
	}

	assert.False(t, file.IsDir())
	assert.Equal(t, now+5000, file.ModTime().UnixMilli())

	dir := &api.File{
		Fid:      "dir123",
		FileName: "folder",
		File:     false,
	}
	assert.True(t, dir.IsDir())
}

func TestSetCookieValue(t *testing.T) {
	assert.Equal(t, "a=1; __puus=new; b=2", setCookieValue("a=1; __puus=old; b=2", "__puus", "new"))
	assert.Equal(t, "a=1; __puus=new", setCookieValue("a=1;", "__puus", "new"))
	assert.Equal(t, "__puus=new", setCookieValue("", "__puus", "new"))
}

func TestNewUploadTarget(t *testing.T) {
	for _, uploadURL := range []string{
		"http://oss-cn-shenzhen.aliyuncs.com",
		"https://oss-cn-shenzhen.aliyuncs.com",
		"https://oss-cn-shenzhen.aliyuncs.com/",
		"oss-cn-shenzhen.aliyuncs.com",
	} {
		pre := &api.UpPreResp{}
		pre.Data.UploadURL = uploadURL
		pre.Data.Bucket = "ul-zb"
		pre.Data.ObjKey = "dir/obj key"
		target, err := newUploadTarget(pre)
		require.NoError(t, err, uploadURL)
		assert.Equal(t, "https://ul-zb.oss-cn-shenzhen.aliyuncs.com/dir/obj%20key", target.url, uploadURL)
	}
	pre := &api.UpPreResp{}
	pre.Data.UploadURL = "https://"
	_, err := newUploadTarget(pre)
	assert.Error(t, err)
}

func TestSpoolToTemp(t *testing.T) {
	file, size, md5sum, sha1sum, cleanup, err := spoolToTemp(strings.NewReader("hello"))
	require.NoError(t, err)
	defer cleanup()
	assert.Equal(t, int64(5), size)
	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", md5sum)
	assert.Equal(t, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", sha1sum)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestRespErr(t *testing.T) {
	assert.NoError(t, (&api.Resp{Status: 200}).Err())
	assert.Error(t, (&api.Resp{Status: 200, Code: 1}).Err())
	assert.Error(t, (&api.Resp{Status: 404}).Err())
	assert.True(t, (&api.Resp{Status: 404, Code: api.CodeFileNotFound}).IsNotFound())
}
