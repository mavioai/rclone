package cloud115

import (
	"io"
	"strings"
	"testing"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestCloud115:",
		NilObject:  (*Object)(nil),
	})
}

func TestParse115Link(t *testing.T) {
	// Format 1: 115://filename|filesize|sha1|block_sha1
	name, size, sha1, block, err := parse115Link("115://test.mp4|1048576|0123456789abcdef0123456789abcdef01234567|fedcba9876543210fedcba9876543210fedcba98")
	require.NoError(t, err)
	assert.Equal(t, "test.mp4", name)
	assert.Equal(t, int64(1048576), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef01234567", sha1)
	assert.Equal(t, "fedcba9876543210fedcba9876543210fedcba98", block)

	// Format 2: without prefix
	name, size, sha1, block, err = parse115Link("test2.mkv|2048|0123456789abcdef0123456789abcdef01234567")
	require.NoError(t, err)
	assert.Equal(t, "test2.mkv", name)
	assert.Equal(t, int64(2048), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef01234567", sha1)
	assert.Equal(t, "", block)

	// Format 3: sha1|filesize|filename
	name, size, sha1, block, err = parse115Link("0123456789abcdef0123456789abcdef01234567|4096|test3.iso")
	require.NoError(t, err)
	assert.Equal(t, "test3.iso", name)
	assert.Equal(t, int64(4096), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef01234567", sha1)
	assert.Equal(t, "", block)

	// Invalid format
	_, _, _, _, err = parse115Link("invalid_link")
	assert.Error(t, err)
}

func TestSpoolToTemp(t *testing.T) {
	data := "abcdefghijklmnopqrstuvwxyz"
	file, size, sums, cleanup, err := spoolToTemp(strings.NewReader(data), hash.NewHashSet(hash.SHA1))
	require.NoError(t, err)
	defer cleanup()
	assert.Equal(t, int64(len(data)), size)
	assert.Equal(t, "32d10c7b8cf96570ca04ce37f2a19d84240d3a89", sums[hash.SHA1])
	got, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestSHA1Range(t *testing.T) {
	sum, err := sha1Range(strings.NewReader("abcdef"), 3)
	require.NoError(t, err)
	assert.Equal(t, "A9993E364706816ABA3E25717850C26C9CD0D89D", sum) // SHA1("abc")

	_, err = sha1Range(strings.NewReader("ab"), 3)
	assert.Error(t, err)
}

func TestQRState(t *testing.T) {
	in := &driver115.QRCodeSession{UID: "uid", Time: 42, Sign: "sign", QrcodeContent: "content"}
	state, err := encodeQRState(in)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(state, qrStatePrefix))
	assert.NotContains(t, state, ",")
	out, err := decodeQRState(state)
	require.NoError(t, err)
	assert.Equal(t, &driver115.QRCodeSession{UID: "uid", Time: 42, Sign: "sign"}, out)
}
