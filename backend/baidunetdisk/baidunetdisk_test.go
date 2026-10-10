package baidunetdisk

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/rclone/rclone/backend/baidunetdisk/api"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestBaiduNetdisk:",
		NilObject:  (*Object)(nil),
	})
}

func TestDecryptMd5(t *testing.T) {
	// Standard valid hex md5 should remain untouched
	validHex := "0123456789abcdef0123456789abcdef"
	assert.Equal(t, validHex, api.DecryptMd5(validHex))

	// Short invalid string returned as-is
	assert.Equal(t, "short", api.DecryptMd5("short"))
}

func TestParseBaiduLink(t *testing.T) {
	// Standard format: md5#slicemd5#size#filename
	name, size, md5, slice, err := parseBaiduLink("0123456789abcdef0123456789abcdef#fedcba9876543210fedcba9876543210#1048576#movie.mp4")
	require.NoError(t, err)
	assert.Equal(t, "movie.mp4", name)
	assert.Equal(t, int64(1048576), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", md5)
	assert.Equal(t, "fedcba9876543210fedcba9876543210", slice)

	// bdlink:// prefix
	name, size, md5, slice, err = parseBaiduLink("bdlink://0123456789abcdef0123456789abcdef#fedcba9876543210fedcba9876543210#2048#doc.pdf")
	require.NoError(t, err)
	assert.Equal(t, "doc.pdf", name)
	assert.Equal(t, int64(2048), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", md5)
	assert.Equal(t, "fedcba9876543210fedcba9876543210", slice)

	// base64 encoded bdlink
	raw := "0123456789abcdef0123456789abcdef#fedcba9876543210fedcba9876543210#4096#archive.zip"
	b64 := base64.StdEncoding.EncodeToString([]byte(raw))
	name, size, md5, slice, err = parseBaiduLink(b64)
	require.NoError(t, err)
	assert.Equal(t, "archive.zip", name)
	assert.Equal(t, int64(4096), size)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", md5)
	assert.Equal(t, "fedcba9876543210fedcba9876543210", slice)

	// Invalid format
	_, _, _, _, err = parseBaiduLink("invalid")
	assert.Error(t, err)
}

func TestHashBlocks(t *testing.T) {
	md5hex := func(b []byte) string {
		sum := md5.Sum(b)
		return hex.EncodeToString(sum[:])
	}
	data := make([]byte, 600*1024+7)
	for i := range data {
		data[i] = byte(i * 7)
	}
	const chunk = 256 * 1024
	var out bytes.Buffer
	h, err := hashBlocks(bytes.NewReader(data), chunk, &out)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), h.size)
	assert.Equal(t, data, out.Bytes())
	assert.Equal(t, []string{md5hex(data[:chunk]), md5hex(data[chunk : 2*chunk]), md5hex(data[2*chunk:])}, h.blockList)
	assert.Equal(t, md5hex(data), h.contentMD5)
	assert.Equal(t, md5hex(data[:sliceMD5Size]), h.sliceMD5)

	// Exact multiple of the chunk size gives no empty trailing block
	h, err = hashBlocks(bytes.NewReader(data[:2*chunk]), chunk, nil)
	require.NoError(t, err)
	assert.Len(t, h.blockList, 2)
}
