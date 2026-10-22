package thunder

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestThunder:",
		NilObject:  (*Object)(nil),
		ChunkedUpload: fstests.ChunkedUploadConfig{
			MinChunkSize: minChunkSize,
			MaxChunkSize: maxChunkSize,
		},
	})
}

func (f *Fs) SetUploadChunkSize(cs fs.SizeSuffix) (fs.SizeSuffix, error) {
	return f.setUploadChunkSize(cs)
}

func (f *Fs) SetUploadCutoff(cs fs.SizeSuffix) (fs.SizeSuffix, error) {
	return f.setUploadCutoff(cs)
}

var (
	_ fstests.SetUploadChunkSizer = (*Fs)(nil)
	_ fstests.SetUploadCutoffer   = (*Fs)(nil)
)

func TestGenerateDeviceSign(t *testing.T) {
	deviceID := "abcdef1234567890"
	pkgName := "com.xunlei.downloadprovider"
	sign := generateDeviceSign(deviceID, pkgName)

	assert.True(t, strings.HasPrefix(sign, "div101."+deviceID))
	assert.Equal(t, len("div101.")+len(deviceID)+32, len(sign))
}

// referenceGcid is a direct implementation of the GCID definition:
// the block size is chosen from the total size up front.
func referenceGcid(data []byte) string {
	size := int64(len(data))
	blockSize := int64(gcidMinBlockSize)
	for float64(size)/float64(blockSize) > gcidMaxBlocks && blockSize < gcidMaxBlockSize {
		blockSize <<= 1
	}
	total := sha1.New()
	for off := int64(0); off < size; off += blockSize {
		block := sha1.Sum(data[off:min(off+blockSize, size)])
		total.Write(block[:])
	}
	return hex.EncodeToString(total.Sum(nil))
}

// gcidOf runs data through the streaming hasher in uneven writes
func gcidOf(data []byte) string {
	h := newGcidHasher()
	for len(data) > 0 {
		n := min(len(data), 100003)
		_, _ = h.Write(data[:n])
		data = data[n:]
	}
	return hex.EncodeToString(h.Sum(nil))
}

func patterned(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func TestGcidKnownValues(t *testing.T) {
	// Test vectors computed independently with shasum and Python:
	//
	//   empty:  the SHA-1 of nothing (no blocks)
	//           printf ''            | shasum
	//   single block: SHA-1 of the SHA-1 digest of the data
	//           printf 'hello world' | shasum | xxd -r -p | shasum
	//   three blocks of 256 KiB (the last partial), byte i = i % 251:
	//           hashlib in Python following the GCID definition
	for _, test := range []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{"hello world", []byte("hello world"), "67becf85308acf0261750da1075681ee5c412f05"},
		{"three blocks", patterned(0x40000*2 + 12345), "d2cd939a1cd25dbdbd3cff663e606a037488cc2b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, gcidOf(test.data), "got = streaming hasher, want = known value")
			assert.Equal(t, test.want, referenceGcid(test.data), "got = reference, want = known value")
		})
	}
}

func TestGcidHasherMatchesReference(t *testing.T) {
	sizes := []int{1, gcidMinBlockSize - 1, gcidMinBlockSize, gcidMinBlockSize + 1, 3*gcidMinBlockSize + 7}
	if !testing.Short() {
		// around the switch from 256 KiB to 512 KiB blocks
		limit := gcidMinBlockSize * gcidMaxBlocks
		sizes = append(sizes, limit, limit+1)
	}
	for _, size := range sizes {
		data := patterned(size)
		assert.Equal(t, referenceGcid(data), gcidOf(data), "size %d", size)
	}
}

func TestGcidHasherSumAndReset(t *testing.T) {
	h := newGcidHasher()
	_, _ = h.Write([]byte("hello "))
	_ = h.Sum(nil) // must not change the state
	_, _ = h.Write([]byte("world"))
	assert.Equal(t, "67becf85308acf0261750da1075681ee5c412f05", hex.EncodeToString(h.Sum(nil)))
	h.Reset()
	assert.Equal(t, "da39a3ee5e6b4b0d3255bfef95601890afd80709", hex.EncodeToString(h.Sum(nil)))
}

func TestGcidRegistered(t *testing.T) {
	hashes, err := hash.StreamTypes(bytes.NewReader([]byte("hello world")), hash.NewHashSet(gcidType))
	require.NoError(t, err)
	assert.Equal(t, "67becf85308acf0261750da1075681ee5c412f05", hashes[gcidType])
}

func TestSpoolToTemp(t *testing.T) {
	data := patterned(gcidMinBlockSize + 10)
	out, gcid, n, cleanup, err := spoolToTemp(bytes.NewReader(data), -1)
	defer cleanup()
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), n)
	assert.Equal(t, referenceGcid(data), gcid)
	got, err := io.ReadAll(out)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	_, _, _, cleanup2, err := spoolToTemp(bytes.NewReader(data), int64(len(data))+1)
	defer cleanup2()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestCommand(t *testing.T) {
	f := &Fs{}
	ctx := context.Background()

	_, err := f.Command(ctx, "nonexistent_command", nil, nil)
	assert.ErrorIs(t, err, fs.ErrorCommandNotFound)

	_, err = f.Command(ctx, "deltask", []string{"a", "b"}, nil)
	assert.Error(t, err)
}
