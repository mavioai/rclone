package aliyundrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestAliyunDrive:",
		NilObject:  (*Object)(nil),
	})
}

func TestCalcProofCode(t *testing.T) {
	f := &Fs{}
	ctx := context.Background()
	data := []byte("hello world, this is a test payload for proof code calculation in aliyundrive")
	size := int64(len(data))
	const token = "test_access_token_123456"

	proof, err := f.calcProofCode(ctx, token, nil, bytes.NewReader(data), size)
	require.NoError(t, err)
	start, length, err := proofRange(token, size)
	require.NoError(t, err)
	assert.True(t, length > 0 && length <= 8)
	assert.Equal(t, base64.StdEncoding.EncodeToString(data[start:start+length]), proof)

	// Zero size returns empty proof code
	proofZero, errZero := f.calcProofCode(ctx, token, nil, bytes.NewReader(nil), 0)
	require.NoError(t, errZero)
	assert.Empty(t, proofZero)
}

func TestJWTSub(t *testing.T) {
	enc := base64.RawURLEncoding.EncodeToString
	token := enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(`{"sub":"user-1"}`)) + ".sig"
	sub, err := jwtSub(token)
	require.NoError(t, err)
	assert.Equal(t, "user-1", sub)

	_, err = jwtSub("not-a-jwt")
	assert.Error(t, err)
}
