//go:build !plan9

package weiyun

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/stretchr/testify/assert"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestWeiyun:",
		NilObject:  (*Object)(nil),
	})
}

func TestNewFsMissingCookie(t *testing.T) {
	ctx := context.Background()
	m := configmap.Simple{}
	_, err := NewFs(ctx, "myweiyun", "", m)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cookies is required")
}
