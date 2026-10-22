package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLink checks that Link hard links a file to another folder, both
// names sharing its data, and that operations.Link refuses an existing
// destination.
func TestLink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "downloads"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "downloads", "film.mkv"), []byte("film"), 0o666))
	f, err := NewFs(ctx, "local", dir, configmap.Simple{})
	require.NoError(t, err)
	require.NotNil(t, f.Features().Link)

	require.NoError(t, operations.Link(ctx, f, f, "movies/Film (1999)/film.mkv", "downloads/film.mkv"))
	src, err := os.Stat(filepath.Join(dir, "downloads", "film.mkv"))
	require.NoError(t, err)
	dst, err := os.Stat(filepath.Join(dir, "movies", "Film (1999)", "film.mkv"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(src, dst), "got = separate files, want = one file linked")

	err = operations.Link(ctx, f, f, "movies/Film (1999)/film.mkv", "downloads/film.mkv")
	assert.ErrorContains(t, err, "already exists")
	err = operations.Link(ctx, f, f, "movies/other.mkv", "downloads/missing.mkv")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}
