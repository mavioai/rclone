package cloud115

import (
	"fmt"
	"io"
	"os"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

// spoolToTemp copies in to a temporary file while computing the hashes
// in types. It is used only when the source can't provide the hash the
// API needs before the upload starts.
//
// The returned file is positioned at the start. The caller must call
// cleanup to close and delete it.
func spoolToTemp(in io.Reader, types hash.Set) (file *os.File, size int64, sums map[hash.Type]string, cleanup func(), err error) {
	file, err = os.CreateTemp("", "rclone-cloud115-")
	if err != nil {
		return nil, 0, nil, nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	cleanup = func() {
		if err := file.Close(); err != nil {
			fs.Debugf(nil, "cloud115: failed to close temporary file: %v", err)
		}
		if err := os.Remove(file.Name()); err != nil {
			fs.Debugf(nil, "cloud115: failed to remove temporary file: %v", err)
		}
	}
	hasher, err := hash.NewMultiHasherTypes(types)
	if err != nil {
		cleanup()
		return nil, 0, nil, nil, err
	}
	size, err = io.Copy(io.MultiWriter(file, hasher), in)
	if err != nil {
		cleanup()
		return nil, 0, nil, nil, fmt.Errorf("failed to spool upload to temporary file: %w", err)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, nil, nil, fmt.Errorf("failed to rewind temporary file: %w", err)
	}
	return file, size, hasher.Sums(), cleanup, nil
}
