package readers

import (
	"fmt"
	"io"

	"github.com/rclone/rclone/fs"
)

// SkipToRange returns the part of body that options ask for, for a server
// that answered a request for a range with the whole object (200 instead
// of 206): it discards the bytes before the range and ends with it.
//
// size is the object's size, -1 when unknown. On error body is closed.
func SkipToRange(body io.ReadCloser, size int64, options []fs.OpenOption) (io.ReadCloser, error) {
	var offset, limit int64 = 0, -1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset, limit = x.Offset, -1
		case *fs.RangeOption:
			if x.Start < 0 && size < 0 {
				_ = body.Close()
				return nil, fmt.Errorf("range from the end of an object of unknown size")
			}
			offset, limit = x.Decode(size)
		}
	}
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, body, offset); err != nil {
			_ = body.Close()
			return nil, fmt.Errorf("skip to offset %d: %w", offset, err)
		}
	}
	return NewLimitedReadCloser(body, limit), nil
}

// RangeRequested reports whether options ask for part of an object.
func RangeRequested(options []fs.OpenOption) bool {
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			if x.Offset != 0 {
				return true
			}
		case *fs.RangeOption:
			if x.Start != 0 || x.End >= 0 {
				return true
			}
		}
	}
	return false
}
