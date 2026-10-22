package seafile

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/linkcache"
)

// Object describes a seafile object (also commonly called a file)
type Object struct {
	fs            *Fs       // what this object is part of
	remote        string    // The remote path (full path containing library name if target at root)
	pathInLibrary string    // Path of the object without the library name
	size          int64     // size of the object
	modTime       time.Time // modification time of the object
	libraryID     string    // Needed to download the file
}

// ==================== Interface fs.DirEntry ====================

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote string
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns last modified time
func (o *Object) ModTime(context.Context) time.Time {
	return o.modTime
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ==================== Interface fs.ObjectInfo ====================

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// ==================== Interface fs.Object ====================

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	key := linkcache.Key(o.fs.Name(), o.libraryID, o.pathInLibrary, strconv.FormatInt(o.size, 10), strconv.FormatInt(o.modTime.UnixNano(), 10))
	fetch := func(ctx context.Context) (*linkcache.Link, error) {
		downloadLink, err := o.fs.getDownloadLink(ctx, o.libraryID, o.pathInLibrary)
		if err != nil {
			return nil, err
		}
		return &linkcache.Link{URL: downloadLink}, nil
	}
	return linkcache.Default.Open(ctx, key, linkLifetime, fetch, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.fs.download(ctx, link.URL, o.Size(), options...)
	})
}

// linkLifetime is how long a download link is used: Seafile's reusable
// download links (reuse=1) last an hour.
const linkLifetime = time.Hour

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	uploadLink, err := o.fs.getUploadLink(ctx, o.libraryID)
	if err != nil {
		return err
	}

	// The upload can't be retried here as the input stream can't be re-read and
	// the upload link is single use, so upload returns a retry
	// error for the caller to retry with a fresh stream.
	uploaded, err := o.fs.upload(ctx, in, uploadLink, o.pathInLibrary)
	if err != nil {
		return err
	}
	// Set the properties from the upload back to the object
	o.size = uploaded.Size

	return nil
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteFile(ctx, o.libraryID, o.pathInLibrary)
}
