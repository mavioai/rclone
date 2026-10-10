package filelu

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/linkcache"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
)

// Object describes a FileLu object
type Object struct {
	fs       *Fs
	remote   string
	size     int64
	modTime  time.Time
	fileCode string // file code from the listing, if known
}

// NewObject creates a new Object for the given remote path
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	var filePath string
	filePath = path.Join(f.root, remote)
	filePath = "/" + strings.Trim(filePath, "/")

	// Get File code
	fileCode, err := f.getFileCode(ctx, filePath)
	if err != nil {
		return nil, fs.ErrorObjectNotFound
	}

	// Get File info
	fileInfos, err := f.getFileInfo(ctx, fileCode)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileInfo := fileInfos.Result[0]
	size, _ := strconv.ParseInt(fileInfo.Size, 10, 64)

	returnedRemote := remote
	return &Object{
		fs:       f,
		remote:   returnedRemote,
		size:     size,
		modTime:  time.Now(),
		fileCode: fileCode,
	}, nil
}

// linkLifetime is how long a direct link is used. FileLu states no
// expiry for the links of /file/direct_link, so this is a conservative
// estimate.
const linkLifetime = 10 * time.Minute

// linkKey returns the key of the object's direct link.
func (o *Object) linkKey() string {
	id := o.fileCode
	if id == "" {
		id = path.Join(o.fs.root, o.remote)
	}
	return linkcache.Key(o.fs.Name(), id, strconv.FormatInt(o.size, 10))
}

// Open opens the object for reading
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	filePath := path.Join(o.fs.root, o.remote)
	fetch := func(ctx context.Context) (*linkcache.Link, error) {
		directLink, size, err := o.fs.getDirectLink(ctx, filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to get direct link: %w", err)
		}
		return &linkcache.Link{URL: directLink, Value: size}, nil
	}
	return linkcache.Default.Open(ctx, o.linkKey(), linkLifetime, fetch, func(ctx context.Context, link *linkcache.Link) (io.ReadCloser, error) {
		return o.download(ctx, link, options)
	})
}

// download reads the range of options from the direct link.
func (o *Object) download(ctx context.Context, link *linkcache.Link, options []fs.OpenOption) (io.ReadCloser, error) {
	size := o.size
	if linkSize, ok := link.Value.(int64); ok && linkSize > 0 {
		size = linkSize
	}

	// Offset and count of the range, for a server that ignores it
	var offset int64
	count := int64(-1)
	fs.FixRangeOption(options, size)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			offset, count = x.Decode(size)
		case *fs.SeekOption:
			offset = x.Offset
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	headers := fs.OpenOptionHeaders(options)

	var reader io.ReadCloser
	err := o.fs.pacer.Call(func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", link.URL, nil)
		if err != nil {
			return false, fmt.Errorf("failed to create download request: %w", err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := o.fs.client.Do(req)
		if err != nil {
			return shouldRetry(err), fmt.Errorf("failed to download file: %w", err)
		}

		switch resp.StatusCode {
		case http.StatusPartialContent:
			reader = resp.Body
			return false, nil
		case http.StatusOK:
			// The server sent the whole file: skip to the offset
			if offset > 0 {
				_, err = io.CopyN(io.Discard, resp.Body, offset)
				if err != nil {
					_ = resp.Body.Close()
					return shouldRetry(err), fmt.Errorf("failed to skip offset: %w", err)
				}
			}
			if count >= 0 {
				reader = readers.NewLimitedReadCloser(resp.Body, count)
			} else {
				reader = resp.Body
			}
			return false, nil
		}
		if err := resp.Body.Close(); err != nil {
			fs.Logf(nil, "Failed to close response body: %v", err)
		}
		err = &statusError{status: resp.StatusCode}
		return shouldRetry(err) || shouldRetryHTTP(resp.StatusCode), err
	})
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// statusError is the error of a download answered with an HTTP status
// other than 200 or 206.
type statusError struct {
	status int
}

// Error returns the error's message.
func (e *statusError) Error() string {
	return fmt.Sprintf("failed to download file: HTTP %d", e.status)
}

// HTTPStatusCode returns the HTTP status of the response, so that a
// refused direct link is recognised.
func (e *statusError) HTTPStatusCode() int {
	return e.status
}

// Update updates the object with new data
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// The new content gets a new file code and link
	linkcache.Default.Drop(o.linkKey(), nil)
	o.fileCode = ""
	size := src.Size()
	counter := readers.NewCountingReader(in)
	in = counter

	if size <= int64(o.fs.opt.UploadCutoff) {
		err := o.fs.uploadFile(ctx, in, o.remote)
		if err != nil {
			return err
		}
	} else {
		fullPath := path.Join(o.fs.root, o.remote)
		err := o.fs.multipartUpload(ctx, in, fullPath)
		if err != nil {
			return fmt.Errorf("failed to upload file: %w", err)
		}
	}

	// Check the source supplied the number of bytes it declared
	// otherwise a truncated file would be stored as a good upload
	if size >= 0 && int64(counter.BytesRead()) != size {
		if removeErr := o.Remove(ctx); removeErr != nil {
			fs.Errorf(o, "Failed to remove partially transferred object: %v", removeErr)
		}
		return fmt.Errorf("expected %d bytes in input, but got %d: %w", size, counter.BytesRead(), io.ErrUnexpectedEOF)
	}

	o.size = size
	o.modTime = src.ModTime(ctx)
	linkcache.Default.Drop(o.linkKey(), nil)
	return nil
}

// Remove deletes the object from FileLu
func (o *Object) Remove(ctx context.Context) error {
	fullPath := "/" + strings.Trim(path.Join(o.fs.root, o.remote), "/")

	err := o.fs.deleteFile(ctx, fullPath)
	if err != nil {
		return err
	}
	fs.Infof(o.fs, "Successfully deleted file: %s", fullPath)
	return nil
}

// Hash returns the MD5 hash of an object
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}

	var fileCode string
	if isFileCode(o.fs.root) {
		fileCode = o.fs.root
	} else {
		matches := regexp.MustCompile(`\((.*?)\)`).FindAllStringSubmatch(o.remote, -1)
		for _, match := range matches {
			if len(match) > 1 && len(match[1]) == 12 {
				fileCode = match[1]
				break
			}
		}
	}
	if fileCode == "" {
		return "", fmt.Errorf("no valid file code found in the remote path")
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/file/info",
		Parameters: url.Values{
			"file_code": {fileCode},
			"key":       {o.fs.opt.Key},
		},
	}

	var result struct {
		Status int    `json:"status"`
		Msg    string `json:"msg"`
		Result []struct {
			Hash string `json:"hash"`
		} `json:"result"`
	}

	err := o.fs.pacer.Call(func() (bool, error) {
		_, err := o.fs.srv.CallJSON(ctx, &opts, nil, &result)
		if err != nil {
			return shouldRetry(err), err
		}
		return false, nil
	})
	if err != nil {
		return "", err
	}

	if result.Status != 200 || len(result.Result) == 0 {
		return "", fmt.Errorf("error: unable to fetch hash: %s", result.Msg)
	}

	return result.Result[0].Hash, nil
}

// String returns a string representation of the object
func (o *Object) String() string {
	return o.remote
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable indicates whether the object is storable
func (o *Object) Storable() bool {
	return true
}
