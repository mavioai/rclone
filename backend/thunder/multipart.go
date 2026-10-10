package thunder

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/rclone/backend/thunder/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/chunksize"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/readers"
	"golang.org/x/sync/errgroup"
)

const (
	bufferSize           = 1024 * 1024     // default size of the pages used in the reader
	bufferCacheSize      = 64              // max number of buffers to keep in cache
	bufferCacheFlushTime = 5 * time.Second // flush the cached buffers after this long
)

// bufferPool is a global pool of buffers
var (
	bufferPool     *pool.Pool
	bufferPoolOnce sync.Once
)

// get a buffer pool
func getPool() *pool.Pool {
	bufferPoolOnce.Do(func() {
		ci := fs.GetConfig(context.Background())
		// Initialise the buffer pool when used
		bufferPool = pool.New(bufferCacheFlushTime, bufferSize, bufferCacheSize, ci.UseMmap)
	})
	return bufferPool
}

// newRW gets a pool.RW using the multipart pool
func newRW() *pool.RW {
	return pool.NewRW(getPool())
}

// newS3Client makes an S3 client for the upload credentials in p
func (f *Fs) newS3Client(ctx context.Context, p *api.ResumableParams) *s3.Client {
	endpoint := strings.TrimPrefix(p.Endpoint, p.Bucket+".")
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	ci := fs.GetConfig(ctx)
	return s3.New(s3.Options{
		Region:                     "xunlei",
		BaseEndpoint:               aws.String(endpoint),
		Credentials:                credentials.NewStaticCredentialsProvider(p.AccessKeyID, p.AccessKeySecret, p.SecurityToken),
		HTTPClient:                 fshttp.NewClient(ctx),
		RetryMaxAttempts:           ci.LowLevelRetries,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

// uploadToS3 uploads in to the storage given by the upload ticket
func (f *Fs) uploadToS3(ctx context.Context, in io.Reader, remote string, size int64, p *api.ResumableParams, options ...fs.OpenOption) error {
	if size < 0 || size >= int64(f.opt.UploadCutoff) {
		w, err := f.newChunkWriter(ctx, remote, size, p, in, options...)
		if err != nil {
			return fmt.Errorf("multipart upload failed to initialise: %w", err)
		}
		return w.Upload(ctx)
	}

	// upload singlepart
	client := f.newS3Client(ctx, p)
	counter := readers.NewCountingReader(in)
	req := &s3.PutObjectInput{
		Bucket:        &p.Bucket,
		Key:           &p.Key,
		Body:          io.NopCloser(io.LimitReader(counter, size)),
		ContentLength: &size,
	}
	applyUploadOptions(options, &req.CacheControl, &req.ContentDisposition, &req.ContentEncoding, &req.ContentType)
	// Can't retry single part uploads as only have an io.Reader
	noRetry := func(o *s3.Options) {
		o.RetryMaxAttempts = 1
	}
	err := f.pacer.CallNoRetry(func() (bool, error) {
		_, err := client.PutObject(ctx, req, noRetry)
		return shouldRetryNoAuth(ctx, nil, err)
	})
	if err != nil {
		return err
	}
	// Check the input was exactly the declared size
	var extra [1]byte
	if n, _ := io.ReadFull(counter, extra[:]); n > 0 {
		return fmt.Errorf("expected %d bytes in input, but got more", size)
	}
	if int64(counter.BytesRead()) != size {
		return fmt.Errorf("expected %d bytes in input, but got %d: %w", size, counter.BytesRead(), io.ErrUnexpectedEOF)
	}
	return nil
}

// applyUploadOptions copies the supported upload options into the
// request fields passed in
func applyUploadOptions(options []fs.OpenOption, cacheControl, contentDisposition, contentEncoding, contentType **string) {
	for _, option := range options {
		key, value := option.Header()
		switch strings.ToLower(key) {
		case "cache-control":
			*cacheControl = aws.String(value)
		case "content-disposition":
			*contentDisposition = aws.String(value)
		case "content-encoding":
			*contentEncoding = aws.String(value)
		case "content-type":
			*contentType = aws.String(value)
		}
	}
}

var warnStreamUpload sync.Once

// chunkWriter is the state of a multipart upload
type chunkWriter struct {
	chunkSize      int64
	size           int64
	con            int
	f              *Fs
	remote         string
	in             io.Reader
	mu             sync.Mutex
	completedParts []types.CompletedPart
	client         *s3.Client
	mOut           *s3.CreateMultipartUploadOutput
}

func (f *Fs) newChunkWriter(ctx context.Context, remote string, size int64, p *api.ResumableParams, in io.Reader, options ...fs.OpenOption) (w *chunkWriter, err error) {
	// calculate size of parts
	chunkSize := f.opt.ChunkSize

	// size can be -1 here meaning we don't know the size of the incoming file.
	if size == -1 {
		warnStreamUpload.Do(func() {
			fs.Logf(f, "Streaming uploads using chunk size %v will have maximum file size of %v",
				f.opt.ChunkSize, fs.SizeSuffix(int64(chunkSize)*int64(maxUploadParts)))
		})
	} else {
		chunkSize = chunksize.Calculator(remote, size, maxUploadParts, chunkSize)
	}

	w = &chunkWriter{
		chunkSize:      int64(chunkSize),
		size:           size,
		con:            max(1, f.opt.UploadConcurrency),
		f:              f,
		remote:         remote,
		in:             in,
		completedParts: make([]types.CompletedPart, 0),
		client:         f.newS3Client(ctx, p),
	}

	req := &s3.CreateMultipartUploadInput{
		Bucket: &p.Bucket,
		Key:    &p.Key,
	}
	applyUploadOptions(options, &req.CacheControl, &req.ContentDisposition, &req.ContentEncoding, &req.ContentType)
	err = f.pacer.Call(func() (bool, error) {
		w.mOut, err = w.client.CreateMultipartUpload(ctx, req)
		return shouldRetryNoAuth(ctx, nil, err)
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload failed: %w", err)
	}
	fs.Debugf(remote, "multipart upload: %q initiated", *w.mOut.UploadId)
	return w, nil
}

// Upload does a multipart upload in parallel
func (w *chunkWriter) Upload(ctx context.Context) (err error) {
	// make concurrency machinery
	tokens := pacer.NewTokenDispenser(w.con)

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer atexit.OnError(&err, func() {
		cancel()
		fs.Debugf(w.remote, "multipart upload: Cancelling...")
		errCancel := w.Abort(ctx)
		if errCancel != nil {
			fs.Debugf(w.remote, "multipart upload: failed to cancel: %v", errCancel)
		}
	})()

	var (
		g, gCtx  = errgroup.WithContext(uploadCtx)
		finished = false
		off      int64
	)

	// Do the accounting manually
	in, acc := accounting.UnWrapAccounting(w.in)

	for partNum := int64(0); !finished; partNum++ {
		// Get a block of memory from the pool and token which limits concurrency.
		tokens.Get()
		rw := newRW()
		if acc != nil {
			rw.SetAccounting(acc.AccountRead)
		}

		free := func() {
			// return the memory and token
			_ = rw.Close() // Can't return an error
			tokens.Put()
		}

		// Fail fast, in case an errgroup managed function returns an error
		if gCtx.Err() != nil {
			free()
			break
		}

		// Read the chunk
		var n int64
		n, err = io.CopyN(rw, in, w.chunkSize)
		if err == io.EOF {
			if n == 0 && partNum != 0 { // end if no data and if not first chunk
				free()
				break
			}
			finished = true
		} else if err != nil {
			free()
			return fmt.Errorf("multipart upload: failed to read source: %w", err)
		}

		partNum := partNum
		partOff := off
		off += n
		g.Go(func() (err error) {
			defer free()
			fs.Debugf(w.remote, "multipart upload: starting chunk %d size %v offset %v/%v", partNum, fs.SizeSuffix(n), fs.SizeSuffix(partOff), fs.SizeSuffix(w.size))
			_, err = w.WriteChunk(gCtx, int32(partNum), rw)
			return err
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	// Check the source supplied the number of bytes it declared
	if w.size >= 0 && off != w.size {
		return fmt.Errorf("multipart upload: expected %d bytes in input, but got %d: %w", w.size, off, io.ErrUnexpectedEOF)
	}

	err = w.Close(ctx)
	if err != nil {
		return fmt.Errorf("multipart upload: failed to finalise: %w", err)
	}
	return nil
}

// add a part number and etag to the completed parts
func (w *chunkWriter) addCompletedPart(part types.CompletedPart) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.completedParts = append(w.completedParts, part)
}

// WriteChunk will write chunk number with reader bytes, where chunk number >= 0
func (w *chunkWriter) WriteChunk(ctx context.Context, chunkNumber int32, reader io.ReadSeeker) (currentChunkSize int64, err error) {
	if chunkNumber < 0 {
		return -1, fmt.Errorf("invalid chunk number provided: %v", chunkNumber)
	}

	partNumber := chunkNumber + 1
	var res *s3.UploadPartOutput
	err = w.f.pacer.Call(func() (bool, error) {
		// Discover the size by seeking to the end
		currentChunkSize, err = reader.Seek(0, io.SeekEnd)
		if err != nil {
			return false, err
		}
		// rewind the reader on retry
		_, err := reader.Seek(0, io.SeekStart)
		if err != nil {
			return false, err
		}
		res, err = w.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        w.mOut.Bucket,
			Key:           w.mOut.Key,
			UploadId:      w.mOut.UploadId,
			PartNumber:    &partNumber,
			Body:          reader,
			ContentLength: &currentChunkSize,
		})
		if err != nil {
			if chunkNumber <= 8 {
				return shouldRetryNoAuth(ctx, nil, err)
			}
			// retry all chunks once have done the first few
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return -1, fmt.Errorf("failed to upload chunk %d with %v bytes: %w", partNumber, currentChunkSize, err)
	}

	w.addCompletedPart(types.CompletedPart{
		PartNumber: &partNumber,
		ETag:       res.ETag,
	})

	fs.Debugf(w.remote, "multipart upload: wrote chunk %d with %v bytes", partNumber, currentChunkSize)
	return currentChunkSize, nil
}

// Abort the multipart upload
func (w *chunkWriter) Abort(ctx context.Context) (err error) {
	err = w.f.pacer.Call(func() (bool, error) {
		_, err = w.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   w.mOut.Bucket,
			Key:      w.mOut.Key,
			UploadId: w.mOut.UploadId,
		})
		return shouldRetryNoAuth(ctx, nil, err)
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload %q: %w", *w.mOut.UploadId, err)
	}
	fs.Debugf(w.remote, "multipart upload: %q aborted", *w.mOut.UploadId)
	return nil
}

// Close and finalise the multipart upload
func (w *chunkWriter) Close(ctx context.Context) (err error) {
	// sort the completed parts by part number
	sort.Slice(w.completedParts, func(i, j int) bool {
		return *w.completedParts[i].PartNumber < *w.completedParts[j].PartNumber
	})
	err = w.f.pacer.Call(func() (bool, error) {
		_, err = w.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   w.mOut.Bucket,
			Key:      w.mOut.Key,
			UploadId: w.mOut.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: w.completedParts,
			},
		})
		return shouldRetryNoAuth(ctx, nil, err)
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	fs.Debugf(w.remote, "multipart upload: %q finished", *w.mOut.UploadId)
	return nil
}
