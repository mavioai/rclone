package cloud115

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	cipher "github.com/SheltonZhu/115driver/pkg/crypto/ec115"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

const (
	// preHashSize is how much of the start of the file the
	// rapid upload pre-check hashes
	preHashSize = 128 * 1024
	// maxProofRounds limits how many range proofs rapid upload answers
	maxProofRounds = 3
	// ossTokenLifetime is how long an OSS token is used before it is
	// refreshed; 115 issues tokens valid for an hour
	ossTokenLifetime = 50 * time.Minute
)

// proveFunc returns the upper case hex SHA1 of the byte range start-end
// (inclusive) of the file being uploaded
type proveFunc func(ctx context.Context, start, end int64) (string, error)

// sha1Range hashes length bytes read from r
func sha1Range(r io.Reader, length int64) (string, error) {
	h := sha1.New()
	n, err := io.CopyN(h, r, length)
	if err != nil {
		return "", fmt.Errorf("read %d of %d bytes for range proof: %w", n, length, err)
	}
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil))), nil
}

// rapidUpload asks 115 to create the file from content it already
// stores, answering range proofs with prove if the server asks for
// them. prove may be nil if the content isn't available.
//
// This mirrors the 115 driver's RapidUpload, which ignores errors from
// the range proof and can loop forever on them.
func (f *Fs) rapidUpload(ctx context.Context, size int64, name, dirID, preID, fileID string, prove proveFunc) (*driver115.UploadInitResp, error) {
	ecdh, err := cipher.NewEcdhCipher()
	if err != nil {
		return nil, err
	}
	target := "U_1_" + dirID
	sizeStr := strconv.FormatInt(size, 10)
	form := url.Values{}
	form.Set("appid", "0")
	form.Set("appversion", f.version)
	form.Set("userid", strconv.FormatInt(f.client.UserID, 10))
	form.Set("filename", name)
	form.Set("filesize", sizeStr)
	form.Set("fileid", fileID)
	form.Set("target", target)
	form.Set("sig", f.client.GenerateSignature(fileID, target))
	form.Set("topupload", "true")

	signKey, signVal := "", ""
	for round := 0; ; round++ {
		var result driver115.UploadInitResp
		err = f.pacer.Call(func() (bool, error) {
			t := driver115.NowMilli()
			token, err := ecdh.EncodeToken(t.ToInt64())
			if err != nil {
				return false, err
			}
			form.Set("t", t.String())
			form.Set("token", f.client.GenerateToken(fileID, preID, t.String(), sizeStr, signKey, signVal))
			if signKey != "" && signVal != "" {
				form.Set("sign_key", signKey)
				form.Set("sign_val", signVal)
			}
			encrypted, err := ecdh.Encrypt([]byte(form.Encode()))
			if err != nil {
				return false, err
			}
			resp, err := f.client.NewRequest().
				SetContext(ctx).
				SetQueryParam("k_ec", token).
				SetBody(encrypted).
				SetHeaderVerbatim("Content-Type", "application/x-www-form-urlencoded").
				SetDoNotParseResponse(true).
				Post(driver115.ApiUploadInit)
			if err != nil {
				return f.shouldRetry(ctx, responseOf(err), err)
			}
			body := resp.RawBody()
			data, err := io.ReadAll(body)
			_ = body.Close()
			if err != nil {
				return f.shouldRetry(ctx, resp.RawResponse, err)
			}
			decrypted, err := ecdh.Decrypt(data)
			if err != nil {
				return false, fmt.Errorf("failed to decrypt upload init response: %w", err)
			}
			result = driver115.UploadInitResp{}
			if err = json.Unmarshal(decrypted, &result); err != nil {
				return false, fmt.Errorf("failed to decode upload init response: %w", err)
			}
			return false, result.Err()
		})
		if err != nil {
			return nil, err
		}
		result.SHA1 = fileID
		if result.Status != 7 {
			return &result, nil
		}

		// The server wants the SHA1 of a range to prove we have the content
		if prove == nil {
			return nil, fmt.Errorf("server requested a proof of range %s but the content is not available", result.SignCheck)
		}
		if round >= maxProofRounds {
			return nil, errors.New("server kept requesting range proofs")
		}
		var start, end int64
		if _, err = fmt.Sscanf(result.SignCheck, "%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= size {
			return nil, fmt.Errorf("invalid range proof request %q", result.SignCheck)
		}
		signKey = result.SignKey
		if signVal, err = prove(ctx, start, end); err != nil {
			return nil, fmt.Errorf("range proof failed: %w", err)
		}
	}
}

// upload uploads in as leaf into the directory dirID.
//
// The rapid upload check needs the SHA1 of the whole file and of its
// first 128 KiB before the upload starts, and may ask for the SHA1 of
// any range. If src supplies the SHA1 and can be read by range, in is
// streamed; otherwise it is spooled to a temporary file first.
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote, leaf, dirID string) (*Object, error) {
	size := src.Size()
	sha1sum := ""
	srcObj := fs.UnWrapObjectInfo(src)
	if size >= 0 && srcObj != nil {
		sha1sum, _ = src.Hash(ctx, hash.SHA1)
	}

	var head []byte
	var prove proveFunc
	if sha1sum == "" {
		tmp, n, sums, cleanup, err := spoolToTemp(in, hash.NewHashSet(hash.SHA1))
		if err != nil {
			return nil, err
		}
		defer cleanup()
		size, sha1sum = n, sums[hash.SHA1]
		head = make([]byte, min(size, preHashSize))
		if _, err = tmp.ReadAt(head, 0); err != nil {
			return nil, fmt.Errorf("failed to read temporary file: %w", err)
		}
		prove = func(ctx context.Context, start, end int64) (string, error) {
			return sha1Range(io.NewSectionReader(tmp, start, end-start+1), end-start+1)
		}
		in = tmp
	} else {
		head = make([]byte, min(size, preHashSize))
		if _, err := io.ReadFull(in, head); err != nil {
			return nil, fmt.Errorf("failed to read start of file: %w", err)
		}
		prove = func(ctx context.Context, start, end int64) (sum string, err error) {
			if end < int64(len(head)) {
				return sha1Range(bytes.NewReader(head[start:]), end-start+1)
			}
			rc, err := srcObj.Open(ctx, &fs.RangeOption{Start: start, End: end})
			if err != nil {
				return "", err
			}
			defer fs.CheckClose(rc, &err)
			return sha1Range(rc, end-start+1)
		}
		in = io.MultiReader(bytes.NewReader(head), in)
	}
	sha1sum = strings.ToUpper(sha1sum)
	preHash := sha1.Sum(head)

	if info := f.client.UploadMetaInfo; info != nil && info.SizeLimit > 0 && size > info.SizeLimit {
		return nil, fmt.Errorf("file size %d exceeds the upload limit %d: %w", size, info.SizeLimit, driver115.ErrUploadTooLarge)
	}

	fastInfo, err := f.rapidUpload(ctx, size, f.opt.Enc.FromStandardName(leaf), dirID,
		strings.ToUpper(hex.EncodeToString(preHash[:])), sha1sum, prove)
	if err != nil {
		return nil, fmt.Errorf("upload init failed: %w", err)
	}
	matched, err := fastInfo.Ok()
	if err != nil {
		return nil, fmt.Errorf("upload init failed: %w", err)
	}

	if matched {
		fs.Debugf(src, "Rapid upload succeeded")
		// The rapid upload response doesn't carry the file ID
		return f.findByPickCode(ctx, remote, dirID, fastInfo.PickCode)
	}

	if fastInfo.Bucket == "" || fastInfo.Object == "" {
		return nil, errors.New("upload init returned no OSS upload target")
	}
	fastInfo.UploadOSSParams.SHA1 = sha1sum
	result, err := f.ossUpload(ctx, &fastInfo.UploadOSSParams, in, size)
	if err != nil {
		return nil, err
	}
	if result.Data.FileID == "" {
		return f.findByPickCode(ctx, remote, dirID, result.Data.PickCode)
	}
	return &Object{
		fs:       f,
		remote:   remote,
		id:       result.Data.FileID,
		size:     size,
		sha1:     strings.ToLower(sha1sum),
		pickCode: result.Data.PickCode,
		modTime:  time.Now(),
	}, nil
}

// findByPickCode finds the uploaded file with pickCode in dirID. 115
// allows duplicate names, so the name alone may match an old version.
func (f *Fs) findByPickCode(ctx context.Context, remote, dirID, pickCode string) (*Object, error) {
	if pickCode == "" {
		return nil, errors.New("upload returned neither file ID nor pick code")
	}
	files, err := f.listAll(ctx, dirID)
	if err != nil {
		return nil, err
	}
	for i := range files {
		if !files[i].IsDirectory && files[i].PickCode == pickCode {
			return f.newObjectWithInfo(remote, &files[i]), nil
		}
	}
	return nil, fmt.Errorf("uploaded file with pick code %s not found", pickCode)
}

// ossBucket is an OSS bucket handle with the token it was made with
type ossBucket struct {
	bucket *oss.Bucket
	token  *driver115.UploadOSSTokenResp
	at     time.Time
}

// newOSSBucket fetches an OSS token and opens the bucket with it
func (f *Fs) newOSSBucket(ctx context.Context, name string) (*ossBucket, error) {
	var token *driver115.UploadOSSTokenResp
	err := f.call(ctx, func() (err error) {
		token, err = f.client.GetOSSToken()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get OSS token: %w", err)
	}
	client, err := oss.New(driver115.OSSEndpoint, token.AccessKeyID, token.AccessKeySecret)
	if err != nil {
		return nil, err
	}
	bucket, err := client.Bucket(name)
	if err != nil {
		return nil, err
	}
	return &ossBucket{bucket: bucket, token: token, at: time.Now()}, nil
}

// refresh replaces the token if it is about to expire
func (b *ossBucket) refresh(ctx context.Context, f *Fs) error {
	if time.Since(b.at) < ossTokenLifetime {
		return nil
	}
	nb, err := f.newOSSBucket(ctx, b.bucket.BucketName)
	if err != nil {
		return err
	}
	*b = *nb
	return nil
}

// options returns the OSS request options for params
func (b *ossBucket) options(ctx context.Context, params *driver115.UploadOSSParams, extra ...oss.Option) []oss.Option {
	return append(append(driver115.OssOption(params, b.token), oss.WithContext(ctx)), extra...)
}

// decodeUploadResult parses the 115 callback result of an OSS upload
func decodeUploadResult(body []byte) (*driver115.UploadResult, error) {
	var result driver115.UploadResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to decode upload result: %w", err)
	}
	if err := result.Err(string(body)); err != nil {
		return nil, err
	}
	return &result, nil
}

// ossUpload uploads size bytes from in to OSS.
//
// Files up to chunk_size are sent in one request, larger ones in parts.
// At most one chunk is buffered in memory, so each request can be
// retried.
func (f *Fs) ossUpload(ctx context.Context, params *driver115.UploadOSSParams, in io.Reader, size int64) (*driver115.UploadResult, error) {
	b, err := f.newOSSBucket(ctx, params.Bucket)
	if err != nil {
		return nil, err
	}

	chunkSize := int64(f.opt.ChunkSize)
	if size <= chunkSize {
		buf := make([]byte, size)
		if _, err = io.ReadFull(in, buf); err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
		var body []byte
		err = f.pacer.Call(func() (bool, error) {
			body = nil
			err := b.bucket.PutObject(params.Object, bytes.NewReader(buf), b.options(ctx, params, oss.CallbackResult(&body))...)
			return f.shouldRetry(ctx, nil, err)
		})
		if err != nil {
			return nil, fmt.Errorf("OSS upload failed: %w", err)
		}
		return decodeUploadResult(body)
	}

	// Keep within the maximum number of parts
	if minSize := (size + maxUploadParts - 1) / maxUploadParts; chunkSize < minSize {
		chunkSize = minSize
	}
	imur, err := b.bucket.InitiateMultipartUpload(params.Object,
		oss.SetHeader(driver115.OssSecurityTokenHeaderName, b.token.SecurityToken),
		oss.UserAgentHeader(driver115.OSSUserAgent),
		oss.EnableSha1(),
		oss.Sequential(),
		oss.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start multipart upload: %w", err)
	}
	result, err := f.ossUploadParts(ctx, b, imur, params, in, size, chunkSize)
	if err != nil {
		if abortErr := b.bucket.AbortMultipartUpload(imur, oss.WithContext(context.Background())); abortErr != nil {
			fs.Debugf(f, "Failed to abort multipart upload: %v", abortErr)
		}
		return nil, err
	}
	return result, nil
}

// ossUploadParts uploads the parts of a multipart upload in order and
// completes it
func (f *Fs) ossUploadParts(ctx context.Context, b *ossBucket, imur oss.InitiateMultipartUploadResult, params *driver115.UploadOSSParams, in io.Reader, size, chunkSize int64) (*driver115.UploadResult, error) {
	buf := make([]byte, chunkSize)
	var parts []oss.UploadPart
	for offset, number := int64(0), 1; offset < size; offset, number = offset+chunkSize, number+1 {
		chunk := buf[:min(chunkSize, size-offset)]
		if _, err := io.ReadFull(in, chunk); err != nil {
			return nil, fmt.Errorf("failed to read part %d: %w", number, err)
		}
		if err := b.refresh(ctx, f); err != nil {
			return nil, err
		}
		var part oss.UploadPart
		err := f.pacer.Call(func() (bool, error) {
			var err error
			part, err = b.bucket.UploadPart(imur, bytes.NewReader(chunk), int64(len(chunk)), number, b.options(ctx, params)...)
			return f.shouldRetry(ctx, nil, err)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to upload part %d: %w", number, err)
		}
		parts = append(parts, part)
	}

	if err := b.refresh(ctx, f); err != nil {
		return nil, err
	}
	var body []byte
	err := f.pacer.Call(func() (bool, error) {
		body = nil
		_, err := b.bucket.CompleteMultipartUpload(imur, parts, b.options(ctx, params, oss.CallbackResult(&body))...)
		return f.shouldRetry(ctx, nil, err)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	return decodeUploadResult(body)
}
