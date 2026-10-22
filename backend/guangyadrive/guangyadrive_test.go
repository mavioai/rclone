package guangyadrive

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/guangyadrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestGuangyaDrive:",
		NilObject:  (*Object)(nil),
	})
}

func TestOSSBaseURL(t *testing.T) {
	for _, test := range []struct {
		endpoint string
		want     string
	}{
		{"oss-cn-hangzhou.aliyuncs.com", "https://bucket.oss-cn-hangzhou.aliyuncs.com"},
		{"https://oss-cn-hangzhou.aliyuncs.com", "https://bucket.oss-cn-hangzhou.aliyuncs.com"},
		{"bucket.oss-cn-hangzhou.aliyuncs.com", "https://bucket.oss-cn-hangzhou.aliyuncs.com"},
		{"http://bucket.oss-cn-hangzhou.aliyuncs.com/", "http://bucket.oss-cn-hangzhou.aliyuncs.com"},
	} {
		got, err := ossBaseURL(test.endpoint, "bucket")
		require.NoError(t, err, test.endpoint)
		assert.Equal(t, test.want, got, test.endpoint)
	}
}

func TestOSSTarget(t *testing.T) {
	token := &api.UploadTokenData{
		ObjectPath: "/dir/a file#?.dat",
		BucketName: "bucket",
		EndPoint:   "oss.example.com",
	}
	token.Creds.AccessKeyID = "id"
	token.Creds.SecretAccessKey = "secret"
	token.Creds.SessionToken = "token"
	target, err := newOSSTarget(token)
	require.NoError(t, err)

	assert.Equal(t, "https://bucket.oss.example.com/dir/a%20file%23%3F.dat", target.url(""))
	assert.Equal(t, "https://bucket.oss.example.com/dir/a%20file%23%3F.dat?partNumber=1&uploadId=X", target.url("partNumber=1&uploadId=X"))

	date := "Sat, 10 Oct 2026 12:00:00 GMT"
	stringToSign := "PUT\n\napplication/octet-stream\n" + date + "\nx-oss-security-token:token\n/bucket/dir/a file#?.dat?partNumber=1&uploadId=X"
	mac := hmac.New(sha1.New, []byte("secret"))
	mac.Write([]byte(stringToSign))
	want := "OSS id:" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	assert.Equal(t, want, target.sign("PUT", "", "application/octet-stream", date, "partNumber=1&uploadId=X"))

	_, err = newOSSTarget(&api.UploadTokenData{})
	assert.Error(t, err)
}

func TestBaseRespErr(t *testing.T) {
	assert.NoError(t, (&api.BaseResp{Msg: "success"}).Err())
	assert.NoError(t, (&api.BaseResp{}).Err())
	assert.Error(t, (&api.BaseResp{Code: 1, Msg: "bad"}).Err())
}

func TestChunkSize(t *testing.T) {
	f := &Fs{opt: Options{ChunkSize: defaultChunkSize}}
	assert.Equal(t, int64(defaultChunkSize), f.chunkSize(0))
	assert.Equal(t, int64(defaultChunkSize), f.chunkSize(int64(defaultChunkSize)*maxParts))
	assert.Equal(t, int64(2*defaultChunkSize), f.chunkSize(int64(defaultChunkSize)*maxParts+1))
}

// newTestFs returns an Fs rooted at the root folder whose API calls go
// to handler
func newTestFs(t *testing.T, handler http.HandlerFunc) *Fs {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	f := &Fs{
		name:        "test",
		opt:         Options{PageSize: 2, Enc: encoder.Standard},
		srv:         rest.NewClient(srv.Client()).SetRoot(srv.URL).SetErrorHandler(errorHandler),
		pacer:       fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		tokenMu:     new(sync.Mutex),
		accessToken: "token",
	}
	f.dirCache = dircache.New("", f.opt.RootFolderID, f)
	return f
}

func TestOffline(t *testing.T) {
	var requests []string
	var bodies []map[string]any
	f := newTestFs(t, func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		switch r.URL.Path {
		case "/cloudcollection/v1/resolve_res":
			_, _ = io.WriteString(w, `{"msg":"success","data":{"url":"magnet:?xt=urn:btih:resolved","btResInfo":{"fileName":"show","fileSize":42,"subfiles":[{"fileIndex":3},{}]}}}`)
		case "/cloudcollection/v1/create_task":
			_, _ = io.WriteString(w, `{"msg":"success","data":{"taskId":"t1"}}`)
		case "/cloudcollection/v1/list_task":
			if body["cursor"] == nil {
				_, _ = io.WriteString(w, `{"msg":"success","data":{"cursor":"c2","list":[{"taskId":"t1","fileName":"show","status":1,"progress":40,"totalSize":42,"createTime":1700000000},{"taskId":"t2","status":5}]}}`)
			} else {
				_, _ = io.WriteString(w, `{"msg":"success","data":{"cursor":"","list":[{"taskId":"t3","status":4}]}}`)
			}
		case "/cloudcollection/v2/delete_task":
			_, _ = io.WriteString(w, `{"msg":"success"}`)
		default:
			http.NotFound(w, r)
		}
	})
	ctx := context.Background()

	task, err := f.AddURL(ctx, "magnet:?xt=urn:btih:x", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "t1", task.ID)
	assert.Equal(t, "show", task.Name)
	assert.Equal(t, int64(42), task.Size)
	create := bodies[1]
	assert.Equal(t, "magnet:?xt=urn:btih:resolved", create["url"])
	assert.Equal(t, "", create["parentId"])
	assert.Equal(t, "show", create["newName"])
	assert.Equal(t, []any{float64(3), float64(1)}, create["fileIndexes"])

	tasks, err := f.ListOfflineTasks(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	assert.Equal(t, fs.OfflineTaskStatusRunning, tasks[0].Status)
	assert.Equal(t, 40.0, tasks[0].Progress)
	assert.Equal(t, time.Unix(1700000000, 0), tasks[0].CreatedAt)
	assert.Equal(t, fs.OfflineTaskStatusComplete, tasks[1].Status)
	assert.Equal(t, "partially completed", tasks[1].ErrorMsg)
	assert.Equal(t, fs.OfflineTaskStatusError, tasks[2].Status)
	assert.Equal(t, "c2", bodies[3]["cursor"])

	require.NoError(t, f.DeleteOfflineTask(ctx, "t1"))
	assert.Equal(t, []any{"t1"}, bodies[len(bodies)-1]["taskIds"])
}

func TestOfflineError(t *testing.T) {
	f := newTestFs(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"code":1,"msg":"invalid url"}`)
	})
	_, err := f.AddURL(context.Background(), "http://example.com/x", map[string]string{})
	assert.ErrorContains(t, err, "invalid url")
}

func TestOfflineName(t *testing.T) {
	for _, test := range []struct {
		url  string
		want string
	}{
		{"https://example.com/dir/a%20b.iso", "a b.iso"},
		{"https://example.com/", "offline_download"},
		{"magnet:?xt=urn:btih:x", "offline_download"},
	} {
		got := offlineName(&api.OfflineResolveResp{}, test.url)
		assert.Equal(t, test.want, got, test.url)
	}
}
