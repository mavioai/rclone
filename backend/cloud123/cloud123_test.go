package cloud123

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud123/api"
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
		RemoteName: "TestCloud123:",
		NilObject:  (*Object)(nil),
	})
}

func TestSignPath(t *testing.T) {
	k, v := signPath("/b/api/file/list", "web", "3")
	assert.NotEmpty(t, k)
	assert.NotEmpty(t, v)
	assert.Contains(t, v, "-")
}

func TestCloud123CommandUnimplemented(t *testing.T) {
	f := &Fs{}
	ctx := context.Background()

	_, err := f.Command(ctx, "unknown-command", nil, nil)
	assert.ErrorIs(t, err, fs.ErrorCommandNotFound)
}

// newTestFs returns an Fs rooted at the root folder whose API calls go
// to handler
func newTestFs(t *testing.T, handler http.HandlerFunc) *Fs {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	ctx := context.Background()
	f := &Fs{
		name:    "test",
		opt:     Options{RootFolderID: "0", Enc: encoder.Standard},
		srv:     rest.NewClient(srv.Client()).SetRoot(srv.URL).SetErrorHandler(errorHandler),
		pacer:   fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
		tokenMu: new(sync.Mutex),
		token:   "token",
	}
	f.dirCache = dircache.New("", f.opt.RootFolderID, f)
	return f
}

func TestCloud123Offline(t *testing.T) {
	var requests []string
	var bodies []map[string]any
	f := newTestFs(t, func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		switch r.URL.Path {
		case "/v2/offline_download/task/resolve":
			_, _ = io.WriteString(w, `{"code":0,"data":{"list":[{"result":0,"id":7,"name":"a.iso","size":42,"files":[{"id":1},{"id":2}]}]}}`)
		case "/v2/offline_download/task/submit":
			_, _ = io.WriteString(w, `{"code":0,"data":{"task_list":[{"task_id":99,"result":0}]}}`)
		case "/offline_download/task/list":
			_, _ = io.WriteString(w, `{"code":0,"data":{"total":2,"list":[{"task_id":99,"name":"a.iso","status":0,"size":42,"progress":50,"speed":10},{"task_id":98,"name":"b.iso","status":1}]}}`)
		case "/offline_download/task/delete":
			_, _ = io.WriteString(w, `{"code":0}`)
		default:
			http.NotFound(w, r)
		}
	})
	ctx := context.Background()

	task, err := f.AddURL(ctx, "magnet:?xt=urn:btih:x", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "99", task.ID)
	assert.Equal(t, "a.iso", task.Name)
	assert.Equal(t, int64(42), task.Size)
	assert.Equal(t, "magnet:?xt=urn:btih:x", bodies[0]["urls"])
	assert.Equal(t, float64(0), bodies[1]["upload_dir"])
	resource := bodies[1]["resource_list"].([]any)[0].(map[string]any)
	assert.Equal(t, float64(7), resource["resource_id"])
	assert.Equal(t, []any{float64(1), float64(2)}, resource["select_file_id"])

	_, err = f.AddURL(ctx, "http://example.com/x", map[string]string{"name": "y"})
	assert.Error(t, err)

	tasks, err := f.ListOfflineTasks(ctx, time.Time{})
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, fs.OfflineTaskStatusRunning, tasks[0].Status)
	assert.Equal(t, 50.0, tasks[0].Progress)
	assert.Equal(t, fs.OfflineTaskStatusError, tasks[1].Status)

	require.NoError(t, f.DeleteOfflineTask(ctx, "99"))
	assert.Equal(t, []any{float64(99)}, bodies[len(bodies)-1]["task_ids"])
	assert.Error(t, f.DeleteOfflineTask(ctx, "not-a-number"))

	assert.Equal(t, []string{
		"/v2/offline_download/task/resolve",
		"/v2/offline_download/task/submit",
		"/offline_download/task/list",
		"/offline_download/task/delete",
	}, requests)
}

func TestCloud123OfflineResolveError(t *testing.T) {
	f := newTestFs(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"code":0,"data":{"list":[{"result":1,"err_code":5,"err_msg":"bad link"}]}}`)
	})
	_, err := f.AddURL(context.Background(), "http://example.com/x", nil)
	assert.ErrorContains(t, err, "bad link")
}

func TestOfflineStatus(t *testing.T) {
	for status, want := range map[int]fs.OfflineTaskStatus{
		api.OfflineStatusRunning:  fs.OfflineTaskStatusRunning,
		api.OfflineStatusFailed:   fs.OfflineTaskStatusError,
		api.OfflineStatusComplete: fs.OfflineTaskStatusComplete,
		api.OfflineStatusRetrying: fs.OfflineTaskStatusRunning,
		api.OfflineStatusWaiting:  fs.OfflineTaskStatusInit,
	} {
		if got := offlineStatus(status); got != want {
			t.Errorf("offlineStatus(%d) got = %q, want = %q", status, got, want)
		}
	}
}

func TestIsTokenExpired(t *testing.T) {
	base := api.BaseResp{Code: api.CodeTokenExpired, Message: "expired"}
	assert.True(t, isTokenExpired(nil, base.Err()))
	assert.True(t, isTokenExpired(&http.Response{StatusCode: http.StatusUnauthorized}, errors.New("x")))
	assert.False(t, isTokenExpired(nil, (&api.BaseResp{Code: 1}).Err()))
	assert.NoError(t, (&api.BaseResp{}).Err())
}
