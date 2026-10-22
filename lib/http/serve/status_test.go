package serve

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
)

type codeMethodError struct{ code int }

func (e codeMethodError) Error() string       { return "method" }
func (e codeMethodError) HTTPStatusCode() int { return e.code }

type codeFieldError struct {
	StatusCode int
}

func (e *codeFieldError) Error() string { return "field" }

type apiCodeError struct {
	Code int // an API code, not an HTTP status
}

func (e apiCodeError) Error() string { return "api" }

func TestErrorStatus(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		want  int
		retry bool
	}{
		{"not found", fmt.Errorf("lookup: %w", fs.ErrorObjectNotFound), http.StatusNotFound, false},
		{"version", ErrVersionChanged, http.StatusPreconditionFailed, false},
		{"method", fmt.Errorf("x: %w", codeMethodError{http.StatusForbidden}), http.StatusForbidden, false},
		{"field", fmt.Errorf("x: %w", &codeFieldError{StatusCode: http.StatusUnauthorized}), http.StatusForbidden, false},
		{"gone", &codeFieldError{StatusCode: http.StatusGone}, http.StatusNotFound, false},
		{"bandwidth", &codeFieldError{StatusCode: 509}, http.StatusTooManyRequests, false},
		{"unavailable", fserrors.RetryError(&codeFieldError{StatusCode: http.StatusServiceUnavailable}), http.StatusServiceUnavailable, false},
		{"joined", errors.Join(errors.New("a"), codeMethodError{http.StatusTooManyRequests}), http.StatusTooManyRequests, false},
		{"retry after", fserrors.NewErrorRetryAfter(time.Minute), http.StatusTooManyRequests, true},
		{"drive quota", errors.New("googleapi: Error 403: The download quota for this file has been exceeded, downloadQuotaExceeded"), http.StatusTooManyRequests, false},
		{"api code", apiCodeError{Code: 10001}, http.StatusBadGateway, false},
		{"other", errors.New("boom"), http.StatusBadGateway, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, retryAfter := ErrorStatus(test.err)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.retry, retryAfter > 0, "retry after %v", retryAfter)
		})
	}
}

func TestObjectWithOpenError(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://example.com/aFile", nil)
	o := mockobject.New("aFile").WithContent([]byte("hello"), mockobject.SeekModeNone)
	ObjectWith(w, r, o, func(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
		return nil, fserrors.NewErrorRetryAfter(90 * time.Second)
	})
	resp := w.Result()
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "90", resp.Header.Get("Retry-After"))
}
