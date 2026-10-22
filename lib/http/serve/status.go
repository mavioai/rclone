package serve

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
)

// ErrVersionChanged reports that an object no longer matches the
// version a client asked for.
var ErrVersionChanged = errors.New("object version changed")

// throttleMessages are parts of the messages of errors that report a
// quota or rate limit without an HTTP status.
var throttleMessages = []string{
	"downloadQuotaExceeded",        // drive's daily download quota
	"Request over quota",           // mega's transfer quota
	"Rate limit reached",           // mega
	"Too many connections on this", // mega
}

// ErrorStatus returns the HTTP status that reports err to a client, and
// how long the client should wait before retrying, if the storage said.
//
// Objects that are not found give 404, changed versions 412, refusals
// 403, quotas and rate limits 429, unavailable storage 503, and other
// errors 502.
func ErrorStatus(err error) (status int, retryAfter time.Duration) {
	if fserrors.IsRetryAfterError(err) {
		retryAfter = max(time.Until(fserrors.RetryAfterErrorTime(err)), 0)
	}
	switch {
	case errors.Is(err, ErrVersionChanged):
		return http.StatusPreconditionFailed, 0
	case errors.Is(err, fs.ErrorObjectNotFound), errors.Is(err, fs.ErrorDirNotFound), errors.Is(err, fs.ErrorIsDir), errors.Is(err, os.ErrNotExist):
		return http.StatusNotFound, 0
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, 0
	}
	for _, msg := range throttleMessages {
		if strings.Contains(err.Error(), msg) {
			return http.StatusTooManyRequests, retryAfter
		}
	}
	switch code := statusCode(err); code {
	case http.StatusNotFound, http.StatusGone:
		return http.StatusNotFound, 0
	case http.StatusUnauthorized, http.StatusForbidden:
		return http.StatusForbidden, 0
	case http.StatusPreconditionFailed:
		return http.StatusPreconditionFailed, 0
	case http.StatusRequestedRangeNotSatisfiable:
		return code, 0
	case http.StatusTooManyRequests, 509: // 509: bandwidth limit exceeded
		return http.StatusTooManyRequests, retryAfter
	case http.StatusServiceUnavailable:
		return http.StatusServiceUnavailable, retryAfter
	}
	if retryAfter > 0 {
		return http.StatusTooManyRequests, retryAfter
	}
	return http.StatusBadGateway, 0
}

// WriteError answers a request with the status of err.
func WriteError(w http.ResponseWriter, err error) {
	status, retryAfter := ErrorStatus(err)
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int((retryAfter+time.Second-1)/time.Second)))
	}
	http.Error(w, http.StatusText(status), status)
}

// statusCode finds the HTTP status of the storage's response in err's
// chain, or returns 0.
func statusCode(err error) int {
	for _, e := range errorChain(err) {
		switch x := e.(type) {
		case interface{ HTTPStatusCode() int }:
			if code := x.HTTPStatusCode(); isHTTPError(code) {
				return code
			}
		case interface{ StatusCode() int }:
			if code := x.StatusCode(); isHTTPError(code) {
				return code
			}
		}
		if code := statusField(e); code != 0 {
			return code
		}
	}
	return 0
}

// statusFields are the names that the backends' error types give the
// HTTP status.
var statusFields = []string{"StatusCode", "HTTPStatusCode", "HTTPCode", "Status", "Code"}

// statusField reads the HTTP status from a field of an error struct.
func statusField(err error) int {
	v := reflect.ValueOf(err)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0
	}
	for _, name := range statusFields {
		f := v.FieldByName(name)
		if !f.IsValid() {
			continue
		}
		var code int
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			code = int(f.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			code = int(f.Uint())
		default:
			continue
		}
		if isHTTPError(code) {
			return code
		}
	}
	return 0
}

func isHTTPError(code int) bool {
	return code >= 400 && code < 600
}

// errorChain returns err and every error it wraps, depth first.
func errorChain(err error) []error {
	var out []error
	var walk func(error)
	walk = func(e error) {
		for e != nil && len(out) < 64 {
			out = append(out, e)
			switch x := e.(type) {
			case interface{ Unwrap() []error }:
				for _, inner := range x.Unwrap() {
					walk(inner)
				}
				return
			case interface{ Unwrap() error }:
				e = x.Unwrap()
			default:
				return
			}
		}
	}
	walk(err)
	return out
}
