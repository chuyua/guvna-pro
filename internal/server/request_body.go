package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	maxRequestBytes    = envInt64("GUVNA_MAX_REQUEST_BYTES", 32<<20)
	requestReadTimeout = envDurationOpt("GUVNA_REQUEST_READ_TIMEOUT", 30*time.Second)
)

// readRequestBody bounds the incoming read without shortening generation or an
// active response stream. The returned bytes are the handler's only body buffer.
// Native HTTP writers support read deadlines; wrappers must expose Unwrap.
// Other writers require Body.Close to unblock Read, as Request.Body specifies.
func readRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	if err := r.Context().Err(); err != nil {
		abortRequestBody(w, r)
		return nil, &clientGoneError{err: err}
	}
	if maxRequestBytes > 0 && r.ContentLength > maxRequestBytes {
		abortRequestBody(w, r)
		return nil, &http.MaxBytesError{Limit: maxRequestBytes}
	}
	if r.Body == nil {
		return nil, nil
	}

	ctx := r.Context()
	cancel := func() {}
	var ownDeadline time.Time
	if requestReadTimeout > 0 {
		ownDeadline = time.Now().Add(requestReadTimeout)
		ctx, cancel = context.WithDeadline(ctx, ownDeadline)
	}
	defer cancel()
	controller := http.NewResponseController(w)
	deadline, hasDeadline := ctx.Deadline()
	deadlineSet := false
	if hasDeadline {
		if err := controller.SetReadDeadline(deadline); err == nil {
			deadlineSet = true
		} else if !errors.Is(err, http.ErrNotSupported) {
			return nil, err
		}
	}

	// Do not create a background reader. Cancellation either interrupts the
	// native transport read or closes a cooperative custom body. Join this
	// callback before clearing a deadline so it cannot reinstall an expired one.
	aborted := make(chan struct{})
	stopAbort := context.AfterFunc(ctx, func() {
		abortRequestBody(w, r)
		close(aborted)
	})
	var joinOnce sync.Once
	stopAndJoin := func() {
		joinOnce.Do(func() {
			if !stopAbort() {
				<-aborted
			}
		})
	}
	defer stopAndJoin()

	body := r.Body
	if maxRequestBytes > 0 {
		body = http.MaxBytesReader(w, body, maxRequestBytes)
	}
	data, err := io.ReadAll(body)
	stopAndJoin()
	var netErr net.Error
	// An HTTP/1 transport read timeout also cancels Request.Context. Preserve
	// the gateway timeout attribution when its own deadline caused that read
	// error; a caller canceled before the read deadline remains neutral.
	ownTimeout := !ownDeadline.IsZero() && !time.Now().Before(ownDeadline) && errors.As(err, &netErr) && netErr.Timeout()
	if callerErr := r.Context().Err(); callerErr != nil && !ownTimeout {
		abortRequestBody(w, r)
		return nil, &clientGoneError{err: callerErr}
	}
	if ownTimeout || ctx.Err() != nil || hasDeadline && !time.Now().Before(deadline) {
		err = errRequestBodyTimeout
	} else if err != nil && hasDeadline {
		if errors.As(err, &netErr) && netErr.Timeout() {
			err = errRequestBodyTimeout
		}
	}
	if err != nil {
		// Keeping an expired read deadline on a failed request prevents the
		// HTTP/1 server's post-handler body drain from hanging. This connection
		// is not reusable; HTTP/2 deadlines only affect this request's stream.
		abortRequestBody(w, r)
		return nil, err
	}
	if deadlineSet {
		if err := controller.SetReadDeadline(time.Time{}); err != nil {
			abortRequestBody(w, r)
			return nil, err
		}
	}
	return data, nil
}

var errRequestBodyTimeout = errors.New("request body read timed out")

func abortRequestBody(w http.ResponseWriter, r *http.Request) {
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); errors.Is(err, http.ErrNotSupported) && r.Body != nil {
		r.Body.Close()
	}
}

// writeRequestBodyError does not expose the input or the underlying read error.
// A caller that disconnected receives no additional write or upstream penalty.
func writeRequestBodyError(w http.ResponseWriter, r *http.Request, err error) {
	if isClientGone(err) || r.Context().Err() != nil && !errors.Is(err, errRequestBodyTimeout) {
		return
	}
	if r.ProtoMajor < 2 {
		w.Header().Set("Connection", "close")
	}
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		responsesErr(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large", "request body exceeds the configured size limit")
	case errors.Is(err, errRequestBodyTimeout):
		responsesErr(w, http.StatusRequestTimeout, "invalid_request_error", "request_timeout", "request body read timed out")
	default:
		responsesErr(w, http.StatusBadRequest, "invalid_request_error", "read_body_failed", "read body")
	}
}
