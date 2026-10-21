// Package requestbody bounds incoming HTTP body reads independently of later
// handler work or response streaming.
package requestbody

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	ErrTimeout    = errors.New("request body read timed out")
	ErrClientGone = errors.New("request body caller canceled")
)

// Read consumes the incoming body within maxBytes and timeout. A non-positive
// limit disables it. Native HTTP writers support read deadlines; wrappers must
// expose Unwrap. Other writers require Body.Close to unblock Read, as the
// http.Request.Body contract specifies. Successful reads clear native deadlines
// before returning, so later handler work and response writes are not limited.
func Read(w http.ResponseWriter, r *http.Request, maxBytes int64, timeout time.Duration) ([]byte, error) {
	if err := r.Context().Err(); err != nil {
		Abort(w, r)
		return nil, fmt.Errorf("%w: %w", ErrClientGone, err)
	}
	if maxBytes > 0 && r.ContentLength > maxBytes {
		Abort(w, r)
		return nil, &http.MaxBytesError{Limit: maxBytes}
	}
	if r.Body == nil {
		return nil, nil
	}

	ctx := r.Context()
	cancel := func() {}
	var ownDeadline time.Time
	if timeout > 0 {
		ownDeadline = time.Now().Add(timeout)
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
		Abort(w, r)
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
	if maxBytes > 0 {
		body = http.MaxBytesReader(w, body, maxBytes)
	}
	data, err := io.ReadAll(body)
	stopAndJoin()
	var netErr net.Error
	// An HTTP/1 transport read timeout also cancels Request.Context. Preserve
	// our own timeout attribution when its deadline caused the read error; a
	// caller canceled before the read deadline remains a caller failure.
	ownTimeout := !ownDeadline.IsZero() && !time.Now().Before(ownDeadline) && errors.As(err, &netErr) && netErr.Timeout()
	if callerErr := r.Context().Err(); callerErr != nil && !ownTimeout {
		Abort(w, r)
		return nil, fmt.Errorf("%w: %w", ErrClientGone, callerErr)
	}
	if ownTimeout || ctx.Err() != nil || hasDeadline && !time.Now().Before(deadline) {
		err = ErrTimeout
	} else if err != nil && hasDeadline {
		if errors.As(err, &netErr) && netErr.Timeout() {
			err = ErrTimeout
		}
	}
	if err != nil {
		// Keeping an expired read deadline on a failed request prevents the
		// HTTP/1 server's post-handler body drain from hanging. This connection
		// is not reusable; HTTP/2 deadlines only affect this request's stream.
		Abort(w, r)
		return nil, err
	}
	if deadlineSet {
		if err := controller.SetReadDeadline(time.Time{}); err != nil {
			Abort(w, r)
			return nil, err
		}
	}
	return data, nil
}

// Abort interrupts the request body read, including a later HTTP/1 drain when
// the handler rejects the request without reading. An HTTP/1 error response
// should also set Connection: close; HTTP/2 read deadlines are stream-local.
func Abort(w http.ResponseWriter, r *http.Request) {
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); errors.Is(err, http.ErrNotSupported) && r.Body != nil {
		r.Body.Close()
	}
}
