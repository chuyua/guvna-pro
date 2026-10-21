package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/creamy-ghost/guvna/internal/requestbody"
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
	data, err := requestbody.Read(w, r, maxRequestBytes, requestReadTimeout)
	if errors.Is(err, requestbody.ErrClientGone) {
		return nil, &clientGoneError{err: err}
	}
	return data, err
}

var errRequestBodyTimeout = requestbody.ErrTimeout

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
