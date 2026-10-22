package server

import (
	"context"
	"net/http"
	"time"

	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/outcome"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/telemetry"
	"github.com/creamy-ghost/guvna/internal/vision"
)

type attemptKey struct{}

// Each execution walk owns this trace. Relays and the retry loop run in the
// same handler goroutine, so the selected attempt can finish after its headers.
type attemptTrace struct {
	chain     string
	req       chatRequest
	hasImages bool
	index     int
	started   time.Time
	recorded  bool
	lease     *keypool.Lease
}

// releaseAttempt is deferred by the execution walk. Terminal recording consumes
// the lease first; canceled relays and downstream write failures release it neutrally.
func releaseAttempt(r *http.Request) {
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok {
		trace.lease.Release()
	}
}

func setAttemptLease(r *http.Request, lease *keypool.Lease) {
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok {
		trace.lease = lease
	}
}

func attemptLease(r *http.Request, key string) *keypool.Lease {
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok && trace.lease != nil && trace.lease.Env() == key {
		return trace.lease
	}
	return nil
}

// completeKeyAttempt resets failures only after a usable successful response.
// Service/network failures release the lease without erasing prior key evidence.
func completeKeyAttempt(lease *keypool.Lease, status int, err error) {
	if lease == nil {
		return
	}
	class := outcome.Classify(status, err)
	if class == "" {
		lease.Success()
	} else if keyClass := outcome.KeyClass(class); keyClass != "" {
		lease.Failure(keypool.Class(keyClass))
	} else {
		lease.Release()
	}
}

func withAttempt(r *http.Request, chain string, req chatRequest, body []byte) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), attemptKey{}, &attemptTrace{
		chain: chain, req: req, hasImages: vision.RequestHasImages(body),
	}))
}

func beginAttempt(r *http.Request) {
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok {
		trace.index++
		trace.started = time.Now()
		trace.recorded = false
	}
}

func (s *Server) recordAttempt(r *http.Request, step router.Step, chain string, req chatRequest, status int, err error, key string, in, out int64) {
	if r.Context().Err() != nil || isClientGone(err) {
		return
	}
	e := telemetry.Event{Provider: step.Provider, Chain: chain, Model: step.Model,
		Stream: req.Stream, Status: status, TokensIn: in, TokensOut: out, Key: key,
		Ts: time.Now(), RequestID: requestID(r), FailureClass: string(outcome.Classify(status, err))}
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok {
		if trace.recorded || trace.index == 0 {
			return
		}
		trace.recorded = true
		e.Attempt = trace.index
		e.LatencyMS = time.Since(trace.started).Milliseconds()
		if chain == "" {
			e.Chain = trace.chain
		}
		e.Stream = trace.req.Stream
	}
	recordAutoAttempt(r, status, err)
	if err != nil {
		e.Err = err.Error()
	}
	s.tm.Record(e)
	if p := s.pools[step.Provider]; p != nil && key != "" {
		p.Record(key, in, out)
	}
}

func traceRequest(r *http.Request) (string, chatRequest) {
	if trace, ok := r.Context().Value(attemptKey{}).(*attemptTrace); ok {
		return trace.chain, trace.req
	}
	return "", chatRequest{}
}
