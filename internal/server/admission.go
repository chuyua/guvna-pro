package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/outcome"
)

// Pool exhaustion is a scheduling condition, not another model failure.
var errNoAvailableKeys = errors.New("no available API keys")

type admissionKey struct{}

func withPermit(r *http.Request, permit *health.Permit) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), admissionKey{}, permit))
}

type requestIDKey struct{}
type deferredBlameKey struct{}
type deferredBlames struct{ items []*health.DeferredFailure }

// withRequestID attaches the per-request correlation id so every telemetry
// event for one candidate walk shares it.
func withRequestID(r *http.Request, id string) *http.Request {
	if requestID(r) != "" {
		return r
	}
	ctx := context.WithValue(r.Context(), requestIDKey{}, id)
	ctx = context.WithValue(ctx, retryBudgetKey{}, &retryBudget{remaining: envInt("GUVNA_RETRY_BUDGET", 16)})
	ctx = context.WithValue(ctx, deferredBlameKey{}, &deferredBlames{})
	return r.WithContext(ctx)
}

// requestID returns the correlation id attached by withRequestID, or "" for
// calls outside a handler (tests, background decider).
func requestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id, ok := r.Context().Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

func (s *Server) mark(r *http.Request, provider, model string, status int, err error) {
	if r.Context().Err() != nil || isClientGone(err) {
		return
	}
	class := outcome.Classify(status, err)
	if class == outcome.ModelIncompatible {
		trace, _ := r.Context().Value(attemptKey{}).(*attemptTrace)
		if trace == nil || !trace.hasImages {
			return
		}
		// An image rejection alone cannot distinguish a request-wide problem
		// from a model incompatibility. Release now and confirm only after a
		// later candidate has completed a valid response to the caller.
		if p, ok := r.Context().Value(admissionKey{}).(*health.Permit); ok {
			if blame := p.DeferFailure(http.StatusBadRequest, err); blame != nil {
				if pending, ok := r.Context().Value(deferredBlameKey{}).(*deferredBlames); ok {
					pending.items = append(pending.items, blame)
				}
			}
		}
		return
	} else if !outcome.ModelFailure(class) {
		return
	}
	if p, ok := r.Context().Value(admissionKey{}).(*health.Permit); ok {
		p.Failure(status, err)
		return
	}
	s.hlth.Mark(provider, model, status, err)
}

func (s *Server) markSuccess(r *http.Request, provider, model string) {
	if r.Context().Err() != nil {
		return
	}
	if p, ok := r.Context().Value(admissionKey{}).(*health.Permit); ok {
		p.Success()
	} else {
		s.hlth.MarkSuccess(provider, model)
	}
	if pending, ok := r.Context().Value(deferredBlameKey{}).(*deferredBlames); ok {
		for _, blame := range pending.items {
			blame.Confirm()
		}
		pending.items = nil
	}
}
