package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/creamy-ghost/guvna/internal/health"
)

// Pool exhaustion is a scheduling condition, not another model failure.
var errNoAvailableKeys = errors.New("no available API keys")

type admissionKey struct{}

func withPermit(r *http.Request, permit *health.Permit) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), admissionKey{}, permit))
}

func (s *Server) mark(r *http.Request, provider, model string, status int, err error) {
	if p, ok := r.Context().Value(admissionKey{}).(*health.Permit); ok {
		p.Failure(status, err)
		return
	}
	s.hlth.Mark(provider, model, status, err)
}

func (s *Server) markSuccess(r *http.Request, provider, model string) {
	if p, ok := r.Context().Value(admissionKey{}).(*health.Permit); ok {
		p.Success()
		return
	}
	s.hlth.MarkSuccess(provider, model)
}
