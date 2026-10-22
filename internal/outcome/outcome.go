// Package outcome is the single failure-classification contract shared by
// model health, key pools and retry decisions. Keeping one classifier here
// stops the three consumers from drifting apart: a status can be a model
// failure in one place and neutral in another.
package outcome

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// Outcome is the mutually exclusive class of one failed attempt.
type Outcome string

const (
	// CallerError is a request/configuration problem (4xx that is not auth,
	// rate limit or model incompatibility). Neither the model nor the key is
	// charged, and it is never retried.
	CallerError Outcome = "caller_error"
	// ModelIncompatible is a 400/422: the request shape does not fit the model.
	// Model health cools only for image requests (the vision self-healing
	// signal); the key is neutral.
	ModelIncompatible Outcome = "model_incompatible"
	// Auth is a 401/403: the credential is bad or out of quota. Only the key
	// is charged; the model stays neutral.
	Auth Outcome = "auth"
	// RateLimit is a 429. The model counts one failure and the key gets a
	// rate-limit quarantine.
	RateLimit Outcome = "rate_limit"
	// UpstreamTransient is a 5xx (or an unusable 2xx body / gateway timeout).
	// The model counts one failure; the key is neutral because a service
	// failure is not evidence that the credential is unhealthy.
	UpstreamTransient Outcome = "upstream_transient"
	// LocalNetwork is a DNS, dial or connection-level failure before any HTTP
	// response. The model counts one failure; the key is neutral.
	LocalNetwork Outcome = "local_network"
	// Canceled is the caller disconnecting or canceling. Nothing is charged.
	Canceled Outcome = "canceled"
)

// Classify maps one failed attempt to its outcome. A non-nil error is either a
// cancellation (caller gone), a gateway budget timeout, or a local network
// failure; an HTTP status drives everything else. Any other 4xx is a caller
// error.
func Classify(status int, err error) Outcome {
	var classified interface{ OutcomeClass() Outcome }
	if errors.As(err, &classified) {
		return classified.OutcomeClass()
	}
	if status >= 200 && status < 300 && err == nil {
		return ""
	}
	// A real HTTP response owns attribution even when callers attach a
	// descriptive error. Such errors are not transport failures.
	if status >= 400 {
		switch {
		case status == http.StatusUnauthorized || status == http.StatusForbidden:
			return Auth
		case status == http.StatusTooManyRequests:
			return RateLimit
		case status >= 500:
			return UpstreamTransient
		case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
			return ModelIncompatible
		default:
			return CallerError
		}
	}
	if err != nil {
		var op *net.OpError
		var dns *net.DNSError
		switch {
		case errors.Is(err, context.Canceled):
			return Canceled
		case errors.As(err, &dns), errors.As(err, &op) && (op.Op == "dial" || op.Op == "connect"):
			return LocalNetwork
		case errors.Is(err, context.DeadlineExceeded):
			return UpstreamTransient
		default:
			return LocalNetwork
		}
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return Auth
	case status == http.StatusTooManyRequests:
		return RateLimit
	case status >= 500:
		return UpstreamTransient
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		return ModelIncompatible
	}
	return CallerError
}

// KeyClass maps an outcome to a keypool quarantine class. Outcomes that are
// neutral for keys return "".
func KeyClass(o Outcome) string {
	switch o {
	case Auth:
		return "auth"
	case RateLimit:
		return "rate_limit"
	}
	return ""
}

// ModelFailure reports whether an outcome counts as one model-health failure.
// Auth, caller errors and cancellation are neutral for model health. Rate
// limits, local network and upstream transient failures count once per step.
func ModelFailure(o Outcome) bool {
	switch o {
	case LocalNetwork, UpstreamTransient, RateLimit:
		return true
	}
	return false
}

// Retryable reports whether an outcome may be retried against the same or
// another key. Auth is a credential failover (a different key, not a timed
// retry); rate limit, upstream transient and local network are transient
// retries with backoff.
func Retryable(o Outcome) bool {
	switch o {
	case Auth, RateLimit, UpstreamTransient, LocalNetwork:
		return true
	}
	return false
}
