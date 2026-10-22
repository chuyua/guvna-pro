package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/adaptors"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/outcome"
	"github.com/creamy-ghost/guvna/internal/router"
)

type retryBudgetKey struct{}
type retryBudget struct {
	remaining int
	exhausted bool
}

func takeRetry(r *http.Request) bool {
	if budget, ok := r.Context().Value(retryBudgetKey{}).(*retryBudget); ok {
		if budget.remaining <= 0 {
			budget.exhausted = true
			return false
		}
		budget.remaining--
	}
	return true
}

func retryAvailable(r *http.Request) bool {
	if budget, ok := r.Context().Value(retryBudgetKey{}).(*retryBudget); ok && budget.remaining <= 0 {
		budget.exhausted = true
		return false
	}
	return true
}

func retryBudgetExhausted(r *http.Request) bool {
	budget, _ := r.Context().Value(retryBudgetKey{}).(*retryBudget)
	return budget != nil && budget.exhausted
}

func retryAfter(value string, now time.Time, cap time.Duration) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	var d time.Duration
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		if seconds >= uint64(cap/time.Second)+1 {
			return cap, true
		}
		d = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		d = at.Sub(now)
	} else {
		return 0, false
	}
	if d < 0 {
		d = 0
	}
	if d > cap {
		d = cap
	}
	return d, true
}

func fullJitter(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 || cap <= 0 {
		return 0
	}
	bound := min(base, cap)
	for i := 1; i < attempt && bound < cap; i++ {
		if bound > cap/2 {
			bound = cap
		} else {
			bound *= 2
		}
	}
	return time.Duration(rand.Int64N(int64(bound)))
}

func (s *Server) retryDelay(attempt int, previous *http.Response) time.Duration {
	cap := envDuration("GUVNA_RETRY_MAX_WAIT", 30*time.Second)
	if previous != nil && (previous.StatusCode == 429 || previous.StatusCode >= 500) {
		if d, ok := retryAfter(previous.Header.Get("Retry-After"), time.Now(), cap); ok {
			return d
		}
	}
	if s.retryWait != nil {
		return min(s.retryWait(attempt), cap)
	}
	return 0
}

// tryUpstream owns only setup retries. A successful response stays open and
// its attempt is completed by the body/stream relay, including full latency.
func (s *Server) tryUpstream(ctx context.Context, r *http.Request, step router.Step, body []byte, mk func(config.Provider, string) (adaptors.Adaptor, error), embed bool) (*http.Response, string, error) {
	p, ok := s.provider(step.Provider)
	if !ok {
		return nil, "", fmt.Errorf("provider %q not in config", step.Provider)
	}
	pool := s.pools[step.Provider]
	if pool == nil {
		return nil, "", fmt.Errorf("provider %q: no key pool", step.Provider)
	}
	payload := mergeParams(withModel(body, step.Model), step.Params)
	used := make(map[string]bool)
	var lastResp *http.Response
	var lastErr error
	var lastKey string
	transient := 0
	index := 0
	lease := pool.Acquire(nil)
	defer func() { lease.Release() }()
	wait := false
	// Service/network failures leave credentials healthy; retry that credential with
	// a fresh epoch-bound lease before considering another unused credential.
	reacquire := func(key string) *keypool.Lease {
		excluded := make(map[string]bool)
		for _, env := range p.AllKeyEnvs() {
			if env != key {
				excluded[env] = true
			}
		}
		if next := pool.Acquire(excluded); next != nil {
			return next
		}
		return pool.Acquire(used)
	}
	finish := func() (*http.Response, string, error) {
		if lastResp != nil {
			return lastResp, lastKey, nil
		}
		if lastErr != nil {
			return nil, lastKey, lastErr
		}
		return nil, "", errNoAvailableKeys
	}
	for lease != nil {
		if err := abortCause(ctx, r); err != nil {
			if lastResp != nil {
				lastResp.Body.Close()
			}
			return nil, "", err
		}
		if index > 0 {
			if !retryAvailable(r) {
				return finish()
			}
			sleep := s.retrySleep
			if sleep == nil {
				sleep = sleepCtx
			}
			if wait {
				if err := sleep(ctx, s.retryDelay(index, lastResp)); err != nil {
					if lastResp != nil {
						lastResp.Body.Close()
					}
					return nil, "", abortCause(ctx, r)
				}
			}
			wait = false
		}
		if !lease.Available() {
			lease.Release()
			lease = pool.Acquire(used)
			if lease == nil {
				return finish()
			}
			continue
		}
		key := lease.Env()
		secret := s.env(key)
		if secret == "" {
			used[key] = true
			lastErr = fmt.Errorf("provider %q: key env %q not set", p.Name, key)
			lease.Release()
			lease = pool.Acquire(used)
			continue
		}
		a, err := mk(p, secret)
		if err != nil {
			return nil, "", err
		}
		if !lease.Available() {
			continue
		}
		if index > 0 && !takeRetry(r) {
			return finish()
		}
		beginAttempt(r)
		beginAutoAttempt(r, step)
		index++
		used[key] = true
		var resp *http.Response
		if embed {
			resp, err = a.Embed(ctx, payload)
		} else {
			resp, err = a.Chat(ctx, payload)
		}
		if err != nil {
			if aborted := abortCause(ctx, r); aborted != nil {
				if isClientGone(aborted) {
					if lastResp != nil {
						lastResp.Body.Close()
					}
					return nil, "", aborted
				}
				err = aborted
			}
			lastErr, lastKey = err, key
			chain, req := traceRequest(r)
			s.recordAttempt(r, step, chain, req, 0, err, key, 0, 0)
			completeKeyAttempt(lease, 0, err)
			if transient >= retries || ctx.Err() != nil {
				return finish()
			}
			transient++
			lease = reacquire(key)
			wait = true
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if lastResp != nil {
				lastResp.Body.Close()
			}
			setAttemptLease(r, lease)
			lease = nil // terminal relay owns this lease until its full outcome
			return resp, key, nil
		}
		// Preserve a bounded error response while trying another credential;
		// callers can still return the best real upstream result on exhaustion.
		_, req := traceRequest(r)
		raw, readErr := readErrorBody(resp, req.Stream)
		resp.Body.Close()
		if r.Context().Err() != nil {
			if lastResp != nil {
				lastResp.Body.Close()
			}
			return nil, "", &clientGoneError{r.Context().Err()}
		}
		resp.Body = io.NopCloser(bytes.NewReader(raw))
		if lastResp != nil {
			lastResp.Body.Close()
		}
		lastResp, lastKey, lastErr = resp, key, nil
		statusErr := fmt.Errorf("status %d", resp.StatusCode)
		if readErr != nil {
			statusErr = fmt.Errorf("status %d, error body: %w", resp.StatusCode, readErr)
		}
		chain, req := traceRequest(r)
		s.recordAttempt(r, step, chain, req, resp.StatusCode, statusErr, key, 0, 0)
		class := outcome.Classify(resp.StatusCode, nil)
		completeKeyAttempt(lease, resp.StatusCode, statusErr)
		switch class {
		case outcome.Auth, outcome.RateLimit:
			lease = pool.Acquire(used) // each credential once, independent of transient budget
			wait = class == outcome.RateLimit
		case outcome.UpstreamTransient:
			if transient >= retries {
				return finish()
			}
			transient++
			lease = reacquire(key)
			wait = true
		default:
			return finish()
		}
	}
	return finish()
}
