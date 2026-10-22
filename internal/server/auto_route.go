// auto_route.go implements POST /v1/auto: one chat completion request in, the
// gateway decides which model serves it.
//
// The body is a normal chat completion plus two optional numbers:
//
//	{
//	  "model": "auto",            // any value; the path is what matters
//	  "messages": [...],
//	  "min_context": 512,         // exclude models with a smaller window
//	  "max_context": 131072       // optional upper bound
//	}
//
// min_context is what makes the endpoint useful. Without it the gateway would
// hand a 20k prompt to a model whose window is 8k and let the upstream reject
// it, which costs a key's quota and a chain step for nothing. The default,
// DefaultMinContext, is small enough to keep every model in the pool.
//
// Ordering among the survivors is the decider model's call. That is the whole
// point of this endpoint and the difference from a chain: a chain's order is
// frozen at config time, which means it encodes whatever looked right the last
// time a script was edited. The decider sees the pool's health at request
// time. It runs as a direct provider/model step, never through the router, so
// it cannot recurse into /v1/auto. Its answer is cached per
// GUVNA_DECIDE_INTERVAL so it is not paid on every request.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/adaptors"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/selector"
	"github.com/creamy-ghost/guvna/internal/telemetry"
	"github.com/creamy-ghost/guvna/internal/vision"
)

// DefaultMinContext applies when a caller does not send min_context.
const DefaultMinContext = 512

// minContextDefault is the service-level default for min_context. Operators
// raise it via GUVNA_AUTO_MIN_CONTEXT (e.g. 262144 for a 256K floor) without a
// rebuild; a request-level min_context still wins over both.
func minContextDefault() int {
	if v := envInt("GUVNA_AUTO_MIN_CONTEXT", 0); v > 0 {
		return v
	}
	return DefaultMinContext
}

// DefaultDecideInterval is how long a decider answer is reused.
const DefaultDecideInterval = 30 * time.Second

// DefaultDeciderRequestWait bounds the synchronous decider call a request may
// make on a cold cache. The background refresh uses the decider client's full
// 90s header timeout; a waiting client must not.
const DefaultDeciderRequestWait = 10 * time.Second

// autoRequest carries only the fields this handler reads; the rest of the body
// is forwarded to the upstream via withModel/mergeParams in tryStep.
type autoRequest struct {
	Model      string `json:"model"`
	MinContext int    `json:"min_context"`
	MaxContext int    `json:"max_context"`
}

// stripAutoFields removes auto-only keys from a chat completion body so the
// upstream never sees them. min_context and max_context are selector knobs that
// upstreams reject with 400 "Unsupported parameter"; model is rewritten
// per-step by withModel, so stripping it here is belt-and-braces.
//
// On any parse error the original body is returned: failing to strip is better
// than failing to forward.
func stripAutoFields(body []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	delete(m, "min_context")
	delete(m, "max_context")
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// handleAuto selects a model, then reuses the same tryStep/relaySuccess path as
// handleChat. It does not build a chain: a chain would make the decision
// visible to local chain management, where an admin could overwrite the pool order.
func (s *Server) handleAuto(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	s.handleAutoBody(w, r, body, started)
}

// handleAutoBody also serves model:auto through the Chat endpoint. Reuse the
// already bounded input and its start time instead of reading it a second time.
func (s *Server) handleAutoBody(w http.ResponseWriter, r *http.Request, body []byte, started time.Time) {
	if requestID(r) == "" {
		r = withRequestID(r, s.newRequestID())
	}
	r, observation := s.startAutoObservation(r, started)
	defer func() {
		if p := recover(); p != nil {
			observation.fail(http.StatusInternalServerError, "gateway_error")
			s.finishAutoObservation(r, observation)
			panic(p)
		}
		s.finishAutoObservation(r, observation)
	}()
	var req autoRequest
	// encoding/json accepts the literal null into a struct without an error.
	// Reject it before a later model rewrite attempts to assign into a nil map.
	if err := json.Unmarshal(body, &req); err != nil || bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		observation.fail(http.StatusBadRequest, "caller_error")
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	var chat chatRequest
	if err := json.Unmarshal(body, &chat); err != nil {
		observation.fail(http.StatusBadRequest, "caller_error")
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	observation.event.Stream = chat.Stream

	minCtx := req.MinContext
	if minCtx <= 0 {
		minCtx = minContextDefault()
	}
	if req.MaxContext > 0 && req.MaxContext < minCtx {
		observation.fail(http.StatusBadRequest, "caller_error")
		http.Error(w, `{"error":{"message":"max_context must be >= min_context"}}`, http.StatusBadRequest)
		return
	}
	// Detect images on the original body: stripAutoFields re-marshals but does
	// not touch messages, so either works — this is just the earlier one.
	hasImages := vision.RequestHasImages(body)

	// Strip the auto-only fields before forwarding: min_context and max_context
	// are selector knobs, not chat completion parameters, and upstreams reject
	// them with 400 "Unsupported parameter". The "model":"auto" value is
	// rewritten per-step by withModel, so leaving it would be harmless, but
	// stripping it keeps the body honest.
	body = stripAutoFields(body)

	pool := s.rtr.ListChains()
	if len(pool) == 0 {
		observation.fail(http.StatusServiceUnavailable, "no_candidates")
		http.Error(w, `{"error":{"message":"no chains configured"}}`, http.StatusServiceUnavailable)
		return
	}

	cands, excluded := selector.Eligible(pool, minCtx, req.MaxContext, s.ctxLen, s.hlth)
	degraded := false
	// Degradation only applies to the service-level default (minContextDefault,
	// e.g. a 256K floor set via GUVNA_AUTO_MIN_CONTEXT): that is an operator
	// preference, and a hard 404 turns it into an outage when the big-window
	// models blip. A caller-supplied min_context is a correctness requirement —
	// the caller asked for a window on purpose, so it still gets the 404 with
	// per-candidate reasons below. max_context requests are likewise left
	// alone: an explicit upper bound is a correctness requirement, not a
	// preference.
	if len(cands) == 0 && req.MinContext <= 0 && req.MaxContext == 0 {
		ctxDropped := 0
		for _, e := range excluded {
			if e.Reason == "context_below_min" {
				ctxDropped++
			}
		}
		if ctxDropped == len(excluded) && ctxDropped > 0 {
			cands, _ = selector.Eligible(pool, 0, 0, s.ctxLen, s.hlth)
			degraded = len(cands) > 0
			if degraded {
				log.Printf("auto: min=%d left no candidates, degrading to unfiltered pool (%d)", minCtx, len(cands))
			}
		}
	}
	if len(cands) == 0 {
		// "no providers available" would be misleading: a provider is available,
		// it just does not meet the requested window. Say which candidates were
		// dropped and why, so the caller can relax the filter instead of
		// guessing that the whole gateway is down.
		detail := make([]string, 0, len(excluded))
		for _, e := range excluded {
			detail = append(detail, fmt.Sprintf("%s ctx=%d (%s)", e.Step, e.Context, e.Reason))
		}
		msg := "no model meets the context filter"
		if len(detail) > 0 {
			msg += ": " + strings.Join(detail, ", ")
		}
		log.Printf("auto: no candidate for min=%d max=%d", minCtx, req.MaxContext)
		observation.fail(http.StatusNotFound, "no_candidates")
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, msg), http.StatusNotFound)
		return
	}
	if !hasImages {
		required := estimateAutoContext(body)
		var oversized []selector.Decided
		cands, oversized = fitAutoContext(cands, required)
		for _, dropped := range oversized {
			log.Printf("auto: skipping %s ctx=%d required_estimate=%d (%s)", dropped.Step, dropped.Context, required, dropped.Reason)
		}
		if len(cands) == 0 {
			observation.fail(http.StatusBadRequest, "caller_error")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"type": "invalid_request_error", "code": "context_length_exceeded",
				"message": "estimated request and output tokens exceed every eligible model context", "estimated_tokens": required,
			}})
			return
		}
	}

	// Charge cold-cache decisions and waits on another decision to this same
	// budget. Keep the original caller context for failure attribution and for a
	// selected active stream; its generation limit is configured separately.
	var deadline time.Time
	decisionCtx := r.Context()
	cancelDecision := func() {}
	if budget := autoTotalBudget(); budget > 0 {
		deadline = started.Add(budget)
		decisionCtx, cancelDecision = context.WithDeadline(r.Context(), deadline)
	}
	defer cancelDecision()
	ordered, _, source := selector.DecideContext(
		decisionCtx,
		cands,
		s.deciderFor(cands, r.Context(), s.decideReqWait),
		s.providerOrder(),
		s.decideInterval,
		&s.decideMu,
		&s.decideCache,
	)
	observation.event.DeciderSource = source
	if r.Context().Err() != nil {
		return
	}
	// Vision filtering happens after ordering, not in Eligible: the decider's
	// cached ranking is keyed on the full pool, so filtering first would force
	// every image request onto a cold cache. Skipping non-vision models in rank
	// order keeps the warm ranking and only narrows the walk.
	if hasImages {
		kept := make([]selector.Candidate, 0, len(ordered))
		for _, c := range ordered {
			if retryBudgetExhausted(r) {
				break
			}
			if s.visionReg.Has(c.Step.Model) {
				kept = append(kept, c)
			}
		}
		log.Printf("auto: images=true kept=%d/%d vision-capable", len(kept), len(ordered))
		ordered = kept
		if len(ordered) == 0 {
			observation.fail(http.StatusNotFound, "no_candidates")
			http.Error(w, `{"error":{"message":"no vision-capable model available for image request"}}`, http.StatusNotFound)
			return
		}
	}
	originalSource := source
	ordered, source = s.applyAutoScoring(r, ordered, source, chat.Stream)
	observation.selected(originalSource, source)
	log.Printf("auto: min=%d max=%d eligible=%d images=%v degraded=%v source=%s order=%s",
		minCtx, req.MaxContext, len(cands), hasImages, degraded, source, joinSteps(ordered))

	// A slow non-streaming step can hold the walk hostage far past the total
	// budget: the budget is checked between candidates, and AutoClientNonStream
	// waits a whole generation for headers (~60s for a thinking model) — one
	// such step at the front of the fallback order starved every later
	// candidate. Non-streaming relays finish synchronously, so a step-scoped
	// deadline is safe there (the body is fully read before the walk returns).
	// Streaming relays outlive the handler loop and keep the request context.
	stepBudget := envDuration("GUVNA_AUTO_STEP_BUDGET", 20*time.Second)

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, c := range ordered {
		if retryBudgetExhausted(r) {
			break
		}
		if r.Context().Err() != nil {
			return
		}
		if walkExpired(deadline) {
			log.Printf("auto: total budget exceeded, stopping walk")
			break
		}
		step := c.Step
		permit, admitted := s.hlth.TryAcquire(step.Provider, step.Model)
		if !admitted {
			log.Printf("auto: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		defer permit.Release()
		r = withPermit(r, permit)
		r = withAttempt(r, c.Chain, chat, body)
		defer releaseAttempt(r)
		stepCtx := r.Context()
		cancelStep := func() {}
		stopSetupBudget := func() {}
		if !chat.Stream {
			stepCtx, cancelStep = stepContext(r, deadline, false, stepBudget)
		} else if !deadline.IsZero() {
			// Bound setup and failed bodies, then remove this setup timer once
			// successful headers select a stream. Cancel remains tied to caller.
			stepCtx, cancelStep = context.WithCancel(r.Context())
			timer := time.AfterFunc(time.Until(deadline), cancelStep)
			stopSetupBudget = func() { timer.Stop() }
			defer stopSetupBudget()
		}
		// A stream_only provider's non-streaming endpoint is broken: the body
		// rides an upstream stream, but the client still waits for one complete
		// JSON body, so the per-step budget applies as for non-streaming.
		stepChat := withChoiceDefault(chat, body, step.Params)
		stepBody := body
		agg := !chat.Stream && s.streamOnly(step.Provider)
		if agg {
			stepBody = forceStreamBody(body)
		}
		resp, keyEnv, err := s.tryStepAuto(stepCtx, r, step, stepBody, chat.Stream)
		if err != nil {
			stopSetupBudget()
			cancelStep()
			if r.Context().Err() != nil {
				return
			}
			if errors.Is(err, errNoAvailableKeys) {
				permit.Release()
				continue
			}
			if isClientGone(err) {
				observation.cancel()
				log.Printf("auto: client gone, stopping the walk at %s/%s: %v", step.Provider, step.Model, err)
				return
			}
			log.Printf("auto: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			if observation.event.Attempts == 0 {
				observation.fail(http.StatusServiceUnavailable, "gateway_error")
			}
			s.mark(r, step.Provider, step.Model, 0, err)
			s.record(r, step, c.Chain, chat, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			stopSetupBudget()
			var res relayResult
			if agg {
				aggResp, aerr := s.aggregateStreamed(resp, stepChat.N)
				if aerr != nil {
					cancelStep()
					if r.Context().Err() != nil {
						return
					}
					log.Printf("auto: step %s/%s aggregate failed: %v", step.Provider, step.Model, aerr)
					s.mark(r, step.Provider, step.Model, http.StatusBadGateway, aerr)
					s.poolRecordResult(r, keyEnv, step, c.Chain, chat, 502, 0, 0, aerr)
					failure := unusableBody(aerr)
					lastStatus, lastBody, lastCT, lastErr = failure.status, failure.body, failure.ct, failure.err
					continue
				}
				res = s.relaySuccess(w, r, step, c.Chain, stepChat, aggResp, keyEnv)
			} else {
				// Health moves inside relaySuccess — see the comment in handleChat.
				res = s.relaySuccess(w, r, step, c.Chain, stepChat, resp, keyEnv)
			}
			cancelStep()
			if res.clientGone {
				observation.cancel()
				return
			}
			if res.outcome == relayCommitted {
				if observation.pending {
					if _, ok := w.(http.Flusher); !ok && chat.Stream {
						observation.fail(http.StatusInternalServerError, "gateway_error")
					}
				}
				return
			}
			// Unusable 2xx body: nothing was written, so keep walking.
			lastStatus, lastBody, lastCT, lastErr = res.status, res.body, res.ct, res.err
			continue
		}
		lastStatus = resp.StatusCode
		lastBody, _ = readErrorBody(resp, chat.Stream)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		stopSetupBudget()
		cancelStep()
		if r.Context().Err() != nil {
			return
		}
		s.mark(r, step.Provider, step.Model, resp.StatusCode, nil)
		log.Printf("auto: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(r, step, c.Chain, chat, resp.StatusCode, fmt.Errorf("status %d", resp.StatusCode), keyEnv)
		permit.Release()
	}
	log.Printf("auto: all candidates failed: %v", lastErr)
	if lastStatus == 0 {
		if observation.event.FailureClass == "" {
			observation.fail(http.StatusServiceUnavailable, "gateway_error")
		} else {
			observation.event.Status = http.StatusServiceUnavailable
		}
		http.Error(w, `{"error":{"message":"no providers available"}}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", lastCT)
	observation.event.Status = lastStatus
	w.WriteHeader(lastStatus)
	w.Write(lastBody)
}

// StartAutoDecider refreshes the decider ranking in the background so requests
// read a warm cache instead of paying the decider's latency. The request path
// still calls the decider on a cache miss (cold start, or the pool changed
// since the last refresh) — the negative cache bounds that to one call per
// interval. This is a process-lifetime goroutine; it stops with the process.
func (s *Server) StartAutoDecider() {
	if s.decideProvider == "" || s.decideModel == "" {
		return
	}
	go func() {
		s.refreshDecide()
		t := time.NewTicker(s.decideInterval)
		defer t.Stop()
		for range t.C {
			s.refreshDecide()
		}
	}()
}

// refreshDecide rebuilds the candidate pool the way handleAuto does with the
// default context filter and runs one Decide to refresh the cache. The cache
// key is the candidate list itself, so this only warms requests that use the
// default min_context — narrower filters fall back to a synchronous call.
func (s *Server) refreshDecide() {
	pool := s.rtr.ListChains()
	if len(pool) == 0 {
		return
	}
	cands, _ := selector.Eligible(pool, minContextDefault(), 0, s.ctxLen, s.hlth)
	if len(cands) <= 1 {
		return
	}
	ordered, _, source := selector.Decide(
		cands,
		s.deciderFor(cands, nil, 0),
		s.providerOrder(),
		s.decideInterval,
		&s.decideMu,
		&s.decideCache,
	)
	log.Printf("auto: background refresh source=%s order=%s", source, joinSteps(ordered))
}

// DefaultBackgroundDecideDeadline caps a whole background decider call,
// including the body read. The decider client only bounds the wait for
// response headers; without this a stalled body would hang the refresh loop
// (and therefore the ranking cache) for the life of the process.
const DefaultBackgroundDecideDeadline = 3 * time.Minute

// deciderFor builds the closure Decide calls. It captures cands and the request
// context rather than storing them on the Server, so two concurrent /v1/auto
// requests cannot hand each other's candidate list to the decider. wait bounds
// the call: a positive value caps how long a request-path caller may block on
// the decider (derived from parent, so a client disconnect cancels at once);
// 0 is the background refresh, which gets a generous total deadline instead.
func (s *Server) deciderFor(cands []selector.Candidate, parent context.Context, wait time.Duration) func(context.Context) (map[string]int, error) {
	if s.decideProvider == "" || s.decideModel == "" {
		return nil
	}
	return func(decisionCtx context.Context) (map[string]int, error) {
		// Keep parent distinct for cancellation attribution. The selector's
		// deadline can be shorter than the advisory call's own wait budget.
		ctx := context.Background()
		if parent != nil {
			ctx = parent
		}
		if deadline, ok := decisionCtx.Deadline(); ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		if wait > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, wait)
			defer cancel()
			return s.callDecider(parent, ctx, cands)
		}
		bgCtx, cancel := context.WithTimeout(ctx, DefaultBackgroundDecideDeadline)
		defer cancel()
		return s.callDecider(parent, bgCtx, cands)
	}
}

// callDecider issues the decider call straight to a provider step, bypassing
// the router. That is deliberate: going through the router would let a
// /v1/auto request reach /v1/auto again.
func (s *Server) callDecider(parent, ctx context.Context, cands []selector.Candidate) (map[string]int, error) {
	if parent != nil && parent.Err() != nil {
		return nil, parent.Err()
	}
	p, ok := s.provider(s.decideProvider)
	if !ok {
		log.Printf("auto: decider provider %q not in config", s.decideProvider)
		return nil, fmt.Errorf("decider provider %q not in config", s.decideProvider)
	}
	pool := s.pools[s.decideProvider]
	if pool == nil {
		log.Printf("auto: decider provider %q has no key pool", s.decideProvider)
		return nil, fmt.Errorf("decider provider %q has no key pool", s.decideProvider)
	}
	lease := pool.Acquire(nil)
	if lease == nil {
		return nil, errNoAvailableKeys
	}
	defer lease.Release()
	keyEnv := lease.Env()
	key := s.env(keyEnv)
	if key == "" {
		log.Printf("auto: decider key env %q not set", keyEnv)
		return nil, fmt.Errorf("decider key env %q not set", keyEnv)
	}
	a, err := adaptors.NewDecider(p, key)
	if err != nil {
		log.Printf("auto: decider adaptor: %v", err)
		return nil, err
	}
	request := map[string]any{
		"model":       s.decideModel,
		"messages":    deciderMessages(s.decidePrompt(cands)),
		"max_tokens":  max(4096, min(32768, envInt("GUVNA_DECIDER_MAX_TOKENS", 8192))),
		"temperature": 0,
	}
	// NVIDIA NIM's Nemotron reasoning mode can spend the entire output budget
	// thinking. Disable it for this small structured advisory task. Keep other
	// providers' payloads portable instead of assuming they accept vendor knobs.
	if s.decideProvider == "nvidia" && strings.Contains(strings.ToLower(s.decideModel), "nemotron") {
		request["chat_template_kwargs"] = map[string]any{"enable_thinking": false}
		request["response_format"] = map[string]string{"type": "json_object"}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		log.Printf("auto: decider payload: %v", err)
		return nil, err
	}
	callerGone := func() bool {
		return parent != nil && parent.Err() != nil || errors.Is(ctx.Err(), context.Canceled)
	}
	// The advisory decider shares provider credentials, but does not change
	// model health. Record exactly one key result for a real upstream attempt.
	finishKey := func(status int, err error, raw []byte) {
		if callerGone() {
			lease.Release()
			return
		}
		completeKeyAttempt(lease, status, err)
		in, out := telemetry.ParseUsage(raw)
		pool.Record(keyEnv, in, out)
	}
	if !lease.Available() {
		return nil, errNoAvailableKeys
	}
	resp, err := a.Chat(ctx, payload)
	if err != nil {
		log.Printf("auto: decider call: %v", err)
		finishKey(0, err, nil)
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := readLimited(resp.Body, maxResponseBytes)
	if err != nil {
		log.Printf("auto: decider read: %v", err)
		status := resp.StatusCode
		if status >= 200 && status < 300 {
			status = http.StatusBadGateway
		}
		finishKey(status, err, raw)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("auto: decider status %d: %s", resp.StatusCode, truncate(raw, 200))
		finishKey(resp.StatusCode, nil, raw)
		return nil, fmt.Errorf("decider status %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	if err := validateChatCompletion(raw); err != nil {
		finishKey(http.StatusBadGateway, err, raw)
		return nil, err
	}
	ranked, err := selector.ParseRanking(raw)
	if err != nil {
		log.Printf("auto: decider parse: %v (body: %s)", err, truncate(raw, 300))
		finishKey(http.StatusBadGateway, err, raw)
		return nil, err
	}
	valid := make(map[string]int)
	for _, c := range cands {
		name := c.Step.Provider + "/" + c.Step.Model
		if n, ok := ranked[name]; ok {
			valid[name] = n
		}
	}
	if len(valid) == 0 {
		err := fmt.Errorf("decider ranked no eligible models")
		finishKey(http.StatusBadGateway, err, raw)
		return nil, err
	}
	finishKey(resp.StatusCode, nil, raw)
	return valid, nil
}

// decidePrompt renders the pool the decider is ordering, with each candidate's
// health state. Without the health figures the decider would only be ranking
// model names, which is a static priority in a smarter font.
func (s *Server) decidePrompt(cands []selector.Candidate) string {
	rows := s.hlth.Snapshot()
	type hstat struct {
		failures int
		down     bool
	}
	stats := make(map[string]hstat, len(rows))
	for _, r := range rows {
		stats[r.Provider+"/"+r.Model] = hstat{failures: r.Failures, down: r.Down || r.Failures >= health.FailureThreshold}
	}
	var b strings.Builder
	for _, c := range cands {
		st := stats[c.Step.Provider+"/"+c.Step.Model]
		fmt.Fprintf(&b, "- %s | provider=%s | context=%d | recent_failures=%d | down=%v\n",
			sanitizeLine(c.Step.Provider+"/"+c.Step.Model), c.Step.Provider, c.Context, st.failures, st.down)
	}
	return b.String()
}

// sanitizeLine strips control characters from a chain-supplied string before it
// goes into the decider prompt. step.Model is admin-writable via local chain
// management with no character validation; without this, a crafted model
// name could inject newlines and forge ranking instructions.
func sanitizeLine(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
}

// providerOrder is the static fallback. GUVNA_PROVIDE_ORDER wins when set;
// unknown names fall out so a typo cannot shadow a real provider.
func (s *Server) providerOrder() []string {
	if v := s.env("GUVNA_PROVIDE_ORDER"); v != "" {
		var out []string
		for _, p := range strings.Fields(v) {
			provider, _, _ := strings.Cut(p, "/")
			if _, ok := s.provider(provider); ok {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	out := make([]string, 0, len(s.cfg.Providers))
	for _, p := range s.cfg.Providers {
		out = append(out, p.Name)
	}
	return out
}

func joinSteps(c []selector.Candidate) string {
	names := make([]string, 0, len(c))
	for _, e := range c {
		names = append(names, e.Step.Provider+"/"+e.Step.Model)
	}
	return strings.Join(names, " > ")
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func deciderMessages(pool string) []map[string]any {
	return []map[string]any{
		{"role": "system", "content": deciderSystemPrompt()},
		{"role": "user", "content": "Candidates:\n" + pool + "\n\nReturn the order now."},
	}
}

// deciderSystemPrompt pins the reply to one JSON object so ParseRanking never
// has to guess at the shape.
func deciderSystemPrompt() string {
	return `You are the model selector for an LLM gateway.

You get candidate models with their provider, context window, and recent
failure count. Order them best first, where best means most likely to answer
this request successfully and quickly.

Rules:
- A model marked down, or with many recent failures, goes near the end. Do not
  omit it; it may recover.
- Do not invent models that are not listed.
- Reply with ONLY a JSON object, no prose, no markdown fences:
{"order":["provider/model", ...]}`
}
