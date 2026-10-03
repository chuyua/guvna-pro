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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/adaptors"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/selector"
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
// visible to POST /v1/chains, where a client could overwrite the pool order.
func (s *Server) handleAuto(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":{"message":"read body"}}`, http.StatusBadRequest)
		return
	}
	var req autoRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	var chat chatRequest
	if err := json.Unmarshal(body, &chat); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}

	minCtx := req.MinContext
	if minCtx <= 0 {
		minCtx = minContextDefault()
	}
	if req.MaxContext > 0 && req.MaxContext < minCtx {
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
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, msg), http.StatusNotFound)
		return
	}

	ordered, _, source := selector.Decide(
		cands,
		s.deciderFor(cands, r.Context(), s.decideReqWait),
		s.providerOrder(),
		s.decideInterval,
		&s.decideMu,
		&s.decideCache,
	)
	// Vision filtering happens after ordering, not in Eligible: the decider's
	// cached ranking is keyed on the full pool, so filtering first would force
	// every image request onto a cold cache. Skipping non-vision models in rank
	// order keeps the warm ranking and only narrows the walk.
	if hasImages {
		kept := make([]selector.Candidate, 0, len(ordered))
		for _, c := range ordered {
			if s.visionReg.Has(c.Step.Model) {
				kept = append(kept, c)
			}
		}
		log.Printf("auto: images=true kept=%d/%d vision-capable", len(kept), len(ordered))
		ordered = kept
		if len(ordered) == 0 {
			http.Error(w, `{"error":{"message":"no vision-capable model available for image request"}}`, http.StatusNotFound)
			return
		}
	}
	log.Printf("auto: min=%d max=%d eligible=%d images=%v degraded=%v source=%s order=%s",
		minCtx, req.MaxContext, len(cands), hasImages, degraded, source, joinSteps(ordered))

	// Total wall-clock budget for the whole candidate walk. Decider miss
	// (≤GUVNA_DECIDER_REQUEST_WAIT) plus a few slow non-stream steps (30s each
	// without this) could exceed any client's patience; cap the walk so a
	// wedged pool degrades to a 503 in bounded time instead of chaining a
	// timeout per candidate.
	deadline := time.Now().Add(envDuration("GUVNA_AUTO_TOTAL_BUDGET", 45*time.Second))

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, c := range ordered {
		if time.Now().After(deadline) {
			log.Printf("auto: total budget exceeded, stopping walk")
			break
		}
		step := c.Step
		if s.hlth.IsDown(step.Provider, step.Model) {
			log.Printf("auto: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		resp, keyEnv, err := s.tryStepAuto(r, step, body, chat.Stream)
		if err != nil {
			log.Printf("auto: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.record(step, c.Chain, chat, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			s.hlth.MarkSuccess(step.Provider, step.Model)
			if p := s.pools[step.Provider]; p != nil {
				p.MarkSuccess(keyEnv)
			}
			s.relaySuccess(w, r, step, c.Chain, chat, resp, keyEnv)
			return
		}
		lastStatus = resp.StatusCode
		lastBody, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		// Mark unconditionally for transient and for 400: Mark itself decides
		// severity — 400 is the vision self-healing signal (a text-only model
		// eating an image request enters single-strike cool-off) and we cannot
		// gate on retryable() or that signal never fires. 404 is different: it
		// means the model is configured on the chain but absent upstream — a
		// scheduling residue, not a model-health signal. Judging it down would
		// spread a config error into /v1/chat's shared health for 5 minutes
		// (and it never heals, so re-probing is wasted). Skip it every request
		// (fast) and leave its state untouched.
		if resp.StatusCode == http.StatusNotFound {
			log.Printf("auto: step %s/%s not found upstream (config residue), skipping", step.Provider, step.Model)
		} else {
			s.hlth.Mark(step.Provider, step.Model, resp.StatusCode, nil)
		}
		log.Printf("auto: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(step, c.Chain, chat, resp.StatusCode, fmt.Errorf("status %d", resp.StatusCode), keyEnv)
	}
	log.Printf("auto: all candidates failed: %v", lastErr)
	if lastStatus == 0 {
		http.Error(w, `{"error":{"message":"no providers available"}}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", lastCT)
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
	return func(context.Context) (map[string]int, error) {
		// The ctx argument from Decide is context.Background; deriving from it
		// would sever the tie to the caller. Derive from parent instead.
		ctx := context.Background()
		if parent != nil {
			ctx = parent
		}
		if wait > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, wait)
			defer cancel()
			return s.callDecider(parent, ctx, cands)
		}
		bgCtx, cancel := context.WithTimeout(context.Background(), DefaultBackgroundDecideDeadline)
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
	keyEnv := pool.Pick()
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
	payload, err := json.Marshal(map[string]any{
		"model":       s.decideModel,
		"messages":    deciderMessages(s.decidePrompt(cands)),
		"max_tokens":  2000,
		"temperature": 0,
	})
	if err != nil {
		log.Printf("auto: decider payload: %v", err)
		return nil, err
	}
	resp, err := a.Chat(ctx, payload)
	if err != nil {
		log.Printf("auto: decider call: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("auto: decider read: %v", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("auto: decider status %d: %s", resp.StatusCode, truncate(raw, 200))
		return nil, fmt.Errorf("decider status %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	ranked, err := selector.ParseRanking(raw)
	if err != nil {
		log.Printf("auto: decider parse: %v (body: %s)", err, truncate(raw, 300))
		return nil, err
	}
	return ranked, nil
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
// goes into the decider prompt. step.Model is client-writable via
// POST /v1/chains with no character validation; without this, a crafted model
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
			if _, ok := s.provider(p); ok {
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
