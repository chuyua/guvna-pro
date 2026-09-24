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
)

// DefaultMinContext applies when a caller does not send min_context.
const DefaultMinContext = 512

// DefaultDecideInterval is how long a decider answer is reused.
const DefaultDecideInterval = 30 * time.Second

// autoRequest carries only the fields this handler reads; the rest of the body
// is forwarded to the upstream via withModel/mergeParams in tryStep.
type autoRequest struct {
	Model      string `json:"model"`
	MinContext int    `json:"min_context"`
	MaxContext int    `json:"max_context"`
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
		minCtx = DefaultMinContext
	}
	if req.MaxContext > 0 && req.MaxContext < minCtx {
		http.Error(w, `{"error":{"message":"max_context must be >= min_context"}}`, http.StatusBadRequest)
		return
	}

	pool := s.rtr.ListChains()
	if len(pool) == 0 {
		http.Error(w, `{"error":{"message":"no chains configured"}}`, http.StatusServiceUnavailable)
		return
	}

	cands, excluded := selector.Eligible(pool, minCtx, req.MaxContext, s.ctxLen, s.hlth)
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

	ordered, decided, source := selector.Decide(
		cands,
		s.deciderFor(cands, r.Context()),
		s.providerOrder(),
		s.decideInterval,
		&s.decideMu,
		&s.decideCache,
	)
	log.Printf("auto: min=%d max=%d eligible=%d source=%s order=%s",
		minCtx, req.MaxContext, len(cands), source, joinDecided(decided))

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, c := range ordered {
		step := c.Step
		if s.hlth.IsDown(step.Provider, step.Model) {
			log.Printf("auto: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		resp, keyEnv, err := s.tryStep(r, step, body)
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
		lastBody, _ = io.ReadAll(resp.Body)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		if retryable(resp.StatusCode, nil) {
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

// deciderFor builds the closure Decide calls. It captures cands and the request
// context rather than storing them on the Server, so two concurrent /v1/auto
// requests cannot hand each other's candidate list to the decider.
func (s *Server) deciderFor(cands []selector.Candidate, parent context.Context) func(context.Context) (map[string]int, error) {
	if s.decideProvider == "" || s.decideModel == "" {
		return nil
	}
	return func(ctx context.Context) (map[string]int, error) {
		return s.callDecider(parent, ctx, cands)
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
		return nil, fmt.Errorf("decider provider %q not in config", s.decideProvider)
	}
	pool := s.pools[s.decideProvider]
	if pool == nil {
		return nil, fmt.Errorf("decider provider %q has no key pool", s.decideProvider)
	}
	keyEnv := pool.Pick()
	key := s.env(keyEnv)
	if key == "" {
		return nil, fmt.Errorf("decider key env %q not set", keyEnv)
	}
	a, err := adaptors.New(p, key)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]any{
		"model":       s.decideModel,
		"messages":    deciderMessages(s.decidePrompt(cands)),
		"max_tokens":  300,
		"temperature": 0,
	})
	if err != nil {
		return nil, err
	}
	resp, err := a.Chat(ctx, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("decider status %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	return selector.ParseRanking(raw)
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
			c.Step.Provider+"/"+c.Step.Model, c.Step.Provider, c.Context, st.failures, st.down)
	}
	return b.String()
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

func joinDecided(d []selector.Decided) string {
	names := make([]string, 0, len(d))
	for _, e := range d {
		names = append(names, e.Step)
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
