// Package server wires the HTTP surface: /healthz, /v1/models, and the
// relays. /v1/chat/completions is raw passthrough; /v1/responses is the same
// chain-based fallback with protocol translation on both edges (see package
// responses and responses_route.go), because no upstream implements the
// Responses API.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/creamy-ghost/guvna/internal/adaptors"
	"github.com/creamy-ghost/guvna/internal/auth"
	"github.com/creamy-ghost/guvna/internal/chains"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/ctxsize"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/logring"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/selector"
	"github.com/creamy-ghost/guvna/internal/telemetry"
	"github.com/creamy-ghost/guvna/internal/vision"
)

// KeepAliveInterval is how long the relay waits without upstream data before
// writing an SSE comment line to hold the connection open.
const KeepAliveInterval = 15 * time.Second

// retries is how many times a step is retried (beyond the first attempt) on
// transient failures (429/5xx/network) before the chain moves to the next step.
// Overridable via GUVNA_RETRIES.
//
// This is a budget multiplier, not a robustness knob: with the transport's
// ResponseHeaderTimeout at H, one step can now sit on the wire for
// (retries+1)*H plus backoff before the chain moves on, so a chain of N steps
// can take up to N*(retries+1)*H. The defaults were 3 steps x 3 attempts x 45s
// = 405s, which no client waits for. A gateway whose job is failover should
// spend that budget on other providers rather than re-hitting one hung
// endpoint, so the live deployment runs GUVNA_RETRIES=1 with a 20s header
// timeout: 3 steps x 2 attempts x 20s = 120s worst case.
const defaultRetries = 2

var retries = envInt("GUVNA_RETRIES", defaultRetries)

// envInt reads a non-negative integer override from the environment, falling
// back to def on any empty, unparsable, or negative value.
func envInt(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

type Server struct {
	cfg       *config.Config
	rtr       *router.Router
	auth      *auth.Authenticator
	tm        *telemetry.Telemetry
	hlth      *health.Tracker
	logs      *logring.Ring
	store     *chains.Store
	pools     map[string]*keypool.Pool
	started   time.Time
	env       func(string) string
	retryWait func(attempt int) time.Duration

	// /v1/auto selector state. ctxLen answers "how big is this model's
	// window"; the decide* fields hold the decider target and its cached
	// answer. decideMu guards decideCandidates, decideCache and the snapshot
	// they are built from.
	ctxLen           func(model string) int
	visionReg        *vision.Registry
	decideProvider   string
	decideModel      string
	decideInterval   time.Duration
	decideReqWait    time.Duration
	decideMu         sync.Mutex
	decideCandidates []selector.Candidate
	decideCache      selector.Cache
}

func New(cfg *config.Config, rtr *router.Router, a *auth.Authenticator, tm *telemetry.Telemetry, hlth *health.Tracker, logs *logring.Ring, store *chains.Store) *Server {
	pools := make(map[string]*keypool.Pool, len(cfg.Providers))
	for _, p := range cfg.Providers {
		pool, err := keypool.New(p.Name, p.AllKeyEnvs(), p.Rotation, p.Quarantine)
		if err != nil {
			// Fatal: a provider that fails to build its key pool would serve
			// requests that die at pick time. Silently dropping it here turns an
			// operator-entered config error into a live 503 (or, in the old lazy
			// path, a concurrent map write). config.Validate rejects empty pools
			// and unknown rotations at startup, so this only fires on any future
			// keypool failure mode — fail loudly, not silently.
			log.Fatalf("key pool for provider %q: %v", p.Name, err)
		}
		pools[p.Name] = pool
	}
	s := &Server{cfg: cfg, rtr: rtr, auth: a, tm: tm, hlth: hlth, logs: logs, store: store, pools: pools, started: time.Now(), env: os.Getenv, retryWait: defaultRetryWait}
	s.ctxLen, s.visionReg, s.decideProvider, s.decideModel, s.decideInterval, s.decideReqWait = loadAutoConfig()
	return s
}

// loadAutoConfig reads the /v1/auto knobs. The registry is advisory and never
// fatal — a missing or malformed file must not take the gateway down.
func loadAutoConfig() (func(string) int, *vision.Registry, string, string, time.Duration, time.Duration) {
	reg, err := ctxsize.Load(envOr("GUVNA_CONTEXT_REGISTRY", ""))
	if err != nil {
		log.Printf("auto: context registry %v (using built-ins)", err)
		reg = &ctxsize.Registry{DefaultContext: ctxsize.DefaultContext}
	}
	if reg == nil {
		reg = &ctxsize.Registry{DefaultContext: ctxsize.DefaultContext}
	}
	forCtx := reg.For
	vreg, err := vision.Load(envOr("GUVNA_VISION_REGISTRY", ""))
	if err != nil {
		log.Printf("auto: vision registry %v (using built-ins)", err)
		vreg = nil
	}
	provider, model := envOr("GUVNA_DECIDER_PROVIDER", ""), envOr("GUVNA_DECIDER_MODEL", "")
	interval := envDuration("GUVNA_DECIDE_INTERVAL", DefaultDecideInterval)
	// Floor the interval: a typo like "10ms" would otherwise hammer the decider
	// (each call costs real tokens) and shrink the negative-cache window to
	// nothing.
	if interval < time.Second {
		log.Printf("auto: GUVNA_DECIDE_INTERVAL=%s below 1s, using 1s", interval)
		interval = time.Second
	}
	// A request that hits a cold cache must not inherit the decider client's
	// 90s header timeout: when the decider provider is down (2026-09-29, nvidia
	// all-day http2 timeouts), the first request paid 90s before falling back,
	// by which time the client was long gone. The background refresh keeps the
	// long timeout; the request path gets this bounded wait instead.
	reqWait := envDuration("GUVNA_DECIDER_REQUEST_WAIT", DefaultDeciderRequestWait)
	return forCtx, vreg, provider, model, interval, reqWait
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return def
}

// defaultRetryWait backs off 250ms then 1s for the two retry attempts.
func defaultRetryWait(attempt int) time.Duration {
	if attempt <= 1 {
		return 250 * time.Millisecond
	}
	return time.Second
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("GET /v1/models", s.auth.Middleware(http.HandlerFunc(s.handleModels)))
	mux.Handle("POST /v1/chat/completions", s.auth.Middleware(http.HandlerFunc(s.handleChat)))
	mux.Handle("POST /v1/embeddings", s.auth.Middleware(http.HandlerFunc(s.handleEmbeddings)))
	mux.Handle("POST /v1/auto", s.auth.Middleware(http.HandlerFunc(s.handleAuto)))
	mux.Handle("POST /v1/responses", s.auth.Middleware(http.HandlerFunc(s.handleResponses)))
	mux.Handle("GET /v1/models/{id}", s.auth.Middleware(http.HandlerFunc(s.handleModelInfo)))
	mux.Handle("GET /v1/responses/{id}", s.auth.Middleware(http.HandlerFunc(s.handleResponseLookup)))
	mux.Handle("DELETE /v1/responses/{id}", s.auth.Middleware(http.HandlerFunc(s.handleResponseDelete)))
	mux.Handle("POST /v1/responses/{id}/cancel", s.auth.Middleware(http.HandlerFunc(s.handleResponseCancel)))
	mux.Handle("GET /v1/chains", s.auth.Middleware(http.HandlerFunc(s.handleChainList)))
	mux.Handle("POST /v1/chains", s.auth.Middleware(http.HandlerFunc(s.handleChainCreate)))
	mux.Handle("DELETE /v1/chains/{name}", s.auth.Middleware(http.HandlerFunc(s.handleChainDelete)))
	mux.Handle("GET /admin/status", s.auth.AdminMiddleware(http.HandlerFunc(s.handleAdminStatus)))
	mux.Handle("GET /admin/logs", s.auth.AdminMiddleware(http.HandlerFunc(s.handleAdminLogs)))
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

// handleAdminStatus reports chain health and usage aggregates to the admin
// (or a remote CLI with the admin key).
func (s *Server) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	stats, err := s.tm.Stats()
	if err != nil {
		http.Error(w, `{"error":{"message":"stats unavailable"}}`, http.StatusInternalServerError)
		return
	}
	steps := s.hlth.Snapshot()
	stepsDown := 0
	for _, st := range steps {
		if st.Down {
			stepsDown++
		}
	}
	keys := make([]keypool.KeyStatus, 0, len(s.pools))
	for _, name := range s.poolNames() {
		keys = append(keys, s.pools[name].Keys()...)
	}
	resp := struct {
		Uptime    string              `json:"uptime"`
		Chains    []string            `json:"chains"`
		Steps     []health.StepStatus `json:"steps"`
		StepsDown int                 `json:"steps_down"`
		Keys      []keypool.KeyStatus `json:"keys"`
		Usage     telemetry.Stats     `json:"usage"`
	}{Uptime: time.Since(s.started).Round(time.Second).String()}
	resp.Chains = s.rtr.ChainNames()
	resp.Steps = steps
	resp.StepsDown = stepsDown
	resp.Keys = keys
	resp.Usage = stats
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 512 {
			n = parsed
		}
	}
	resp := struct {
		Lines []string `json:"lines"`
	}{Lines: s.logs.Tail(n)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	names := s.rtr.ChainNames()
	resp := struct {
		Object string `json:"object"`
		Data   []struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}{Object: "list"}
	for _, n := range names {
		resp.Data = append(resp.Data, struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
			OwnedBy string `json:"owned_by"`
		}{ID: n, Object: "model", Created: time.Now().Unix(), OwnedBy: "guvna"})
	}
	// "auto" is a virtual model: the gateway picks the best real model at
	// request time. Listing it lets clients discover it from /v1/models.
	resp.Data = append(resp.Data, struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}{ID: "auto", Object: "model", Created: time.Now().Unix(), OwnedBy: "guvna"})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleChainList returns the current chains (config + runtime) with sources.
func (s *Server) handleChainList(w http.ResponseWriter, r *http.Request) {
	resp := struct {
		Chains []chainInfo `json:"chains"`
	}{}
	for _, c := range s.rtr.ListChains() {
		steps := make([]chainStep, 0, len(c.Steps))
		for _, st := range c.Steps {
			steps = append(steps, chainStep{Provider: st.Provider, Model: st.Model, Params: st.Params})
		}
		resp.Chains = append(resp.Chains, chainInfo{Name: c.Name, Source: c.Source, Steps: steps})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

type chainStep struct {
	Provider string         `json:"provider"`
	Model    string         `json:"model"`
	Params   map[string]any `json:"params,omitempty"`
}

type chainInfo struct {
	Name   string      `json:"name"`
	Source string      `json:"source"`
	Steps  []chainStep `json:"steps"`
}

type chainCreateRequest struct {
	Name  string      `json:"name"`
	Steps []chainStep `json:"steps"`
}

// handleChainCreate adds a runtime chain and persists it. Any valid key
// (admin or client) may create chains — apps self-provision their routes.
func (s *Server) handleChainCreate(w http.ResponseWriter, r *http.Request) {
	var req chainCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	steps := make([]router.Step, 0, len(req.Steps))
	for _, st := range req.Steps {
		steps = append(steps, router.Step{Provider: st.Provider, Model: st.Model, Params: st.Params})
	}
	if err := s.rtr.AddChain(req.Name, steps); err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already exists") {
			status = http.StatusConflict
		}
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), status)
		return
	}
	if s.store != nil {
		if err := s.store.Save(s.rtr); err != nil {
			log.Printf("chains: persist failed: %v", err)
			http.Error(w, `{"error":{"message":"chain added but not persisted"}}`, http.StatusInternalServerError)
			return
		}
	}
	log.Printf("chains: created %q (%d steps)", req.Name, len(steps))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(chainInfo{Name: req.Name, Source: "runtime", Steps: req.Steps})
}

// handleChainDelete removes a runtime chain. Config-defined chains are
// refused — they live in config.yaml.
func (s *Server) handleChainDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.rtr.RemoveChain(name); err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), status)
		return
	}
	if s.store != nil {
		if err := s.store.Save(s.rtr); err != nil {
			log.Printf("chains: persist failed: %v", err)
			http.Error(w, `{"error":{"message":"chain removed but not persisted"}}`, http.StatusInternalServerError)
			return
		}
	}
	log.Printf("chains: deleted %q", name)
	w.WriteHeader(http.StatusNoContent)
}

type chatRequest struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":{"message":"read body"}}`, http.StatusBadRequest)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	if strings.EqualFold(req.Model, "auto") {
		r.Body = io.NopCloser(bytes.NewReader(body))
		s.handleAuto(w, r)
		return
	}
	chain, steps, err := s.rtr.Resolve(req.Model)
	if err != nil {
		if errors.Is(err, router.ErrNotFound) {
			http.Error(w, fmt.Sprintf(`{"error":{"message":"unknown chain or model %q — create a chain via POST /v1/chains or use a provider-prefixed model"}}`, req.Model), http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":{"message":"resolve failed"}}`, http.StatusInternalServerError)
		return
	}
	log.Printf("chat: chain=%s model=%q steps=%d stream=%v", chain, req.Model, len(steps), req.Stream)

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, step := range steps {
		if s.hlth.IsDown(step.Provider, step.Model) {
			log.Printf("chat: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		// A stream_only provider's non-streaming endpoint is broken: rewrite
		// the body to upstream streaming and aggregate below.
		stepBody := body
		agg := !req.Stream && s.streamOnly(step.Provider)
		if agg {
			stepBody = forceStreamBody(body)
		}
		resp, keyEnv, err := s.tryStep(r, step, stepBody)
		if err != nil {
			log.Printf("chat: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.record(step, chain, req, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if agg {
				aggResp, aerr := s.aggregateStreamed(resp)
				if aerr != nil {
					log.Printf("chat: step %s/%s aggregate failed: %v", step.Provider, step.Model, aerr)
					s.hlth.Mark(step.Provider, step.Model, 0, aerr)
					s.record(step, chain, req, 0, aerr, keyEnv)
					continue
				}
				// Health is marked inside relaySuccess, after the aggregated
				// body is fully written to the client.
				s.relaySuccess(w, r, step, chain, req, aggResp, keyEnv)
				return
			}
			// Health is marked inside relaySuccess/relayStream, after the body
			// actually lands: a 2xx header followed by a broken body must not
			// reset the step's failure count (the pre-body MarkSuccess here
			// used to do exactly that, so a truncating upstream never reached
			// cool-off).
			s.relaySuccess(w, r, step, chain, req, resp, keyEnv)
			return
		}
		lastStatus = resp.StatusCode
		lastBody, _ = io.ReadAll(resp.Body)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		if retryable(resp.StatusCode, nil) {
			s.hlth.Mark(step.Provider, step.Model, resp.StatusCode, nil)
		}
		log.Printf("chat: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(step, chain, req, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
	}
	// All steps failed: propagate the last upstream response as-is.
	log.Printf("chat: all steps failed for chain %q: %v", chain, lastErr)
	if lastStatus == 0 {
		http.Error(w, `{"error":{"message":"no providers available"}}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", lastCT)
	w.WriteHeader(lastStatus)
	w.Write(lastBody)
}

// handleEmbeddings relays an embeddings request to the resolved provider.
// Embeddings are non-streaming: the request is forwarded raw to the upstream's
// /v1/embeddings endpoint and the response is passed through byte-identical.
// Model names follow the same provider-prefixed convention as chat (e.g.
// nvidia/nemotron-3-embed-1b), resolved through the router's passthrough path
// when the model is not a named chain.
func (s *Server) handleEmbeddings(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":{"message":"read body"}}`, http.StatusBadRequest)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	if req.Model == "" {
		http.Error(w, `{"error":{"message":"missing model"}}`, http.StatusBadRequest)
		return
	}
	chain, steps, err := s.rtr.Resolve(req.Model)
	if err != nil {
		if errors.Is(err, router.ErrNotFound) {
			http.Error(w, fmt.Sprintf(`{"error":{"message":"unknown model %q — use a provider-prefixed model"}}`, req.Model), http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":{"message":"resolve failed"}}`, http.StatusInternalServerError)
		return
	}
	log.Printf("embed: model=%q steps=%d", req.Model, len(steps))

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, step := range steps {
		if s.hlth.IsDown(step.Provider, step.Model) {
			log.Printf("embed: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		resp, keyEnv, err := s.tryEmbed(r, step, body)
		if err != nil {
			log.Printf("embed: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.record(step, chain, req, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Read the body before committing a status: a silently truncated
			// 200 with a partial embedding would be worse than a 502 — the
			// caller cannot tell an incomplete vector from a complete one.
			embedBody, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				log.Printf("embed: body read failed from %s/%s: %v", step.Provider, step.Model, readErr)
				if r.Context().Err() == nil {
					s.hlth.Mark(step.Provider, step.Model, 0, readErr)
					s.poolRecordResult(keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, readErr)
					http.Error(w, `{"error":{"message":"upstream read failed"}}`, http.StatusBadGateway)
				}
				return
			}
			s.hlth.MarkSuccess(step.Provider, step.Model)
			s.poolRecord(keyEnv, step, chain, req, resp.StatusCode, 0, 0, "")
			w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
			w.WriteHeader(resp.StatusCode)
			w.Write(embedBody)
			return
		}
		lastStatus = resp.StatusCode
		lastBody, _ = io.ReadAll(resp.Body)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		if retryable(resp.StatusCode, nil) {
			s.hlth.Mark(step.Provider, step.Model, resp.StatusCode, nil)
		}
		log.Printf("embed: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(step, chain, req, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
	}
	log.Printf("embed: all steps failed for model %q: %v", req.Model, lastErr)
	if lastStatus == 0 {
		http.Error(w, `{"error":{"message":"no providers available"}}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", lastCT)
	w.WriteHeader(lastStatus)
	w.Write(lastBody)
}

// tryEmbed is the embeddings sibling of tryStepAdaptor: same key-pool rotation
// and retry, but the payload goes to the upstream's /v1/embeddings endpoint.
func (s *Server) tryEmbed(r *http.Request, step router.Step, body []byte) (*http.Response, string, error) {
	p, ok := s.provider(step.Provider)
	if !ok {
		return nil, "", fmt.Errorf("provider %q not in config", step.Provider)
	}
	pool := s.pools[step.Provider]
	if pool == nil {
		return nil, "", fmt.Errorf("provider %q: no key pool", step.Provider)
	}
	payload := mergeParams(withModel(body, step.Model), step.Params)
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && s.retryWait != nil {
			time.Sleep(s.retryWait(attempt))
		}
		keyEnv := pool.Pick()
		key := s.env(keyEnv)
		if key == "" {
			err := fmt.Errorf("provider %q: key env %q not set", p.Name, keyEnv)
			log.Printf("embed: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			lastErr = err
			continue
		}
		a, err := adaptors.New(p, key)
		if err != nil {
			log.Printf("embed: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			lastErr = err
			continue
		}
		resp, err := a.Embed(r.Context(), payload)
		if err != nil {
			log.Printf("embed: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			pool.Mark(keyEnv, keypool.ClassFor(0, err))
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, keyEnv, nil
		}
		log.Printf("embed: %s/%s attempt %d: status %d", step.Provider, step.Model, attempt+1, resp.StatusCode)
		if !retryable(resp.StatusCode, nil) && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
			return resp, keyEnv, nil // other 4xx: config/request error, never retried
		}
		pool.Mark(keyEnv, keypool.ClassFor(resp.StatusCode, nil))
		if attempt == retries {
			return resp, keyEnv, nil // transient failures exhausted: last response
		}
		resp.Body.Close()
	}
	return nil, "", lastErr
}

// tryStep attempts one chain step, retrying transient failures (429, 5xx,
// network errors) up to retries times with backoff. Each attempt picks a key
// from the provider's pool; quarantinable failures rotate to the next key.
// Streaming is safe: a retry only happens when no 2xx response has been
// received yet — once the upstream starts streaming we are committed to that
// stream.
//
// Returns (resp, keyEnv, nil) when an upstream response was obtained (2xx or
// not — body left open for the caller to relay), or (nil, "", err) when every
// attempt failed at the network level.
func (s *Server) tryStep(r *http.Request, step router.Step, body []byte) (*http.Response, string, error) {
	return s.tryStepAdaptor(r.Context(), r, step, body, adaptors.New)
}

// streamOnly reports whether the provider's non-streaming endpoint is broken
// and non-stream requests must ride an aggregated upstream stream instead.
func (s *Server) streamOnly(name string) bool {
	p, ok := s.provider(name)
	return ok && p.StreamOnly
}

// tryStepAuto serves one /v1/auto step through the auto clients: streams get
// a short first-token budget, non-stream completions a longer whole-generation
// budget (the upstream sends headers only after generating the whole body), so
// a slow upstream costs seconds instead of the chain-sized wait.
func (s *Server) tryStepAuto(ctx context.Context, r *http.Request, step router.Step, body []byte, stream bool) (*http.Response, string, error) {
	mk := func(p config.Provider, key string) (adaptors.Adaptor, error) {
		return adaptors.NewAuto(p, key, stream)
	}
	return s.tryStepAdaptor(ctx, r, step, body, mk)
}

func (s *Server) tryStepAdaptor(ctx context.Context, r *http.Request, step router.Step, body []byte, mkAdaptor func(config.Provider, string) (adaptors.Adaptor, error)) (*http.Response, string, error) {
	p, ok := s.provider(step.Provider)
	if !ok {
		return nil, "", fmt.Errorf("provider %q not in config", step.Provider)
	}
	pool := s.pools[step.Provider]
	if pool == nil {
		return nil, "", fmt.Errorf("provider %q: no key pool", step.Provider)
	}
	payload := mergeParams(withModel(body, step.Model), step.Params)
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && s.retryWait != nil {
			time.Sleep(s.retryWait(attempt))
		}
		keyEnv := pool.Pick()
		key := s.env(keyEnv)
		if key == "" {
			err := fmt.Errorf("provider %q: key env %q not set", p.Name, keyEnv)
			log.Printf("chat: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			lastErr = err
			continue
		}
		a, err := mkAdaptor(p, key)
		if err != nil {
			log.Printf("chat: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			lastErr = err
			continue
		}
		resp, err := a.Chat(ctx, payload)
		if err != nil {
			log.Printf("chat: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			pool.Mark(keyEnv, keypool.ClassFor(0, err))
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, keyEnv, nil
		}
		log.Printf("chat: %s/%s attempt %d: status %d", step.Provider, step.Model, attempt+1, resp.StatusCode)
		if !retryable(resp.StatusCode, nil) && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
			return resp, keyEnv, nil // other 4xx: config/request error, never retried
		}
		pool.Mark(keyEnv, keypool.ClassFor(resp.StatusCode, nil))
		if attempt == retries {
			return resp, keyEnv, nil // transient failures exhausted: last response
		}
		resp.Body.Close()
	}
	return nil, "", lastErr
}

// retryable reports whether a failure is transient: network errors, rate
// limits, and 5xx. Client errors (4xx) are config or request problems and are
// never retried or counted against provider health.
func retryable(status int, err error) bool {
	if err != nil {
		return true
	}
	return status == http.StatusTooManyRequests || status >= 500
}

// withModel rewrites the "model" field of a chat completion body to the given
// model name, passing everything else through untouched.
func withModel(body []byte, model string) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	// Set unconditionally, not just when the field exists: a /v1/auto request
	// may omit "model" entirely, and every upstream requires it — omitting the
	// rewrite would fail the whole candidate walk with 400s.
	enc, err := json.Marshal(model)
	if err == nil {
		m["model"] = enc
	}
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// mergeParams applies a step's params as fill-missing defaults: a param is set
// on the body only when the client didn't supply that field. Explicit client
// values always win. The body is returned unchanged on any parse failure.
func mergeParams(body []byte, params map[string]any) []byte {
	if len(params) == 0 {
		return body
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	for k, v := range params {
		if _, present := m[k]; present {
			continue
		}
		enc, err := json.Marshal(v)
		if err != nil {
			continue
		}
		m[k] = enc
	}
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

func (s *Server) provider(name string) (config.Provider, bool) {
	for _, p := range s.cfg.Providers {
		if p.Name == name {
			return p, true
		}
	}
	return config.Provider{}, false
}

// poolNames returns provider names that have a key pool, sorted.
func (s *Server) poolNames() []string {
	names := make([]string, 0, len(s.pools))
	for n := range s.pools {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *Server) relaySuccess(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response, keyEnv string) {
	defer resp.Body.Close()
	if !req.Stream {
		// Non-streaming: relay body, best-effort token count.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			if r.Context().Err() != nil {
				// Client disconnected — not an upstream failure.
				return
			}
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.poolRecordResult(keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, err)
			http.Error(w, `{"error":{"message":"upstream read failed"}}`, http.StatusBadGateway)
			return
		}
		s.hlth.MarkSuccess(step.Provider, step.Model)
		in, out := telemetry.ParseUsage(body)
		s.poolRecord(keyEnv, step, chain, req, resp.StatusCode, in, out, "")
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
		return
	}
	s.relayStream(w, r, step, chain, req, resp, keyEnv)
}

func (s *Server) relayStream(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response, keyEnv string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":{"message":"streaming unsupported"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
	w.WriteHeader(resp.StatusCode)
	flusher.Flush()

	lines, stop := readStreamLines(r.Context(), resp.Body)
	defer stop()
	timer := time.NewTimer(KeepAliveInterval)
	defer timer.Stop()
	var in, out int64
	var data sseData
	for {
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
			timer.Reset(KeepAliveInterval)
		case next := <-lines:
			line, err := next.line, next.err
			if r.Context().Err() != nil {
				return
			}
			if len(line) > 0 {
				if _, werr := w.Write(line); werr != nil {
					return
				}
				flusher.Flush()
				resetKeepAlive(timer)
				if payload, ok := data.line(line); ok {
					if i, o := telemetry.ParseUsage([]byte(payload)); i != 0 || o != 0 {
						in, out = i, o
					}
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if payload, ok := data.flush(); ok {
						if i, o := telemetry.ParseUsage([]byte(payload)); i != 0 || o != 0 {
							in, out = i, o
						}
					}
					s.hlth.MarkSuccess(step.Provider, step.Model)
					s.poolRecord(keyEnv, step, chain, req, resp.StatusCode, in, out, "")
					return
				}
				log.Printf("chat: stream error from %q: %v", step.Provider, err)
				io.WriteString(w, `data: {"error":{"message":"upstream stream error"}}`+"\n\n")
				flusher.Flush()
				s.hlth.Mark(step.Provider, step.Model, 0, err)
				s.poolRecordResult(keyEnv, step, chain, req, 502, in, out, keypool.ClassTransient, err)
				return
			}
		}
	}
}

// poolRecord records usage against the serving key and marks its pool state:
// failures for class != "", success otherwise.
func (s *Server) poolRecord(keyEnv string, step router.Step, chain string, req chatRequest, status int, in, out int64, class keypool.Class) {
	s.poolRecordResult(keyEnv, step, chain, req, status, in, out, class, nil)
}

// poolRecordResult commits one terminal request event, including an error and
// any partial usage, rather than recording separate token and error rows.
func (s *Server) poolRecordResult(keyEnv string, step router.Step, chain string, req chatRequest, status int, in, out int64, class keypool.Class, err error) {
	p := s.pools[step.Provider]
	if p != nil && keyEnv != "" {
		if class != "" {
			p.Mark(keyEnv, class)
		} else {
			p.MarkSuccess(keyEnv)
		}
		p.Record(keyEnv, in, out)
	}
	e := telemetry.Event{Provider: step.Provider, Chain: chain, Model: step.Model, Stream: req.Stream, Status: status, TokensIn: in, TokensOut: out, Key: keyEnv, Ts: time.Now()}
	if err != nil {
		e.Err = err.Error()
	}
	s.tm.Record(e)
}

// dataPayload returns the JSON payload of an SSE data: line, or nil.
func dataPayload(line []byte) []byte {
	s := strings.TrimSpace(string(line))
	if !strings.HasPrefix(s, "data:") {
		return nil
	}
	return []byte(strings.TrimSpace(s[len("data:"):]))
}

func (s *Server) record(step router.Step, chain string, req chatRequest, status int, err error, key string) {
	e := telemetry.Event{
		Provider: step.Provider,
		Chain:    chain,
		Model:    step.Model,
		Stream:   req.Stream,
		Status:   status,
		Key:      key,
		Ts:       time.Now(),
	}
	if err != nil {
		e.Err = err.Error()
	}
	s.tm.Record(e)
}

func (s *Server) recordTokens(step router.Step, chain string, req chatRequest, status int, in, out int64, key string) {
	e := telemetry.Event{
		Provider:  step.Provider,
		Chain:     chain,
		Model:     step.Model,
		Stream:    req.Stream,
		Status:    status,
		TokensIn:  in,
		TokensOut: out,
		Key:       key,
		Ts:        time.Now(),
	}
	s.tm.Record(e)
}
