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
	"sync/atomic"
	"time"

	"github.com/creamy-ghost/guvna/internal/adaptors"
	"github.com/creamy-ghost/guvna/internal/auth"
	"github.com/creamy-ghost/guvna/internal/chains"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/ctxsize"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/logring"
	"github.com/creamy-ghost/guvna/internal/outcome"
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
	cfg        *config.Config
	rtr        *router.Router
	auth       *auth.Authenticator
	tm         *telemetry.Telemetry
	hlth       *health.Tracker
	logs       *logring.Ring
	reqSeq     atomic.Uint64
	store      *chains.Store
	pools      map[string]*keypool.Pool
	started    time.Time
	env        func(string) string
	retryWait  func(attempt int) time.Duration
	retrySleep func(context.Context, time.Duration) error

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
	autoScoring      autoScoringState
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

// defaultRetryWait spreads retries over a capped exponential Full Jitter window.
func defaultRetryWait(attempt int) time.Duration {
	return fullJitter(attempt, envDuration("GUVNA_RETRY_BASE", 250*time.Millisecond), envDuration("GUVNA_RETRY_MAX_WAIT", 30*time.Second))
}

// newRequestID returns a process-unique correlation id for one candidate walk.
// The nanosecond prefix keeps it sortable; the sequence disambiguates same-ns
// requests.
func (s *Server) newRequestID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), s.reqSeq.Add(1))
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
	mux.Handle("GET /admin/auto/scoring", s.auth.AdminMiddleware(http.HandlerFunc(s.handleAutoScoring)))
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
		Uptime     string               `json:"uptime"`
		Chains     []string             `json:"chains"`
		Steps      []health.StepStatus  `json:"steps"`
		StepsDown  int                  `json:"steps_down"`
		Keys       []keypool.KeyStatus  `json:"keys"`
		Usage      telemetry.Stats      `json:"usage"`
		Telemetry  telemetry.FlushStats `json:"telemetry"`
		UsageError string               `json:"usage_error,omitempty"`
	}{Uptime: time.Since(s.started).Round(time.Second).String()}
	resp.Chains = s.rtr.ChainNames()
	resp.Steps = steps
	resp.StepsDown = stepsDown
	resp.Keys = keys
	resp.Usage = stats
	resp.Telemetry = s.tm.FlushStats()
	if err != nil {
		resp.UsageError = "persisted usage unavailable"
	}
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
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	var req chainCreateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	steps := make([]router.Step, 0, len(req.Steps))
	for _, st := range req.Steps {
		steps = append(steps, router.Step{Provider: st.Provider, Model: st.Model, Params: st.Params})
	}
	if s.store != nil {
		err = s.store.AddChain(s.rtr, req.Name, steps)
	} else {
		err = s.rtr.AddChain(req.Name, steps)
	}
	if err != nil {
		if errors.Is(err, chains.ErrPersistence) {
			log.Printf("chains: persist failed: %v", err)
			http.Error(w, `{"error":{"message":"chain could not be persisted"}}`, http.StatusInternalServerError)
			return
		}
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already exists") {
			status = http.StatusConflict
		}
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), status)
		return
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
	var err error
	if s.store != nil {
		err = s.store.RemoveChain(s.rtr, name)
	} else {
		err = s.rtr.RemoveChain(name)
	}
	if err != nil {
		if errors.Is(err, chains.ErrPersistence) {
			log.Printf("chains: persist failed: %v", err)
			http.Error(w, `{"error":{"message":"chain could not be persisted"}}`, http.StatusInternalServerError)
			return
		}
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q}}`, err.Error()), status)
		return
	}
	log.Printf("chains: deleted %q", name)
	w.WriteHeader(http.StatusNoContent)
}

type chatRequest struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
	N      int    `json:"n"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	r = withRequestID(r, s.newRequestID())
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":{"message":"invalid json"}}`, http.StatusBadRequest)
		return
	}
	if strings.EqualFold(req.Model, "auto") {
		s.handleAutoBody(w, r, body, started)
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
	deadline := walkDeadline(chatTotalBudget)
	for _, step := range steps {
		if retryBudgetExhausted(r) {
			break
		}
		if r.Context().Err() != nil {
			return
		}
		permit, admitted := s.hlth.TryAcquire(step.Provider, step.Model)
		if !admitted {
			log.Printf("chat: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		defer permit.Release()
		r = withPermit(r, permit)
		r = withAttempt(r, chain, req, body)
		defer releaseAttempt(r)
		if walkExpired(deadline) {
			log.Printf("chat: request budget exhausted, stopping the walk")
			break
		}
		// A stream_only provider's non-streaming endpoint is broken: rewrite
		// the body to upstream streaming and aggregate below.
		stepReq := withChoiceDefault(req, body, step.Params)
		stepBody := body
		agg := !req.Stream && s.streamOnly(step.Provider)
		if agg {
			stepBody = forceStreamBody(body)
		}
		stepCtx, cancel := stepContext(r, deadline, req.Stream, chatStepBudget)
		resp, keyEnv, err := s.tryStepCtx(stepCtx, r, step, stepBody)
		if err != nil {
			cancel()
			if r.Context().Err() != nil {
				return
			}
			if errors.Is(err, errNoAvailableKeys) {
				permit.Release()
				continue
			}
			if isClientGone(err) {
				log.Printf("chat: client gone, stopping the walk at %s/%s: %v", step.Provider, step.Model, err)
				return
			}
			log.Printf("chat: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.mark(r, step.Provider, step.Model, 0, err)
			s.record(r, step, chain, req, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var res relayResult
			if agg {
				aggResp, aerr := s.aggregateStreamed(resp, stepReq.N)
				if aerr != nil {
					cancel()
					if r.Context().Err() != nil {
						return
					}
					log.Printf("chat: step %s/%s aggregate failed: %v", step.Provider, step.Model, aerr)
					s.mark(r, step.Provider, step.Model, 0, aerr)
					s.poolRecordResult(r, keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, aerr)
					failure := unusableBody(aerr)
					lastStatus, lastBody, lastCT, lastErr = failure.status, failure.body, failure.ct, failure.err
					continue
				}
				// Health is marked inside relaySuccess, after the aggregated
				// body is fully written to the client.
				res = s.relaySuccess(w, r, step, chain, stepReq, aggResp, keyEnv)
			} else {
				// Health is marked inside relaySuccess/relayStream, after the body
				// actually lands: a 2xx header followed by a broken body must not
				// reset the step's failure count (the pre-body MarkSuccess here
				// used to do exactly that, so a truncating upstream never reached
				// cool-off).
				res = s.relaySuccess(w, r, step, chain, stepReq, resp, keyEnv)
			}
			cancel()
			if res.clientGone {
				return
			}
			if res.outcome == relayCommitted {
				return
			}
			// Nothing was written downstream: the body was unusable or the read
			// failed, so the walk may still try the next step.
			lastStatus, lastBody, lastCT, lastErr = res.status, res.body, res.ct, res.err
			continue
		}
		lastStatus = resp.StatusCode
		lastBody, _ = readErrorBody(resp, req.Stream)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		cancel()
		if r.Context().Err() != nil {
			return
		}
		s.mark(r, step.Provider, step.Model, resp.StatusCode, nil)
		log.Printf("chat: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(r, step, chain, req, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
		permit.Release()
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
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	r = withRequestID(r, s.newRequestID())
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
	deadline := walkDeadline(chatTotalBudget)
	for _, step := range steps {
		if retryBudgetExhausted(r) {
			break
		}
		if r.Context().Err() != nil {
			return
		}
		permit, admitted := s.hlth.TryAcquire(step.Provider, step.Model)
		if !admitted {
			log.Printf("embed: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		defer permit.Release()
		r = withPermit(r, permit)
		r = withAttempt(r, chain, req, body)
		defer releaseAttempt(r)
		if walkExpired(deadline) {
			log.Printf("embed: request budget exhausted, stopping the walk")
			break
		}
		stepCtx, cancel := stepContext(r, deadline, false, chatStepBudget)
		resp, keyEnv, err := s.tryEmbed(stepCtx, r, step, body)
		if err != nil {
			cancel()
			if r.Context().Err() != nil {
				return
			}
			if errors.Is(err, errNoAvailableKeys) {
				permit.Release()
				continue
			}
			if isClientGone(err) {
				log.Printf("embed: client gone, stopping the walk at %s/%s: %v", step.Provider, step.Model, err)
				return
			}
			log.Printf("embed: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.mark(r, step.Provider, step.Model, 0, err)
			s.record(r, step, chain, req, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Read the body before committing a status: a silently truncated
			// 200 with a partial embedding would be worse than a 502 — the
			// caller cannot tell an incomplete vector from a complete one.
			embedBody, readErr := readLimited(resp.Body, maxResponseBytes)
			embedCT := resp.Header.Get("Content-Type")
			resp.Body.Close()
			cancel()
			if r.Context().Err() != nil {
				return
			}
			if readErr != nil {
				log.Printf("embed: body read failed from %s/%s: %v", step.Provider, step.Model, readErr)
				s.mark(r, step.Provider, step.Model, 0, readErr)
				s.poolRecordResult(r, keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, readErr)
				failure := unusableBody(readErr)
				lastStatus, lastBody, lastCT, lastErr = failure.status, failure.body, failure.ct, failure.err
				continue
			}
			if verr := validateEmbeddings(embedBody, embeddingRequestContract(body, step.Params)); verr != nil {
				log.Printf("embed: %s/%s returned an unusable 2xx body: %v", step.Provider, step.Model, verr)
				s.mark(r, step.Provider, step.Model, 0, verr)
				s.poolRecordResult(r, keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, verr)
				failure := unusableBody(verr)
				lastStatus, lastBody, lastCT, lastErr = failure.status, failure.body, failure.ct, failure.err
				continue
			}
			w.Header().Set("Content-Type", embedCT)
			w.WriteHeader(resp.StatusCode)
			if _, err := w.Write(embedBody); err != nil || r.Context().Err() != nil {
				return
			}
			s.markSuccess(r, step.Provider, step.Model)
			s.poolRecord(r, keyEnv, step, chain, req, resp.StatusCode, 0, 0, "")
			return
		}
		lastStatus = resp.StatusCode
		lastBody, _ = readErrorBody(resp, false)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		cancel()
		if r.Context().Err() != nil {
			return
		}
		s.mark(r, step.Provider, step.Model, resp.StatusCode, nil)
		log.Printf("embed: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(r, step, chain, req, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
		permit.Release()
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
func (s *Server) tryEmbed(ctx context.Context, r *http.Request, step router.Step, body []byte) (*http.Response, string, error) {
	return s.tryUpstream(ctx, r, step, body, adaptors.New, true)
}

// tryStep attempts one chain step with separate credential failover and
// transient retry bounds, both subject to the request's shared retry budget.
// Auth failover is immediate; rate limits and transient retries use backoff.
// Streaming is safe: a retry only happens when no 2xx response has been
// received yet — once the upstream starts streaming we are committed to that
// stream.
//
// Returns (resp, keyEnv, nil) when an upstream response was obtained (2xx or
// not — body left open for the caller to relay), or (nil, "", err) when every
// attempt failed at the network level.
func (s *Server) tryStep(r *http.Request, step router.Step, body []byte) (*http.Response, string, error) {
	return s.tryStepCtx(r.Context(), r, step, body)
}

// tryStepCtx is tryStep with a caller-supplied context, so the walk can bound
// one attempt (per-step budget and remaining whole-request budget) without
// touching the client's own request context.
func (s *Server) tryStepCtx(ctx context.Context, r *http.Request, step router.Step, body []byte) (*http.Response, string, error) {
	return s.tryStepAdaptor(ctx, r, step, body, adaptors.New)
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
	return s.tryUpstream(ctx, r, step, body, mkAdaptor, false)
}

// retryable reports whether a failure may be retried, using the shared
// outcome classifier: credential failover (auth), rate limits, upstream
// transient failures and local network errors are retryable; caller errors,
// model incompatibility and cancellation are not.
func retryable(status int, err error) bool {
	return outcome.Retryable(outcome.Classify(status, err))
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

// relaySuccess delivers one successful upstream response to the client and
// reports how far it got. A non-streaming body is read in full and validated
// before anything is written, so an unusable 2xx leaves the walk free to try
// the next step. A streaming response is committed as soon as its status line
// and headers are sent and never fails over afterwards.
func (s *Server) relaySuccess(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response, keyEnv string) relayResult {
	defer resp.Body.Close()
	if !req.Stream {
		// Non-streaming: read the whole body and validate it before anything is
		// written. A 2xx that cannot be a chat completion is a failed attempt,
		// so the walk can still use the next step instead of handing the caller
		// a broken success.
		body, err := readLimited(resp.Body, maxResponseBytes)
		ct := resp.Header.Get("Content-Type")
		if r.Context().Err() != nil {
			return relayResult{outcome: relayUncommitted, clientGone: true, err: r.Context().Err()}
		}
		if err != nil {
			s.mark(r, step.Provider, step.Model, 0, err)
			s.poolRecordResult(r, keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, err)
			log.Printf("chat: %s/%s body read failed: %v", step.Provider, step.Model, err)
			return unusableBody(err)
		}
		if verr := validateChatCompletion(body, req.N); verr != nil {
			s.mark(r, step.Provider, step.Model, 0, verr)
			s.poolRecordResult(r, keyEnv, step, chain, req, 502, 0, 0, keypool.ClassTransient, verr)
			log.Printf("chat: %s/%s returned an unusable 2xx body: %v", step.Provider, step.Model, verr)
			return unusableBody(verr)
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(body); err != nil || r.Context().Err() != nil {
			return relayResult{outcome: relayCommitted, clientGone: true}
		}
		s.markSuccess(r, step.Provider, step.Model)
		in, out := telemetry.ParseUsage(body)
		s.poolRecord(r, keyEnv, step, chain, req, resp.StatusCode, in, out, "")
		return relayResult{outcome: relayCommitted}
	}
	s.relayStream(w, r, step, chain, req, resp, keyEnv)
	return relayResult{outcome: relayCommitted}
}

func (s *Server) relayStream(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response, keyEnv string) {
	streamDeadline, durationC, stopDuration := streamDurationTimer()
	defer stopDuration()
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
	var pending bytes.Buffer
	progress := chatSSEProgress{expected: req.N}
	var idleC <-chan time.Time
	idleTimer := time.NewTimer(time.Hour)
	defer idleTimer.Stop()
	if streamIdle > 0 {
		idleTimer.Reset(streamIdle)
		idleC = idleTimer.C
	}

	// fail reports an upstream stream that did not end with a valid terminator.
	// The stream is already committed to the client, so this never fails over;
	// the point is that the step is charged with a failure instead of being
	// reset to healthy by an EOF that only means the socket closed.
	fail := func(err error) {
		if r.Context().Err() != nil {
			return // client went away: not an upstream failure
		}
		log.Printf("chat: stream from %q did not complete: %v", step.Provider, err)
		payload := `data: {"error":{"message":"upstream stream did not complete"}}`
		var budgetErr *stepTimeoutError
		if errors.As(err, &budgetErr) {
			payload = `data: {"error":{"message":"upstream stream exceeded maximum duration","code":"upstream_stream_timeout"}}`
		}
		io.WriteString(w, payload+"\n\n")
		flusher.Flush()
		s.mark(r, step.Provider, step.Model, 0, err)
		s.poolRecordResult(r, keyEnv, step, chain, req, 502, in, out, keypool.ClassTransient, err)
	}

	for {
		if walkExpired(streamDeadline) {
			fail(streamDurationError())
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-durationC:
			fail(streamDurationError())
			return
		case <-idleC:
			fail(fmt.Errorf("no upstream data for %s", streamIdle))
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
				if maxResponseBytes > 0 && int64(pending.Len()+len(line)) > maxResponseBytes {
					fail(errors.New("upstream SSE frame exceeds body limit"))
					return
				}
				pending.Write(line)
				payload, complete, frameErr := data.line(line)
				if frameErr != nil {
					fail(frameErr)
					return
				}
				if complete {
					kind, verr := progress.observe(payload)
					if verr != nil {
						// Never relay a frame the gateway could not parse.
						fail(verr)
						return
					}
					if i, o := telemetry.ParseUsage([]byte(payload)); i != 0 || o != 0 {
						in, out = i, o
					}
					if kind == chatSSEDone {
						if verr := progress.complete(requireStreamTerminator); verr != nil {
							fail(verr)
							return
						}
						if _, werr := w.Write(pending.Bytes()); werr != nil {
							return
						}
						flusher.Flush()
						s.markSuccess(r, step.Provider, step.Model)
						s.poolRecord(r, keyEnv, step, chain, req, resp.StatusCode, in, out, "")
						return
					}
					if _, werr := w.Write(pending.Bytes()); werr != nil {
						return
					}
					pending.Reset()
					flusher.Flush()
				}
				if len(bytes.TrimSpace(line)) == 0 && pending.Len() > 0 {
					if _, werr := w.Write(pending.Bytes()); werr != nil {
						return
					}
					pending.Reset()
					flusher.Flush()
				}
				resetKeepAlive(timer)
				if streamIdle > 0 {
					if !idleTimer.Stop() {
						select {
						case <-idleTimer.C:
						default:
						}
					}
					idleTimer.Reset(streamIdle)
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if payload, ok := data.flush(); ok {
						_, verr := progress.observe(payload)
						if verr != nil {
							fail(verr)
							return
						}
						if i, o := telemetry.ParseUsage([]byte(payload)); i != 0 || o != 0 {
							in, out = i, o
						}
					}
					if verr := progress.complete(requireStreamTerminator); verr != nil {
						fail(verr)
						return
					}
					if pending.Len() > 0 {
						if _, werr := w.Write(pending.Bytes()); werr != nil {
							return
						}
						flusher.Flush()
					}
					s.markSuccess(r, step.Provider, step.Model)
					s.poolRecord(r, keyEnv, step, chain, req, resp.StatusCode, in, out, "")
					return
				}
				log.Printf("chat: stream error from %q: %v", step.Provider, err)
				fail(err)
				return
			}
		}
	}
}

// poolRecord records usage against the serving key and marks its pool state:
// failures for class != "", success otherwise.
func (s *Server) poolRecord(r *http.Request, keyEnv string, step router.Step, chain string, req chatRequest, status int, in, out int64, class keypool.Class) {
	s.poolRecordResult(r, keyEnv, step, chain, req, status, in, out, class, nil)
}

// poolRecordResult commits one terminal request event, including an error and
// any partial usage, rather than recording separate token and error rows.
func (s *Server) poolRecordResult(r *http.Request, keyEnv string, step router.Step, chain string, req chatRequest, status int, in, out int64, class keypool.Class, err error) {
	lease := attemptLease(r, keyEnv)
	if r.Context().Err() != nil || isClientGone(err) {
		lease.Release()
		return
	}
	if lease != nil {
		if class != "" {
			lease.Failure(class)
		} else {
			lease.Success()
		}
	}
	s.recordAttempt(r, step, chain, req, status, err, keyEnv, in, out)
}

// dataPayload returns the JSON payload of an SSE data: line, or nil.
func dataPayload(line []byte) []byte {
	s := strings.TrimSpace(string(line))
	if !strings.HasPrefix(s, "data:") {
		return nil
	}
	return []byte(strings.TrimSpace(s[len("data:"):]))
}

func (s *Server) record(r *http.Request, step router.Step, chain string, req chatRequest, status int, err error, key string) {
	s.recordAttempt(r, step, chain, req, status, err, key, 0, 0)
}
