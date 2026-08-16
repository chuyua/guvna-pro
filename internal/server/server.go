// Package server wires the HTTP surface: /healthz, /v1/models, and the
// /v1/chat/completions relay with chain-based fallback and SSE passthrough.
package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alisa/bruvroute/internal/adaptors"
	"github.com/alisa/bruvroute/internal/auth"
	"github.com/alisa/bruvroute/internal/config"
	"github.com/alisa/bruvroute/internal/health"
	"github.com/alisa/bruvroute/internal/logring"
	"github.com/alisa/bruvroute/internal/router"
	"github.com/alisa/bruvroute/internal/telemetry"
)

// KeepAliveInterval is how long the relay waits without upstream data before
// writing an SSE comment line to hold the connection open.
const KeepAliveInterval = 15 * time.Second

// retries is how many times a step is retried (beyond the first attempt) on
// transient failures (429/5xx/network) before the chain moves to the next step.
const retries = 2

type Server struct {
	cfg       *config.Config
	rtr       *router.Router
	auth      *auth.Authenticator
	tm        *telemetry.Telemetry
	hlth      *health.Tracker
	logs      *logring.Ring
	started   time.Time
	env       func(string) string
	retryWait func(attempt int) time.Duration
}

func New(cfg *config.Config, rtr *router.Router, a *auth.Authenticator, tm *telemetry.Telemetry, hlth *health.Tracker, logs *logring.Ring) *Server {
	return &Server{cfg: cfg, rtr: rtr, auth: a, tm: tm, hlth: hlth, logs: logs, started: time.Now(), env: os.Getenv, retryWait: defaultRetryWait}
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
	resp := struct {
		Uptime    string              `json:"uptime"`
		Chains    []string            `json:"chains"`
		Default   string              `json:"default_chain"`
		Steps     []health.StepStatus `json:"steps"`
		StepsDown int                 `json:"steps_down"`
		Usage     telemetry.Stats     `json:"usage"`
	}{Uptime: time.Since(s.started).Round(time.Second).String()}
	resp.Chains = s.rtr.ChainNames()
	resp.Default = s.cfg.DefaultChain
	resp.Steps = steps
	resp.StepsDown = stepsDown
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
		}{ID: n, Object: "model", Created: time.Now().Unix(), OwnedBy: "bruvroute"})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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
	chain, steps, ok := s.rtr.Resolve(req.Model)
	if !ok {
		http.Error(w, `{"error":{"message":"no default chain configured"}}`, http.StatusInternalServerError)
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
		resp, err := s.tryStep(r, step, body)
		if err != nil {
			log.Printf("chat: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.record(step, chain, req, 0, err)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			s.hlth.MarkSuccess(step.Provider, step.Model)
			s.relaySuccess(w, r, step, chain, req, resp)
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
		s.record(step, chain, req, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)))
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

// tryStep attempts one chain step, retrying transient failures (429, 5xx,
// network errors) up to retries times with backoff. Streaming is safe: a
// retry only happens when no 2xx response has been received yet — once the
// upstream starts streaming we are committed to that stream.
//
// Returns (resp, nil) when an upstream response was obtained (2xx or not —
// body left open for the caller to relay), or (nil, err) when every attempt
// failed at the network level.
func (s *Server) tryStep(r *http.Request, step router.Step, body []byte) (*http.Response, error) {
	p, ok := s.provider(step.Provider)
	if !ok {
		return nil, fmt.Errorf("provider %q not in config", step.Provider)
	}
	key := s.env(p.KeyEnv)
	if key == "" {
		return nil, fmt.Errorf("provider %q: key env %q not set", p.Name, p.KeyEnv)
	}
	a, err := adaptors.New(p, key)
	if err != nil {
		return nil, err
	}
	payload := withModel(body, step.Model)
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && s.retryWait != nil {
			time.Sleep(s.retryWait(attempt))
		}
		resp, err := a.Chat(r.Context(), payload)
		if err != nil {
			log.Printf("chat: %s/%s attempt %d: %v", step.Provider, step.Model, attempt+1, err)
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		log.Printf("chat: %s/%s attempt %d: status %d", step.Provider, step.Model, attempt+1, resp.StatusCode)
		if !retryable(resp.StatusCode, nil) {
			return resp, nil // 4xx: config/request error, never retried
		}
		if attempt == retries {
			return resp, nil // transient failures exhausted: last response
		}
		resp.Body.Close()
	}
	return nil, lastErr
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
	if m["model"] != nil {
		enc, err := json.Marshal(model)
		if err == nil {
			m["model"] = enc
		}
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

func (s *Server) relaySuccess(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response) {
	defer resp.Body.Close()
	if !req.Stream {
		// Non-streaming: relay body, best-effort token count.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			if r.Context().Err() != nil {
				// Client disconnected — not an upstream failure.
				return
			}
			s.record(step, chain, req, 502, err)
			http.Error(w, `{"error":{"message":"upstream read failed"}}`, http.StatusBadGateway)
			return
		}
		in, out := telemetry.ParseUsage(body)
		s.recordTokens(step, chain, req, resp.StatusCode, in, out)
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
		return
	}
	s.relayStream(w, r, step, chain, req, resp)
}

func (s *Server) relayStream(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req chatRequest, resp *http.Response) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":{"message":"streaming unsupported"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)
	flusher.Flush()

	br := bufio.NewReader(resp.Body)
	lastWrite := time.Now()
	var in, out int64
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		if time.Since(lastWrite) > KeepAliveInterval {
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
			lastWrite = time.Now()
			continue
		}
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := w.Write(line); werr != nil {
				return
			}
			flusher.Flush()
			lastWrite = time.Now()
			// Streaming usage arrives in the final data: chunk; capture it
			// best-effort for telemetry.
			if payload := dataPayload(line); payload != nil {
				if i, o := telemetry.ParseUsage(payload); i != 0 || o != 0 {
					in, out = i, o
				}
			}
		}
		if err != nil {
			if r.Context().Err() != nil {
				return // client disconnected mid-stream
			}
			if errors.Is(err, io.EOF) {
				s.hlth.MarkSuccess(step.Provider, step.Model)
				s.recordTokens(step, chain, req, resp.StatusCode, in, out)
				return
			}
			// Mid-stream failure: propagate as an error chunk, then close.
			// Mark the provider so later requests skip it during cool-off.
			log.Printf("chat: stream error from %q: %v", step.Provider, err)
			io.WriteString(w, `data: {"error":{"message":"upstream stream error"}}`+"\n\n")
			flusher.Flush()
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.recordTokens(step, chain, req, 502, in, out)
			return
		}
	}
}

// dataPayload returns the JSON payload of an SSE data: line, or nil.
func dataPayload(line []byte) []byte {
	s := strings.TrimSpace(string(line))
	if !strings.HasPrefix(s, "data:") {
		return nil
	}
	return []byte(strings.TrimSpace(s[len("data:"):]))
}

func (s *Server) record(step router.Step, chain string, req chatRequest, status int, err error) {
	e := telemetry.Event{
		Provider: step.Provider,
		Chain:    chain,
		Model:    step.Model,
		Stream:   req.Stream,
		Status:   status,
		Ts:       time.Now(),
	}
	if err != nil {
		e.Err = err.Error()
	}
	s.tm.Record(e)
}

func (s *Server) recordTokens(step router.Step, chain string, req chatRequest, status int, in, out int64) {
	e := telemetry.Event{
		Provider:  step.Provider,
		Chain:     chain,
		Model:     step.Model,
		Stream:    req.Stream,
		Status:    status,
		TokensIn:  in,
		TokensOut: out,
		Ts:        time.Now(),
	}
	s.tm.Record(e)
}
