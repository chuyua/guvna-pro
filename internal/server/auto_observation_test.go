package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

func readAutoObservation(t *testing.T, s *Server) telemetry.AutoRequestEvent {
	t.Helper()
	if err := s.tm.Flush(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.tm.DB().QueryRow(`SELECT count(*) FROM auto_requests`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want exactly one Auto request summary, got %d", count)
	}
	var e telemetry.AutoRequestEvent
	var started, finished string
	err := s.tm.DB().QueryRow(`SELECT request_id, started_at, finished_at, mode,
		policy, decider_source, stream, attempts, candidates, success, canceled,
		status, failure_class, latency_ms FROM auto_requests`).Scan(&e.RequestID,
		&started, &finished, &e.Mode, &e.Policy, &e.DeciderSource, &e.Stream,
		&e.Attempts, &e.Candidates, &e.Success, &e.Canceled, &e.Status,
		&e.FailureClass, &e.LatencyMS)
	if err != nil {
		t.Fatal(err)
	}
	if e.StartedAt, err = time.Parse(time.RFC3339Nano, started); err != nil {
		t.Fatal(err)
	}
	if e.FinishedAt, err = time.Parse(time.RFC3339Nano, finished); err != nil {
		t.Fatal(err)
	}
	if e.RequestID == "" || e.FinishedAt.Before(e.StartedAt) || e.LatencyMS < 0 {
		t.Fatalf("invalid summary identity/timing: %+v", e)
	}
	return e
}

func observationRequest(ctx context.Context, path string, stream bool) *http.Request {
	body := `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`
	if stream {
		body = `{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
	r := httptest.NewRequest("POST", path, strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func observationServer(t *testing.T, upstream string) *Server {
	t.Helper()
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: upstream, KeyEnv: "A"}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
	return s
}

func TestAutoObservationModesPreserveActualOrder(t *testing.T) {
	up := completionUpstream(t, 200, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, false)
	defer up.Close()
	for _, tc := range []struct {
		name, path, mode, approved, wantStep, wantPolicy string
	}{
		{"default", "/v1/auto", "", "", "a/m", "static"},
		{"model_alias", "/v1/chat/completions", "", "", "a/m", "static"},
		{"shadow", "/v1/auto", "shadow", "", "a/m", "static"},
		{"telemetry_approved", "/v1/auto", "telemetry", "true", "b/m", "telemetry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, env, _ := shadowTestServer(t, up.URL)
			env["GUVNA_AUTO_ORDER_MODE"], env["GUVNA_AUTO_TELEMETRY_APPROVED"] = tc.mode, tc.approved
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, observationRequest(context.Background(), tc.path, false))
			if w.Code != 200 || w.Header().Get("X-Guvna-Step") != tc.wantStep {
				t.Fatalf("route changed: status=%d step=%s", w.Code, w.Header().Get("X-Guvna-Step"))
			}
			e := readAutoObservation(t, s)
			if e.Policy != tc.wantPolicy || e.DeciderSource != "decider_unavailable" || !e.Success || e.Canceled || e.Attempts != 1 || e.Candidates != 1 || e.Status != 200 {
				t.Fatalf("summary=%+v", e)
			}
			wantMode := tc.mode
			if wantMode == "" {
				wantMode = "decider"
			}
			if e.Mode != wantMode {
				t.Fatalf("mode=%q want %q", e.Mode, wantMode)
			}
		})
	}
}

func TestAutoObservationRetriesAndCandidateFallback(t *testing.T) {
	for _, sameCandidate := range []bool{true, false} {
		name := "candidate_fallback"
		if sameCandidate {
			name = "credential_failover"
		}
		t.Run(name, func(t *testing.T) {
			good := completionUpstream(t, 200, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, false)
			defer good.Close()
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if sameCandidate && r.Header.Get("Authorization") == "Bearer good" {
					io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
					return
				}
				if sameCandidate {
					w.WriteHeader(401)
				} else {
					w.WriteHeader(400)
				}
			}))
			defer bad.Close()
			providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnvs: []string{"A1", "A2"}}}
			steps := []config.Step{{Provider: "a", Model: "m"}}
			if !sameCandidate {
				providers[0].KeyEnvs = []string{"A1"}
				providers = append(providers, config.Provider{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"})
				steps = append(steps, config.Step{Provider: "b", Model: "m"})
			}
			s, _ := testServer(t, providers, []config.Chain{{Name: "main", Steps: steps}}, map[string]string{"A1": "bad", "A2": "good", "B": "dummy"})
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", false))
			e := readAutoObservation(t, s)
			wantCandidates := 2
			if sameCandidate {
				wantCandidates = 1
			}
			if w.Code != 200 || !e.Success || e.Attempts != 2 || e.Candidates != wantCandidates {
				t.Fatalf("status=%d summary=%+v", w.Code, e)
			}
			stats, err := s.tm.AutoRequestStats(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Second))
			if err != nil || len(stats.Policies) != 1 {
				t.Fatalf("stats=%+v err=%v", stats, err)
			}
			wantFallback := 1
			if sameCandidate {
				wantFallback = 0
			}
			if p := stats.Policies[0]; p.Requests != 1 || p.Attempts != 2 || p.FirstAttemptSuccesses != 0 || p.FallbackRequests != wantFallback {
				t.Fatalf("retries/fallback double counted: %+v", p)
			}
		})
	}
}

func TestAutoObservationRequiresCompleteResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body      string
		stream, success bool
	}{
		{"valid_json", `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, false, true},
		{"invalid_json_2xx", `{}`, false, false},
		{"complete_stream", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", true, true},
		{"premature_stream_eof", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := completionUpstream(t, 200, tc.body, false)
			defer up.Close()
			s := observationServer(t, up.URL)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", tc.stream))
			e := readAutoObservation(t, s)
			if e.Success != tc.success || e.Canceled || e.Attempts != 1 || e.Candidates != 1 {
				t.Fatalf("status=%d summary=%+v", w.Code, e)
			}
			if !tc.success && (e.Status != 502 || e.FailureClass != "upstream_transient") {
				t.Fatalf("unusable 2xx treated as success: %+v", e)
			}
		})
	}
}

type observationBrokenWriter struct {
	header       http.Header
	panicOnWrite bool
}

func (w *observationBrokenWriter) Header() http.Header { return w.header }
func (w *observationBrokenWriter) WriteHeader(int)     {}
func (w *observationBrokenWriter) Write([]byte) (int, error) {
	if w.panicOnWrite {
		panic("downstream writer panic")
	}
	return 0, errors.New("downstream closed")
}

type observationBrokenFlusher struct{ *observationBrokenWriter }

func (*observationBrokenFlusher) Flush() {}

func TestAutoObservationDownstreamExitsAreNeutral(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "buffered", true: "stream"}[stream], func(t *testing.T) {
			up := completionUpstream(t, 200, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, stream)
			defer up.Close()
			s := observationServer(t, up.URL)
			w := &observationBrokenFlusher{&observationBrokenWriter{header: make(http.Header)}}
			s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", stream))
			e := readAutoObservation(t, s)
			if e.Success || !e.Canceled || e.FailureClass != "canceled" || e.Attempts != 1 {
				t.Fatalf("downstream exit misattributed: %+v", e)
			}
			for _, st := range s.hlth.Snapshot() {
				if st.Failures != 0 {
					t.Fatalf("downstream exit penalized health: %+v", st)
				}
			}
		})
	}
}

func TestAutoObservationIncompleteFallbackDoesNotReuseFailure(t *testing.T) {
	bad := completionUpstream(t, 400, `{"error":"request does not fit"}`, false)
	defer bad.Close()
	good := completionUpstream(t, 200, "", true)
	defer good.Close()
	s, _ := testServer(t, []config.Provider{
		{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A"},
		{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"},
	}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "m"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	w := &observationBrokenFlusher{&observationBrokenWriter{header: make(http.Header)}}
	s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", true))
	e := readAutoObservation(t, s)
	if !e.Canceled || e.Success || e.Status != 0 || e.FailureClass != "canceled" || e.Attempts != 2 || e.Candidates != 2 {
		t.Fatalf("incomplete last attempt reused prior failure: %+v", e)
	}
}

func TestAutoObservationLocalWriterFailures(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		stream, panicOnWrite bool
	}{
		{"stream_without_flusher", true, false}, {"writer_panic", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := completionUpstream(t, 200, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, tc.stream)
			defer up.Close()
			s := observationServer(t, up.URL)
			w := &observationBrokenWriter{header: make(http.Header), panicOnWrite: tc.panicOnWrite}
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", tc.stream))
			}()
			if panicked != tc.panicOnWrite {
				t.Fatalf("panic propagation changed: got %v", panicked)
			}
			e := readAutoObservation(t, s)
			if e.Success || e.Canceled || e.Status != 500 || e.FailureClass != "gateway_error" {
				t.Fatalf("local failure misattributed: %+v", e)
			}
		})
	}
}

func TestAutoObservationCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer up.Close()
	defer close(release)
	s := observationServer(t, up.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(httptest.NewRecorder(), observationRequest(ctx, "/v1/auto", false))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Auto did not stop after cancellation")
	}
	e := readAutoObservation(t, s)
	if e.Success || !e.Canceled || e.Attempts != 1 || e.FailureClass != "canceled" {
		t.Fatalf("canceled summary=%+v", e)
	}
}

func TestAutoObservationEarlyErrorsAndOriginalStart(t *testing.T) {
	up := completionUpstream(t, 200, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`, false)
	defer up.Close()
	for _, tc := range []struct {
		name, body, class string
		status, attempts  int
	}{
		{"invalid_json", `{`, "caller_error", 400, 0},
		{"invalid_context_range", `{"min_context":1000,"max_context":512}`, "caller_error", 400, 0},
		{"no_eligible_model", `{"min_context":999999999}`, "no_candidates", 404, 0},
		{"success", `{"messages":[{"role":"user","content":"hi"}]}`, "", 200, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := observationServer(t, up.URL)
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/v1/auto", nil)
			start := time.Now().Add(-200 * time.Millisecond)
			s.handleAutoBody(w, r, []byte(tc.body), start)
			e := readAutoObservation(t, s)
			if e.Status != tc.status || e.Attempts != tc.attempts || e.FailureClass != tc.class || e.LatencyMS < 200 || !e.StartedAt.Equal(start) {
				t.Fatalf("summary=%+v original start=%s", e, start)
			}
			if tc.attempts == 0 && e.Policy != "none" {
				t.Fatalf("unselected policy=%q", e.Policy)
			}
		})
	}
}

func TestAutoObservationSourceMapping(t *testing.T) {
	for _, source := range []string{"decider", "decider_cached", "decider_stale", "decider_shared", "single_candidate", "decider_unavailable"} {
		o := &autoObservation{}
		o.selected(source, source)
		want := "decider"
		if source == "single_candidate" || source == "decider_unavailable" {
			want = "static"
		}
		if o.event.Policy != want || o.event.DeciderSource != source {
			t.Fatalf("source=%s summary=%+v", source, o.event)
		}
		o.selected(source, "telemetry")
		if o.event.Policy != "telemetry" || o.event.DeciderSource != source {
			t.Fatalf("telemetry overwrote source: %+v", o.event)
		}
	}
}

func TestAutoObservationNoUsableCredentialsIsGatewayFailure(t *testing.T) {
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: "http://unused", KeyEnv: "A"}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, observationRequest(context.Background(), "/v1/auto", false))
	e := readAutoObservation(t, s)
	if w.Code != http.StatusServiceUnavailable || e.Status != http.StatusServiceUnavailable || e.Attempts != 0 || e.Candidates != 0 || e.FailureClass != "gateway_error" || e.Canceled || e.Success {
		t.Fatalf("unavailable credentials mislabeled as no candidate: status=%d summary=%+v", w.Code, e)
	}
}
