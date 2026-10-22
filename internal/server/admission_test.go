package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/selector"
)

// Keep the historical failure count while making its quarantine expired. This
// verifies recovery/cancellation without relying on reusing quarantined keys.
func seedRecoveredKey(t *testing.T, s *Server) func() {
	t.Helper()
	var tick atomic.Int64
	tick.Store(time.Now().UnixNano())
	advance := func() { tick.Add(int64(time.Minute)) }
	p, err := keypool.NewWithClock("a", []string{"A"}, "round_robin", config.QuarantineCfg{Transient: config.Duration{Duration: time.Second}}, func() time.Time { return time.Unix(0, tick.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	p.Mark("A", keypool.ClassTransient)
	advance()
	s.pools["a"] = p
	return advance
}

type admissionRoute struct {
	name, route       string
	stream, aggregate bool
}

var admissionRoutes = []admissionRoute{
	{"chat", "/v1/chat/completions", false, false},
	{"responses", "/v1/responses", false, false},
	{"auto", "/v1/auto", false, false},
	{"embeddings", "/v1/embeddings", false, false},
	{"chat_stream", "/v1/chat/completions", true, false},
	{"responses_stream", "/v1/responses", true, false},
	{"auto_stream", "/v1/auto", true, false},
	{"chat_aggregate", "/v1/chat/completions", false, true},
	{"responses_aggregate", "/v1/responses", false, true},
	{"auto_aggregate", "/v1/auto", false, true},
}

func (tc admissionRoute) request(ctx context.Context) *http.Request {
	stream := "false"
	if tc.stream {
		stream = "true"
	}
	r := httptest.NewRequest("POST", tc.route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}],"stream":`+stream+`}`)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func (tc admissionRoute) respond(w http.ResponseWriter) {
	if tc.stream || tc.aggregate {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	} else if tc.route == "/v1/embeddings" {
		io.WriteString(w, `{"data":[{"embedding":[1,2]}]}`)
	} else {
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}
}

func seedHalfOpen(s *Server) {
	var tick atomic.Int64
	tick.Store(time.Now().UnixNano())
	s.hlth = health.NewWithClock(func() time.Time { return time.Unix(0, tick.Load()) })
	for i := 0; i < health.FailureThreshold; i++ {
		s.hlth.Mark("a", "m", 500, nil)
	}
	tick.Add(int64(health.CooldownBase))
}

func admissionServer(t *testing.T, tc admissionRoute, primary, backup string) *Server {
	t.Helper()
	s, _ := testServer(t, []config.Provider{
		{Name: "a", Type: "openai", BaseURL: primary, KeyEnv: "A", StreamOnly: tc.aggregate},
		{Name: "b", Type: "openai", BaseURL: backup, KeyEnv: "B", StreamOnly: tc.aggregate},
	}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "backup"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	seedHalfOpen(s)
	return s
}

func requireProbeReleased(t *testing.T, s *Server) {
	t.Helper()
	p, ok := s.hlth.TryAcquire("a", "m")
	if !ok {
		t.Fatal("abandoned probe leaked")
	}
	p.Release()
	for _, st := range s.hlth.Snapshot() {
		if st.Provider == "a" && st.Failures != health.FailureThreshold {
			t.Fatalf("neutral result changed health: %+v", st)
		}
	}
}

func TestHalfOpenConcurrentWalksUseBackupAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var first sync.Once
			var primaryHits, backupHits atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryHits.Add(1)
				first.Do(func() { close(started) })
				<-release
				tc.respond(w)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupHits.Add(1); tc.respond(w) }))
			defer backup.Close()
			s := admissionServer(t, tc, primary.URL, backup.URL)
			done := make(chan int, 64)
			call := func() {
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				done <- w.Code
			}
			go call()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				close(release)
				t.Fatal("probe did not start")
			}
			for i := 0; i < 63; i++ {
				go call()
			}
			for i := 0; i < 63; i++ {
				select {
				case code := <-done:
					if code != 200 {
						t.Errorf("backup status=%d", code)
					}
				case <-time.After(3 * time.Second):
					close(release)
					t.Fatal("backup blocked on probe")
				}
			}
			if primaryHits.Load() != 1 || backupHits.Load() != 63 {
				t.Errorf("half-open hits=%d/%d", primaryHits.Load(), backupHits.Load())
			}
			close(release)
			if code := <-done; code != 200 {
				t.Fatalf("probe status=%d", code)
			}
			call()
			if code := <-done; code != 200 || primaryHits.Load() != 2 {
				t.Fatalf("traffic did not recover: status=%d primary=%d", code, primaryHits.Load())
			}
		})
	}
}

func TestAllKeysQuarantinedSkipWithoutHealthPenaltyAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			var primaryHits, backupHits atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { primaryHits.Add(1); tc.respond(w) }))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupHits.Add(1); tc.respond(w) }))
			defer backup.Close()
			s := admissionServer(t, tc, primary.URL, backup.URL)
			s.pools["a"].Mark("A", keypool.ClassAuth)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, tc.request(context.Background()))
			if w.Code != 200 || primaryHits.Load() != 0 || backupHits.Load() != 1 {
				t.Fatalf("status=%d hits=%d/%d", w.Code, primaryHits.Load(), backupHits.Load())
			}
			requireProbeReleased(t, s)
			var failures int
			if err := s.tm.DB().QueryRow("SELECT count(*) FROM requests WHERE provider='a'").Scan(&failures); err != nil || failures != 0 {
				t.Fatalf("unattempted provider events=%d err=%v", failures, err)
			}
		})
	}
}

func TestHalfOpenCancellationReleasesAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer primary.Close()
			s := admissionServer(t, tc, primary.URL, "http://unused")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); s.Handler().ServeHTTP(httptest.NewRecorder(), tc.request(ctx)) }()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("cancel did not finish")
			}
			requireProbeReleased(t, s)
			if s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatal("cancellation quarantined key")
			}
		})
	}
}

func TestHalfOpenFailedProbeCoolsDownAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.WriteHeader(503)
				io.WriteString(w, `{"error":"unavailable"}`)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tc.respond(w) }))
			defer backup.Close()
			s := admissionServer(t, tc, primary.URL, backup.URL)
			for i := 0; i < 2; i++ {
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				if w.Code != 200 {
					t.Fatalf("fallback status=%d", w.Code)
				}
			}
			if hits.Load() != int32(retries+1) || !s.hlth.IsDown("a", "m") {
				t.Fatalf("failed probe did not cool down: hits=%d", hits.Load())
			}
			for _, st := range s.hlth.Snapshot() {
				if st.Provider == "a" && st.Failures != health.FailureThreshold+1 {
					t.Fatalf("failure counted more than once: %+v", st)
				}
			}
			if n := s.pools["a"].Keys()[0].Failures; n != 0 {
				t.Fatalf("key failures=%d", n)
			}
		})
	}
}

func TestHalfOpenDownstreamFailureReleasesAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tc.respond(w) }))
			defer primary.Close()
			s := admissionServer(t, tc, primary.URL, "http://unused")
			w := &runtimeFailWriter{ResponseRecorder: httptest.NewRecorder()}
			s.Handler().ServeHTTP(w, tc.request(context.Background()))
			requireProbeReleased(t, s)
			if s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatal("downstream failure quarantined key")
			}
		})
	}
}

func TestAllKeysQuarantinedWithoutBackupIsUnavailable(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
			defer up.Close()
			s := admissionServer(t, tc, up.URL, up.URL)
			s.pools["a"].Mark("A", keypool.ClassAuth)
			s.pools["b"].Mark("B", keypool.ClassAuth)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, tc.request(context.Background()))
			if w.Code != 503 || hits.Load() != 0 {
				t.Fatalf("status=%d upstream calls=%d", w.Code, hits.Load())
			}
			requireProbeReleased(t, s)
			runtimeTelemetry(t, s, 0, 0, false)
		})
	}
}

func TestDeciderWithAllKeysQuarantinedUsesStaticFallback(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer up.Close()
	s := runtimeServer(t, up.URL)
	s.decideProvider, s.decideModel = "a", "m"
	s.pools["a"].Mark("A", keypool.ClassAuth)
	cands := []selector.Candidate{{Chain: "a"}, {Chain: "b"}}
	ranked, err := s.callDecider(nil, context.Background(), cands)
	if !errors.Is(err, errNoAvailableKeys) || ranked != nil || hits.Load() != 0 {
		t.Fatalf("ranking=%v err=%v calls=%d", ranked, err, hits.Load())
	}
	ordered, _, source := selector.Decide(cands, s.deciderFor(cands, nil, 0), nil, time.Minute, nil, nil)
	if len(ordered) != 2 || source != "decider_unavailable" {
		t.Fatalf("fallback source=%s candidates=%d", source, len(ordered))
	}
	if len(s.hlth.Snapshot()) != 0 {
		t.Fatal("unavailable decider key generated health failure")
	}
	runtimeTelemetry(t, s, 0, 0, false)
}

func TestLateSuccessPreservesNewKeyQuarantineAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var hits atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hits.Add(1) == 1 {
					close(started)
					<-release
					tc.respond(w)
					return
				}
				w.WriteHeader(429) // Exercise both key and model isolation with a rate limit.
				io.WriteString(w, `{"error":"rate limited"}`)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tc.respond(w) }))
			defer backup.Close()
			s := admissionServer(t, tc, primary.URL, backup.URL)
			s.hlth = health.New()
			for i := 0; i < health.FailureThreshold-1; i++ {
				s.hlth.Mark("a", "m", 500, nil)
			}
			done := make(chan int, 1)
			go func() {
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				done <- w.Code
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				close(release)
				t.Fatal("old attempt did not start")
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, tc.request(context.Background()))
			if w.Code != 200 {
				close(release)
				t.Fatalf("new failure did not use backup: %d", w.Code)
			}
			close(release)
			if code := <-done; code != 200 {
				t.Fatalf("old response status=%d", code)
			}
			if s.pools["a"].Pick() != "" || s.pools["a"].Keys()[0].Failures != 1 {
				t.Fatal("old success cleared a newer key quarantine")
			}
			if !s.hlth.IsDown("a", "m") {
				t.Fatal("old success cleared a newer model cooldown")
			}
		})
	}
}
