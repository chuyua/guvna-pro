package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
)

func TestFourRateLimitedCredentialsIndependentOfRetries(t *testing.T) {
	old := retries
	retries = 0
	t.Cleanup(func() { retries = old })
	for _, route := range admissionRoutes {
		t.Run(route.name, func(t *testing.T) {
			seen := map[string]int{}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen[r.Header.Get("Authorization")]++
				w.WriteHeader(429)
				io.WriteString(w, `{"error":"limited"}`)
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"K1", "K2", "K3", "K4"}, StreamOnly: route.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"K1": "1", "K2": "2", "K3": "3", "K4": "4"})
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, route.request(context.Background()))
			if w.Code != 429 || len(seen) != 4 {
				t.Fatalf("status=%d credentials=%v", w.Code, seen)
			}
			for _, n := range seen {
				if n != 1 {
					t.Fatalf("credential repeated: %v", seen)
				}
			}
			if err := s.tm.Flush(); err != nil {
				t.Fatal(err)
			}
			rows, err := s.tm.DB().Query(`SELECT request_id, attempt, failure_class FROM requests ORDER BY id`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			id := ""
			n := 0
			for rows.Next() {
				var got, cl string
				var attempt int
				if err := rows.Scan(&got, &attempt, &cl); err != nil {
					t.Fatal(err)
				}
				n++
				if id == "" {
					id = got
				}
				if id == "" || got != id || attempt != n || cl != "rate_limit" {
					t.Fatalf("id=%s/%s attempt=%d class=%s", id, got, attempt, cl)
				}
			}
			if n != 4 {
				t.Fatalf("rows=%d", n)
			}
			if st := s.hlth.Snapshot(); len(st) != 1 || st[0].Failures != 1 {
				t.Fatalf("health=%+v", st)
			}
		})
	}
}

func TestRetryAfterAndFullJitter(t *testing.T) {
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		v    string
		want time.Duration
		ok   bool
	}{
		{"5", 5 * time.Second, true}, {now.Add(7 * time.Second).Format(http.TimeFormat), 7 * time.Second, true},
		{"999999999999999999", 10 * time.Second, true}, {"-1", 0, false}, {"invalid", 0, false}, {now.Add(-time.Second).Format(http.TimeFormat), 0, true},
	} {
		got, ok := retryAfter(tc.v, now, 10*time.Second)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s=%s/%v", tc.v, got, ok)
		}
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 64; i++ {
		d := fullJitter(3, 10*time.Millisecond, 100*time.Millisecond)
		if d < 0 || d >= 40*time.Millisecond {
			t.Fatal(d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("no jitter")
	}
	if d := fullJitter(10000, time.Second, 2*time.Second); d < 0 || d >= 2*time.Second {
		t.Fatal(d)
	}
}

func TestRetryAfterAppliedAndCanceled(t *testing.T) {
	for _, value := range []string{"5", time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)} {
		t.Run(value, func(t *testing.T) {
			var hits atomic.Int32
			started := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hits.Add(1) == 1 {
					close(started)
				}
				w.Header().Set("Retry-After", value)
				w.WriteHeader(429)
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"K1", "K2"}}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"K1": "1", "K2": "2"})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.Handler().ServeHTTP(httptest.NewRecorder(), admissionRoutes[0].request(ctx))
			}()
			<-started
			time.Sleep(30 * time.Millisecond)
			if hits.Load() != 1 {
				cancel()
				t.Fatal("Retry-After ignored")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("backoff ignored cancellation")
			}
			if hits.Load() != 1 {
				t.Fatal(hits.Load())
			}
			if keys := s.pools["a"].Keys(); keys[0].Failures != 1 || keys[1].Failures != 0 {
				t.Fatalf("keys=%+v", keys)
			}
			if st := s.hlth.Snapshot(); len(st) != 1 || st[0].Failures != 0 {
				t.Fatalf("canceled retry created model penalty: %+v", st)
			}
		})
	}
}

func TestRetryBudgetSharedAcrossCandidateWalk(t *testing.T) {
	t.Setenv("GUVNA_RETRY_BUDGET", "1")
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(401)
		io.WriteString(w, `{"error":"auth"}`)
	}))
	defer up.Close()
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"A1", "A2", "A3"}}, {Name: "b", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"B1", "B2", "B3"}}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "m"}}}}, map[string]string{"A1": "1", "A2": "2", "A3": "3", "B1": "4", "B2": "5", "B3": "6"})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, admissionRoutes[0].request(context.Background()))
	if w.Code != 401 || hits.Load() != 2 {
		t.Fatalf("status=%d hits=%d", w.Code, hits.Load())
	}
}

func TestAttemptLatencyAndFallbackAcrossRoutes(t *testing.T) {
	for _, route := range admissionRoutes {
		t.Run(route.name, func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(8 * time.Millisecond); w.WriteHeader(400) }))
			defer bad.Close()
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(15 * time.Millisecond); route.respond(w) }))
			defer good.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: route.aggregate}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B", StreamOnly: route.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "backup"}}}}, map[string]string{"A": "1", "B": "2"})
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, route.request(context.Background()))
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if err := s.tm.Flush(); err != nil {
				t.Fatal(err)
			}
			rows, err := s.tm.DB().Query(`SELECT request_id,attempt,latency_ms,failure_class FROM requests ORDER BY id`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			id := ""
			n := 0
			for rows.Next() {
				var got, cl string
				var attempt int
				var latency int64
				rows.Scan(&got, &attempt, &latency, &cl)
				n++
				if id == "" {
					id = got
				}
				if got == "" || got != id || attempt != 1 || latency < 5 {
					t.Fatalf("id=%s attempt=%d latency=%d", got, attempt, latency)
				}
				if n == 2 && cl != "" {
					t.Fatalf("success classified %s", cl)
				}
			}
			if n != 2 {
				t.Fatal(n)
			}
		})
	}
}

func TestImageIncompatibilityAcrossRoutes(t *testing.T) {
	for _, route := range admissionRoutes {
		if route.route == "/v1/embeddings" {
			continue
		}
		for _, status := range []int{400, 422} {
			t.Run(fmt.Sprintf("%s/%d", route.name, status), func(t *testing.T) {
				s, _ := statusAttributionServer(t, route, status)
				s.visionReg = nil // nil registry uses built-ins; m is added below via built-in name.
				// Use a model known to support images, otherwise Auto correctly filters it.
				body := `{"model":"main","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/a.png"}]}],"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}],"stream":false}`
				// Call the ordinary chain paths with model m; Auto needs a vision registry override.

				r := httptest.NewRequest("POST", route.route, strings.NewReader(body))
				r.Header.Set("Authorization", "Bearer client-1")
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != status {
					t.Fatal(w.Code, w.Body.String())
				}
				if st := s.hlth.Snapshot(); len(st) != 1 || st[0].Down || st[0].Failures != 0 {
					t.Fatalf("health=%+v", st)
				}
			})
		}
	}
}

func TestLocalDialTimeoutDoesNotQuarantineKey(t *testing.T) {
	err := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	if c := keypool.ClassFor(0, err); c != "" {
		t.Fatal(c)
	}
}

func TestAdminStatusKeepsTelemetryWhenUsageUnavailable(t *testing.T) {
	s := runtimeServer(t, "http://unused")
	if _, err := s.tm.DB().Exec("DROP TABLE requests"); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/admin/status", nil)
	r.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var got struct {
		Telemetry map[string]any `json:"telemetry"`
		Error     string         `json:"usage_error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || got.Error == "" || len(got.Telemetry) < 5 {
		t.Fatalf("status=%d response=%s", w.Code, w.Body.String())
	}
}
