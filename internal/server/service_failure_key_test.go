package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestServiceFailureRetriesKeepHealthyCredential(t *testing.T) {
	old := retries
	t.Cleanup(func() { retries = old })
	for _, route := range admissionRoutes {
		for _, tc := range []struct {
			name                     string
			network                  bool
			retries, budget, recover int
			want                     int
		}{
			{"exhausted", false, 2, 16, 0, 3},
			{"shared_budget", false, 2, 1, 0, 2},
			{"disabled", false, 0, 16, 0, 1},
			{"recovered", false, 2, 16, 2, 2},
			{"network_exhausted", true, 2, 16, 0, 3},
			{"network_recovered", true, 2, 16, 2, 2},
		} {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				retries = tc.retries
				t.Setenv("GUVNA_RETRY_BUDGET", strconv.Itoa(tc.budget))
				var mu sync.Mutex
				seen := make(map[string]int)
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					mu.Lock()
					seen[r.Header.Get("Authorization")]++
					n := seen[r.Header.Get("Authorization")]
					mu.Unlock()
					if tc.recover > 0 && n >= tc.recover {
						route.respond(w)
					} else if tc.network {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conn.Close()
					} else {
						w.WriteHeader(http.StatusServiceUnavailable)
						io.WriteString(w, `{"error":"unavailable"}`)
					}
				}))
				defer up.Close()
				s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"K1", "K2", "K3"}, StreamOnly: route.aggregate}},
					[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"K1": "1", "K2": "2", "K3": "3"})
				var waits int
				s.retryWait = func(int) time.Duration { return 10 * time.Millisecond }
				s.retrySleep = func(ctx context.Context, delay time.Duration) error {
					waits++
					if delay != 10*time.Millisecond {
						t.Errorf("backoff=%s", delay)
					}
					return ctx.Err()
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, route.request(context.Background()))
				if tc.recover > 0 && w.Code != http.StatusOK || tc.recover == 0 && w.Code < 500 {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
				mu.Lock()
				if len(seen) != 1 || seen["Bearer 1"] != tc.want {
					t.Errorf("credentials=%v, want only K1 for %d attempts", seen, tc.want)
				}
				mu.Unlock()
				if waits != tc.want-1 {
					t.Errorf("backoff waits=%d want=%d", waits, tc.want-1)
				}
				for i, key := range s.pools["a"].Keys() {
					wantRequests := int64(0)
					if i == 0 {
						wantRequests = int64(tc.want)
					}
					if key.State != "healthy" || key.Failures != 0 || key.Reason != "" || key.Requests != wantRequests {
						t.Errorf("key=%+v want requests=%d", key, wantRequests)
					}
				}
				wantFailures := 1
				if tc.recover > 0 {
					wantFailures = 0
				}
				if states := s.hlth.Snapshot(); len(states) != 1 || states[0].Failures != wantFailures {
					t.Fatalf("health=%+v want failures=%d", states, wantFailures)
				}
				if err := s.tm.Flush(); err != nil {
					t.Fatal(err)
				}
				rows, err := s.tm.DB().Query(`SELECT request_id, attempt, key, failure_class FROM requests ORDER BY id`)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				id, n := "", 0
				for rows.Next() {
					var got, key, class string
					var attempt int
					if err := rows.Scan(&got, &attempt, &key, &class); err != nil {
						t.Fatal(err)
					}
					n++
					if n == 1 {
						id = got
					}
					wantClass := "upstream_transient"
					if tc.network {
						wantClass = "local_network"
					}
					if tc.recover > 0 && n == tc.want {
						wantClass = ""
					}
					if got == "" || got != id || attempt != n || key != "K1" || class != wantClass {
						t.Errorf("event=%s/%d/%s/%s want attempt=%d class=%s", got, attempt, key, class, n, wantClass)
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if n != tc.want {
					t.Fatalf("events=%d want=%d", n, tc.want)
				}
			})
		}
	}
}

func TestServiceRetryAfterCancellationKeepsKeyNeutral(t *testing.T) {
	for _, route := range admissionRoutes {
		t.Run(route.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A", StreamOnly: route.aggregate}},
				[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := 0
			s.retrySleep = func(ctx context.Context, delay time.Duration) error {
				waits++
				if delay != 5*time.Second {
					t.Errorf("Retry-After wait=%s", delay)
				}
				cancel()
				return sleepCtx(ctx, delay)
			}
			s.Handler().ServeHTTP(httptest.NewRecorder(), route.request(ctx))
			if waits != 1 || hits.Load() != 1 {
				t.Fatalf("waits=%d calls=%d", waits, hits.Load())
			}
			if key := s.pools["a"].Keys()[0]; key.Failures != 0 || key.State != "healthy" || key.Requests != 1 {
				t.Fatalf("key=%+v", key)
			}
			if states := s.hlth.Snapshot(); len(states) != 1 || states[0].Failures != 0 {
				t.Fatalf("cancel added a model failure: %+v", states)
			}
			runtimeTelemetry(t, s, 1, http.StatusServiceUnavailable, true)
		})
	}
}

func TestDeciderNeutralFailurePreservesPriorKeyEvidence(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{400, `{"error":"bad request"}`},
		{503, `{"error":"unavailable"}`},
		{200, `{broken`},
		{200, validDeciderBody}, // No eligible candidates: an unusable ranking.
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.body), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.decideProvider, s.decideModel = "a", "m"
			seedRecoveredKey(t, s)
			before := s.pools["a"].Keys()[0]
			if _, err := s.callDecider(nil, context.Background(), nil); err == nil {
				t.Fatal("bad decider response accepted")
			}
			if key := s.pools["a"].Keys()[0]; key.Failures != before.Failures || key.Reason != before.Reason || key.State != "healthy" || key.Requests != 1 {
				t.Fatalf("neutral failure changed prior evidence: before=%+v after=%+v", before, key)
			}
		})
	}
}
