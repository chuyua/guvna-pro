package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestConcurrentOld429CountsAttemptsWithoutAmplifyingKeyCooldown(t *testing.T) {
	for _, tc := range admissionRoutes {
		t.Run(tc.name, func(t *testing.T) {
			var entered atomic.Int32
			ready, release := make(chan struct{}), make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if entered.Add(1) == 8 {
					close(ready)
				}
				<-release
				w.WriteHeader(429)
				io.WriteString(w, `{"error":"limited"}`)
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A", StreamOnly: tc.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "key"})
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					w := httptest.NewRecorder()
					s.Handler().ServeHTTP(w, tc.request(context.Background()))
					if w.Code != 429 {
						t.Errorf("status=%d", w.Code)
					}
				}()
			}
			select {
			case <-ready:
			case <-time.After(3 * time.Second):
				close(release)
				wg.Wait()
				t.Fatal("attempts did not enter concurrently")
			}
			close(release)
			wg.Wait()
			key := s.pools["a"].Keys()[0]
			if key.Failures != 1 || key.Requests != 8 {
				t.Fatalf("key=%+v", key)
			}
			if err := s.tm.Flush(); err != nil {
				t.Fatal(err)
			}
			var events int
			if err := s.tm.DB().QueryRow("SELECT count(*) FROM requests WHERE status=429").Scan(&events); err != nil || events != 8 {
				t.Fatalf("events=%d err=%v", events, err)
			}
		})
	}
}

func TestLateStreamFailureCannotPunishRecoveredKey(t *testing.T) {
	for _, tc := range admissionRoutes {
		if !tc.stream {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch hits.Add(1) {
				case 1:
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"old\"}}]}\n\n")
					w.(http.Flusher).Flush()
					close(started)
					<-release
				case 2:
					w.WriteHeader(429)
				default:
					tc.respond(w)
				}
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "key"})
			advance := seedRecoveredKey(t, s)
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.Handler().ServeHTTP(httptest.NewRecorder(), tc.request(context.Background()))
			}()
			<-started
			s.Handler().ServeHTTP(httptest.NewRecorder(), tc.request(context.Background()))
			advance()
			advance() // The seeded failure makes this second quarantine two minutes.
			recovered := httptest.NewRecorder()
			s.Handler().ServeHTTP(recovered, tc.request(context.Background()))
			if recovered.Code != http.StatusOK || hits.Load() != 3 {
				t.Fatalf("recovery was not served: status=%d hits=%d", recovered.Code, hits.Load())
			}
			close(release)
			<-done
			key := s.pools["a"].Keys()[0]
			if key.Failures != 0 || key.State != "healthy" || key.Requests != 3 {
				t.Fatalf("late stream result changed recovered key: %+v", key)
			}
		})
	}
}
