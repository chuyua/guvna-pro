package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
)

func TestBackoffRechecksSelectedKeyAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		for _, spare := range []bool{false, true} {
			name := tc.name + "/exhausted"
			if spare {
				name = tc.name + "/spare"
			}
			t.Run(name, func(t *testing.T) {
				var hits, waits atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hits.Add(1)
					switch r.Header.Get("Authorization") {
					case "Bearer key1":
						w.WriteHeader(http.StatusTooManyRequests)
						io.WriteString(w, `{"error":{"message":"original rate-limit failure"}}`)
					case "Bearer key3":
						tc.respond(w)
					default:
						t.Error("sent a key quarantined during backoff")
						w.WriteHeader(500)
					}
				}))
				defer up.Close()
				envs := []string{"A1", "A2"}
				if spare {
					envs = append(envs, "A3")
				}
				s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: envs, StreamOnly: tc.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A1": "key1", "A2": "key2", "A3": "key3"})
				s.retryWait = func(int) time.Duration {
					waits.Add(1)
					// Another request fails the selected key during this backoff.
					s.pools["a"].Mark("A2", keypool.ClassAuth)
					return 0
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				wantHits, wantStatus := int32(1), http.StatusTooManyRequests
				if spare {
					wantHits, wantStatus = 2, http.StatusOK
				}
				if hits.Load() != wantHits || waits.Load() != 1 || w.Code != wantStatus {
					t.Fatalf("hits=%d waits=%d status=%d body=%s", hits.Load(), waits.Load(), w.Code, w.Body)
				}
				if !spare && !strings.Contains(w.Body.String(), "original rate-limit failure") {
					t.Fatalf("lost original upstream response: %s", w.Body)
				}
			})
		}
	}
}
