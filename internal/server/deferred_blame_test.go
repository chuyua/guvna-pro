package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func imageRequest(tc admissionRoute, ctx context.Context, alias bool) *http.Request {
	model := "main"
	if alias {
		model = "auto"
	}
	body := fmt.Sprintf(`{"model":%q,"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/a.png"}]}],"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}],"stream":%t}`, model, tc.stream)
	r := httptest.NewRequest("POST", tc.route, strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func TestDeferredImageBlameRequiresUsableCompletionAcrossRoutes(t *testing.T) {
	routes := append([]admissionRoute(nil), admissionRoutes...)
	for _, tc := range admissionRoutes {
		if tc.route == "/v1/chat/completions" {
			tc.name += "_auto_alias"
			routes = append(routes, tc)
		}
	}
	for _, tc := range routes {
		if tc.route == "/v1/embeddings" {
			continue
		}
		for _, result := range []string{"all_400", "all_422", "success", "malformed", "downstream", "canceled", "timeout"} {
			t.Run(tc.name+"/"+result, func(t *testing.T) {
				if result == "timeout" {
					oldStep, oldStream := chatStepBudget, streamMaxDuration
					chatStepBudget, streamMaxDuration = 30*time.Millisecond, 30*time.Millisecond
					t.Cleanup(func() { chatStepBudget, streamMaxDuration = oldStep, oldStream })
					t.Setenv("GUVNA_AUTO_STEP_BUDGET", "30ms")
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					status := 400
					if result == "all_422" {
						status = 422
					}
					w.WriteHeader(status)
					io.WriteString(w, `{"error":"image rejected"}`)
				}))
				defer bad.Close()
				good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch result {
					case "all_400":
						w.WriteHeader(400)
					case "all_422":
						w.WriteHeader(422)
					case "malformed":
						if tc.stream || tc.aggregate {
							w.Header().Set("Content-Type", "text/event-stream")
							io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
						} else {
							io.WriteString(w, `{"choices":[]}`)
						}
					case "canceled":
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
						cancel()
						<-r.Context().Done()
					case "timeout":
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
						<-r.Context().Done()
					default:
						tc.respond(w)
					}
				}))
				defer good.Close()
				s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: tc.aggregate}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B", StreamOnly: tc.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m1"}, {Provider: "b", Model: "m2"}}}}, map[string]string{"A": "a", "B": "b"})
				s.visionReg = nil
				w := httptest.NewRecorder()
				var writer http.ResponseWriter = w
				if result == "downstream" {
					writer = &runtimeFailWriter{ResponseRecorder: w}
				}
				s.Handler().ServeHTTP(writer, imageRequest(tc, ctx, strings.Contains(tc.name, "_auto_alias")))
				for _, st := range s.hlth.Snapshot() {
					if st.Provider == "a" {
						if st.Down != (result == "success") {
							t.Fatalf("result=%s status=%d health=%+v body=%s", result, w.Code, st, w.Body)
						}
					}
				}
				if keys := s.pools["a"].Keys(); keys[0].Failures != 0 {
					t.Fatalf("image rejection charged key: %+v", keys)
				}
				if err := s.tm.Flush(); err != nil {
					t.Fatal(err)
				}
				var events int
				if err := s.tm.DB().QueryRow("SELECT count(*) FROM requests WHERE provider='a' AND failure_class='model_incompatible'").Scan(&events); err != nil || events != 1 {
					t.Fatalf("deferred rejection events=%d err=%v", events, err)
				}
			})
		}
	}
}

func TestAuthFailoverIsImmediateAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		for _, status := range []int{401, 403} {
			for _, budget := range []string{"0", "1"} {
				t.Run(fmt.Sprintf("%s/%d/budget%s", tc.name, status, budget), func(t *testing.T) {
					t.Setenv("GUVNA_RETRY_BUDGET", budget)
					var hits, waits atomic.Int32
					up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						hits.Add(1)
						if r.Header.Get("Authorization") == "Bearer first" {
							w.WriteHeader(status)
							return
						}
						tc.respond(w)
					}))
					defer up.Close()
					s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"A1", "A2"}, StreamOnly: tc.aggregate}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A1": "first", "A2": "second"})
					s.retrySleep = func(context.Context, time.Duration) error { waits.Add(1); return nil }
					w := httptest.NewRecorder()
					s.Handler().ServeHTTP(w, tc.request(context.Background()))
					wantHits, wantStatus := int32(1), status
					if budget == "1" {
						wantHits, wantStatus = 2, 200
					}
					if waits.Load() != 0 || hits.Load() != wantHits || w.Code != wantStatus {
						t.Fatalf("waits=%d hits=%d status=%d", waits.Load(), hits.Load(), w.Code)
					}
				})
			}
		}
	}
}
