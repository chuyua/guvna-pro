package server

import (
	"github.com/creamy-ghost/guvna/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReviewImage400Blame(t *testing.T) {
	for _, laterSuccess := range []bool{false, true} {
		name := "all_400"
		if laterSuccess {
			name = "later_success"
		}
		t.Run(name, func(t *testing.T) {
			var hits int
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				if hits == 2 && laterSuccess {
					io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
					return
				}
				w.WriteHeader(400)
				io.WriteString(w, `{"error":{"message":"invalid image"}}`)
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: up.URL, KeyEnv: "B"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m1"}, {Provider: "b", Model: "m2"}}}}, map[string]string{"A": "a", "B": "b"})
			s.visionReg = nil
			req := httptest.NewRequest("POST", "/v1/auto", strings.NewReader(`{"model":"main","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/invalid.png"}}]}]}`))
			req.Header.Set("Authorization", "Bearer client-1")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)
			if hits != 2 {
				t.Fatalf("expected2 actual candidates calls=%d status=%d", hits, w.Code)
			}
			states := s.hlth.Snapshot()
			down := 0
			for _, st := range states {
				if st.Down {
					down++
				}
			}
			t.Logf("laterSuccess=%v status=%d states=%+v", laterSuccess, w.Code, states)
			if laterSuccess {
				if w.Code != 200 || down != 1 {
					t.Fatalf("need only400candidate blamed status=%d down=%d", w.Code, down)
				}
			} else {
				if w.Code != 400 || down != 0 {
					t.Fatalf("request-wide400 poisoned healthy candidate pool status=%d down=%d", w.Code, down)
				}
			}
		})
	}
}
