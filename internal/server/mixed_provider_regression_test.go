package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

// Exercise the real gateway pipeline, rather than only adaptor constructors:
// a failed first provider must not leak its key, model or defaults into Gemini.
func TestMixedProviderFallbackIsolation(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/responses", "/v1/embeddings"} {
		t.Run(route, func(t *testing.T) {
			type captured struct {
				path, auth string
				body       map[string]any
				err        error
			}
			calls := make(chan captured, 4)
			capture := func(r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				var body map[string]any
				if err == nil {
					err = json.Unmarshal(raw, &body)
				}
				calls <- captured{r.URL.Path, r.Header.Get("Authorization"), body, err}
			}
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capture(r)
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"error":{"message":"unsupported model"}}`)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capture(r)
				w.Header().Set("Content-Type", "application/json")
				if route == "/v1/embeddings" {
					io.WriteString(w, `{"data":[{"embedding":[0.1,0.2]}],"model":"gemini-model"}`)
					return
				}
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer second.Close()
			_, gateway := testServer(t, []config.Provider{
				{Name: "a", Type: "openai", BaseURL: first.URL, KeyEnv: "A"},
				{Name: "g", Type: "gemini", BaseURL: second.URL + "///", KeyEnv: "G"},
			}, []config.Chain{{Name: "main", Steps: []config.Step{
				{Provider: "a", Model: "openai-model", Params: map[string]any{"frequency_penalty": 0.7, "temperature": 0.1}},
				{Provider: "g", Model: "gemini-model", Params: map[string]any{"top_p": 0.9, "temperature": 0.2}},
			}}}, map[string]string{"A": "dummy-a", "G": "dummy-g"})
			r, err := http.NewRequest("POST", gateway.URL+route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}],"temperature":0.3}`))
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer client-1")
			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("fallback status=%d body=%s err=%v", resp.StatusCode, body, err)
			}
			if len(calls) != 2 {
				t.Fatalf("upstream calls=%d, want exactly two", len(calls))
			}
			a, g := <-calls, <-calls
			upstreamPath := "/chat/completions"
			if route == "/v1/embeddings" {
				upstreamPath = "/embeddings"
			}
			if a.err != nil || g.err != nil || a.path != "/v1"+upstreamPath || g.path != "/v1beta/openai"+upstreamPath {
				t.Fatalf("request decode/path mismatch: a=%+v g=%+v", a, g)
			}
			if a.auth != "Bearer dummy-a" || g.auth != "Bearer dummy-g" || a.body["model"] != "openai-model" || g.body["model"] != "gemini-model" {
				t.Fatalf("credential/model isolation failed: a=%+v g=%+v", a, g)
			}
			if a.body["frequency_penalty"] != 0.7 || g.body["top_p"] != 0.9 || a.body["temperature"] != 0.3 || g.body["temperature"] != 0.3 {
				t.Fatalf("defaults/client precedence failed: a=%v g=%v", a.body, g.body)
			}
			if _, leaked := g.body["frequency_penalty"]; leaked {
				t.Fatal("first provider default leaked into Gemini request")
			}
			if _, leaked := a.body["top_p"]; leaked {
				t.Fatal("Gemini default leaked into first provider request")
			}
		})
	}
}
