package server

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/router"
)

type secretParamMarshaler struct{}

func (secretParamMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New("private-parameter-value")
}

func TestMergeParamsFailsInsteadOfDroppingValues(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), map[any]any{1: "original"}, secretParamMarshaler{}} {
		merged, err := mergeParams([]byte(`{"model":"original"}`), map[string]any{"custom": value})
		if err == nil || len(merged) != 0 || !strings.Contains(err.Error(), `"custom"`) || strings.Contains(err.Error(), "private-parameter-value") {
			t.Fatalf("invalid param was not rejected safely: body=%q err=%v", merged, err)
		}
	}
	for _, body := range []string{`null`, `[1]`, `{"model":`} {
		if _, err := mergeParams([]byte(body), map[string]any{"temperature": 0.2}); err == nil {
			t.Fatalf("cannot merge into invalid object %q", body)
		}
	}
	// A present client field still wins; its configured default is not applied.
	if _, err := mergeParams([]byte(`{"custom":0}`), map[string]any{"custom": math.NaN()}); err != nil {
		t.Fatalf("an unused default changed client-field precedence: %v", err)
	}
}

func TestStepParamsPreserveJSONValuesUpstream(t *testing.T) {
	requests := make(chan map[string]json.RawMessage, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode upstream body: %v", err)
		}
		requests <- body
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer up.Close()
	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "original", Params: map[string]any{
		"seed": int64(9007199254740993), "temperature": 0.2, "nested": map[string]any{"items": []any{int64(9007199254740995), "END", true, nil}},
	}}}}}
	_, ts := testServer(t, providers, chains, map[string]string{"A": "dummy"})
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(`{"model":"main","messages":[{"role":"user","content":"hi"}],"temperature":0,"future":9007199254740997}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer client-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	got := <-requests
	for key, want := range map[string]string{"model": `"original"`, "seed": "9007199254740993", "temperature": "0", "nested": `{"items":[9007199254740995,"END",true,null]}`, "future": "9007199254740997"} {
		if string(got[key]) != want {
			t.Errorf("forwarded %s=%s, want %s", key, got[key], want)
		}
	}
}

func TestInvalidProgrammaticParamsSkipUpstreamAndAllowFallback(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/embeddings", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			var badHits, goodHits atomic.Int32
			upstream := func(hits *atomic.Int32) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hits.Add(1)
					if strings.HasSuffix(r.URL.Path, "/embeddings") {
						io.WriteString(w, `{"data":[{"index":0,"embedding":[1,2]}]}`)
					} else {
						io.WriteString(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
					}
				}))
			}
			bad, good := upstream(&badHits), upstream(&goodHits)
			defer bad.Close()
			defer good.Close()
			providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"}}
			chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "first"}, {Provider: "b", Model: "backup"}}}}
			s, ts := testServer(t, providers, chains, map[string]string{"A": "dummy-a", "B": "dummy-b"})
			// Simulate a caller bypassing Config.Validate. Supported config and
			// runtime creation reject this value before it can reach routing.
			unvalidated := *s.cfg
			unvalidated.Chains = []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "first", Params: map[string]any{"temperature": math.NaN()}}, {Provider: "b", Model: "backup"}}}}
			s.rtr = router.New(&unvalidated)
			body := `{"model":"main","messages":[{"role":"user","content":"hi"}]}`
			if path != "/v1/chat/completions" {
				body = `{"model":"main","input":"hi"}`
			}
			req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer client-1")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			response, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK || badHits.Load() != 0 || goodHits.Load() != 1 {
				t.Fatalf("invalid params were forwarded or fallback failed: status=%d hits=%d/%d body=%s", resp.StatusCode, badHits.Load(), goodHits.Load(), response)
			}
			if key := s.pools["a"].Keys()[0]; key.Failures != 0 || key.State != "healthy" || key.Requests != 0 {
				t.Fatalf("invalid local params isolated a healthy credential: %+v", key)
			}
		})
	}
}
