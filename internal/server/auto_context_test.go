package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestAutoContextEstimateUsesToolsUnicodeAndExplicitOutput(t *testing.T) {
	base := []byte(`{"messages":[{"role":"user","content":"你好"}],"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]}`)
	withOutput := []byte(strings.TrimSuffix(string(base), "}") + `,"max_tokens":100,"max_completion_tokens":200}`)
	if estimateAutoContext(withOutput) < estimateAutoContext(base)+200 {
		t.Fatal("explicit output reserve was not included")
	}
	if estimateAutoContext([]byte(`{"messages":["你好"]}`)) < 6 {
		t.Fatal("multilingual bytes were discounted as English text")
	}
	if estimateAutoContext([]byte(`{"max_tokens":9223372036854775807}`)) != int(^uint(0)>>1) {
		t.Fatal("context estimate overflowed")
	}
}

func TestAutoRequestContextFitAndServiceDegradation(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		small, large                     int
		output                           int
		image                            bool
		serviceFloor                     bool
		wantStatus, wantSmall, wantLarge int
	}{
		{"long_fits", 8192, 16384, 0, false, false, 200, 1, 0},
		{"skip_small", 512, 8192, 0, false, false, 200, 0, 1},
		{"all_too_small", 512, 600, 0, false, false, 400, 0, 0},
		{"reserve_output", 2048, 8192, 2000, false, false, 200, 0, 1},
		{"image_not_estimated", 512, 600, 0, true, false, 200, 1, 0},
		{"degraded_still_checked", 512, 600, 0, false, true, 400, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small, smallCalls := countingUpstream(t, 200, `{"choices":[{"message":{"content":"small"},"finish_reason":"stop"}]}`)
			large, largeCalls := countingUpstream(t, 200, `{"choices":[{"message":{"content":"large"},"finish_reason":"stop"}]}`)
			s, ts := autoServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: small.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: large.URL, KeyEnv: "B"}}, []config.Chain{{Name: "a", Steps: []config.Step{{Provider: "a", Model: "gpt-4o"}}}, {Name: "b", Steps: []config.Step{{Provider: "b", Model: "large"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
			s.ctxLen = func(model string) int {
				if model == "gpt-4o" {
					return tc.small
				}
				return tc.large
			}
			if tc.serviceFloor {
				t.Setenv("GUVNA_AUTO_MIN_CONTEXT", "262144")
			}
			content := any(strings.Repeat("x", 3000))
			if tc.image {
				content = []any{map[string]any{"type": "text", "text": strings.Repeat("x", 3000)}, map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://example.invalid/test.png"}}}
			}
			extra := map[string]any{"messages": []any{map[string]any{"role": "user", "content": content}}}
			if !tc.serviceFloor {
				extra["min_context"] = 1
			}
			if tc.output > 0 {
				extra["max_tokens"] = tc.output
			}
			resp := doAuto(t, ts, extra)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantStatus || int(smallCalls.Load()) != tc.wantSmall || int(largeCalls.Load()) != tc.wantLarge {
				t.Fatalf("status=%d hits=%d/%d body=%s", resp.StatusCode, smallCalls.Load(), largeCalls.Load(), body)
			}
			if tc.wantStatus == 400 && !strings.Contains(string(body), `"code":"context_length_exceeded"`) {
				t.Fatalf("missing context error: %s", body)
			}
		})
	}
}

func TestAutoTotalBudgetIncludesColdDecider(t *testing.T) {
	t.Setenv("GUVNA_AUTO_TOTAL_BUDGET", "80ms")
	var deciderCalls, candidateCalls atomic.Int32
	dec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deciderCalls.Add(1)
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer dec.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidateCalls.Add(1)
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer up.Close()
	s, _ := autoServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: up.URL, KeyEnv: "B"}, {Name: "dec", Type: "openai", BaseURL: dec.URL, KeyEnv: "D"}}, []config.Chain{{Name: "a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}, {Name: "b", Steps: []config.Step{{Provider: "b", Model: "m-b"}}}}, map[string]string{"A": "dummy", "B": "dummy", "D": "dummy"})
	s.decideProvider, s.decideModel, s.decideReqWait = "dec", "rank", time.Second
	r := httptest.NewRequest("POST", "/v1/auto", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	start := time.Now()
	s.Handler().ServeHTTP(w, r)
	if time.Since(start) > 600*time.Millisecond || w.Code != 503 || deciderCalls.Load() != 1 || candidateCalls.Load() != 0 {
		t.Fatalf("decision escaped budget: elapsed=%s status=%d calls=%d/%d body=%s", time.Since(start), w.Code, deciderCalls.Load(), candidateCalls.Load(), w.Body)
	}
}

func TestAutoNonpositiveTotalBudgetDisablesDeadline(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GUVNA_AUTO_TOTAL_BUDGET", value)
			if autoTotalBudget() > 0 {
				t.Fatal("nonpositive budget was not disabled")
			}
		})
	}
}

func TestAutoBudgetDoesNotBecomeCallerCancellation(t *testing.T) {
	t.Setenv("GUVNA_AUTO_TOTAL_BUDGET", "80ms")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	s, _ := autoServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "a", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
	r := httptest.NewRequest("POST", "/v1/auto", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`)).WithContext(context.Background())
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 502 || len(s.hlth.Snapshot()) != 1 || s.hlth.Snapshot()[0].Failures != 1 {
		t.Fatalf("budget misattributed: status=%d health=%+v", w.Code, s.hlth.Snapshot())
	}
	if err := json.Unmarshal(w.Body.Bytes(), new(map[string]any)); err != nil {
		t.Fatal(err)
	}
}
