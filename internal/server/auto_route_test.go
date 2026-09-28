package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

// doAuto posts an auto request and returns the response.
func doAuto(t *testing.T, ts *httptest.Server, extra map[string]any) *http.Response {
	t.Helper()
	body := map[string]any{"model": "auto", "messages": []any{"hi"}}
	for k, v := range extra {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/auto", strings.NewReader(string(raw)))
	req.Header.Set("Authorization", "Bearer client-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// countingUpstream returns an upstream that counts calls and always answers the
// given status.
func countingUpstream(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	count := &atomic.Int32{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts, count
}

// okUpstream is countingUpstream when the caller does not care about the count.
func okUpstream(t *testing.T, body string) *httptest.Server {
	t.Helper()
	ts, _ := countingUpstream(t, http.StatusOK, body)
	return ts
}

// statusUpstream answers a fixed status without counting.
func statusUpstream(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	ts, _ := countingUpstream(t, status, body)
	return ts
}

// autoServer builds a server with no default_chain: auto's tests use family
// chains (fam-a, fam-b) that never satisfy the "main" default.
func autoServer(t *testing.T, providers []config.Provider, ch []config.Chain, env map[string]string) (*Server, *httptest.Server) {
	t.Helper()
	return testServerDefault(t, providers, ch, env, "")
}

func TestAutoServesEligibleModel(t *testing.T) {
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)

	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	_, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k"})

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), "ok") {
		t.Errorf("expected upstream body, got %s", got)
	}
}

// With no decider configured the gateway must fall back to the static provider
// order and still serve. /v1/auto must never fail just because the decider is
// absent.
func TestAutoWorksWithoutDecider(t *testing.T) {
	good := okUpstream(t, `{"choices":[{}]}`)

	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	_, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k"})

	resp := doAuto(t, ts, map[string]any{"min_context": 512})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// A filter everything fails must not report "no providers available" — that
// would tell the caller the gateway is down when the real problem is the
// requested window.
func TestAutoRejectsInfeasibleFilter(t *testing.T) {
	good := okUpstream(t, `{"choices":[{}]}`)

	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k"})
	// Force a small window on this model so the 20000 filter excludes it.
	s.ctxLen = func(string) int { return 8192 }

	resp := doAuto(t, ts, map[string]any{"min_context": 20000})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), "context_below_min") {
		t.Errorf("expected context_below_min reason, got %s", got)
	}
}

func TestAutoRejectsBadContextBounds(t *testing.T) {
	good := okUpstream(t, `{"choices":[{}]}`)
	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	_, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k"})

	resp := doAuto(t, ts, map[string]any{"min_context": 1000, "max_context": 500})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAutoFallsBackAcrossCandidates(t *testing.T) {
	bad := statusUpstream(t, http.StatusTooManyRequests, `{"error":"429"}`)
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)

	providers := []config.Provider{
		{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "KEY_A"},
		{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_B"},
	}
	// Two distinct chains, one step each: auto must treat them as two
	// candidates and fall over when the first fails.
	chains := []config.Chain{
		{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}},
		{Name: "fam-b", Steps: []config.Step{{Provider: "b", Model: "m-b"}}},
	}
	_, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_B": "k"})

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), "ok") {
		t.Errorf("expected the good upstream's body, got %s", got)
	}
}

// The decider's ranking must actually reorder the pool. Static order here is
// a then b; the decider says b first. If b is the only healthy candidate, a
// must never be called.
func TestAutoHonorsDeciderOrder(t *testing.T) {
	dec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		// message.content is a JSON string holding the ranking object.
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"order\":[\"b/m-b\"]}"}}]}`))
	}))
	t.Cleanup(dec.Close)

	// "dead" is first in static provider order (a sorts before b). The decider
	// puts b first, so if its order is honored dead is never asked.
	dead, deadCalls := countingUpstream(t, http.StatusServiceUnavailable, `{"error":"503"}`)
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)

	providers := []config.Provider{
		{Name: "a", Type: "openai", BaseURL: dead.URL, KeyEnv: "KEY_A"},
		{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_B"},
		{Name: "dec", Type: "openai", BaseURL: dec.URL, KeyEnv: "KEY_DEC"},
	}
	chains := []config.Chain{
		{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}},
		{Name: "fam-b", Steps: []config.Step{{Provider: "b", Model: "m-b"}}},
	}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_B": "k", "KEY_DEC": "k"})
	s.decideProvider = "dec"
	s.decideModel = "selector"
	s.decideInterval = -1 // never reuse, so the decider is consulted each time

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), "ok") {
		t.Errorf("expected the good upstream's body, got %s", got)
	}
	// The decider put b first, so the statically-first candidate must not have
	// been asked. If the decider order were ignored, dead would have been hit.
	if n := deadCalls.Load(); n != 0 {
		t.Fatalf("decider order ignored: dead candidate was called %d times", n)
	}
}

func TestAutoDeciderFailureStillServes(t *testing.T) {
	// Decider answers prose instead of JSON; ParseRanking must fail and the
	// gateway must fall back to static order rather than error.
	dec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"I think b is better"}}]}`))
	}))
	t.Cleanup(dec.Close)

	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)

	providers := []config.Provider{
		{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"},
		{Name: "dec", Type: "openai", BaseURL: dec.URL, KeyEnv: "KEY_DEC"},
	}
	chains := []config.Chain{{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_DEC": "k"})
	s.decideProvider = "dec"
	s.decideModel = "selector"
	s.decideInterval = -1

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("decider garbage should not fail the request: status = %d", resp.StatusCode)
	}
}

func TestAutoNoChains(t *testing.T) {
	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: "http://127.0.0.1:1", KeyEnv: "KEY_A"}}
	_, ts := autoServer(t, providers, nil, map[string]string{"KEY_A": "k"})

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

// A /v1/auto request may omit "model" entirely; withModel must set it per step
// rather than only rewriting an existing value, or every upstream 400s.
func TestAutoWithoutModelField(t *testing.T) {
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)
	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_A"}}
	chains := []config.Chain{{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k"})
	_ = s

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/auto",
		strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer client-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
}

// A single 400 must put the model into single-strike cool-off: the next
// request's Eligible pass excludes it, so the 400ing upstream is not asked
// again (that is the vision self-healing promise).
func TestAutoMarks400Down(t *testing.T) {
	bad, badCalls := countingUpstream(t, http.StatusBadRequest, `{"error":"bad request"}`)
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)
	providers := []config.Provider{
		{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "KEY_A"},
		{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_B"},
	}
	// "a" sorts first in static order, so the first request hits it.
	chains := []config.Chain{
		{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}},
		{Name: "fam-b", Steps: []config.Step{{Provider: "b", Model: "m-b"}}},
	}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_B": "k"})

	resp := doAuto(t, ts, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request status = %d, want 200 via failover", resp.StatusCode)
	}
	if n := badCalls.Load(); n != 1 {
		t.Fatalf("bad upstream called %d times on first request; want 1", n)
	}
	if !s.hlth.IsDown("a", "m-a") {
		t.Fatal("single 400 did not put the model into cool-off")
	}

	// Second request: the 400ing candidate is excluded before the walk.
	resp = doAuto(t, ts, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", resp.StatusCode)
	}
	if n := badCalls.Load(); n != 1 {
		t.Fatalf("bad upstream called %d times total; want 1 (cool-off excluded it)", n)
	}
}

// A 404 (model configured but absent upstream) must not put the model into
// permanent cool-off: that would spread a scheduling residue into /v1/chat's
// shared health. It is skipped fast on every request instead.
func TestAuto404DoesNotMarkDown(t *testing.T) {
	nf, nfCalls := countingUpstream(t, http.StatusNotFound, `{"error":"model not found"}`)
	good := okUpstream(t, `{"choices":[{"message":{"content":"ok"}}]}`)
	providers := []config.Provider{
		{Name: "a", Type: "openai", BaseURL: nf.URL, KeyEnv: "KEY_A"},
		{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_B"},
	}
	chains := []config.Chain{
		{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "m-a"}}},
		{Name: "fam-b", Steps: []config.Step{{Provider: "b", Model: "m-b"}}},
	}
	s, ts := autoServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_B": "k"})

	for i := 0; i < 2; i++ {
		resp := doAuto(t, ts, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200 via failover", i, resp.StatusCode)
		}
	}
	if n := nfCalls.Load(); n != 2 {
		t.Fatalf("404 upstream called %d times over 2 requests; want 2 (skipped fast each time, not cool-off)", n)
	}
	if s.hlth.IsDown("a", "m-a") {
		t.Fatal("404 put the model into cool-off; a scheduling residue must not be judged down")
	}
}
