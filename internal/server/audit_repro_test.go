package server

import (
	"bufio"
	"github.com/creamy-ghost/guvna/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuditMixedGeminiFallback(t *testing.T) {
	bad := completionUpstream(t, 400, `{"error":{"message":"unsupported model"}}`, false)
	defer bad.Close()
	paths := make(chan string, 10)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if r.URL.Path != "/v1beta/openai/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer good.Close()
	_, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A"}, {Name: "g", Type: "gemini", BaseURL: good.URL, KeyEnv: "G"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "g", Model: "gemini"}}}}, map[string]string{"A": "dummy", "G": "dummy"})
	resp := doChat(t, ts, "main", false)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	t.Logf("fallback path=%s downstream=%d body=%s", <-paths, resp.StatusCode, body)
	if resp.StatusCode != 200 {
		t.Fatalf("mixed chain should reach official Gemini endpoint, got %d", resp.StatusCode)
	}
}

func TestAuditResponsesInvalidStreams(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"malformed_json", "data: {broken-json}\n\n"},
		{"unfinished_eof", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"},
		{"upstream_error", "data: {\"error\":{\"message\":\"quota exhausted\"}}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Write([]byte(tc.payload))
			}))
			defer up.Close()
			s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
			req, _ := http.NewRequest("POST", ts.URL+"/v1/responses", strings.NewReader(`{"model":"main","input":"hi","stream":true}`))
			req.Header.Set("Authorization", "Bearer client-1")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			completed := strings.Contains(string(body), "event: response.completed")
			t.Logf("status=%d completed=%v health=%v", resp.StatusCode, completed, s.hlth.Snapshot())
			if completed {
				t.Fatalf("invalid stream reported completed instead of failed/incomplete; upstream=%q", tc.payload)
			}
		})
	}
}

func TestAuditNonstreamBodyFailureHealth(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":`))
	}))
	defer up.Close()
	s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
	s.hlth.Mark("a", "m", 500, nil)
	s.hlth.Mark("a", "m", 500, nil)
	resp := doChat(t, ts, "main", false)
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
	ss := s.hlth.Snapshot()
	t.Logf("before failures=2, downstream=%d after health=%v", resp.StatusCode, ss)
	if resp.StatusCode != 502 {
		t.Fatalf("expected truncated-body 502, got %d", resp.StatusCode)
	}
	if len(ss) != 1 || ss[0].Failures != 3 || !ss[0].Down {
		t.Fatalf("body read failure must count as third failure; got %v", ss)
	}
}

func TestAuditIdleStreamKeepAlive(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer up.Close()
	_, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
	resp := doChat(t, ts, "main", true)
	defer resp.Body.Close()
	lines := make(chan string, 1)
	go func() {
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		if err != nil {
			lines <- "read error: " + err.Error()
		} else {
			lines <- line
		}
	}()
	select {
	case line := <-lines:
		if !strings.Contains(line, "keep-alive") {
			t.Fatalf("expected keep-alive, got %q", line)
		}
	case <-time.After(KeepAliveInterval + time.Second):
		t.Fatalf("no SSE heartbeat after %v of silent upstream", KeepAliveInterval+time.Second)
	}
}

func TestAuditBodyFailureHealthAcrossRoutes(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto", "/v1/embeddings"} {
		t.Run(route, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "1000")
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"choices":`))
			}))
			defer up.Close()
			s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
			s.hlth.Mark("a", "m", 500, nil)
			s.hlth.Mark("a", "m", 500, nil)
			req, _ := http.NewRequest("POST", ts.URL+route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}]}`))
			req.Header.Set("Authorization", "Bearer client-1")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, readErr := io.ReadAll(resp.Body)
			ss := s.hlth.Snapshot()
			t.Logf("route=%s status=%d body=%q readErr=%v health=%v", route, resp.StatusCode, body, readErr, ss)
			if len(ss) != 1 || ss[0].Failures != 3 || !ss[0].Down {
				t.Fatalf("truncated response must retain/count failures: %v", ss)
			}
		})
	}
}
