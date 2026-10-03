package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/responses"
	"github.com/creamy-ghost/guvna/internal/router"
)

func runtimeServer(t *testing.T, url string) *Server {
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: url, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
	return s
}

func runtimeTelemetry(t *testing.T, s *Server, wantCount, wantStatus int, wantErr bool) {
	t.Helper()
	if err := s.tm.Flush(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.tm.DB().QueryRow("SELECT COUNT(*) FROM requests").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != wantCount {
		t.Fatalf("telemetry rows=%d want %d", count, wantCount)
	}
	if count == 0 {
		return
	}
	var status int
	var msg string
	if err := s.tm.DB().QueryRow("SELECT status, err FROM requests ORDER BY id DESC LIMIT 1").Scan(&status, &msg); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || (msg != "") != wantErr {
		t.Fatalf("telemetry status=%d err=%q", status, msg)
	}
}

func TestRuntimeResponsesTermination(t *testing.T) {
	for _, tc := range []struct {
		name, payload, terminal string
		failed                  bool
	}{
		{"stop_eof", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n", "completed", false},
		{"done_no_finish", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n", "completed", false},
		{"length", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}\n\n", "incomplete", false},
		{"filter", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\n", "incomplete", false},
		{"multiline_usage", ": upstream comment\n\ndata: {\"choices\": [\n" + "data: {\"delta\": {\"content\": \"joined\"}, \"finish_reason\": \"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n", "completed", false},
		{"tools", "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n", "completed", false},
		{"bad_json", "data: {bad}\n\n", "failed", true},
		{"error_object", "data: {\"error\":{\"message\":\"denied\"}}\n\n", "failed", true},
		{"unfinished", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", "failed", true},
		{"unfinished_tools", "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"f\",\"arguments\":\"{\"}}]}}]}\n\n", "failed", true},
		{"empty_eof", ":comment\n\n", "failed", true},
		{"null", "data: null\n\n", "failed", true},
		{"unknown_finish", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"mystery\"}]}\n\n", "failed", true},
		{"error_after_finish", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"error\":\"failed\"}\n\n", "failed", true},
		{"ignore_after_done", "data: [DONE]\n\ndata: {bad}\n\n", "completed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := runtimeServer(t, "http://unused")
			s.hlth.Mark("a", "m", 500, nil)
			s.pools["a"].Mark("A", keypool.ClassTransient)
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/v1/responses", nil)
			s.relayResponsesStream(w, r, router.Step{Provider: "a", Model: "m"}, "main", &responses.Request{Model: "main", Stream: true}, &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.payload))}, "A")
			body := w.Body.String()
			if strings.Count(body, "event: response."+tc.terminal+"\n") != 1 || strings.Count(body, "data: [DONE]") != 1 {
				t.Fatalf("wrong terminal: %s", body)
			}
			if tc.terminal != "completed" && strings.Contains(body, "event: response.completed") {
				t.Fatalf("incorrect completed event: %s", body)
			}
			wantFailures, wantStatus := 0, 200
			if tc.failed {
				wantFailures, wantStatus = 2, 502
			}
			if s.hlth.Snapshot()[0].Failures != wantFailures || s.pools["a"].Keys()[0].Failures != wantFailures {
				t.Fatalf("health/key=%v/%v", s.hlth.Snapshot(), s.pools["a"].Keys())
			}
			runtimeTelemetry(t, s, 1, wantStatus, tc.failed)
			if tc.name == "multiline_usage" {
				var in, out int64
				if err := s.tm.DB().QueryRow("SELECT tokens_in, tokens_out FROM requests").Scan(&in, &out); err != nil {
					t.Fatal(err)
				}
				if in != 2 || out != 3 {
					t.Fatalf("usage=%d/%d", in, out)
				}
			}
		})
	}
}

func TestRuntimeBodiesFailureAndRecovery(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto", "/v1/embeddings"} {
		t.Run(route, func(t *testing.T) {
			var broken atomic.Bool
			broken.Store(true)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if broken.Load() {
					w.Header().Set("Content-Length", "1000")
					io.WriteString(w, `{"choices":`)
					return
				}
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.hlth.Mark("a", "m", 500, nil)
			s.pools["a"].Mark("A", keypool.ClassTransient)
			call := func() *httptest.ResponseRecorder {
				r := httptest.NewRequest("POST", route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}],"input_context":1}`))
				r.Header.Set("Authorization", "Bearer client-1")
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				return w
			}
			if w := call(); w.Code != 502 || strings.Contains(w.Body.String(), `{"choices":`) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if s.hlth.Snapshot()[0].Failures != 2 || s.pools["a"].Keys()[0].Failures != 2 {
				t.Fatalf("failure accounting=%v/%v", s.hlth.Snapshot(), s.pools["a"].Keys())
			}
			runtimeTelemetry(t, s, 1, 502, true)
			broken.Store(false)
			if w := call(); w.Code != 200 {
				t.Fatalf("recovery status=%d body=%s", w.Code, w.Body)
			}
			if s.hlth.Snapshot()[0].Failures != 0 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatal("successful complete body did not recover step/key")
			}
			runtimeTelemetry(t, s, 2, 200, false)
		})
	}
}

type runtimeBody struct {
	*io.PipeReader
	once        sync.Once
	startOnce   sync.Once
	closeOnce   sync.Once
	readStarted chan struct{}
	closed      chan struct{}
	readExited  chan struct{}
}

func (b *runtimeBody) Read(p []byte) (int, error) {
	b.startOnce.Do(func() { close(b.readStarted) })
	n, err := b.PipeReader.Read(p)
	if err != nil {
		b.once.Do(func() { close(b.readExited) })
	}
	return n, err
}

func (b *runtimeBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return b.PipeReader.Close()
}

type runtimeFailWriter struct {
	*httptest.ResponseRecorder
	responses bool
}

func (w *runtimeFailWriter) Write(p []byte) (int, error) {
	if !w.responses || strings.Contains(string(p), "response.output_item.added") {
		return 0, errors.New("client disconnected")
	}
	return w.ResponseRecorder.Write(p)
}

func TestRuntimeStreamCancellationAndWriteFailure(t *testing.T) {
	for _, surface := range []string{"chat", "responses"} {
		for _, failure := range []string{"cancel", "write"} {
			t.Run(surface+"_"+failure, func(t *testing.T) {
				s := runtimeServer(t, "http://unused")
				s.hlth.Mark("a", "m", 500, nil)
				s.pools["a"].Mark("A", keypool.ClassTransient)
				pr, pw := io.Pipe()
				defer pw.Close()
				body := &runtimeBody{PipeReader: pr, readStarted: make(chan struct{}), closed: make(chan struct{}), readExited: make(chan struct{})}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				r := httptest.NewRequest("POST", "/", nil).WithContext(ctx)
				var w http.ResponseWriter = httptest.NewRecorder()
				if failure == "write" {
					w = &runtimeFailWriter{ResponseRecorder: httptest.NewRecorder(), responses: surface == "responses"}
				}
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					resp := &http.Response{StatusCode: 200, Body: body}
					if surface == "chat" {
						s.relaySuccess(w, r, router.Step{Provider: "a", Model: "m"}, "main", chatRequest{Model: "main", Stream: true}, resp, "A")
					} else {
						s.relayResponsesStream(w, r, router.Step{Provider: "a", Model: "m"}, "main", &responses.Request{Model: "main", Stream: true}, resp, "A")
					}
				}()
				select {
				case <-body.readStarted:
				case <-time.After(time.Second):
					t.Fatal("reader did not start")
				}
				if failure == "cancel" {
					cancel()
				} else {
					_, _ = io.WriteString(pw, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
				}
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("relay/reader did not exit")
				}
				select {
				case <-body.closed:
				default:
					t.Fatal("upstream body was not closed")
				}
				if failure == "cancel" {
					select {
					case <-body.readExited:
					default:
						t.Fatal("blocked Read did not exit")
					}
				}
				if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
					t.Fatal("downstream failure changed upstream health")
				}
				runtimeTelemetry(t, s, 0, 0, false)
			})
		}
	}
}

func TestRuntimeIdleHeartbeat(t *testing.T) {
	for _, surface := range []string{"chat", "responses"} {
		t.Run(surface, func(t *testing.T) {
			t.Parallel()
			exited := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-r.Context().Done():
						return
					case <-ticker.C:
						if surface == "responses" {
							io.WriteString(w, ": upstream keepalive\n\n")
							w.(http.Flusher).Flush()
						}
					}
				}
			}))
			defer up.Close()
			s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
			_ = s
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			path := "/v1/chat/completions"
			if surface == "responses" {
				path = "/v1/responses"
			}
			r, _ := http.NewRequestWithContext(ctx, "POST", ts.URL+path, strings.NewReader(`{"model":"main","input":"hi","stream":true}`))
			r.Header.Set("Authorization", "Bearer client-1")
			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got := make(chan error, 1)
			go func() {
				br := bufio.NewReader(resp.Body)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						got <- err
						return
					}
					if line == ": keep-alive\n" {
						got <- nil
						return
					}
				}
			}()
			select {
			case err := <-got:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(16 * time.Second):
				t.Fatal("no 15-second keepalive")
			}
			cancel()
			resp.Body.Close()
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("upstream HTTP reader not cancelled")
			}
		})
	}
}
