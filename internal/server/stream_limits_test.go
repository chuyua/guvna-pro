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
	"github.com/creamy-ghost/guvna/internal/responses"
	"github.com/creamy-ghost/guvna/internal/router"
)

func TestReadStreamLinesRejectsOversizedUnterminatedLine(t *testing.T) {
	lines, stop := readStreamLines(context.Background(), io.NopCloser(strings.NewReader(strings.Repeat("x", maxStreamEventBytes+1))))
	defer stop()
	next := <-lines
	if next.err == nil || len(next.line) != 0 {
		t.Fatalf("oversized line was buffered: len=%d err=%v", len(next.line), next.err)
	}
}

func durationRequest(route string, ctx context.Context) *http.Request {
	r := httptest.NewRequest("POST", route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}],"stream":true}`)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func TestActiveStreamDurationBoundsProgressAcrossRoutes(t *testing.T) {
	oldMax, oldIdle := streamMaxDuration, streamIdle
	streamMaxDuration, streamIdle = 80*time.Millisecond, time.Second
	t.Cleanup(func() { streamMaxDuration, streamIdle = oldMax, oldIdle })
	for _, route := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto"} {
		t.Run(route, func(t *testing.T) {
			closed := make(chan struct{})
			var primaryHits, backupHits, chunks atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryHits.Add(1)
				defer close(closed)
				w.Header().Set("Content-Type", "text/event-stream")
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					if _, err := io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					chunks.Add(1)
					select {
					case <-r.Context().Done():
						return
					case <-ticker.C:
					}
				}
			}))
			defer up.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backupHits.Add(1)
			}))
			defer backup.Close()
			s, _ := testServer(t, []config.Provider{
				{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"},
				{Name: "b", Type: "openai", BaseURL: backup.URL, KeyEnv: "B"},
			}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "backup"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
			w := httptest.NewRecorder()
			start := time.Now()
			s.Handler().ServeHTTP(w, durationRequest(route, context.Background()))
			elapsed := time.Since(start)
			if elapsed > time.Second || elapsed < streamMaxDuration || chunks.Load() < 2 {
				t.Fatalf("active stream was not bounded by duration: elapsed=%s chunks=%d", elapsed, chunks.Load())
			}
			body := w.Body.String()
			if w.Code != http.StatusOK || !strings.Contains(body, "upstream stream exceeded maximum duration") || strings.Contains(body, "event: response.completed\n") || route != "/v1/responses" && strings.Contains(body, "data: [DONE]") {
				t.Fatalf("incorrect timeout event: status=%d body=%s", w.Code, body)
			}
			if route == "/v1/responses" && !strings.Contains(body, "event: response.failed\n") {
				t.Fatalf("missing Responses failed event: %s", body)
			}
			if route != "/v1/responses" && !strings.Contains(body, `"code":"upstream_stream_timeout"`) {
				t.Fatalf("missing Chat timeout code: %s", body)
			}
			if primaryHits.Load() != 1 || backupHits.Load() != 0 || s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatalf("timeout retried or was not charged once: hits=%d/%d health=%v keys=%v", primaryHits.Load(), backupHits.Load(), s.hlth.Snapshot(), s.pools["a"].Keys())
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("stream reader did not close the upstream body")
			}
			runtimeTelemetry(t, s, 1, http.StatusBadGateway, true)
		})
	}
}

func TestActiveStreamCompletesBeforeDurationAcrossRoutes(t *testing.T) {
	oldMax, oldIdle := streamMaxDuration, streamIdle
	streamMaxDuration, streamIdle = time.Second, time.Second
	t.Cleanup(func() { streamMaxDuration, streamIdle = oldMax, oldIdle })
	for _, route := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto"} {
		t.Run(route, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.hlth.Mark("a", "m", 500, nil)
			seedRecoveredKey(t, s)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, durationRequest(route, context.Background()))
			terminal := "data: [DONE]"
			if route == "/v1/responses" {
				terminal = "event: response.completed\n"
			}
			if !strings.Contains(w.Body.String(), terminal) || strings.Contains(w.Body.String(), "maximum duration") || s.hlth.Snapshot()[0].Failures != 0 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatalf("valid stream did not succeed: %s", w.Body)
			}
			runtimeTelemetry(t, s, 1, http.StatusOK, false)
		})
	}
}

func TestDisabledStreamDurationStillUsesIdleDeadline(t *testing.T) {
	oldMax, oldIdle := streamMaxDuration, streamIdle
	streamMaxDuration, streamIdle = 0, 40*time.Millisecond
	t.Cleanup(func() { streamMaxDuration, streamIdle = oldMax, oldIdle })
	for _, route := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(route, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			w := httptest.NewRecorder()
			start := time.Now()
			s.Handler().ServeHTTP(w, durationRequest(route, context.Background()))
			if time.Since(start) > time.Second || strings.Contains(w.Body.String(), "maximum duration") || s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatalf("disabled duration changed idle timeout: %s", w.Body)
			}
			runtimeTelemetry(t, s, 1, http.StatusBadGateway, true)
		})
	}
}

func TestCanceledStreamIsNeutralEvenWhenDurationExpired(t *testing.T) {
	oldMax := streamMaxDuration
	streamMaxDuration = time.Nanosecond
	t.Cleanup(func() { streamMaxDuration = oldMax })
	for _, route := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(route, func(t *testing.T) {
			s := runtimeServer(t, "http://unused")
			s.hlth.Mark("a", "m", 500, nil)
			seedRecoveredKey(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r := durationRequest(route, ctx)
			w := httptest.NewRecorder()
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: [DONE]\n\n"))}
			step := router.Step{Provider: "a", Model: "m"}
			if route == "/v1/responses" {
				s.relayResponsesStream(w, r, step, "main", &responses.Request{Model: "main", Stream: true}, resp, "A")
			} else {
				s.relayStream(w, r, step, "main", chatRequest{Model: "main", Stream: true}, resp, "A")
			}
			if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 || strings.Contains(w.Body.String(), "maximum duration") || strings.Contains(w.Body.String(), "response.failed") {
				t.Fatalf("cancel was charged as duration timeout: %s", w.Body)
			}
			runtimeTelemetry(t, s, 0, 0, false)
		})
	}
}

func TestSSEDataBoundsMultiLineEventsAndResetsAfterDispatch(t *testing.T) {
	var d sseData
	half := []byte("data: " + strings.Repeat("x", maxStreamEventBytes/2))
	if _, _, err := d.line(half); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.line(half); err == nil {
		t.Fatal("oversized multi-line event was accepted")
	}
	if _, ok := d.flush(); ok {
		t.Fatal("oversized event left pending data")
	}
	for _, line := range []string{"data: first\n", "data: second\n"} {
		if _, _, err := d.line([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := d.line([]byte("\n"))
	if err != nil || !ok || got != "first\nsecond" {
		t.Fatalf("multi-line dispatch = %q %v %v", got, ok, err)
	}
	if _, ok := d.flush(); ok {
		t.Fatal("event was dispatched twice")
	}
}
