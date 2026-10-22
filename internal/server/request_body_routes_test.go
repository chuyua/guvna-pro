package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestRequestBodyFailuresStayNeutralAcrossRoutes(t *testing.T) {
	for _, route := range admissionRoutes {
		for _, failure := range []string{"declared", "actual", "timeout", "canceled", "read_error"} {
			t.Run(route.name+"/"+failure, func(t *testing.T) {
				requestBodyTestLimits(t, 64, 20*time.Millisecond)
				var calls atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					route.respond(w)
				}))
				defer up.Close()
				s := admissionServer(t, route, up.URL, up.URL)
				seedRecoveredKey(t, s)
				before := s.hlth.Snapshot()[0].Failures
				r := route.request(context.Background())
				r.ContentLength = -1
				status, code := 413, "request_too_large"
				switch failure {
				case "declared":
					r.ContentLength, r.Body = 65, &requestBodyReadSpy{}
				case "actual":
					r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", 65)))
				case "timeout":
					r.Body = newRequestBodyBlocked()
					status, code = 408, "request_timeout"
				case "canceled":
					ctx, cancel := context.WithCancel(r.Context())
					cancel()
					r = r.WithContext(ctx)
					r.Body = &requestBodyReadSpy{}
					status = 0
				case "read_error":
					r.Body = &requestBodyReadSpy{}
					status, code = 400, "read_body_failed"
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if status == 0 {
					if w.Body.Len() != 0 {
						t.Fatalf("canceled input produced output: %s", w.Body)
					}
				} else if w.Code != status || !strings.Contains(w.Body.String(), `"code":"`+code+`"`) {
					t.Fatalf("wrong input failure: status=%d body=%s", w.Code, w.Body)
				}
				if calls.Load() != 0 || s.hlth.Snapshot()[0].Failures != before {
					t.Fatalf("input failure reached upstream: calls=%d health=%+v", calls.Load(), s.hlth.Snapshot())
				}
				if key := s.pools["a"].Keys()[0]; key.Failures != 1 || key.State != "healthy" {
					t.Fatalf("input failure changed key: %+v", key)
				}
				runtimeTelemetry(t, s, 0, 0, false)
				requireProbeReleased(t, s)
			})
		}
	}
}

func TestRequestBodyNetworkTimeoutAcrossRoutes(t *testing.T) {
	requestBodyTestLimits(t, 1024, 30*time.Millisecond)
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto", "/v1/embeddings", "/v1/chains"} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			defer up.Close()
			s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy"})
			token := "client-1"
			if path == "/v1/chains" {
				ts = testAdminServer(t, s)
				token = "admin-secret"
			}
			conn, err := net.DialTimeout("tcp", ts.Listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer %s\r\nContent-Length: 100\r\n\r\n{", path, token)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 408 || !strings.Contains(string(body), `"code":"request_timeout"`) || !resp.Close {
				t.Fatalf("network timeout not returned: status=%d close=%v err=%v body=%s", resp.StatusCode, resp.Close, err, body)
			}
			if calls.Load() != 0 || len(s.hlth.Snapshot()) != 0 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatalf("network timeout penalized upstream: calls=%d health=%+v", calls.Load(), s.hlth.Snapshot())
			}
			if len(s.rtr.RuntimeChains()) != 0 {
				t.Fatal("network timeout created a runtime chain")
			}
		})
	}
}

func TestRequestBodyAutoAliasReadsOnceAndChargesUploadTime(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprint(slow), func(t *testing.T) {
			requestBodyTestLimits(t, 1024, time.Second)
			budget := "1s"
			if slow {
				budget = "40ms"
			}
			t.Setenv("GUVNA_AUTO_TOTAL_BUDGET", budget)
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			body := `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer client-1")
			if slow {
				reader, writer := io.Pipe()
				defer reader.Close()
				defer writer.Close()
				r.Body, r.ContentLength = reader, -1
				go func() {
					time.Sleep(80 * time.Millisecond)
					io.WriteString(writer, body)
					writer.Close()
				}()
			}
			w := &requestBodyDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
			s.Handler().ServeHTTP(w, r)
			if slow {
				if w.Code != 503 || calls.Load() != 0 {
					t.Fatalf("Auto forgot upload time: status=%d calls=%d body=%s", w.Code, calls.Load(), w.Body)
				}
			} else if w.Code != 200 || calls.Load() != 1 {
				t.Fatalf("Auto alias failed: status=%d calls=%d body=%s", w.Code, calls.Load(), w.Body)
			}
			w.mu.Lock()
			defer w.mu.Unlock()
			if len(w.deadlines) != 2 || !w.deadlines[1].IsZero() {
				t.Fatalf("Auto alias read the input again or retained deadline: %v", w.deadlines)
			}
		})
	}
}
