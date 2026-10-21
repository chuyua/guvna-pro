package dashboard

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func formGuardDashboard(t *testing.T, handler http.Handler) *Dashboard {
	t.Helper()
	gw := httptest.NewServer(handler)
	t.Cleanup(gw.Close)
	d, err := New(gw.URL, "test-admin-key", testDashUser, testDashPass)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func formGuardRequest(d *Dashboard, path, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if path == "/chains" {
		r.AddCookie(&http.Cookie{Name: sessionCookie, Value: d.auth.issue(testDashUser, time.Now())})
		r.Header.Set("HX-Request", "true")
	}
	return r
}

func formGuardInput(path string) string {
	if path == "/login" {
		return url.Values{"user": {testDashUser}, "password": {testDashPass}}.Encode()
	}
	return url.Values{"name": {"newchain"}, "steps": {"groq:llama"}}.Encode()
}

type formGuardBody struct {
	reads   atomic.Int32
	blocked chan struct{}
	closed  sync.Once
}

func (b *formGuardBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	if b.blocked != nil {
		<-b.blocked
		return 0, io.ErrClosedPipe
	}
	return 0, errors.New("private-password read failure")
}

func (b *formGuardBody) Close() error {
	if b.blocked != nil {
		b.closed.Do(func() { close(b.blocked) })
	}
	return nil
}

func TestDashboardFormFailuresRejectBeforeGateway(t *testing.T) {
	for _, path := range []string{"/login", "/chains"} {
		for _, failure := range []string{"declared", "actual", "timeout", "canceled", "read_error", "malformed"} {
			t.Run(path+"/"+failure, func(t *testing.T) {
				var calls atomic.Int32
				d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
				d.formMaxBytes, d.formReadTimeout = 64, 20*time.Millisecond
				r := formGuardRequest(d, path, "user=private-user&password=private-password")
				r.ContentLength = -1
				spy := &formGuardBody{}
				status, message := http.StatusRequestEntityTooLarge, "form submission is too large"
				switch failure {
				case "declared":
					r.ContentLength, r.Body = 65, spy
				case "actual":
					r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", 65)))
				case "timeout":
					r.Body = &formGuardBody{blocked: make(chan struct{})}
					status, message = http.StatusRequestTimeout, "form submission timed out"
				case "canceled":
					ctx, cancel := context.WithCancel(r.Context())
					cancel()
					r, status = r.WithContext(ctx), 0
					r.Body = spy
				case "read_error":
					r.Body = spy
					status, message = http.StatusBadRequest, "bad form submission"
				case "malformed":
					r.Body = io.NopCloser(strings.NewReader("password=private-password&bad=%zz"))
					status, message = http.StatusBadRequest, "bad form submission"
				}
				w := httptest.NewRecorder()
				d.Handler().ServeHTTP(w, r)
				if status == 0 {
					if w.Body.Len() != 0 {
						t.Fatal("canceled input wrote a response")
					}
				} else if w.Code != status || !strings.Contains(w.Body.String(), message) || w.Header().Get("X-Guvna-Form-Error") != "true" {
					t.Fatalf("status=%d body=%s", w.Code, w.Body)
				}
				for _, secret := range []string{"private-password", "private-user", "read failure", "test-admin-key"} {
					if strings.Contains(w.Body.String(), secret) {
						t.Fatalf("response exposed %q", secret)
					}
				}
				if calls.Load() != 0 || len(w.Result().Cookies()) != 0 || len(d.auth.attempts) != 0 {
					t.Fatalf("rejected input had side effects: gateway=%d cookies=%v attempts=%v", calls.Load(), w.Result().Cookies(), d.auth.attempts)
				}
				if (failure == "declared" || failure == "canceled") && spy.reads.Load() != 0 {
					t.Fatal("preflight rejection consumed the body")
				}
			})
		}
	}
}

func TestDashboardFormExactLimitAndQuerySemantics(t *testing.T) {
	for _, path := range []string{"/login", "/chains"} {
		for _, unknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unknown=%v", path, unknown), func(t *testing.T) {
				var posts atomic.Int32
				d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						posts.Add(1)
						w.WriteHeader(http.StatusCreated)
						return
					}
					io.WriteString(w, `{"chains":[]}`)
				}))
				input := formGuardInput(path)
				d.formMaxBytes = int64(len(input))
				r := formGuardRequest(d, path, input)
				if unknown {
					r.ContentLength = -1
				}
				// Body fields keep precedence over query fields, as before.
				r.URL.RawQuery = "user=wrong&password=wrong&name=wrong"
				w := httptest.NewRecorder()
				d.Handler().ServeHTTP(w, r)
				if path == "/login" {
					if w.Code != http.StatusFound || len(w.Result().Cookies()) != 1 {
						t.Fatalf("exact-limit login: status=%d body=%s", w.Code, w.Body)
					}
				} else if w.Code != http.StatusOK || posts.Load() != 1 {
					t.Fatalf("exact-limit chain: status=%d posts=%d body=%s", w.Code, posts.Load(), w.Body)
				}
			})
		}
	}
}

func TestDashboardConfiguredFormLimitAboveParseFormDefault(t *testing.T) {
	d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("login contacted gateway") }))
	input := formGuardInput("/login") + "&padding=" + strings.Repeat("x", 11<<20)
	d.formMaxBytes, d.formReadTimeout = int64(len(input)), 0
	w := httptest.NewRecorder()
	d.Handler().ServeHTTP(w, formGuardRequest(d, "/login", input))
	if w.Code != http.StatusFound || len(w.Result().Cookies()) != 1 {
		t.Fatalf("configured form limit was replaced by ParseForm default: status=%d", w.Code)
	}
}

func TestDashboardFormConfigurationCapturedPerInstance(t *testing.T) {
	for _, tc := range []struct {
		bytes, timeout string
		wantBytes      int64
		wantTimeout    time.Duration
	}{
		{"", "", defaultFormMaxBytes, defaultFormReadTimeout},
		{"0", "-1s", defaultFormMaxBytes, defaultFormReadTimeout},
		{"-1", "bad", defaultFormMaxBytes, defaultFormReadTimeout},
		{"9223372036854775808", "9223372036854775808h", defaultFormMaxBytes, defaultFormReadTimeout},
		{"bad", "0", defaultFormMaxBytes, 0},
		{" 64 ", " 25ms ", 64, 25 * time.Millisecond},
	} {
		t.Run(tc.bytes+"/"+tc.timeout, func(t *testing.T) {
			t.Setenv("GUVNA_DASHBOARD_MAX_FORM_BYTES", tc.bytes)
			t.Setenv("GUVNA_DASHBOARD_FORM_READ_TIMEOUT", tc.timeout)
			d, err := New("http://unused", "test-admin-key", testDashUser, testDashPass)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("GUVNA_DASHBOARD_MAX_FORM_BYTES", "128")
			t.Setenv("GUVNA_DASHBOARD_FORM_READ_TIMEOUT", "1s")
			if d.formMaxBytes != tc.wantBytes || d.formReadTimeout != tc.wantTimeout {
				t.Fatalf("limits changed after New: bytes=%d timeout=%s", d.formMaxBytes, d.formReadTimeout)
			}
		})
	}
}

func TestDashboardUnauthenticatedFormsDoNotRead(t *testing.T) {
	for _, htmx := range []bool{false, true} {
		t.Run(fmt.Sprint(htmx), func(t *testing.T) {
			var calls atomic.Int32
			d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			spy := &formGuardBody{}
			r := httptest.NewRequest(http.MethodPost, "/chains", nil)
			r.Body, r.ContentLength = spy, 100
			if htmx {
				r.Header.Set("HX-Request", "true")
			}
			w := httptest.NewRecorder()
			d.Handler().ServeHTTP(w, r)
			if spy.reads.Load() != 0 || calls.Load() != 0 || w.Header().Get("Connection") != "close" {
				t.Fatalf("unauthenticated body read or forwarded: reads=%d calls=%d headers=%v", spy.reads.Load(), calls.Load(), w.Header())
			}
			if htmx {
				if w.Code != 401 || w.Header().Get("HX-Redirect") != "/login" {
					t.Fatalf("HTMX auth changed: status=%d headers=%v", w.Code, w.Header())
				}
			} else if w.Code != 302 || w.Header().Get("Location") != "/login" {
				t.Fatalf("navigation auth changed: status=%d headers=%v", w.Code, w.Header())
			}
		})
	}
}

func TestDashboardSlowFormNetworkTimeout(t *testing.T) {
	for _, path := range []string{"/login", "/chains", "/chains-unauthenticated"} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			d.formReadTimeout = 25 * time.Millisecond
			s := httptest.NewServer(d.Handler())
			defer s.Close()
			conn, err := net.DialTimeout("tcp", s.Listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			cookie, target, want := "", path, http.StatusRequestTimeout
			if path == "/chains" {
				cookie = "Cookie: " + sessionCookie + "=" + d.auth.issue(testDashUser, time.Now()) + "\r\n"
			} else if path == "/chains-unauthenticated" {
				target, want = "/chains", http.StatusFound
			}
			fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n%s\r\nx", target, cookie)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if _, err := io.Copy(io.Discard, resp.Body); err != nil || resp.StatusCode != want || !resp.Close || calls.Load() != 0 {
				t.Fatalf("slow upload: status=%d close=%v gateway=%d err=%v", resp.StatusCode, resp.Close, calls.Load(), err)
			}
		})
	}
}

func TestDashboardSuccessfulFormClearsDeadlineAndReusesConnection(t *testing.T) {
	var posts atomic.Int32
	d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			time.Sleep(80 * time.Millisecond)
			w.WriteHeader(http.StatusCreated)
			return
		}
		io.WriteString(w, `{"chains":[]}`)
	}))
	d.formReadTimeout = 25 * time.Millisecond
	s := httptest.NewServer(d.Handler())
	defer s.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for i := 0; i < 2; i++ {
		r, err := http.NewRequest(http.MethodPost, s.URL+"/chains", strings.NewReader(formGuardInput("/chains")))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: sessionCookie, Value: d.auth.issue(testDashUser, time.Now())})
		reused := false
		r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK || resp.Close || reused != (i > 0) {
			t.Fatalf("post-form response: status=%d close=%v reused=%v err=%v", resp.StatusCode, resp.Close, reused, readErr)
		}
	}
	if posts.Load() != 2 {
		t.Fatalf("gateway posts=%d", posts.Load())
	}
}

func TestDashboardSlowFormHTTP2Timeout(t *testing.T) {
	for _, path := range []string{"/login", "/chains"} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			d := formGuardDashboard(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			d.formReadTimeout = 25 * time.Millisecond
			s := httptest.NewUnstartedServer(d.Handler())
			s.EnableHTTP2 = true
			s.StartTLS()
			defer s.Close()
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			written := make(chan struct{})
			go func() {
				defer close(written)
				_, _ = io.WriteString(writer, "user=")
			}()
			r, err := http.NewRequest(http.MethodPost, s.URL+path, reader)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if path == "/chains" {
				r.AddCookie(&http.Cookie{Name: sessionCookie, Value: d.auth.issue(testDashUser, time.Now())})
			}
			client := s.Client()
			client.Timeout = 2 * time.Second
			resp, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if _, err := io.Copy(io.Discard, resp.Body); err != nil || resp.StatusCode != 408 || resp.ProtoMajor != 2 || resp.Header.Get("Connection") != "" || calls.Load() != 0 {
				t.Fatalf("HTTP2 form timeout: status=%d proto=%s gateway=%d err=%v", resp.StatusCode, resp.Proto, calls.Load(), err)
			}
			select {
			case <-written:
			case <-time.After(time.Second):
				t.Fatal("request writer did not finish")
			}
		})
	}
}
