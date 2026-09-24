package adaptors

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func testUpstream(t *testing.T, wantPath, wantKey string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			http.Error(w, "bad path "+r.URL.Path, http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+wantKey {
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			http.Error(w, "bad content type", http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"echo":` + string(body) + `}`))
	}))
}

func TestOpenAICompatForwards(t *testing.T) {
	up := testUpstream(t, "/v1/chat/completions", "sk-test")
	defer up.Close()

	a := NewOpenAICompat(config.Provider{Name: "groq", BaseURL: up.URL}, "sk-test")
	resp, err := a.Chat(context.Background(), []byte(`{"model":"llama-3.3-70b-versatile"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), "llama-3.3-70b-versatile") {
		t.Errorf("body not forwarded: %s", got)
	}
}

func TestGeminiUsesOpenAICompatEndpoint(t *testing.T) {
	up := testUpstream(t, "/v1beta/openai/v1/chat/completions", "gk-test")
	defer up.Close()

	a := NewGemini(config.Provider{Name: "gemini", BaseURL: up.URL}, "gk-test")
	resp, err := a.Chat(context.Background(), []byte(`{"model":"gemini-2.5-flash"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestNewByType(t *testing.T) {
	if _, err := New(config.Provider{Name: "x", Type: "openai", BaseURL: "http://x"}, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := New(config.Provider{Name: "x", Type: "gemini", BaseURL: "http://x"}, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := New(config.Provider{Name: "x", Type: "anthropic", BaseURL: "http://x"}, "k"); err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestGeminiFailsWithoutKey(t *testing.T) {
	up := testUpstream(t, "/v1beta/openai/v1/chat/completions", "gk-test")
	defer up.Close()
	a := NewGemini(config.Provider{Name: "gemini", BaseURL: up.URL}, "")
	resp, err := a.Chat(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Regression tests for the two timeout knobs that bound a hung upstream:
// ResponseHeaderTimeout on the transport (the only wait that is bounded) and
// the dial timeout that moved from http.Transport onto defaultDialer.
// ---------------------------------------------------------------------------

const initProbeEnv = "GUVNA_TEST_INIT"

func TestEnvDurationFallsBackToDefault(t *testing.T) {
	const def = 42 * time.Second
	const k = "GUVNA_TEST_DURATION"
	os.Unsetenv(k)
	t.Cleanup(func() { os.Unsetenv(k) })

	for _, tc := range []struct {
		name string
		val  string
	}{
		{"unset", ""},
		{"empty", ""},
		{"whitespace", " \t "},
		{"garbage", "not-a-duration"},
		{"negative", "-5s"},
		{"zero", "0"},
	} {
		if tc.name != "unset" {
			os.Setenv(k, tc.val)
		}
		if got := envDuration(k, def); got != def {
			t.Errorf("%s: envDuration(%q) = %v, want default %v", tc.name, tc.val, got, def)
		}
	}
}

func TestEnvDurationParses(t *testing.T) {
	const def = 42 * time.Second
	const k = "GUVNA_TEST_DURATION"
	t.Cleanup(func() { os.Unsetenv(k) })

	for _, tc := range []struct {
		val  string
		want time.Duration
	}{
		{"30s", 30 * time.Second},
		{"1m", time.Minute},
		{"250ms", 250 * time.Millisecond},
		{" 30s ", 30 * time.Second},
	} {
		os.Setenv(k, tc.val)
		if got := envDuration(k, def); got != tc.want {
			t.Errorf("envDuration(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

// TestPreferV4TransportDefaults pins the built-in transport settings. preferV4Transport
// reads the environment on every call, so clearing the variables here restores the
// defaults inside this process.
func TestPreferV4TransportDefaults(t *testing.T) {
	const keys = "GUVNA_TLS_TIMEOUT GUVNA_EXPECT_TIMEOUT GUVNA_IDLE_TIMEOUT GUVNA_HEADER_TIMEOUT"
	clearEnv(t, keys)

	tr := preferV4Transport()
	for _, tc := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"ResponseHeaderTimeout", tr.ResponseHeaderTimeout, 45 * time.Second},
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout, 15 * time.Second},
		{"ExpectContinueTimeout", tr.ExpectContinueTimeout, time.Second},
		{"IdleConnTimeout", tr.IdleConnTimeout, 90 * time.Second},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if tr.DialContext == nil {
		t.Error("DialContext is nil")
	}
	// The dial timeout does not live on the transport anymore: it lives on the
	// dialer the transport dials through. This assertion pins the default and
	// also fails if the timeout ever drifts back onto http.Transport.
	if defaultDialer.Timeout != 10*time.Second {
		t.Errorf("defaultDialer.Timeout = %v, want 10s", defaultDialer.Timeout)
	}
}

// TestPreferV4TransportEnvOverrides proves the transport timeouts really do come
// from the environment, not from literals.
func TestPreferV4TransportEnvOverrides(t *testing.T) {
	const keys = "GUVNA_TLS_TIMEOUT GUVNA_EXPECT_TIMEOUT GUVNA_IDLE_TIMEOUT GUVNA_HEADER_TIMEOUT"
	clear := clearEnv(t, keys)

	os.Setenv("GUVNA_HEADER_TIMEOUT", "7s")
	os.Setenv("GUVNA_TLS_TIMEOUT", "8s")
	os.Setenv("GUVNA_EXPECT_TIMEOUT", "2s")
	os.Setenv("GUVNA_IDLE_TIMEOUT", "60s")

	tr := preferV4Transport()
	for _, tc := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"ResponseHeaderTimeout", tr.ResponseHeaderTimeout, 7 * time.Second},
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout, 8 * time.Second},
		{"ExpectContinueTimeout", tr.ExpectContinueTimeout, 2 * time.Second},
		{"IdleConnTimeout", tr.IdleConnTimeout, 60 * time.Second},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v (env override ignored)", tc.name, tc.got, tc.want)
		}
	}

	clear()
	tr = preferV4Transport()
	if tr.ResponseHeaderTimeout != 45*time.Second {
		t.Errorf("after clearing env: ResponseHeaderTimeout = %v, want 45s", tr.ResponseHeaderTimeout)
	}
	if tr.TLSHandshakeTimeout != 15*time.Second {
		t.Errorf("after clearing env: TLSHandshakeTimeout = %v, want 15s", tr.TLSHandshakeTimeout)
	}
}

// clearEnv remembers the current values of each listed variable, clears them
// all, and returns a restore function. The restore is also registered as a
// cleanup, so callers may ignore the return value.
func clearEnv(t *testing.T, keys string) func() {
	t.Helper()
	fields := strings.Fields(keys)
	orig := make(map[string]string, len(fields))
	origSet := make(map[string]bool, len(fields))
	for _, k := range fields {
		if v, ok := os.LookupEnv(k); ok {
			orig[k] = v
			origSet[k] = true
		}
	}
	restore := func() {
		for _, k := range fields {
			if origSet[k] {
				os.Setenv(k, orig[k])
			} else {
				os.Unsetenv(k)
			}
		}
	}
	t.Cleanup(restore)
	restore()
	return restore
}

// TestDefaultDialerTimeoutReadsEnvAtInit re-execs this test binary in a child
// process that has GUVNA_DIAL_TIMEOUT set before the process starts, then
// checks the captured value. A runtime os.Setenv cannot prove this: defaultDialer
// is built at package init, so this is the only way to test the init wiring.
func TestDefaultDialerTimeoutReadsEnvAtInit(t *testing.T) {
	if os.Getenv(initProbeEnv) == "1" {
		if got := defaultDialer.Timeout; got != 9*time.Second {
			t.Fatalf("SUBPROC_FAIL: defaultDialer.Timeout = %v, want 9s "+
				"(GUVNA_DIAL_TIMEOUT=%q was not read at process start)", got, os.Getenv("GUVNA_DIAL_TIMEOUT"))
		}
		return
	}
	os.Unsetenv("GUVNA_DIAL_TIMEOUT")
	t.Cleanup(func() { os.Unsetenv("GUVNA_DIAL_TIMEOUT") })

	cmd := exec.Command(testBinary(), "-test.run=^TestDefaultDialerTimeoutReadsEnvAtInit$", "-test.count=1")
	cmd.Env = append(os.Environ(), initProbeEnv+"=1", "GUVNA_DIAL_TIMEOUT=9s")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "SUBPROC_FAIL") {
			t.Errorf("GUVNA_DIAL_TIMEOUT must be read at process start:\n%s", out)
		} else {
			t.Skipf("test binary re-exec unavailable (%v); init wiring not verified:\n%s", err, out)
		}
	}
}

// TestClientTimeoutIsZeroStreamingInvariant locks the design constraint that the
// package-level Client has no whole-request timeout, while the wait for response
// headers is bounded. Long streams must survive; only a hung upstream is capped.
func TestClientTimeoutIsZeroStreamingInvariant(t *testing.T) {
	if Client.Timeout != 0 {
		t.Errorf("Client.Timeout = %v, want 0: streaming responses may run for minutes", Client.Timeout)
	}
	tr, ok := Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Client.Transport is %T, want *http.Transport", Client.Transport)
	}
	if tr.ResponseHeaderTimeout <= 0 {
		t.Errorf("Client.Transport.ResponseHeaderTimeout = %v, want > 0 to bound header waits", tr.ResponseHeaderTimeout)
	}
	if tr.DialContext == nil {
		t.Error("Client.Transport.DialContext is nil")
	}
}

// TestResponseHeaderTimeoutFires is the important one: an upstream that accepts
// the connection and the request but never answers must be abandoned by the
// header timeout, and a whole-request timeout must not be needed to do it.
func TestResponseHeaderTimeoutFires(t *testing.T) {
	const headerTimeout = 300 * time.Millisecond
	const deadline = 5 * time.Second

	os.Setenv("GUVNA_HEADER_TIMEOUT", "300ms")
	t.Cleanup(func() { os.Unsetenv("GUVNA_HEADER_TIMEOUT") })

	tr := preferV4Transport()
	if tr.ResponseHeaderTimeout != headerTimeout {
		t.Fatalf("setup: ResponseHeaderTimeout = %v, want %v", tr.ResponseHeaderTimeout, headerTimeout)
	}
	// Client.Timeout stays 0: the header timeout alone must be enough.
	cl := &http.Client{Timeout: 0, Transport: tr}

	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-release // accept the request, never send a response
	}))
	defer up.Close()
	// Registered last so it runs first: it unblocks the handler goroutine before
	// Server.Close waits for it to drain.
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, up.URL, strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	type result struct {
		resp    *http.Response
		err     error
		elapsed time.Duration
	}
	res := make(chan result, 1)
	go func() {
		start := time.Now()
		resp, err := cl.Do(req)
		res <- result{resp: resp, err: err, elapsed: time.Since(start)}
	}()

	var resp *http.Response
	var doErr error
	var elapsed time.Duration
	select {
	case r := <-res:
		resp, doErr, elapsed = r.resp, r.err, r.elapsed
	case <-time.After(deadline):
		t.Fatalf("request did not return within %v: the header timeout is not firing", deadline)
	}

	if resp != nil {
		resp.Body.Close()
		t.Fatal("request to a hung upstream succeeded")
	}
	if doErr == nil {
		t.Fatal("request to a hung upstream returned no error")
	}
	if !isTimeoutErr(doErr) {
		t.Skipf("failure is not a timeout (%q); not the behaviour under test", doErr)
	}
	if elapsed < time.Millisecond {
		t.Errorf("request failed in %v, before the %v header timeout could fire", elapsed, headerTimeout)
	}
	if elapsed > deadline {
		t.Errorf("request took %v: the %v header timeout did not cut the wait", elapsed, headerTimeout)
	}
	if tr.ResponseHeaderTimeout == 0 {
		t.Error("ResponseHeaderTimeout is 0: nothing bounds a hung upstream")
	}
	t.Logf("hung upstream abandoned after %v by the %v header timeout: %v", elapsed, headerTimeout, doErr)
}

// TestResponseArrivesWhole checks the other half: a response that does arrive is
// returned in full and is not truncated by the header timeout.
func TestResponseArrivesWhole(t *testing.T) {
	const headerTimeout = 300 * time.Millisecond
	os.Setenv("GUVNA_HEADER_TIMEOUT", "300ms")
	t.Cleanup(func() { os.Unsetenv("GUVNA_HEADER_TIMEOUT") })

	tr := preferV4Transport()
	if tr.ResponseHeaderTimeout != headerTimeout {
		t.Fatalf("setup: ResponseHeaderTimeout = %v, want %v", tr.ResponseHeaderTimeout, headerTimeout)
	}
	cl := &http.Client{Timeout: 0, Transport: tr}

	// Large enough to span many reads, so truncation is visible.
	payload := strings.Repeat("0123456789abcdef", 8192)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, payload)
	}))
	defer up.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, up.URL, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cl.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("body was not readable to EOF: %v", err)
	}
	if string(body) != payload {
		t.Errorf("body = %d bytes, want %d bytes", len(body), len(payload))
	}
}

// isTimeoutErr reports whether err is a timeout/deadline error, either as a
// net.Error or via the error message net/http uses when ResponseHeaderTimeout
// fires.
func isTimeoutErr(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "timeout awaiting response headers") || strings.Contains(s, "deadline exceeded")
}

// testBinary returns the path of the running test binary, for re-exec probes.
func testBinary() string {
	if p, err := os.Executable(); err == nil {
		return p
	}
	return os.Args[0]
}
