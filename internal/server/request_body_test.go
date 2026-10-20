package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func requestBodyTestLimits(t *testing.T, size int64, timeout time.Duration) {
	t.Helper()
	oldSize, oldTimeout := maxRequestBytes, requestReadTimeout
	maxRequestBytes, requestReadTimeout = size, timeout
	t.Cleanup(func() { maxRequestBytes, requestReadTimeout = oldSize, oldTimeout })
}

func requestBodyTestHandler(w http.ResponseWriter, r *http.Request) {
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	w.Write(body)
}

type requestBodyReadSpy struct {
	reads atomic.Int32
}

func (b *requestBodyReadSpy) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, errors.New("private input detail")
}

func (b *requestBodyReadSpy) Close() error { return nil }

func TestRequestBodyContentLengthRejectedBeforeRead(t *testing.T) {
	requestBodyTestLimits(t, 8, time.Second)
	body := &requestBodyReadSpy{}
	r := httptest.NewRequest("POST", "/", nil)
	r.Body, r.ContentLength = body, 9
	w := httptest.NewRecorder()
	_, err := readRequestBody(w, r)
	writeRequestBodyError(w, r, err)
	if body.reads.Load() != 0 || w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), `"code":"request_too_large"`) {
		t.Fatalf("preflight: reads=%d status=%d body=%s", body.reads.Load(), w.Code, w.Body)
	}
}

func TestRequestBodyExactAndUnknownLengthLimits(t *testing.T) {
	requestBodyTestLimits(t, 8, time.Second)
	for _, tc := range []struct {
		name   string
		input  string
		length int64
		status int
	}{
		{"known below", "123", 3, 200},
		{"known exact", "12345678", 8, 200},
		{"unknown exact", "12345678", -1, 200},
		{"unknown above", "123456789", -1, 413},
		{"dishonest length", "123456789", 2, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", strings.NewReader(tc.input))
			r.ContentLength = tc.length
			w := httptest.NewRecorder()
			requestBodyTestHandler(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.status == 200 && w.Body.String() != tc.input {
				t.Fatalf("body changed: %s", w.Body)
			}
		})
	}
}

type requestBodyBlocked struct {
	started   chan struct{}
	closed    chan struct{}
	returned  chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	readOnce  sync.Once
}

func newRequestBodyBlocked() *requestBodyBlocked {
	return &requestBodyBlocked{started: make(chan struct{}), closed: make(chan struct{}), returned: make(chan struct{})}
}

func (b *requestBodyBlocked) Read([]byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.closed
	b.readOnce.Do(func() { close(b.returned) })
	return 0, io.ErrClosedPipe
}

func (b *requestBodyBlocked) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestRequestBodyCustomBodyTimeoutUnblocksRead(t *testing.T) {
	requestBodyTestLimits(t, 32, 30*time.Millisecond)
	body := newRequestBodyBlocked()
	r := httptest.NewRequest("POST", "/", nil)
	r.Body, r.ContentLength = body, -1
	w := httptest.NewRecorder()
	start := time.Now()
	_, err := readRequestBody(w, r)
	writeRequestBodyError(w, r, err)
	if !errors.Is(err, errRequestBodyTimeout) || w.Code != 408 || time.Since(start) > time.Second {
		t.Fatalf("timeout not enforced: err=%v status=%d elapsed=%s", err, w.Code, time.Since(start))
	}
	select {
	case <-body.returned:
	default:
		t.Fatal("body reader did not exit before helper returned")
	}
}

func TestRequestBodyCallerCancellationIsNeutral(t *testing.T) {
	requestBodyTestLimits(t, 32, time.Second)
	for _, preCanceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "during read", true: "before read"}[preCanceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := newRequestBodyBlocked()
			r := httptest.NewRequest("POST", "/", nil).WithContext(ctx)
			r.Body, r.ContentLength = body, -1
			w := httptest.NewRecorder()
			if preCanceled {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := readRequestBody(w, r)
				writeRequestBodyError(w, r, err)
				done <- err
			}()
			if !preCanceled {
				select {
				case <-body.started:
				case <-time.After(time.Second):
					t.Fatal("read did not start")
				}
				cancel()
			}
			select {
			case err := <-done:
				if !isClientGone(err) || w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
					t.Fatalf("cancellation not neutral: err=%v body=%s", err, w.Body)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled body read did not stop")
			}
		})
	}
}

func TestRequestBodyTimeoutDisabled(t *testing.T) {
	requestBodyTestLimits(t, 32, 0)
	reader, writer := io.Pipe()
	t.Cleanup(func() { reader.Close(); writer.Close() })
	r := httptest.NewRequest("POST", "/", reader)
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(60 * time.Millisecond)
		io.WriteString(writer, "slow but permitted")
		writer.Close()
	}()
	body, err := readRequestBody(httptest.NewRecorder(), r)
	<-done
	if err != nil || string(body) != "slow but permitted" {
		t.Fatalf("disabled timeout rejected body: err=%v body=%q", err, body)
	}
}

func TestRequestBodyCallerDeadlineIsNeutral(t *testing.T) {
	requestBodyTestLimits(t, 32, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	body := newRequestBodyBlocked()
	r := httptest.NewRequest("POST", "/", nil).WithContext(ctx)
	r.Body, r.ContentLength = body, -1
	w := httptest.NewRecorder()
	_, err := readRequestBody(w, r)
	writeRequestBodyError(w, r, err)
	if !isClientGone(err) || w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" {
		t.Fatalf("caller deadline treated as gateway timeout: err=%v body=%s", err, w.Body)
	}
}

type requestBodyDeadlineWriter struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
}

func (w *requestBodyDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

type requestBodyWrappedWriter struct{ http.ResponseWriter }

func (w requestBodyWrappedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestRequestBodyWrappedDeadlineClearedWithoutLateCallback(t *testing.T) {
	requestBodyTestLimits(t, 32, 30*time.Millisecond)
	w := &requestBodyDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest("POST", "/", strings.NewReader("ok"))
	if _, err := readRequestBody(requestBodyWrappedWriter{w}, r); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.deadlines) != 2 || w.deadlines[0].IsZero() || !w.deadlines[1].IsZero() {
		t.Fatalf("deadline not cleared or callback fired late: %v", w.deadlines)
	}
}

func TestRequestBodyReadErrorDoesNotExposeInput(t *testing.T) {
	requestBodyTestLimits(t, 32, time.Second)
	r := httptest.NewRequest("POST", "/", nil)
	r.Body = &requestBodyReadSpy{}
	w := httptest.NewRecorder()
	requestBodyTestHandler(w, r)
	if w.Code != 400 || strings.Contains(w.Body.String(), "private") || !strings.Contains(w.Body.String(), `"code":"read_body_failed"`) {
		t.Fatalf("unsafe read error: status=%d body=%s", w.Code, w.Body)
	}
}

func TestRequestBodyNetworkIncompleteUploads(t *testing.T) {
	requestBodyTestLimits(t, 8, 50*time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(requestBodyTestHandler))
	t.Cleanup(server.Close)
	for _, tc := range []struct {
		name, wire string
		status     int
	}{
		{"slow chunked", "Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n", 408},
		{"large content length without body", "Content-Length: 9\r\n\r\n", 413},
		{"oversized incomplete chunked", "Transfer-Encoding: chunked\r\n\r\n9\r\n123456789\r\n", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: test\r\n"+tc.wire); err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("server did not promptly reject incomplete upload: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != tc.status || !resp.Close {
				t.Fatalf("status=%d close=%v body=%s err=%v", resp.StatusCode, resp.Close, body, err)
			}
		})
	}
}

func TestRequestBodyNetworkSuccessDoesNotLimitResponseOrReuse(t *testing.T) {
	requestBodyTestLimits(t, 32, 100*time.Millisecond)
	for _, h2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "http1", true: "http2"}[h2], func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := readRequestBody(w, r)
				if err != nil {
					writeRequestBodyError(w, r, err)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: "+string(body)+"\n\n")
				w.(http.Flusher).Flush()
				time.Sleep(250 * time.Millisecond)
				io.WriteString(w, "data: complete\n\n")
			}))
			server.EnableHTTP2 = h2
			server.StartTLS()
			t.Cleanup(server.Close)
			client := server.Client()
			client.Timeout = 3 * time.Second
			for i := 0; i < 2; i++ {
				var reused bool
				req, err := http.NewRequest("POST", server.URL, strings.NewReader("ok"))
				if err != nil {
					t.Fatal(err)
				}
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 200 || h2 && resp.ProtoMajor != 2 || string(body) != "data: ok\n\ndata: complete\n\n" || i > 0 && !reused {
					t.Fatalf("response limited: err=%v status=%d proto=%s reused=%v body=%s", err, resp.StatusCode, resp.Proto, reused, body)
				}
			}
		})
	}
}

func TestRequestBodyHTTP2SlowUploadTimesOut(t *testing.T) {
	requestBodyTestLimits(t, 32, 50*time.Millisecond)
	server := httptest.NewUnstartedServer(http.HandlerFunc(requestBodyTestHandler))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	reader, writer := io.Pipe()
	release, writerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		io.WriteString(writer, "x")
		<-release
		writer.Close()
	}()
	t.Cleanup(func() {
		close(release)
		reader.Close()
		writer.Close()
		<-writerDone
	})
	client := server.Client()
	client.Timeout = 2 * time.Second
	resp, err := client.Post(server.URL, "application/json", reader)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.ProtoMajor != 2 || resp.StatusCode != 408 || !strings.Contains(string(body), `"code":"request_timeout"`) {
		t.Fatalf("HTTP/2 timeout: err=%v proto=%s status=%d body=%s", err, resp.Proto, resp.StatusCode, body)
	}
}
