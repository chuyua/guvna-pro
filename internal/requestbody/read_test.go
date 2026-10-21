package requestbody

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedBody struct {
	started, closed, returned chan struct{}
	startOnce, closeOnce      sync.Once
}

func newBlockedBody() *blockedBody {
	return &blockedBody{started: make(chan struct{}), closed: make(chan struct{}), returned: make(chan struct{})}
}

func (b *blockedBody) Read([]byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.closed
	close(b.returned)
	return 0, io.ErrClosedPipe
}

func (b *blockedBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestRequestBodyClientCancellationPreservesCause(t *testing.T) {
	for _, beforeRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "during read", true: "before read"}[beforeRead], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := newBlockedBody()
			defer body.Close()
			r := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
			r.Body, r.ContentLength = body, -1
			if beforeRead {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := Read(httptest.NewRecorder(), r, 32, time.Second)
				done <- err
			}()
			if !beforeRead {
				select {
				case <-body.started:
				case <-time.After(time.Second):
					t.Fatal("body read did not start")
				}
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, ErrClientGone) || !errors.Is(err, context.Canceled) || errors.Is(err, ErrTimeout) {
					t.Fatalf("caller cancellation lost its classification: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("caller cancellation did not stop the read")
			}
		})
	}
}

func TestRequestBodyDeadlineClassification(t *testing.T) {
	for _, callerDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "read timeout", true: "caller deadline"}[callerDeadline], func(t *testing.T) {
			ctx := context.Background()
			cancel := func() {}
			timeout := 20 * time.Millisecond
			if callerDeadline {
				ctx, cancel = context.WithTimeout(ctx, timeout)
				timeout = time.Second
			}
			defer cancel()
			body := newBlockedBody()
			defer body.Close()
			r := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
			r.Body, r.ContentLength = body, -1
			_, err := Read(httptest.NewRecorder(), r, 32, timeout)
			if callerDeadline {
				if !errors.Is(err, ErrClientGone) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrTimeout) {
					t.Fatalf("caller deadline lost its classification: %v", err)
				}
			} else if !errors.Is(err, ErrTimeout) || errors.Is(err, ErrClientGone) {
				t.Fatalf("read deadline treated as caller cancellation: %v", err)
			}
			select {
			case <-body.returned:
			default:
				t.Fatal("Read returned before its body reader stopped")
			}
		})
	}
}

func TestRequestBodyLimitsPreserveMaxBytesError(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		length      int64
		tooLarge    bool
	}{
		{"declared above", "", 9, true},
		{"actual above", "123456789", -1, true},
		{"exact", "12345678", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.input))
			r.ContentLength = tc.length
			body, err := Read(httptest.NewRecorder(), r, 8, time.Second)
			var tooLarge *http.MaxBytesError
			if tc.tooLarge {
				if !errors.As(err, &tooLarge) || tooLarge.Limit != 8 {
					t.Fatalf("body limit lost MaxBytesError: %v", err)
				}
			} else if err != nil || string(body) != tc.input {
				t.Fatalf("exact-limit body rejected: body=%q err=%v", body, err)
			}
		})
	}
}
