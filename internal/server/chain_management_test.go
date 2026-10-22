package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/chains"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/router"
)

func chainManagementServer(t *testing.T) *Server {
	t.Helper()
	s, _ := testServer(t,
		[]config.Provider{{Name: "a", Type: "openai", BaseURL: "http://unused", KeyEnv: "A"}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}},
		map[string]string{"A": "dummy"})
	return s
}

func chainManagementRequest(s *Server, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	return w
}

func TestChainCreateBodyFailuresDoNotMutate(t *testing.T) {
	const input = `{"name":"new-chain","steps":[{"provider":"a","model":"m"}]}`
	for _, failure := range []string{"declared", "actual", "timeout", "canceled", "read_error"} {
		t.Run(failure, func(t *testing.T) {
			requestBodyTestLimits(t, int64(len(input)), 20*time.Millisecond)
			s := chainManagementServer(t)
			dataDir := t.TempDir()
			s.store = chains.New(dataDir)
			before := s.rtr.ListChains()
			r := httptest.NewRequest(http.MethodPost, "/v1/chains", strings.NewReader(input))
			r.Header.Set("Authorization", "Bearer admin-secret")
			r.ContentLength = -1
			status, code := http.StatusRequestEntityTooLarge, "request_too_large"
			spy := &requestBodyReadSpy{}
			switch failure {
			case "declared":
				r.ContentLength, r.Body = int64(len(input)+1), spy
			case "actual":
				r.Body = io.NopCloser(strings.NewReader(input + " "))
			case "timeout":
				r.Body = newRequestBodyBlocked()
				status, code = http.StatusRequestTimeout, "request_timeout"
			case "canceled":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r, status = r.WithContext(ctx), 0
				r.Body = spy
			case "read_error":
				r.Body = spy
				status, code = http.StatusBadRequest, "read_body_failed"
			}
			w := httptest.NewRecorder()
			s.AdminHandler().ServeHTTP(w, r)
			if status == 0 {
				if w.Body.Len() != 0 {
					t.Fatalf("canceled creation wrote a response: %s", w.Body)
				}
			} else if w.Code != status || !strings.Contains(w.Body.String(), `"code":"`+code+`"`) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if (failure == "declared" || failure == "canceled") && spy.reads.Load() != 0 {
				t.Fatal("rejected creation consumed its body")
			}
			if !reflect.DeepEqual(before, s.rtr.ListChains()) {
				t.Fatal("body failure changed the router")
			}
			if _, err := os.Stat(filepath.Join(dataDir, "chains.yaml")); !os.IsNotExist(err) {
				t.Fatalf("body failure touched the runtime store: %v", err)
			}
			if len(s.hlth.Snapshot()) != 0 || s.pools["a"].Keys()[0].Failures != 0 {
				t.Fatal("body failure changed upstream health")
			}
		})
	}
}

func TestChainCreateExactLimitAndTrailingData(t *testing.T) {
	const input = `{"name":"new-chain","steps":[{"provider":"a","model":"m"}]}`
	for _, tc := range []struct {
		name, suffix string
		unknown      bool
		status       int
	}{
		{name: "known exact", status: http.StatusCreated},
		{name: "unknown exact", unknown: true, status: http.StatusCreated},
		{name: "whitespace", suffix: " \n\t", status: http.StatusCreated},
		{name: "second json", suffix: ` {"name":"ignored"}`, status: http.StatusBadRequest},
		{name: "garbage", suffix: " garbage", status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := input + tc.suffix
			requestBodyTestLimits(t, int64(len(body)), time.Second)
			s := chainManagementServer(t)
			r := httptest.NewRequest(http.MethodPost, "/v1/chains", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer admin-secret")
			if tc.unknown {
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			s.AdminHandler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			_, exists := s.rtr.RuntimeChains()["new-chain"]
			if exists != (tc.status == http.StatusCreated) {
				t.Fatalf("unexpected router mutation: %+v", s.rtr.RuntimeChains())
			}
		})
	}
}

func TestChainMutationPersistenceFailureIsAtomicAndRetryable(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			s := chainManagementServer(t)
			parent := t.TempDir()
			dataDir := filepath.Join(parent, "data")
			s.store = chains.New(dataDir)
			for _, name := range []string{"first", "second"} {
				body := `{"name":"` + name + `","steps":[{"provider":"a","model":"shared"}]}`
				if w := chainManagementRequest(s, http.MethodPost, "/v1/chains", body); w.Code != http.StatusCreated {
					t.Fatalf("seed creation: status=%d body=%s", w.Code, w.Body)
				}
			}
			before := s.rtr.ListChains()
			original, err := os.ReadFile(filepath.Join(dataDir, "chains.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			// Make the same Store's directory unavailable without altering its
			// previous file. This works without platform-specific file locking.
			savedDir := filepath.Join(parent, "saved")
			if err := os.Rename(dataDir, savedDir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dataDir, []byte("blocked"), 0o600); err != nil {
				t.Fatal(err)
			}
			path, body, success := "/v1/chains", `{"name":"third","steps":[{"provider":"a","model":"shared"}]}`, http.StatusCreated
			if method == http.MethodDelete {
				path, body, success = "/v1/chains/first", "", http.StatusNoContent
			}
			w := chainManagementRequest(s, method, path, body)
			if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "but not persisted") {
				t.Fatalf("persistence failure: status=%d body=%s", w.Code, w.Body)
			}
			if !reflect.DeepEqual(before, s.rtr.ListChains()) {
				t.Fatalf("failed %s changed the router: %+v", method, s.rtr.ListChains())
			}
			if name, _, err := s.rtr.Resolve("shared"); err != nil || name != "second" {
				t.Fatalf("failed %s changed alias priority: name=%q err=%v", method, name, err)
			}
			preserved, err := os.ReadFile(filepath.Join(savedDir, "chains.yaml"))
			if err != nil || !bytes.Equal(original, preserved) {
				t.Fatalf("failed mutation changed saved bytes: err=%v", err)
			}
			// Validation must still win over an unavailable persistence path.
			for _, tc := range []struct {
				method, path, body string
				status             int
			}{
				{http.MethodPost, "/v1/chains", `{"name":"first","steps":[{"provider":"a","model":"m"}]}`, http.StatusConflict},
				{http.MethodDelete, "/v1/chains/main", "", http.StatusBadRequest},
				{http.MethodDelete, "/v1/chains/missing", "", http.StatusNotFound},
			} {
				if w := chainManagementRequest(s, tc.method, tc.path, tc.body); w.Code != tc.status {
					t.Fatalf("validation status=%d want=%d body=%s", w.Code, tc.status, w.Body)
				}
			}
			if err := os.Remove(dataDir); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(savedDir, dataDir); err != nil {
				t.Fatal(err)
			}
			w = chainManagementRequest(s, method, path, body)
			if w.Code != success {
				t.Fatalf("retry: status=%d want=%d body=%s", w.Code, success, w.Body)
			}
			reloaded := router.New(s.cfg)
			if err := s.store.Load(reloaded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(s.rtr.RuntimeChains(), reloaded.RuntimeChains()) {
				t.Fatal("successful retry did not persist the published chains")
			}
		})
	}
}

func TestChainMutationsWithoutStore(t *testing.T) {
	s := chainManagementServer(t)
	s.store = nil
	w := chainManagementRequest(s, http.MethodPost, "/v1/chains", `{"name":"memory","steps":[{"provider":"a","model":"shared"}]}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("in-memory create: status=%d body=%s", w.Code, w.Body)
	}
	if name, _, err := s.rtr.Resolve("shared"); err != nil || name != "memory" {
		t.Fatalf("in-memory create did not route: name=%q err=%v", name, err)
	}
	w = chainManagementRequest(s, http.MethodDelete, "/v1/chains/memory", "")
	if w.Code != http.StatusNoContent || len(s.rtr.RuntimeChains()) != 0 {
		t.Fatalf("in-memory delete: status=%d chains=%+v", w.Code, s.rtr.RuntimeChains())
	}
}
