package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
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
	"gopkg.in/yaml.v3"
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

func chainManagementList(t *testing.T, s *Server) []chainInfo {
	t.Helper()
	w := chainManagementRequest(s, http.MethodGet, "/v1/chains", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list: status=%d body=%s", w.Code, w.Body)
	}
	var result struct {
		Chains []chainInfo `json:"chains"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode chain list: %v", err)
	}
	return result.Chains
}

func seedDormantManagementStore(t *testing.T, s *Server) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	data := []byte(`version: 2
chains:
  - name: main
    steps:
      - provider: a
        model: shadowed
        params:
          max_tokens: 42
          stop: [END]
  - name: offline
    steps:
      - provider: unavailable
        model: original
`)
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	s.store = chains.New(dir)
	if err := s.store.Load(s.rtr); err != nil {
		t.Fatal(err)
	}
	return dir, data
}

func TestChainListIncludesDormantAndConfigShadow(t *testing.T) {
	s := chainManagementServer(t)
	seedDormantManagementStore(t, s)
	want := []chainInfo{
		{Name: "main", Source: "config", Steps: []chainStep{{Provider: "a", Model: "m"}}},
		{Name: "main", Source: "dormant", Steps: []chainStep{{Provider: "a", Model: "shadowed", Params: map[string]any{
			"max_tokens": float64(42), "stop": []any{"END"},
		}}}},
		{Name: "offline", Source: "dormant", Steps: []chainStep{{Provider: "unavailable", Model: "original"}}},
	}
	if got := chainManagementList(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("chain list = %+v, want %+v", got, want)
	}
	if _, _, err := s.rtr.Resolve("offline"); err == nil {
		t.Fatal("listing a dormant record activated it")
	}
}

func TestChainListPreservesDormantYAMLParams(t *testing.T) {
	for _, tc := range []struct {
		name, params, provider, source string
	}{
		{name: "non-string nested key", params: "          custom:\n            1: original\n", provider: "unavailable", source: "dormant"},
		{name: "non-finite float", params: "          temperature: .nan\n", provider: "unavailable", source: "dormant"},
		{name: "active non-finite float", params: "          temperature: .nan\n", provider: "a", source: "runtime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := chainManagementServer(t)
			dir := t.TempDir()
			original := []byte("chains:\n  - name: yaml-only\n    steps:\n      - provider: " + tc.provider + "\n        model: original\n        params:\n" + tc.params)
			path := filepath.Join(dir, "chains.yaml")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			s.store = chains.New(dir)
			if err := s.store.Load(s.rtr); err != nil {
				t.Fatal(err)
			}
			listed := chainManagementList(t, s)
			if len(listed) != 2 || listed[1].Name != "yaml-only" || listed[1].Source != tc.source || len(listed[1].Steps) != 1 {
				t.Fatalf("YAML-only record missing from JSON list: %+v", listed)
			}
			step := listed[1].Steps[0]
			if step.Params != nil || step.ParamsYAML == "" {
				t.Fatalf("non-JSON params did not use YAML fallback: %+v", step)
			}
			var params map[string]any
			if err := yaml.Unmarshal([]byte(step.ParamsYAML), &params); err != nil {
				t.Fatalf("invalid YAML fallback: %q err=%v", step.ParamsYAML, err)
			}
			if tc.name == "non-string nested key" {
				mapping, ok := params["custom"].(map[any]any)
				if !ok || mapping[1] != "original" {
					t.Fatalf("YAML fallback lost the typed key: %#v", params)
				}
			} else if value, ok := params["temperature"].(float64); !ok || !math.IsNaN(value) {
				t.Fatalf("YAML fallback lost the non-finite float: %#v", params)
			}
			preserved, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(original, preserved) {
				t.Fatalf("listing changed stored YAML: err=%v", err)
			}
			if w := chainManagementRequest(s, http.MethodDelete, "/v1/chains/yaml-only", ""); w.Code != http.StatusNoContent {
				t.Fatalf("delete YAML-only record: status=%d body=%s", w.Code, w.Body)
			}
			if got := chainManagementList(t, s); len(got) != 1 || got[0].Source != "config" {
				t.Fatalf("YAML-only record remains listed after delete: %+v", got)
			}
			reloaded := router.New(&config.Config{
				Providers: []config.Provider{{Name: "a"}, {Name: "unavailable"}},
				Chains:    s.cfg.Chains,
			})
			if err := chains.New(dir).Load(reloaded); err != nil {
				t.Fatal(err)
			}
			if _, _, err := reloaded.Resolve("yaml-only"); err == nil {
				t.Fatal("deleted YAML-only record returned after reload")
			}
		})
	}
}

func TestChainCreateDoesNotEchoListOnlyYAMLParams(t *testing.T) {
	s := chainManagementServer(t)
	w := chainManagementRequest(s, http.MethodPost, "/v1/chains", `{"name":"created","steps":[{"provider":"a","model":"original","params_yaml":"temperature: .nan\n"}]}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: status=%d body=%s", w.Code, w.Body)
	}
	var created chainInfo
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || len(created.Steps) != 1 {
		t.Fatalf("decode created chain: err=%v body=%s", err, w.Body)
	}
	if created.Steps[0].ParamsYAML != "" || created.Steps[0].Params != nil {
		t.Fatalf("create echoed unpersisted list metadata: %+v", created.Steps)
	}
	if steps := s.rtr.RuntimeChains()["created"]; len(steps) != 1 || steps[0].Params != nil {
		t.Fatalf("create used list-only YAML params: %+v", steps)
	}
}

func TestChainDeleteDormantPreservesConfigAndPersists(t *testing.T) {
	for _, name := range []string{"offline", "main"} {
		t.Run(name, func(t *testing.T) {
			s := chainManagementServer(t)
			dir, _ := seedDormantManagementStore(t, s)
			before := s.rtr.ListChains()
			w := chainManagementRequest(s, http.MethodDelete, "/v1/chains/"+name, "")
			if w.Code != http.StatusNoContent {
				t.Fatalf("delete dormant: status=%d body=%s", w.Code, w.Body)
			}
			if !reflect.DeepEqual(before, s.rtr.ListChains()) {
				t.Fatal("deleting a dormant record changed active routes")
			}
			for _, ch := range chainManagementList(t, s) {
				if ch.Name == name && ch.Source == "dormant" {
					t.Fatalf("deleted dormant record remains listed: %+v", ch)
				}
			}
			data, err := os.ReadFile(filepath.Join(dir, "chains.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var snapshot struct {
				Chains []struct{ Name string } `yaml:"chains"`
			}
			if err := yaml.Unmarshal(data, &snapshot); err != nil {
				t.Fatal(err)
			}
			for _, ch := range snapshot.Chains {
				if ch.Name == name {
					t.Fatalf("deleted dormant record remains on disk: %s", data)
				}
			}
			// Restore every provider and remove the config conflict: an explicitly
			// deleted file record must still stay absent after a fresh load.
			reloaded := router.New(&config.Config{Providers: []config.Provider{{Name: "a"}, {Name: "unavailable"}}})
			if err := chains.New(dir).Load(reloaded); err != nil {
				t.Fatal(err)
			}
			if _, _, err := reloaded.Resolve(name); err == nil {
				t.Fatalf("deleted dormant record %q returned after restart", name)
			}
			status := http.StatusNotFound
			if name == "main" {
				status = http.StatusBadRequest // The config-only chain remains protected.
			}
			if w := chainManagementRequest(s, http.MethodDelete, "/v1/chains/"+name, ""); w.Code != status {
				t.Fatalf("second delete: status=%d want=%d body=%s", w.Code, status, w.Body)
			}
		})
	}
}

func TestChainDeleteDormantPersistenceFailureIsAtomic(t *testing.T) {
	for _, name := range []string{"offline", "main"} {
		t.Run(name, func(t *testing.T) {
			s := chainManagementServer(t)
			dir, original := seedDormantManagementStore(t, s)
			before, routes := chainManagementList(t, s), s.rtr.ListChains()
			savedDir := filepath.Join(t.TempDir(), "saved")
			if err := os.Rename(dir, savedDir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("blocked"), 0o600); err != nil {
				t.Fatal(err)
			}
			w := chainManagementRequest(s, http.MethodDelete, "/v1/chains/"+name, "")
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("failed dormant delete: status=%d body=%s", w.Code, w.Body)
			}
			if !reflect.DeepEqual(before, chainManagementList(t, s)) || !reflect.DeepEqual(routes, s.rtr.ListChains()) {
				t.Fatal("failed dormant delete changed records or active routes")
			}
			preserved, err := os.ReadFile(filepath.Join(savedDir, "chains.yaml"))
			if err != nil || !bytes.Equal(original, preserved) {
				t.Fatalf("failed dormant delete changed stored bytes: err=%v", err)
			}
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(savedDir, dir); err != nil {
				t.Fatal(err)
			}
			if w := chainManagementRequest(s, http.MethodDelete, "/v1/chains/"+name, ""); w.Code != http.StatusNoContent {
				t.Fatalf("retry dormant delete: status=%d body=%s", w.Code, w.Body)
			}
		})
	}
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
	want := []chainInfo{
		{Name: "main", Source: "config", Steps: []chainStep{{Provider: "a", Model: "m"}}},
		{Name: "memory", Source: "runtime", Steps: []chainStep{{Provider: "a", Model: "shared"}}},
	}
	if got := chainManagementList(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("in-memory list = %+v, want %+v", got, want)
	}
	w = chainManagementRequest(s, http.MethodDelete, "/v1/chains/memory", "")
	if w.Code != http.StatusNoContent || len(s.rtr.RuntimeChains()) != 0 {
		t.Fatalf("in-memory delete: status=%d chains=%+v", w.Code, s.rtr.RuntimeChains())
	}
}
