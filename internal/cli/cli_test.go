package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/creamy-ghost/guvna/internal/localadmin"
)

type recordedCLIRequest struct {
	method, path, authorization string
	body                        []byte
}

func cliTestSocket(t *testing.T, handler http.Handler) string {
	t.Helper()
	root := os.TempDir()
	if runtime.GOOS == "windows" {
		root = os.Getenv("PUBLIC") // AF_UNIX needs a short ASCII path on Windows.
	}
	dir, err := os.MkdirTemp(root, "gc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "admin.sock")
	listener, err := localadmin.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close(); listener.Close() })
	return path
}

func cliTestGateway(t *testing.T) (string, <-chan recordedCLIRequest) {
	t.Helper()
	requests := make(chan recordedCLIRequest, 8)
	s := cliTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests <- recordedCLIRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"), body}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"name":"test-chain","source":"runtime","steps":[{"provider":"a","model":"one"},{"provider":"b","model":"two"}]}`)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			io.WriteString(w, `{"chains":[]}`)
		}
	}))
	return s, requests
}

func captureCLIOutput(t *testing.T, run func(command) error) (string, error) {
	t.Helper()
	var output bytes.Buffer
	err := run(command{out: &output, errOut: io.Discard})
	return output.String(), err
}

func TestChainsExplicitListUsesFlags(t *testing.T) {
	s, requests := cliTestGateway(t)
	t.Setenv("GUVNA_ADMIN_SOCKET", s)
	t.Setenv("GUVNA_ADMIN_KEY", "environment-token")
	output, err := captureCLIOutput(t, func(c command) error {
		return c.chainsCmd([]string{"list", "--socket", s, "--token", "flag-token", "--json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var result chainResp
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("list did not emit JSON: %q", output)
	}
	if req := <-requests; req.method != http.MethodGet || req.path != "/v1/chains" || req.authorization != "Bearer flag-token" {
		t.Fatalf("list flags were ignored: %+v", req)
	}
}

func TestChainsListAndRemoveDormant(t *testing.T) {
	const response = `{"chains":[{"name":"active","source":"runtime","steps":[{"provider":"a","model":"current","params":{"temperature":0.2}}]},{"name":"shared","source":"config","steps":[{"provider":"a","model":"configured"}]},{"name":"shared","source":"dormant","steps":[{"provider":"missing","model":"original","params":{"max_tokens":42,"stop":["END"]}}]},{"name":"yaml-only","source":"dormant","steps":[{"provider":"a","model":"original","params_yaml":"temperature: .nan\n"}]}]}`
	for _, jsonOutput := range []bool{false, true} {
		name := "table"
		if jsonOutput {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			requests := make(chan recordedCLIRequest, 2)
			socket := cliTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- recordedCLIRequest{method: r.Method, path: r.URL.Path, authorization: r.Header.Get("Authorization")}
				if r.Method == http.MethodDelete {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, response)
			}))
			t.Setenv("GUVNA_ADMIN_SOCKET", socket)
			t.Setenv("GUVNA_ADMIN_KEY", "dummy-token")
			args := []string{"list"}
			if jsonOutput {
				args = append(args, "--json")
			}
			output, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd(args) })
			if err != nil {
				t.Fatal(err)
			}
			if req := <-requests; req.method != http.MethodGet || req.path != "/v1/chains" || req.authorization != "Bearer dummy-token" {
				t.Fatalf("wrong list request: %+v", req)
			}
			if jsonOutput {
				var got chainResp
				if err := json.Unmarshal([]byte(output), &got); err != nil || len(got.Chains) != 4 {
					t.Fatalf("invalid chain JSON: %q err=%v", output, err)
				}
				active, configured, dormant := got.Chains[0], got.Chains[1], got.Chains[2]
				if active.Source != "runtime" || len(active.Steps) != 1 || active.Steps[0].Params["temperature"] != 0.2 {
					t.Fatalf("active params lost: %+v", active)
				}
				if configured.Name != "shared" || configured.Source != "config" || dormant.Name != "shared" || dormant.Source != "dormant" || len(dormant.Steps) != 1 {
					t.Fatalf("config and dormant records lost: %+v", got.Chains)
				}
				step := dormant.Steps[0]
				stop, ok := step.Params["stop"].([]any)
				if step.Provider != "missing" || step.Model != "original" || step.Params["max_tokens"] != float64(42) || !ok || len(stop) != 1 || stop[0] != "END" {
					t.Fatalf("dormant steps or params lost: %+v", step)
				}
				if yamlOnly := got.Chains[3]; yamlOnly.Name != "yaml-only" || yamlOnly.Source != "dormant" || len(yamlOnly.Steps) != 1 || yamlOnly.Steps[0].ParamsYAML != "temperature: .nan\n" || yamlOnly.Steps[0].Params != nil {
					t.Fatalf("YAML-only params lost: %+v", yamlOnly)
				}
				var raw struct {
					Chains []struct {
						Steps []map[string]any `json:"steps"`
					} `json:"chains"`
				}
				if err := json.Unmarshal([]byte(output), &raw); err != nil {
					t.Fatal(err)
				}
				if _, exists := raw.Chains[1].Steps[0]["params"]; exists {
					t.Fatal("a chain without params gained an empty params field")
				}
			} else {
				lines := strings.Split(strings.TrimSpace(output), "\n")
				if len(lines) != 5 || strings.Join(strings.Fields(lines[0]), " ") != "NAME SOURCE STEPS" || strings.Join(strings.Fields(lines[2]), " ") != "shared config a:configured" || strings.Join(strings.Fields(lines[3]), " ") != "shared dormant missing:original" || strings.Join(strings.Fields(lines[4]), " ") != "yaml-only dormant a:original" {
					t.Fatalf("config and dormant table rows lost: %q", output)
				}
			}
			removed, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd([]string{"rm", "shared"}) })
			if err != nil || !strings.Contains(removed, `removed chain "shared"`) {
				t.Fatalf("remove dormant: output=%q err=%v", removed, err)
			}
			if req := <-requests; req.method != http.MethodDelete || req.path != "/v1/chains/shared" || req.authorization != "Bearer dummy-token" {
				t.Fatalf("wrong dormant delete request: %+v", req)
			}
		})
	}
}

func TestChainsMutationTrailingJSONFlag(t *testing.T) {
	for _, operation := range []string{"add", "rm"} {
		t.Run(operation, func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("command panicked: %v", value)
				}
			}()
			s, requests := cliTestGateway(t)
			t.Setenv("GUVNA_ADMIN_SOCKET", s)
			t.Setenv("GUVNA_ADMIN_KEY", "dummy-token")
			args := []string{operation, "test-chain"}
			if operation == "add" {
				args = append(args, "--step", "a:one", "--step", "b:two")
			}
			args = append(args, "--json")
			output, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd(args) })
			if err != nil {
				t.Fatal(err)
			}
			req := <-requests
			if operation == "add" {
				var submitted struct {
					Name  string      `json:"name"`
					Steps []chainStep `json:"steps"`
				}
				if err := json.Unmarshal(req.body, &submitted); err != nil || submitted.Name != "test-chain" || len(submitted.Steps) != 2 || submitted.Steps[0].Model != "one" || submitted.Steps[1].Model != "two" {
					t.Fatalf("repeated steps lost: %s err=%v", req.body, err)
				}
				if !json.Valid([]byte(output)) || req.method != http.MethodPost || req.path != "/v1/chains" {
					t.Fatalf("wrong add output/request: %q %+v", output, req)
				}
			} else if req.method != http.MethodDelete || req.path != "/v1/chains/test-chain" {
				t.Fatalf("wrong delete request: %+v", req)
			}
		})
	}
}

func TestChainsInvalidFlagsDoNotSendRequests(t *testing.T) {
	for _, args := range [][]string{
		{"list", "--socket"},
		{"list", "--token", "--json"},
		{"list", "--unknown", "dummy"},
		{"list", "unexpected"},
		{"add", "test-chain", "--step", "a:one", "--token"},
		{"add", "test-chain", "--step", "a:one", "--token", "--json"},
		{"add", "test-chain", "--step"},
		{"add", "test-chain", "--step", "a:one", "--unknown"},
		{"rm", "test-chain", "--socket"},
		{"rm", "test-chain", "--unknown"},
		{"rm", "test-chain", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			s, requests := cliTestGateway(t)
			t.Setenv("GUVNA_ADMIN_SOCKET", s)
			t.Setenv("GUVNA_ADMIN_KEY", "private-environment-token")
			_, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd(args) })
			if err == nil {
				t.Fatal("invalid arguments were accepted")
			}
			if strings.Contains(err.Error(), "private-environment-token") {
				t.Fatal("parse error exposed admin key")
			}
			select {
			case req := <-requests:
				t.Fatalf("invalid arguments sent request: %+v", req)
			default:
			}
		})
	}
}

func TestChainsFlagsBeforeNameAndEqualsValues(t *testing.T) {
	for _, args := range [][]string{
		{"add", "--json=true", "--step=a:one", "--step=b:two", "test-chain"},
		{"add", "--step", "a:one", "--", "test-chain"},
		{"rm", "--json=false", "test-chain"},
		{"rm", "--json", "--", "test-chain"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			s, requests := cliTestGateway(t)
			t.Setenv("GUVNA_ADMIN_SOCKET", s)
			t.Setenv("GUVNA_ADMIN_KEY", "dummy-token")
			_, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd(args) })
			if err != nil {
				t.Fatal(err)
			}
			req := <-requests
			if req.authorization != "Bearer dummy-token" {
				t.Fatalf("environment credential was not used: %+v", req)
			}
			if args[0] == "add" {
				var submitted chainInfo
				if err := json.Unmarshal(req.body, &submitted); err != nil || submitted.Name != "test-chain" || len(submitted.Steps) == 0 {
					t.Fatalf("flags changed the submitted chain: %s err=%v", req.body, err)
				}
			} else if req.path != "/v1/chains/test-chain" {
				t.Fatalf("flags changed the deleted chain: %+v", req)
			}
		})
	}
}

func TestChainsDefaultListAcceptsFlags(t *testing.T) {
	s, requests := cliTestGateway(t)
	t.Setenv("GUVNA_ADMIN_SOCKET", "http://unused.invalid")
	t.Setenv("GUVNA_ADMIN_KEY", "unused-token")
	output, err := captureCLIOutput(t, func(c command) error {
		return c.chainsCmd([]string{"--socket=" + s, "--token=-flag-token", "--json"})
	})
	if err != nil || !json.Valid([]byte(output)) {
		t.Fatalf("implicit list failed: output=%q err=%v", output, err)
	}
	if req := <-requests; req.method != http.MethodGet || req.authorization != "Bearer -flag-token" {
		t.Fatalf("equals-value flags were not applied: %+v", req)
	}
}

func TestCommonFlagsHelpHidesEnvironmentCredential(t *testing.T) {
	t.Setenv("GUVNA_ADMIN_KEY", "private-help-token")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var output bytes.Buffer
	fs.SetOutput(&output)
	var cf commonFlags
	parseCommon(fs, &cf)
	if err := fs.Parse([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help failed: %v", err)
	}
	if cf.token != "private-help-token" || strings.Contains(output.String(), "private-help-token") {
		t.Fatal("help exposed the credential or changed its runtime default")
	}
}

func TestChainsRemoveEscapesName(t *testing.T) {
	for _, tc := range []struct{ name, escaped string }{
		{"plain", "plain"},
		{"bad/name", "bad%2Fname"},
		{"bad?name", "bad%3Fname"},
		{"bad#name", "bad%23name"},
		{"bad%name", "bad%25name"},
		{"bad%2Fname", "bad%252Fname"},
		{"bad/../name", "bad%2F..%2Fname"},
		{".", "%2E"},
		{"..", "%2E%2E"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type target struct {
				name, escapedPath, rawQuery, fragment, authorization string
			}
			targets := make(chan target, 1)
			mux := http.NewServeMux()
			mux.HandleFunc("DELETE /v1/chains/{name}", func(w http.ResponseWriter, r *http.Request) {
				targets <- target{r.PathValue("name"), r.URL.EscapedPath(), r.URL.RawQuery, r.URL.Fragment, r.Header.Get("Authorization")}
				w.WriteHeader(http.StatusNoContent)
			})
			t.Setenv("GUVNA_ADMIN_SOCKET", cliTestSocket(t, mux))
			t.Setenv("GUVNA_ADMIN_KEY", "dummy-token")
			output, err := captureCLIOutput(t, func(c command) error {
				return c.chainsCmd([]string{"rm", tc.name})
			})
			if err != nil || !strings.Contains(output, "removed chain ") {
				t.Fatalf("remove: output=%q err=%v", output, err)
			}
			got := <-targets
			if got.name != tc.name || got.escapedPath != "/v1/chains/"+tc.escaped || got.rawQuery != "" || got.fragment != "" || got.authorization != "Bearer dummy-token" {
				t.Fatalf("delete target = %+v, want name=%q escaped=%q with no query or fragment", got, tc.name, tc.escaped)
			}
		})
	}
}

func TestChainsListJSONPreservesResponse(t *testing.T) {
	const response = `{"chains":[{"name":"dormant","source":"dormant","steps":[{"provider":"missing","model":"original","params":{"seed":9007199254740993,"nested":[{"seed":18446744073709551615,"fraction":0.123456789012345678901}]},"future_step":{"count":9007199254740995}}],"future_chain":true}],"future_root":{"count":9007199254740997}}`
	for _, jsonOutput := range []bool{false, true} {
		name := "table"
		if jsonOutput {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			socket := cliTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, response)
			}))
			t.Setenv("GUVNA_ADMIN_SOCKET", socket)
			t.Setenv("GUVNA_ADMIN_KEY", "dummy-token")
			args := []string{"list"}
			if jsonOutput {
				args = append(args, "--json")
			}
			output, err := captureCLIOutput(t, func(c command) error { return c.chainsCmd(args) })
			if err != nil {
				t.Fatal(err)
			}
			if jsonOutput {
				var compact bytes.Buffer
				if err := json.Compact(&compact, []byte(output)); err != nil {
					t.Fatalf("invalid JSON output: %q err=%v", output, err)
				}
				if compact.String() != response {
					t.Fatalf("chain response changed: got=%s want=%s", compact.String(), response)
				}
			} else if got := strings.Join(strings.Fields(output), " "); got != "NAME SOURCE STEPS dormant dormant missing:original" {
				t.Fatalf("table output changed: %q", output)
			}
		})
	}
}
