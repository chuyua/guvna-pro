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
