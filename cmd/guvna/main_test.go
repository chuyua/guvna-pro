package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestServiceDispatchPreservesLegacyArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{"default", nil, nil},
		{"legacy", []string{"-config", "old.yaml", "-data", "old-data"}, []string{"-config", "old.yaml", "-data", "old-data"}},
		{"healthcheck", []string{"--healthcheck", "-config", "old.yaml"}, []string{"--healthcheck", "-config", "old.yaml"}},
		{"serve", []string{"serve", "--config", "new.yaml", "--data", "new-data"}, []string{"--config", "new.yaml", "--data", "new-data"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			wantErr := errors.New("test service result")
			var output bytes.Buffer
			err := run(tc.args, io.Discard, &output, func(args []string, stderr io.Writer) error {
				calls++
				if !reflect.DeepEqual(args, tc.want) || stderr != &output {
					t.Fatalf("service arguments changed: got %v want %v", args, tc.want)
				}
				return wantErr
			})
			if calls != 1 || !errors.Is(err, wantErr) {
				t.Fatalf("service result lost: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestHelpVersionAndUnknownCommandsDoNotStartService(t *testing.T) {
	t.Setenv(envAdminKey, "")
	t.Setenv(envAPIKeys, "")
	for _, tc := range []struct {
		args []string
		want string
		fail bool
	}{
		{[]string{"--help"}, "guvna serve", false},
		{[]string{"-h"}, "guvna chains", false},
		{[]string{"help"}, "management flags:", false},
		{[]string{"--version"}, "guvna " + version + "\n", false},
		{[]string{"bogus"}, "", true},
		{[]string{"--version", "unexpected"}, "", true},
		{[]string{"help", "unexpected"}, "", true},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var output bytes.Buffer
			err := run(tc.args, &output, io.Discard, func([]string, io.Writer) error {
				t.Fatal("non-service invocation started the gateway")
				return nil
			})
			if (err != nil) != tc.fail || !strings.Contains(output.String(), tc.want) {
				t.Fatalf("output=%q err=%v", output.String(), err)
			}
		})
	}
}

func TestServeHelpAndInvalidArgumentsReturnBeforeStartup(t *testing.T) {
	t.Setenv(envAdminKey, "")
	t.Setenv(envAPIKeys, "")
	for _, tc := range []struct {
		args []string
		help bool
	}{
		{[]string{"serve", "--help"}, true},
		{[]string{"serve", "--unknown"}, false},
		{[]string{"--unknown"}, false},
		{[]string{"serve", "-config"}, false},
		{[]string{"serve", "unexpected"}, false},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var output bytes.Buffer
			err := run(tc.args, io.Discard, &output, serve)
			if err == nil || errors.Is(err, flag.ErrHelp) != tc.help {
				t.Fatalf("err=%v help=%v", err, tc.help)
			}
			if tc.help && (!strings.Contains(output.String(), "-config") || !strings.Contains(output.String(), "-healthcheck")) {
				t.Fatalf("service help missing flags: %q", output.String())
			}
		})
	}
}

func TestParseServeRetainsExistingFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-config", "old.yaml", "-data", "old-data", "-healthcheck"},
		{"--config=old.yaml", "--data=old-data", "--healthcheck=true"},
	} {
		opts, err := parseServe(args, io.Discard)
		if err != nil || opts.configPath != "old.yaml" || opts.dataDir != "old-data" || !opts.healthCheck {
			t.Fatalf("legacy options changed: %+v err=%v", opts, err)
		}
	}
}

func TestManagementDispatchUsesExistingGateway(t *testing.T) {
	t.Setenv(envAdminKey, "")
	t.Setenv(envAPIKeys, "")
	for _, tc := range []struct {
		args []string
		path string
	}{
		{[]string{"status"}, "/admin/status"},
		{[]string{"logs", "-n", "7"}, "/admin/logs?n=7"},
		{[]string{"chains", "list"}, "/v1/chains"},
	} {
		t.Run(tc.args[0], func(t *testing.T) {
			var requests atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet || r.URL.RequestURI() != tc.path || r.Header.Get("Authorization") != "Bearer dummy-admin" {
					t.Errorf("unexpected management request: %s %s auth=%q", r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"))
				}
				io.WriteString(w, `{"chains":[],"lines":[],"future_field":true}`)
			}))
			defer gateway.Close()
			args := append(append([]string{}, tc.args...), "--url", gateway.URL, "--token", "dummy-admin", "--json")
			var output bytes.Buffer
			if err := run(args, &output, io.Discard, func([]string, io.Writer) error {
				t.Fatal("management command started the gateway")
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 || !json.Valid(output.Bytes()) {
				t.Fatalf("requests=%d output=%q", requests.Load(), output.String())
			}
		})
	}
}

func TestHealthcheckWorksWithLegacyAndServeInvocation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv(envAdminKey, "")
	t.Setenv(envAPIKeys, "")
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/healthz" || r.Method != http.MethodGet {
					t.Errorf("unexpected healthcheck: %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(status)
			}))
			defer gateway.Close()
			_, port, err := net.SplitHostPort(gateway.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(configPath, []byte("port: "+port+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, prefix := range [][]string{nil, {"serve"}} {
				args := append(append([]string{}, prefix...), "--healthcheck", "-config", configPath)
				err := run(args, io.Discard, io.Discard, serve)
				if (err != nil) != (status != http.StatusOK) {
					t.Fatalf("healthcheck %v status=%d err=%v", args, status, err)
				}
			}
			if requests.Load() != 2 {
				t.Fatalf("healthcheck sent %d requests, want 2", requests.Load())
			}
		})
	}
}
