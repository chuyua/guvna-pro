package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the real service entrypoint in a subprocess, keeping its signal
// handler and SQLite lifetime isolated from the test process.
func TestGatewayProcess(t *testing.T) {
	if os.Getenv("GUVNA_TEST_GATEWAY_PROCESS") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"guvna"}, os.Args[i+1:]...)
				main()
				os.Exit(0)
			}
		}
		os.Exit(2)
	}

	for _, prefix := range [][]string{nil, {"serve"}} {
		name := "legacy"
		if len(prefix) != 0 {
			name = prefix[0]
		}
		t.Run(name, func(t *testing.T) {
			var upstreamRequests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamRequests.Add(1)
				http.Error(w, "unexpected inference request", http.StatusBadGateway)
			}))
			defer upstream.Close()
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()

			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.yaml")
			dataDir := filepath.Join(dir, "data")
			config := fmt.Sprintf("port: %d\nproviders:\n  - name: local\n    type: openai\n    base_url: %s\n    key_env: GUVNA_TEST_UPSTREAM_KEY\n    models: [smoke]\n", port, upstream.URL)
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			args := append([]string{"-test.run=^TestGatewayProcess$", "--"}, prefix...)
			args = append(args, "-config", configPath, "-data", dataDir)
			process := exec.CommandContext(ctx, os.Args[0], args...)
			process.Env = append(os.Environ(),
				"GUVNA_TEST_GATEWAY_PROCESS=1", "HOME="+dir, "USERPROFILE="+dir,
				"ADMIN_KEY=smoke-admin", "API_KEYS=smoke-client", "GUVNA_TEST_UPSTREAM_KEY=dummy",
				"GUVNA_DECIDER_PROVIDER=", "GUVNA_DECIDER_MODEL=")
			var logs bytes.Buffer
			process.Stdout, process.Stderr = &logs, &logs
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				process.Process.Kill()
				process.Wait()
				if t.Failed() {
					t.Log(logs.String())
				}
			}()

			baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
			client := &http.Client{Timeout: 200 * time.Millisecond}
			deadline := time.Now().Add(5 * time.Second)
			for {
				resp, err := client.Get(baseURL + "/healthz")
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("gateway did not become healthy")
				}
				time.Sleep(10 * time.Millisecond)
			}

			t.Setenv("GUVNA_URL", baseURL)
			t.Setenv("GUVNA_ADMIN_KEY", "smoke-admin")
			manage := func(args ...string) []byte {
				t.Helper()
				var output bytes.Buffer
				if err := run(args, &output, io.Discard, func([]string, io.Writer) error {
					t.Fatal("management invocation started another gateway")
					return nil
				}); err != nil {
					t.Fatalf("guvna %v: %v", args, err)
				}
				return output.Bytes()
			}
			if status := manage("status", "--json"); !json.Valid(status) {
				t.Fatalf("invalid status JSON: %s", status)
			}
			manage("logs", "-n", "5")
			manage("chains", "add", "smoke-chain", "--step", "local:smoke")
			if chains := manage("chains", "list", "--json"); !bytes.Contains(chains, []byte("smoke-chain")) {
				t.Fatalf("created chain missing: %s", chains)
			}
			if _, err := os.Stat(filepath.Join(dataDir, "chains.yaml")); err != nil {
				t.Fatalf("chain was not persisted: %v", err)
			}
			manage("chains", "rm", "smoke-chain")
			if chains := manage("chains", "list", "--json"); strings.Contains(string(chains), "smoke-chain") {
				t.Fatalf("removed chain still present: %s", chains)
			}
			if got := upstreamRequests.Load(); got != 0 {
				t.Fatalf("management/startup sent %d inference requests", got)
			}
		})
	}
}
