package cli

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/creamy-ghost/guvna/internal/auth"
	"github.com/creamy-ghost/guvna/internal/chains"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/server"
	"gopkg.in/yaml.v3"
)

func TestChainsRemoveSpecialDormantNamesEndToEnd(t *testing.T) {
	cases := []struct{ label, name string }{
		{"slash", "bad/name"},
		{"query", "bad?name"},
		{"fragment", "bad#name"},
		{"percent", "bad%name"},
		{"encoded-slash", "bad%2Fname"},
		{"space", "bad name"},
		{"unicode", "坏/名字"},
		{"dot", "."},
		{"dot-dot", ".."},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			cfg := &config.Config{Providers: []config.Provider{{
				Name: "available", Type: "openai", BaseURL: "http://unused.invalid", KeyEnv: "CLI_DORMANT_TEST_KEY",
			}}}
			rtr := router.New(cfg)
			dir := t.TempDir()
			path := filepath.Join(dir, "chains.yaml")
			fixture := struct {
				Version int            `yaml:"version"`
				Chains  []config.Chain `yaml:"chains"`
			}{Version: 2, Chains: []config.Chain{{Name: "bad", Steps: []config.Step{{Provider: "available", Model: "active"}}}}}
			for _, record := range cases {
				fixture.Chains = append(fixture.Chains, config.Chain{
					Name:  record.name,
					Steps: []config.Step{{Provider: "missing", Model: "old", Params: map[string]any{"marker": record.name}}},
				})
			}
			data, err := yaml.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			store := chains.New(dir)
			if err := store.Load(rtr); err != nil {
				t.Fatal(err)
			}
			before := store.ListChains(rtr)
			if len(before) != len(cases)+1 {
				t.Fatalf("seeded records missing: %+v", before)
			}
			var want []router.ChainInfo
			for _, ch := range before {
				if ch.Name == "bad" {
					if ch.Source != "runtime" {
						t.Fatalf("sentinel must be an active runtime chain: %+v", ch)
					}
				} else if ch.Source != "dormant" {
					t.Fatalf("invalid file name must remain dormant: %+v", ch)
				}
				if ch.Name != tc.name {
					want = append(want, ch)
				}
			}

			const token = "special-name-test-admin"
			gateway := server.New(cfg, rtr, auth.New(token, nil), nil, nil, nil, store)
			admin := gateway.AdminHandler()
			type observedRequest struct{ method, name, query string }
			requests := make(chan observedRequest, 16)
			socket := cliTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				admin.ServeHTTP(w, r)
				requests <- observedRequest{r.Method, r.PathValue("name"), r.URL.RawQuery}
			}))
			t.Setenv("GUVNA_ADMIN_SOCKET", socket)
			t.Setenv("GUVNA_ADMIN_KEY", token)
			var output bytes.Buffer
			if err := Run([]string{"chains", "rm", tc.name}, &output, io.Discard); err != nil {
				t.Fatalf("remove %q: %v (output %q)", tc.name, err, output.String())
			}
			select {
			case req := <-requests:
				if req.method != http.MethodDelete || req.name != tc.name || req.query != "" {
					t.Fatalf("delete reached a different route/name: %+v, want %q", req, tc.name)
				}
			default:
				t.Fatal("delete did not reach the management server")
			}
			select {
			case req := <-requests:
				t.Fatalf("delete unexpectedly followed a redirect: %+v", req)
			default:
			}
			if got := store.ListChains(rtr); !reflect.DeepEqual(got, want) {
				t.Fatalf("delete changed other live/dormant records: got=%+v want=%+v", got, want)
			}
			if name, steps, err := rtr.Resolve("bad"); err != nil || name != "bad" || len(steps) != 1 || steps[0].Model != "active" {
				t.Fatalf("delete damaged the active prefix chain: name=%q steps=%+v err=%v", name, steps, err)
			}

			persisted, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var snapshot struct {
				Chains []config.Chain `yaml:"chains"`
			}
			if err := yaml.Unmarshal(persisted, &snapshot); err != nil {
				t.Fatal(err)
			}
			gotRecords := make(map[string]config.Chain, len(snapshot.Chains))
			for _, ch := range snapshot.Chains {
				if _, duplicate := gotRecords[ch.Name]; duplicate {
					t.Fatalf("duplicate persisted record %q", ch.Name)
				}
				gotRecords[ch.Name] = ch
			}
			wantRecords := make(map[string]config.Chain, len(fixture.Chains)-1)
			for _, ch := range fixture.Chains {
				if ch.Name != tc.name {
					wantRecords[ch.Name] = ch
				}
			}
			if !reflect.DeepEqual(gotRecords, wantRecords) {
				t.Fatalf("delete changed another persisted definition: got=%+v want=%+v", gotRecords, wantRecords)
			}
			// Restore the missing provider too: the deleted name must remain absent,
			// while all retained invalid names and the active prefix survive a restart.
			reloaded := router.New(&config.Config{Providers: []config.Provider{{Name: "available"}, {Name: "missing"}}})
			reloadedStore := chains.New(dir)
			if err := reloadedStore.Load(reloaded); err != nil {
				t.Fatal(err)
			}
			if got := reloadedStore.ListChains(reloaded); !reflect.DeepEqual(got, want) {
				t.Fatalf("restart changed retained records or resurrected the target: got=%+v want=%+v", got, want)
			}
		})
	}
}
