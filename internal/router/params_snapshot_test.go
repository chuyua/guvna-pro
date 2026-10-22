package router

import (
	"reflect"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestChainParamsSnapshotsAreIsolated(t *testing.T) {
	params := func() map[string]any {
		return map[string]any{
			"nested":        map[string]any{"items": []any{map[string]any{"value": "original"}}},
			"yaml":          map[string]any{"value": []any{"original"}},
			"stop":          []string{"original"},
			"nil_map":       map[string]any(nil),
			"nil_slice":     []any(nil),
			"nil_strings":   []string(nil),
			"empty_strings": []string{},
		}
	}
	mutate := func(steps []Step) {
		steps[0].Model = "changed"
		steps[0].Params["nested"].(map[string]any)["items"].([]any)[0].(map[string]any)["value"] = "changed"
		steps[0].Params["yaml"].(map[string]any)["value"].([]any)[0] = "changed"
		steps[0].Params["stop"].([]string)[0] = "changed"
	}
	for _, source := range []string{"config", "runtime"} {
		t.Run(source, func(t *testing.T) {
			cfg := testConfig()
			input := []Step{{Provider: "groq", Model: "snapshot-model", Params: params()}}
			if source == "config" {
				cfg.Chains = append(cfg.Chains, config.Chain{Name: "snapshot", Steps: []config.Step{{Provider: input[0].Provider, Model: input[0].Model, Params: input[0].Params}}})
			}
			r := New(cfg)
			if source == "runtime" {
				if err := r.AddChain("snapshot", input); err != nil {
					t.Fatal(err)
				}
			}
			mutate(input)
			assertOriginal := func() {
				t.Helper()
				_, got, err := r.Resolve("snapshot")
				if err != nil {
					t.Fatal(err)
				}
				if got[0].Model != "snapshot-model" || !reflect.DeepEqual(got[0].Params, params()) {
					t.Fatalf("router params mutated through a caller: %#v", got)
				}
			}
			assertOriginal()
			for _, name := range []string{"snapshot", "snapshot-model"} {
				_, steps, err := r.Resolve(name)
				if err != nil {
					t.Fatal(err)
				}
				mutate(steps)
				assertOriginal()
			}
			for _, chain := range r.ListChains() {
				if chain.Name == "snapshot" {
					mutate(chain.Steps)
				}
			}
			assertOriginal()
			if source == "runtime" {
				mutate(r.RuntimeChains()["snapshot"])
				assertOriginal()
			}
		})
	}
}
