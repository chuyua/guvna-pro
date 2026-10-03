package router

import (
	"errors"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestPassthroughPreservesPrefixSemanticsAndOrder(t *testing.T) {
	first := config.Provider{Name: "first", Prefixes: []string{"shared", "second"}, Models: []string{"m"}}
	second := config.Provider{Name: "second", Models: []string{"m"}}
	for _, tc := range []struct {
		name      string
		providers []config.Provider
		model     string
		want      Step
		notFound  bool
	}{
		{"alias_first", []config.Provider{first, second}, "shared/m", Step{Provider: "first", Model: "m"}, false},
		{"alias_beats_later_name", []config.Provider{first, second}, "second/m", Step{Provider: "first", Model: "m"}, false},
		{"reversed_name_beats_alias", []config.Provider{second, first}, "second/m", Step{Provider: "second", Model: "m"}, false},
		{"unknown_catalog_model_keeps_namespace", []config.Provider{first, second}, "shared/brand-new", Step{Provider: "first", Model: "shared/brand-new"}, false},
		{"explicit_aliases_replace_name_fallback", []config.Provider{first, second}, "first/m", Step{}, true},
		{"prefix_requires_slash", []config.Provider{first, second}, "shared-other/m", Step{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := New(&config.Config{Providers: tc.providers})
			for i := 0; i < 100; i++ {
				name, steps, err := r.Resolve(tc.model)
				if tc.notFound {
					if !errors.Is(err, ErrNotFound) {
						t.Fatalf("Resolve(%q) error = %v, want ErrNotFound", tc.model, err)
					}
					continue
				}
				if err != nil || name != "" || len(steps) != 1 ||
					steps[0].Provider != tc.want.Provider || steps[0].Model != tc.want.Model {
					t.Fatalf("Resolve(%q) = %q, %+v, %v; want %+v", tc.model, name, steps, err, tc.want)
				}
			}
		})
	}
}

func TestSharedModelIndexAcrossRemovalOrders(t *testing.T) {
	orders := [][]string{
		{"old", "middle", "new"},
		{"old", "new", "middle"},
		{"middle", "old", "new"},
		{"middle", "new", "old"},
		{"new", "old", "middle"},
		{"new", "middle", "old"},
	}
	for _, order := range orders {
		t.Run(order[0]+"/"+order[1]+"/"+order[2], func(t *testing.T) {
			r := New(&config.Config{
				Providers: []config.Provider{{Name: "a"}, {Name: "b"}},
				Chains: []config.Chain{
					{Name: "config-old", Steps: []config.Step{{Provider: "a", Model: "x"}}},
					{Name: "config-new", Steps: []config.Step{{Provider: "b", Model: "x"}, {Provider: "a", Model: "x"}}},
				},
			})
			assertModelChain(t, r, "x", "config-new", "b")
			for _, chain := range []string{"old", "middle", "new"} {
				provider := "a"
				if chain == "middle" {
					provider = "b"
				}
				// Repeated models within one chain must create one ownership entry.
				if err := r.AddChain(chain, []Step{{Provider: provider, Model: "x"}, {Provider: provider, Model: "x"}}); err != nil {
					t.Fatal(err)
				}
			}
			assertModelChain(t, r, "x", "new", "a")
			if err := r.RemoveChain("config-new"); err == nil {
				t.Fatal("configuration chain deletion must be rejected")
			}
			assertModelChain(t, r, "x", "new", "a")
			remaining := map[string]bool{"old": true, "middle": true, "new": true}
			for _, chain := range order {
				if err := r.RemoveChain(chain); err != nil {
					t.Fatal(err)
				}
				delete(remaining, chain)
				want, provider := "config-new", "b"
				for _, candidate := range []string{"new", "middle", "old"} {
					if remaining[candidate] {
						want, provider = candidate, "a"
						if candidate == "middle" {
							provider = "b"
						}
						break
					}
				}
				assertModelChain(t, r, "x", want, provider)
				if err := r.RemoveChain(chain); err == nil {
					t.Fatal("removing an absent chain must be rejected")
				}
				assertModelChain(t, r, "x", want, provider)
			}
			// Reusing a removed name must become the newest owner.
			if err := r.AddChain("middle", []Step{{Provider: "a", Model: "x"}, {Provider: "a", Model: "y"}}); err != nil {
				t.Fatal(err)
			}
			assertModelChain(t, r, "x", "middle", "a")
			if err := r.RemoveChain("middle"); err != nil {
				t.Fatal(err)
			}
			assertModelChain(t, r, "x", "config-new", "b")
			if _, _, err := r.Resolve("y"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("unowned model y must be removed from index, got %v", err)
			}
		})
	}
}

func assertModelChain(t *testing.T, r *Router, model, chain, provider string) {
	t.Helper()
	name, steps, err := r.Resolve(model)
	if err != nil || name != chain || len(steps) == 0 || steps[0].Provider != provider {
		t.Fatalf("Resolve(%q) = %q, %+v, %v; want chain=%q provider=%q", model, name, steps, err, chain, provider)
	}
}
