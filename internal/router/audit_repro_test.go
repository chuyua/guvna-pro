package router

import (
	"github.com/creamy-ghost/guvna/internal/config"
	"testing"
)

func TestAuditConflictingPrefixes(t *testing.T) {
	cfg := &config.Config{Providers: []config.Provider{
		{Name: "a", Type: "openai", BaseURL: "http://localhost", KeyEnv: "A", Prefixes: []string{"shared"}, Models: []string{"m"}},
		{Name: "b", Type: "openai", BaseURL: "http://localhost", KeyEnv: "B", Prefixes: []string{"shared"}, Models: []string{"m"}},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid shared-prefix configuration rejected: %v", err)
	}
	r := New(cfg)
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		_, steps, err := r.Resolve("shared/m")
		if err != nil {
			t.Fatal(err)
		}
		counts[steps[0].Provider]++
	}
	t.Logf("selected providers over 10000 calls: %v", counts)
	if counts["b"] != 0 {
		t.Fatalf("provider selection violates documented configuration order: %v", counts)
	}
}

func TestAuditDeleteSharedModelIndex(t *testing.T) {
	r := New(&config.Config{Providers: []config.Provider{{Name: "a"}}, Chains: []config.Chain{
		{Name: "fast", Steps: []config.Step{{Provider: "a", Model: "x"}}},
	}})
	if err := r.AddChain("custom", []Step{{Provider: "a", Model: "x"}}); err != nil {
		t.Fatal(err)
	}
	before, _, err := r.Resolve("x")
	if err != nil {
		t.Fatal(err)
	}
	if before != "custom" {
		t.Fatalf("latest added chain must win, got %q", before)
	}
	if err := r.RemoveChain("custom"); err != nil {
		t.Fatal(err)
	}
	after, _, err := r.Resolve("x")
	t.Logf("before=%q after=%q error=%v remaining=%v", before, after, err, r.ChainNames())
	if err != nil {
		t.Fatalf("remaining chain fast must still resolve shared model x: %v", err)
	}
	if after != "fast" {
		t.Fatalf("shared model must fall back to fast, got %q", after)
	}
}
