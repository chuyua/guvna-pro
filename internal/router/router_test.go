package router

import (
	"errors"
	"testing"

	"github.com/alisa/bruvroute/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		Providers: []config.Provider{
			{Name: "groq", Models: []string{"llama-3.3-70b-versatile"}},
			{Name: "bazaarlink", Models: []string{"qwen3.7-flash:free", "deepseek-v4-flash:free"}},
			{Name: "gemini", Models: []string{"gemini-2.5-flash"}},
			{Name: "orcarouter", Models: []string{"orcarouter/free"}, Prefixes: []string{"orcarouter", "openai", "anthropic", "google"}},
		},
		Chains: []config.Chain{
			{
				Name: "fast",
				Steps: []config.Step{
					{Provider: "groq", Model: "llama-3.3-70b-versatile"},
					{Provider: "bazaarlink", Model: "qwen3.7-flash:free"},
				},
			},
			{
				Name: "smart",
				Steps: []config.Step{
					{Provider: "gemini", Model: "gemini-2.5-flash"},
				},
			},
		},
	}
}

func testRouter(t *testing.T) *Router {
	t.Helper()
	return New(testConfig())
}

func TestResolveByChainName(t *testing.T) {
	r := testRouter(t)
	name, steps, err := r.Resolve("fast")
	if err != nil {
		t.Fatal(err)
	}
	if name != "fast" || len(steps) != 2 {
		t.Fatalf("got %q with %d steps", name, len(steps))
	}
	if steps[0].Provider != "groq" || steps[0].Model != "llama-3.3-70b-versatile" {
		t.Errorf("bad first step: %+v", steps[0])
	}
}

func TestResolveByModelName(t *testing.T) {
	r := testRouter(t)
	name, steps, err := r.Resolve("gemini-2.5-flash")
	if err != nil {
		t.Fatal(err)
	}
	if name != "smart" || len(steps) != 1 {
		t.Fatalf("got %q with %d steps", name, len(steps))
	}
}

func TestResolveUnknownReturnsNotFound(t *testing.T) {
	r := testRouter(t)
	if _, _, err := r.Resolve("totally-unknown-model"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestResolvePassthroughStripWhenInModels(t *testing.T) {
	r := testRouter(t)
	name, steps, err := r.Resolve("groq/llama-3.3-70b-versatile")
	if err != nil {
		t.Fatal(err)
	}
	if name != "" || len(steps) != 1 || steps[0].Provider != "groq" || steps[0].Model != "llama-3.3-70b-versatile" {
		t.Fatalf("got name=%q steps=%+v", name, steps)
	}
}

func TestResolvePassthroughFullNameWhenNotInModels(t *testing.T) {
	r := testRouter(t)
	name, steps, err := r.Resolve("openai/gpt-5.6-luna")
	if err != nil {
		t.Fatal(err)
	}
	if name != "" || len(steps) != 1 || steps[0].Provider != "orcarouter" || steps[0].Model != "openai/gpt-5.6-luna" {
		t.Fatalf("got name=%q steps=%+v", name, steps)
	}
}

func TestResolvePassthroughProviderNamePrefix(t *testing.T) {
	r := testRouter(t)
	name, steps, err := r.Resolve("orcarouter/free")
	if err != nil {
		t.Fatal(err)
	}
	if name != "" || len(steps) != 1 || steps[0].Provider != "orcarouter" || steps[0].Model != "orcarouter/free" {
		t.Fatalf("got name=%q steps=%+v", name, steps)
	}
}

func TestChainNameBeatsModelMatch(t *testing.T) {
	cfg := &config.Config{
		Providers: testConfig().Providers,
		Chains: []config.Chain{
			{Name: "gemini-2.5-flash", Steps: []config.Step{{Provider: "groq", Model: "llama-3.3-70b-versatile"}}},
			{Name: "smart", Steps: []config.Step{{Provider: "gemini", Model: "gemini-2.5-flash"}}},
		},
	}
	r := New(cfg)
	name, _, err := r.Resolve("gemini-2.5-flash")
	if err != nil {
		t.Fatal(err)
	}
	if name != "gemini-2.5-flash" {
		t.Errorf("chain name should win over model match, got %q", name)
	}
}

func TestAddChainLooseValidation(t *testing.T) {
	r := testRouter(t)
	if err := r.AddChain("my-chain", []Step{{Provider: "groq", Model: "llama-3.3-70b-versatile"}}); err != nil {
		t.Fatalf("valid chain rejected: %v", err)
	}
	// unknown models are allowed — the upstream is the source of truth
	if err := r.AddChain("fresh-model", []Step{{Provider: "orcarouter", Model: "brand-new-free-model"}}); err != nil {
		t.Fatalf("unknown-to-config model rejected: %v", err)
	}
	if err := r.AddChain("Bad_Name", []Step{{Provider: "groq", Model: "x"}}); err == nil {
		t.Fatal("bad name accepted")
	}
	if err := r.AddChain("no-steps", nil); err == nil {
		t.Fatal("empty chain accepted")
	}
	if err := r.AddChain("unknown-provider", []Step{{Provider: "nope", Model: "x"}}); err == nil {
		t.Fatal("unknown provider accepted")
	}
	if err := r.AddChain("empty-model", []Step{{Provider: "groq", Model: ""}}); err == nil {
		t.Fatal("empty model accepted")
	}
	if err := r.AddChain("fast", []Step{{Provider: "groq", Model: "x"}}); err == nil {
		t.Fatal("duplicate chain accepted")
	}
}

func TestAddChainResolvesAndPersists(t *testing.T) {
	r := testRouter(t)
	if err := r.AddChain("custom", []Step{{Provider: "gemini", Model: "gemini-2.5-flash"}}); err != nil {
		t.Fatal(err)
	}
	name, _, err := r.Resolve("custom")
	if err != nil || name != "custom" {
		t.Fatalf("resolve after add: %q %v", name, err)
	}
	runtime := r.RuntimeChains()
	if len(runtime) != 1 || runtime["custom"] == nil {
		t.Fatalf("runtime chains = %+v", runtime)
	}
	if err := r.RemoveChain("custom"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("custom"); !errors.Is(err, ErrNotFound) {
		t.Fatal("chain should be gone after remove")
	}
	if err := r.RemoveChain("fast"); err == nil {
		t.Fatal("config chain must not be removable via runtime API")
	}
}

func TestChainNamesSorted(t *testing.T) {
	r := testRouter(t)
	names := r.ChainNames()
	if len(names) != 2 || names[0] != "fast" || names[1] != "smart" {
		t.Errorf("got %v", names)
	}
}
