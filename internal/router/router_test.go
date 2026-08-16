package router

import (
	"testing"

	"github.com/alisa/bruvroute/internal/config"
)

func testRouter(t *testing.T) *Router {
	t.Helper()
	cfg := &config.Config{
		DefaultChain: "fallback",
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
			{
				Name: "fallback",
				Steps: []config.Step{
					{Provider: "bazaarlink", Model: "deepseek-v4-flash:free"},
				},
			},
		},
	}
	return New(cfg)
}

func TestResolveByChainName(t *testing.T) {
	r := testRouter(t)
	name, steps, ok := r.Resolve("fast")
	if !ok {
		t.Fatal("expected resolution")
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
	name, steps, ok := r.Resolve("gemini-2.5-flash")
	if !ok {
		t.Fatal("expected resolution")
	}
	if name != "smart" || len(steps) != 1 {
		t.Fatalf("got %q with %d steps", name, len(steps))
	}
}

func TestResolveUnknownFallsToDefault(t *testing.T) {
	r := testRouter(t)
	name, steps, ok := r.Resolve("totally-unknown-model")
	if !ok {
		t.Fatal("expected resolution to default")
	}
	if name != "fallback" || len(steps) != 1 {
		t.Fatalf("got %q with %d steps", name, len(steps))
	}
	if steps[0].Provider != "bazaarlink" {
		t.Errorf("bad default step: %+v", steps[0])
	}
}

func TestChainNameBeatsModelMatch(t *testing.T) {
	cfg := &config.Config{
		DefaultChain: "fallback",
		Chains: []config.Chain{
			{Name: "gemini-2.5-flash", Steps: []config.Step{{Provider: "groq", Model: "x"}}},
			{Name: "smart", Steps: []config.Step{{Provider: "gemini", Model: "gemini-2.5-flash"}}},
		},
	}
	r := New(cfg)
	name, _, _ := r.Resolve("gemini-2.5-flash")
	if name != "gemini-2.5-flash" {
		t.Errorf("chain name should win over model match, got %q", name)
	}
}

func TestChainNames(t *testing.T) {
	r := testRouter(t)
	names := r.ChainNames()
	if len(names) != 3 {
		t.Errorf("got %d chain names", len(names))
	}
}
