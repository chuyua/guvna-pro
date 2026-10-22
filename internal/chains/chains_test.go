package chains

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/router"
	"gopkg.in/yaml.v3"
)

func newRouter() *router.Router {
	return router.New(&config.Config{
		Providers: []config.Provider{
			{Name: "groq", Models: []string{"llama-3.3-70b-versatile"}},
			{Name: "gemini", Models: []string{"gemini-2.5-flash"}},
		},
		Chains: []config.Chain{{Name: "from-config", Steps: []config.Step{{Provider: "groq", Model: "llama-3.3-70b-versatile"}}}},
	})
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r := newRouter()
	if err := r.AddChain("custom", []router.Step{{Provider: "gemini", Model: "gemini-2.5-flash"}}); err != nil {
		t.Fatal(err)
	}
	s := New(dir)
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "chains.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("chains.yaml empty")
	}
	if !bytes.Contains(raw, []byte("version: 2")) {
		t.Fatalf("chains.yaml missing version field:\n%s", raw)
	}
	r2 := newRouter()
	if err := s.Load(r2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r2.Resolve("custom"); err != nil {
		t.Fatalf("custom chain not restored: %v", err)
	}
	if _, _, err := r2.Resolve("from-config"); err != nil {
		t.Fatalf("config chain lost: %v", err)
	}
}

func TestSaveLoadParamsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r := newRouter()
	params := map[string]any{"reasoning_effort": "high", "max_tokens": 8192, "stop": []any{"END"}}
	if err := r.AddChain("tuned", []router.Step{{Provider: "gemini", Model: "gemini-2.5-flash", Params: params}}); err != nil {
		t.Fatal(err)
	}
	s := New(dir)
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	r2 := newRouter()
	if err := s.Load(r2); err != nil {
		t.Fatal(err)
	}
	_, steps, err := r2.Resolve("tuned")
	if err != nil {
		t.Fatal(err)
	}
	got := steps[0].Params
	if got["reasoning_effort"] != "high" || got["max_tokens"] != 8192 && got["max_tokens"] != float64(8192) {
		t.Fatalf("params not round-tripped: %#v", got)
	}
	if stop, ok := got["stop"].([]any); !ok || len(stop) != 1 || stop[0] != "END" {
		t.Fatalf("non-scalar param not round-tripped: %#v", got["stop"])
	}
}

func TestLoadLegacyFormatNoVersion(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: old\n    steps:\n      - provider: groq\n        model: llama-3.3-70b-versatile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	if err := New(dir).Load(r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("old"); err != nil {
		t.Fatalf("legacy chain should load: %v", err)
	}
}

func TestLoadMissingFileIsNoop(t *testing.T) {
	r := newRouter()
	s := New(t.TempDir())
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
}

func TestLoadConflictSkipped(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: from-config\n    steps:\n      - provider: unavailable\n        model: old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	s := New(dir)
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	// Config precedence applies even when the shadowed file record is invalid.
	_, steps, err := r.Resolve("from-config")
	if err != nil || len(steps) != 1 || steps[0].Provider != "groq" {
		t.Fatalf("config chain changed: steps=%+v err=%v", steps, err)
	}
	if err := r.RemoveChain("from-config"); err == nil {
		t.Fatal("config chain wrongly replaced by runtime file")
	}
	if err := s.AddChain(r, "added", []router.Step{{Provider: "groq", Model: "new"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	if _, exists := storedChains(t, dir)["from-config"]; exists {
		t.Fatal("shadowed file record was persisted as a dormant chain")
	}
}

func TestLoadUnknownProviderSkipped(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: bad\n    steps:\n      - provider: nope\n        model: x\n  - name: good\n    steps:\n      - provider: groq\n        model: llama-3.3-70b-versatile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	if err := New(dir).Load(r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("bad"); err == nil {
		t.Fatal("invalid chain should have been skipped")
	}
	if _, _, err := r.Resolve("good"); err != nil {
		t.Fatalf("valid chain should load: %v", err)
	}
}

func TestSaveOnlyRuntimeChains(t *testing.T) {
	dir := t.TempDir()
	r := newRouter()
	if err := r.AddChain("custom", []router.Step{{Provider: "gemini", Model: "gemini-2.5-flash"}}); err != nil {
		t.Fatal(err)
	}
	s := New(dir)
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	r2 := newRouter()
	if err := s.Load(r2); err != nil {
		t.Fatal(err)
	}
	if len(r2.RuntimeChains()) != 1 {
		t.Fatalf("runtime chains = %+v", r2.RuntimeChains())
	}
}

func storedChains(t *testing.T, dir string) map[string]fileChain {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "chains.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var f fileFormat
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	chains := make(map[string]fileChain, len(f.Chains))
	for _, ch := range f.Chains {
		if _, duplicate := chains[ch.Name]; duplicate {
			t.Fatalf("duplicate stored chain %q", ch.Name)
		}
		chains[ch.Name] = ch
	}
	return chains
}

func TestDormantChainsSurviveWritesAndRecover(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: dormant\n    steps:\n      - provider: temporarily-missing\n        model: x\n        params:\n          max_tokens: 42\n  - name: good\n    steps:\n      - provider: groq\n        model: llama-3.3-70b-versatile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	s := New(dir)
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("dormant"); err == nil {
		t.Fatal("unavailable chain was published")
	}
	operations := []func() error{
		func() error { return s.Save(r) },
		func() error {
			return s.AddChain(r, "added", []router.Step{{Provider: "gemini", Model: "gemini-2.5-flash"}})
		},
		func() error { return s.RemoveChain(r, "good") },
	}
	for i, operation := range operations {
		if err := operation(); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
		ch, ok := storedChains(t, dir)["dormant"]
		if !ok || len(ch.Steps) != 1 || ch.Steps[0].Provider != "temporarily-missing" || ch.Steps[0].Params["max_tokens"] != 42 {
			t.Fatalf("operation %d lost dormant chain: %+v", i, ch)
		}
	}
	restored := router.New(&config.Config{Providers: []config.Provider{
		{Name: "groq"}, {Name: "gemini"}, {Name: "temporarily-missing"},
	}})
	if err := New(dir).Load(restored); err != nil {
		t.Fatal(err)
	}
	_, steps, err := restored.Resolve("dormant")
	if err != nil || len(steps) != 1 || steps[0].Provider != "temporarily-missing" || steps[0].Params["max_tokens"] != 42 {
		t.Fatalf("restored provider did not reactivate chain: steps=%+v err=%v", steps, err)
	}
	if _, _, err := restored.Resolve("good"); err == nil {
		t.Fatal("removed active chain returned after restart")
	}
}

func TestRuntimeChainReplacesDormantRecord(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: replace\n    steps:\n      - provider: unavailable\n        model: old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	s := New(dir)
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	if err := s.AddChain(r, "replace", []router.Step{{Provider: "groq", Model: "new"}}); err != nil {
		t.Fatal(err)
	}
	ch := storedChains(t, dir)["replace"]
	if len(ch.Steps) != 1 || ch.Steps[0].Provider != "groq" || ch.Steps[0].Model != "new" {
		t.Fatalf("runtime chain did not replace dormant record: %+v", ch)
	}
	if err := s.RemoveChain(r, "replace"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	if _, exists := storedChains(t, dir)["replace"]; exists {
		t.Fatal("removed replacement resurrected dormant record")
	}
}

func TestInvalidChainRecordSurvivesSave(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: invalid-model\n    steps:\n      - provider: groq\n        model: \"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRouter()
	s := New(dir)
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	ch, exists := storedChains(t, dir)["invalid-model"]
	if !exists || len(ch.Steps) != 1 || ch.Steps[0].Provider != "groq" || ch.Steps[0].Model != "" {
		t.Fatalf("invalid record was erased: %+v", ch)
	}
}
