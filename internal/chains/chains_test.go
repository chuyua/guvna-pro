package chains

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestLoadConflictPreserved(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chains.yaml"), []byte("chains:\n  - name: from-config\n    steps:\n      - provider: unavailable\n        model: old\n        params:\n          max_tokens: 42\n          stop: [END]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	original := storedChains(t, dir)["from-config"]
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
	operations := []func() error{
		func() error { return s.Save(r) },
		func() error { return s.AddChain(r, "added", []router.Step{{Provider: "groq", Model: "new"}}) },
		func() error { return s.RemoveChain(r, "added") },
	}
	for i, operation := range operations {
		if err := operation(); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
		if got := storedChains(t, dir)["from-config"]; !reflect.DeepEqual(got, original) {
			t.Fatalf("operation %d changed shadowed file record: %+v", i, got)
		}
	}
	// Once the config no longer owns the name, the preserved record can activate.
	restored := router.New(&config.Config{Providers: []config.Provider{{Name: "unavailable"}}})
	if err := New(dir).Load(restored); err != nil {
		t.Fatal(err)
	}
	_, steps, err = restored.Resolve("from-config")
	if err != nil || len(steps) != 1 || steps[0].Provider != "unavailable" || steps[0].Params["max_tokens"] != 42 {
		t.Fatalf("shadowed file record did not reactivate: steps=%+v err=%v", steps, err)
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

func TestCorruptFilePreservedBeforeWrites(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	if err := r.AddChain("existing", []router.Step{{Provider: "groq", Model: "old"}}); err != nil {
		t.Fatal(err)
	}
	original := []byte("# keep this exact broken document\nchains: [\n")
	if err := os.WriteFile(s.path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	loadErr := s.Load(r)
	if loadErr == nil {
		t.Fatal("malformed file should report a parse error")
	}
	backups, err := filepath.Glob(s.path + ".corrupt-*")
	if err != nil || len(backups) != 1 {
		t.Fatalf("corrupt backups = %v, err = %v", backups, err)
	}
	if !strings.Contains(loadErr.Error(), backups[0]) {
		t.Fatalf("load error does not identify backup: %v", loadErr)
	}
	if _, err := os.Stat(s.path); !os.IsNotExist(err) {
		t.Fatalf("original path should be freed only after backup: %v", err)
	}
	if _, _, err := r.Resolve("from-config"); err != nil {
		t.Fatalf("config chain unavailable after parse failure: %v", err)
	}
	operations := []func() error{
		func() error { return s.Save(r) },
		func() error { return s.AddChain(r, "added", []router.Step{{Provider: "gemini", Model: "new"}}) },
		func() error { return s.RemoveChain(r, "existing") },
	}
	for i, operation := range operations {
		if err := operation(); err != nil {
			t.Fatalf("operation %d after backup: %v", i, err)
		}
		storedChains(t, dir)
		got, err := os.ReadFile(backups[0])
		if err != nil || !bytes.Equal(got, original) {
			t.Fatalf("operation %d changed backup: %q, err = %v", i, got, err)
		}
	}
	if _, exists := storedChains(t, dir)["added"]; !exists {
		t.Fatal("new chain missing after corrupt file recovery")
	}
}

func TestCorruptBackupsAreIndependent(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	existing := s.path + ".corrupt-existing"
	if err := os.WriteFile(existing, []byte("older backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, original := range []string{"chains: [\n", "chains: {\n"} {
		if err := os.WriteFile(s.path, []byte(original), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := s.Load(r); err == nil {
			t.Fatal("malformed file should report a parse error")
		}
		if err := s.Load(r); err != nil {
			t.Fatalf("reloading after quarantine: %v", err)
		}
	}
	backups, err := filepath.Glob(s.path + ".corrupt-*")
	if err != nil || len(backups) != 3 {
		t.Fatalf("backups = %v, err = %v", backups, err)
	}
	want := map[string]bool{"older backup": false, "chains: [\n": false, "chains: {\n": false}
	for _, backup := range backups {
		data, err := os.ReadFile(backup)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := want[string(data)]; !exists {
			t.Fatalf("unexpected backup content: %q", data)
		}
		want[string(data)] = true
	}
	for data, found := range want {
		if !found {
			t.Fatalf("backup content lost: %q", data)
		}
	}
}

func TestLoadFailuresBlockWritesUntilRecovery(t *testing.T) {
	for _, failure := range []string{"read", "backup"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			s := New(dir)
			r := newRouter()
			if err := r.AddChain("existing", []router.Step{{Provider: "groq", Model: "old"}}); err != nil {
				t.Fatal(err)
			}
			var original []byte
			if failure == "read" {
				if err := os.Mkdir(s.path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				// The source component is legal, but appending the backup suffix
				// exceeds the filesystem component limit without permission tricks.
				s.path = filepath.Join(dir, strings.Repeat("c", 240))
				original = []byte("chains: [\n")
				if err := os.WriteFile(s.path, original, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Load(r); err == nil {
				t.Fatal("load failure not reported")
			}
			operations := []func() error{
				func() error { return s.Save(r) },
				func() error { return s.AddChain(r, "blocked", []router.Step{{Provider: "gemini", Model: "new"}}) },
				func() error { return s.RemoveChain(r, "existing") },
			}
			for i, operation := range operations {
				if err := operation(); !errors.Is(err, ErrPersistence) {
					t.Fatalf("operation %d should be blocked by load failure: %v", i, err)
				}
			}
			if _, _, err := r.Resolve("blocked"); err == nil {
				t.Fatal("failed persistence published a new chain")
			}
			if _, _, err := r.Resolve("existing"); err != nil {
				t.Fatalf("failed persistence removed an existing chain: %v", err)
			}
			if failure == "backup" {
				got, err := os.ReadFile(s.path)
				if err != nil || !bytes.Equal(got, original) {
					t.Fatalf("backup failure changed original: %q, err = %v", got, err)
				}
			}
			if err := os.Remove(s.path); err != nil {
				t.Fatal(err)
			}
			// Restore a normal path and valid file, then explicitly reload.
			s.path = filepath.Join(dir, "chains.yaml")
			if err := os.WriteFile(s.path, []byte("chains: []\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.Load(r); err != nil {
				t.Fatalf("successful reload: %v", err)
			}
			if err := s.AddChain(r, "recovered", []router.Step{{Provider: "gemini", Model: "new"}}); err != nil {
				t.Fatalf("writes remained blocked after successful reload: %v", err)
			}
		})
	}
}

func TestCorruptReloadPreservesKnownDormantChains(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	if err := os.WriteFile(s.path, []byte("chains:\n  - name: dormant\n    steps:\n      - provider: unavailable\n        model: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.path, []byte("chains: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(r); err == nil {
		t.Fatal("malformed reload should report a parse error")
	}
	if err := s.Load(r); err != nil {
		t.Fatalf("reload after corrupt backup: %v", err)
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	if _, exists := storedChains(t, dir)["dormant"]; !exists {
		t.Fatal("corrupt reload discarded previously preserved dormant chain")
	}
}

func TestListChainsIncludesIsolatedDormantRecords(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	fixture := "chains:\n  - name: z-dormant\n    steps:\n      - provider: unavailable\n        model: old\n        params:\n          nested:\n            list: [original]\n          numeric:\n            1: [{value: original}]\n  - name: from-config\n    steps:\n      - provider: unavailable\n        model: shadowed\n  - name: a-runtime\n    steps:\n      - provider: groq\n        model: active\n"
	if err := os.WriteFile(s.path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	listed := s.ListChains(r)
	var got []string
	for _, ch := range listed {
		got = append(got, ch.Name+":"+ch.Source)
	}
	want := []string{"a-runtime:runtime", "from-config:config", "from-config:dormant", "z-dormant:dormant"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	listed[3].Steps[0].Provider = "changed"
	listed[3].Steps[0].Params["nested"].(map[string]any)["list"].([]any)[0] = "changed"
	listed[3].Steps[0].Params["numeric"].(map[any]any)[1].([]any)[0].(map[string]any)["value"] = "changed"
	listed[0].Steps[0].Model = "changed"
	again := s.ListChains(r)
	if again[3].Steps[0].Provider != "unavailable" || again[3].Steps[0].Params["nested"].(map[string]any)["list"].([]any)[0] != "original" {
		t.Fatalf("list caller changed dormant data: %+v", again[3])
	}
	if again[3].Steps[0].Params["numeric"].(map[any]any)[1].([]any)[0].(map[string]any)["value"] != "original" {
		t.Fatalf("list caller changed non-string YAML map data: %+v", again[3])
	}
	if again[0].Steps[0].Model != "active" {
		t.Fatalf("list caller changed active data: %+v", again[0])
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	if got := storedChains(t, dir)["z-dormant"].Steps[0]; got.Provider != "unavailable" || got.Params["nested"].(map[string]any)["list"].([]any)[0] != "original" {
		t.Fatalf("list caller changed persisted data: %+v", got)
	}
}

func TestRemoveDormantChain(t *testing.T) {
	for _, name := range []string{"dormant", "from-config"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s := New(dir)
			r := newRouter()
			fixture := "chains:\n  - name: " + name + "\n    steps:\n      - provider: unavailable\n        model: old\n"
			if err := os.WriteFile(s.path, []byte(fixture), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.Load(r); err != nil {
				t.Fatal(err)
			}
			if err := s.RemoveChain(r, name); err != nil {
				t.Fatal(err)
			}
			if _, exists := storedChains(t, dir)[name]; exists {
				t.Fatal("removed dormant record remains on disk")
			}
			for _, ch := range s.ListChains(r) {
				if ch.Name == name && ch.Source == "dormant" {
					t.Fatal("removed dormant record remains listed")
				}
			}
			_, steps, err := r.Resolve("from-config")
			if err != nil || len(steps) != 1 || steps[0].Provider != "groq" {
				t.Fatalf("dormant removal changed config chain: steps=%+v err=%v", steps, err)
			}
			if err := s.RemoveChain(r, "from-config"); err == nil {
				t.Fatal("config-only chain removal should fail")
			}
			if err := s.RemoveChain(r, "absent"); err == nil || !strings.Contains(err.Error(), "not found") {
				t.Fatalf("unknown removal error = %v", err)
			}
			restored := router.New(&config.Config{Providers: []config.Provider{{Name: "unavailable"}}})
			if err := New(dir).Load(restored); err != nil {
				t.Fatal(err)
			}
			if _, _, err := restored.Resolve(name); err == nil {
				t.Fatal("removed dormant record returned when provider recovered")
			}
		})
	}
}

func TestFailedDormantRemovalPreservesState(t *testing.T) {
	for _, failure := range []string{"write", "load"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			s := New(dir)
			r := newRouter()
			original := []byte("chains:\n  - name: from-config\n    steps:\n      - provider: unavailable\n        model: old\n")
			if err := os.WriteFile(s.path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.Load(r); err != nil {
				t.Fatal(err)
			}
			before := s.ListChains(r)
			if failure == "load" {
				s.loadErr = errors.New("prior read failure")
			} else {
				// An ordinary file cannot be used as the parent directory.
				s.path = filepath.Join(s.path, "blocked.yaml")
			}
			if err := s.RemoveChain(r, "from-config"); !errors.Is(err, ErrPersistence) {
				t.Fatalf("remove should report persistence failure: %v", err)
			}
			if after := s.ListChains(r); !reflect.DeepEqual(after, before) {
				t.Fatalf("failed removal changed chain list: before=%+v after=%+v", before, after)
			}
			data, err := os.ReadFile(filepath.Join(dir, "chains.yaml"))
			if err != nil || !bytes.Equal(data, original) {
				t.Fatalf("failed removal changed original file: %q err=%v", data, err)
			}
		})
	}
}

func TestRepeatedLoadDoesNotMakeRuntimeDormant(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	fixture := "chains:\n  - name: active\n    steps:\n      - provider: groq\n        model: first\n  - name: active\n    steps:\n      - provider: unavailable\n        model: duplicate\n"
	if err := os.WriteFile(s.path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.Load(r); err != nil {
			t.Fatal(err)
		}
		for _, ch := range s.ListChains(r) {
			if ch.Name == "active" && (ch.Source != "runtime" || ch.Steps[0].Model != "first") {
				t.Fatalf("load %d created a duplicate dormant record: %+v", i, ch)
			}
		}
		if _, exists := s.dormant["active"]; exists {
			t.Fatal("existing runtime record became dormant")
		}
	}
}

func TestRemoveRuntimeDoesNotReviveStaleDormantRecord(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	if err := os.WriteFile(s.path, []byte("chains:\n  - name: replace\n    steps:\n      - provider: unavailable\n        model: old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	// A caller that bypasses Store can leave a dormant record behind an active
	// runtime chain. Listing and removal must still treat it as one runtime chain.
	if err := r.AddChain("replace", []router.Step{{Provider: "groq", Model: "new"}}); err != nil {
		t.Fatal(err)
	}
	for _, ch := range s.ListChains(r) {
		if ch.Name == "replace" && ch.Source != "runtime" {
			t.Fatal("stale dormant record was listed alongside its runtime replacement")
		}
	}
	if err := s.RemoveChain(r, "replace"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("replace"); err == nil {
		t.Fatal("active runtime chain was not removed")
	}
	if _, exists := storedChains(t, dir)["replace"]; exists {
		t.Fatal("removing runtime chain revived its stale dormant record")
	}
}
