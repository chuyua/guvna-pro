package ctxsize

import "testing"

func TestForUsesRegistryThenBuiltin(t *testing.T) {
	r := mustLoad("")
	if got := r.For("nemotron-3-ultra-550b-a55b"); got != 524288 {
		t.Fatalf("nemotron-3-ultra = %d, want 524288", got)
	}
	if got := r.For("nvidia/nemotron-3-ultra-550b-a55b"); got != 524288 {
		t.Fatalf("namespaced nemotron-3-ultra = %d, want 524288 (tail fallback)", got)
	}
}

func TestForFallsBackToDefaultForUnknown(t *testing.T) {
	r := mustLoad("")
	if got := r.For("some-brand-new-model"); got != DefaultContext {
		t.Fatalf("unknown model = %d, want DefaultContext %d", got, DefaultContext)
	}
}

// The whole point of the file: an unknown model must not read as 0, or a
// min_context filter would silently exclude every model the daily discover
// script shipped after this table was last edited.
func TestForNeverReturnsZero(t *testing.T) {
	r := mustLoad("")
	for _, m := range []string{"", "unknown", "deepseek-v4-flash", "kira/ling-3.0-flash"} {
		if got := r.For(m); got == 0 {
			t.Fatalf("For(%q) = 0; an unknown model must not read as zero context", m)
		}
	}
}

func TestForTailOfNamespacedModel(t *testing.T) {
	r := mustLoad("")
	// deepseek-v4.1-flash is registered under both the bare and the
	// nvidia-namespaced key; both must resolve to the same number.
	bare := r.For("deepseek-v4.1-flash")
	nested := r.For("nvidia/deepseek-ai/deepseek-v4.1-flash")
	if bare != nested {
		t.Fatalf("bare=%d nested=%d; namespaced tail must match the bare entry", bare, nested)
	}
}

func TestLoadMissingFileReturnsBuiltins(t *testing.T) {
	r, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if r.For("glm-5.3") != 204800 {
		t.Fatalf("empty path must return builtins, got %d", r.For("glm-5.3"))
	}
	r2, err := Load("/nonexistent/path/to/registry.yaml")
	if err != nil {
		t.Fatalf("Load(missing) must not error: %v", err)
	}
	if r2.For("glm-5.3") != 204800 {
		t.Fatalf("missing file must fall back to builtins, got %d", r2.For("glm-5.3"))
	}
}

func TestLoadMergesExternalOverBuiltins(t *testing.T) {
	old := readFile
	defer func() { readFile = old }()
	readFile = func(string) ([]byte, error) {
		return []byte("models:\n  brand-new-model: 999999\n"), nil
	}
	r, err := Load("external.yaml")
	if err != nil {
		t.Fatalf("Load(external): %v", err)
	}
	if got := r.For("brand-new-model"); got != 999999 {
		t.Fatalf("external entry lost: For(brand-new-model) = %d, want 999999", got)
	}
	if got := r.For("glm-5.3"); got != 204800 {
		t.Fatalf("builtin dropped: For(glm-5.3) = %d, want 204800", got)
	}
}

func mustLoad(path string) *Registry {
	r, err := Load(path)
	if err != nil {
		panic(err)
	}
	return r
}
