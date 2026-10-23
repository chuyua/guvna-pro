package chains

import (
	"math"
	"os"
	"testing"

	"github.com/creamy-ghost/guvna/internal/router"
)

func TestKnownProviderNonJSONParamsRemainDormantAcrossWrites(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	r := newRouter()
	fixture := `chains:
  - name: bad-nan
    steps:
      - provider: groq
        model: original
        params:
          temperature: .nan
  - name: bad-map
    steps:
      - provider: groq
        model: original
        params:
          custom: {1: original}
`
	if err := os.WriteFile(s.path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(r); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bad-nan", "bad-map"} {
		if _, _, err := r.Resolve(name); err == nil {
			t.Fatalf("invalid parameters activated %q", name)
		}
	}
	listed := s.ListChains(r)
	if len(listed) != 3 || listed[0].Source != "dormant" || listed[1].Source != "dormant" {
		t.Fatalf("invalid records are not visible as dormant: %+v", listed)
	}
	for i, operation := range []func() error{
		func() error { return s.Save(r) },
		func() error { return s.AddChain(r, "valid", []router.Step{{Provider: "groq", Model: "valid"}}) },
		func() error { return s.RemoveChain(r, "valid") },
	} {
		if err := operation(); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
		stored := storedChains(t, dir)
		nan, numericMap := stored["bad-nan"], stored["bad-map"]
		if len(nan.Steps) != 1 || !math.IsNaN(nan.Steps[0].Params["temperature"].(float64)) || len(numericMap.Steps) != 1 || numericMap.Steps[0].Params["custom"].(map[any]any)[1] != "original" {
			t.Fatalf("operation %d lost invalid params: %+v", i, stored)
		}
	}
}
