package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInvalidAutoRegistriesRetainBuiltins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(path, []byte("models: [unclosed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GUVNA_CONTEXT_REGISTRY", path)
	t.Setenv("GUVNA_VISION_REGISTRY", path)
	contextFor, visionReg, _, _, _, _ := loadAutoConfig()
	if got := contextFor("glm-5.3"); got != 204800 {
		t.Fatalf("invalid context file discarded builtins: got %d, want 204800", got)
	}
	if visionReg == nil || visionReg.Has("deepseek-v4-flash") {
		t.Fatal("invalid vision file discarded built-in text-only model classification")
	}
	if !visionReg.Has("unknown-new-vision-model") {
		t.Fatal("unknown model should retain optimistic vision fallback")
	}
}
