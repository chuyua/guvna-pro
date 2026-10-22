package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sample = `
port: 20128
providers:
  - name: groq
    type: openai
    base_url: https://api.groq.com/openai
    key_env: GROQ_1_KEY
    models:
      - llama-3.3-70b-versatile
  - name: gemini
    type: gemini
    base_url: https://generativelanguage.googleapis.com
    key_env: GEMINI_1_KEY
    models:
      - gemini-2.5-flash
chains:
  - name: fast
    steps:
      - provider: groq
        model: llama-3.3-70b-versatile
      - provider: gemini
        model: gemini-2.5-flash
default_chain: fast
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 20128 {
		t.Errorf("port = %d, want 20128", cfg.Port)
	}
	if len(cfg.Providers) != 2 || len(cfg.Chains) != 1 {
		t.Fatalf("got %d providers, %d chains", len(cfg.Providers), len(cfg.Chains))
	}
	if cfg.DefaultChain != "fast" {
		t.Errorf("default_chain = %q", cfg.DefaultChain)
	}
}

func TestLoadDefaultPort(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Port = 0
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("port = %d, want %d", cfg.Port, DefaultPort)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestValidateDuplicateProvider(t *testing.T) {
	dup := sample + "\n"
	cfg, err := Load(writeTemp(t, dup))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Providers = append(cfg.Providers, cfg.Providers[0])
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected duplicate provider error")
	}
}

func TestValidateUnknownChainProvider(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Chains[0].Steps[0].Provider = "nope"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown provider error")
	}
}

func TestValidateNoChainsOK(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Chains = nil
	cfg.DefaultChain = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("no-chains config must validate: %v", err)
	}
	cfg, err = Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultChain = "missing"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing default chain error")
	}
}

func TestValidateEmptyModelsOK(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Providers[0].Models = nil
	cfg.Providers[0].Prefixes = []string{"groq"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("models-less provider must validate: %v", err)
	}
}

func TestValidateEmptyStep(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Chains[0].Steps[0].Model = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected empty model error")
	}
}

func TestValidateUnknownProviderType(t *testing.T) {
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Providers[0].Type = "anthropic"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown type error")
	}
}

func TestLoadRejectsDuplicateKeyEnvs(t *testing.T) {
	const secret = "test-value-must-not-appear-in-config-errors"
	t.Setenv("GROQ_1_KEY", secret)
	for _, envs := range []string{
		"key_envs: [GROQ_1_KEY, GROQ_1_KEY]",
		"key_envs: [GROQ_1_KEY, GROQ_2_KEY, GROQ_1_KEY]",
	} {
		t.Run(envs, func(t *testing.T) {
			content := strings.Replace(sample, "key_env: GROQ_1_KEY", envs, 1)
			_, err := Load(writeTemp(t, content))
			if err == nil || !strings.Contains(err.Error(), `provider "groq"`) || !strings.Contains(err.Error(), `duplicate key env name "GROQ_1_KEY"`) {
				t.Fatalf("expected a provider-scoped duplicate error, got %v", err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatal("config error exposed the key value")
			}
		})
	}
}

func TestValidateKeyEnvsCompatibility(t *testing.T) {
	for _, envs := range [][]string{nil, {"GROQ_1_KEY"}, {"GROQ_1_KEY", "GROQ_2_KEY"}} {
		cfg, err := Load(writeTemp(t, sample))
		if err != nil {
			t.Fatal(err)
		}
		if envs != nil {
			cfg.Providers[0].KeyEnv = ""
			cfg.Providers[0].KeyEnvs = envs
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("valid key configuration %v rejected: %v", envs, err)
		}
	}
	// Providers have independent pools; sharing an env between them is allowed.
	cfg, err := Load(writeTemp(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	for i := range cfg.Providers {
		cfg.Providers[i].KeyEnv = ""
		cfg.Providers[i].KeyEnvs = []string{"SHARED_KEY"}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cross-provider key reference rejected: %v", err)
	}
}

func TestValidatePortBounds(t *testing.T) {
	for _, port := range []int{-1, 65536} {
		cfg := &Config{Port: port}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "port") {
			t.Fatalf("port %d should be rejected, got %v", port, err)
		}
	}
	for _, port := range []int{0, 1, 65535} {
		cfg := &Config{Port: port}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("port %d should validate: %v", port, err)
		}
		if port == 0 && cfg.Port != DefaultPort {
			t.Fatalf("zero port did not preserve its default: %d", cfg.Port)
		}
	}
}

func TestValidateBaseURL(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"https://api.example.com/openai/", ""},
		{"http://localhost:8080/prefix", ""},
		{"http://[::1]:8080/prefix", ""},
		{"https://user:password@example.com/prefix", ""},
		{"api.example.com", "absolute http or https"},
		{"//api.example.com/prefix", "absolute http or https"},
		{"ftp://api.example.com", "absolute http or https"},
		{"https:///prefix", "absolute http or https"},
		{"http://:8080/prefix", "absolute http or https"},
		{"https://api.example.com?key=secret", "query or fragment"},
		{"https://api.example.com?", "query or fragment"},
		{"https://api.example.com#endpoint", "query or fragment"},
		{"https://api.example.com#", "query or fragment"},
		{"http://localhost:0", "port"},
		{"http://localhost:65536", "port"},
		{"http://localhost:not-a-port", "absolute http or https"},
		{"https://private-user:private-password@api.example.com?api_key=private-api-key", "query or fragment"},
		{"https://private-user:private-password@api.example.com:not-a-port", "absolute http or https"},
	} {
		t.Run(tc.url, func(t *testing.T) {
			cfg := &Config{Providers: []Provider{{Name: "p", Type: "openai", BaseURL: tc.url, KeyEnv: "KEY"}}}
			err := cfg.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("valid base URL rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), `provider "p"`) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected provider-scoped %q error, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), tc.url) {
				t.Fatal("base URL error echoed potential credentials")
			}
			for _, credential := range []string{"private-user", "private-password", "private-api-key"} {
				if strings.Contains(err.Error(), credential) {
					t.Fatal("base URL error exposed embedded credentials")
				}
			}
		})
	}
}
