package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultPort = 20128

type Config struct {
	Port         int        `yaml:"port"`
	Providers    []Provider `yaml:"providers"`
	Chains       []Chain    `yaml:"chains"`
	DefaultChain string     `yaml:"default_chain"`
}

type Provider struct {
	Name       string        `yaml:"name"`
	Type       string        `yaml:"type"`
	BaseURL    string        `yaml:"base_url"`
	KeyEnv     string        `yaml:"key_env"`
	KeyEnvs    []string      `yaml:"key_envs"`
	Rotation   string        `yaml:"rotation"`
	Quarantine QuarantineCfg `yaml:"quarantine"`
	Models     []string      `yaml:"models"`
	Prefixes   []string      `yaml:"prefixes"`
	// StreamOnly marks upstreams whose non-streaming endpoint is broken
	// (e.g. a billing layer that rejects every non-stream POST). For these,
	// non-streaming client requests are rewritten to upstream stream=true and
	// the SSE chunks are aggregated back into a standard chat.completion.
	StreamOnly bool `yaml:"stream_only"`
}

// AllKeyEnvs returns the provider's key env vars as a list: KeyEnvs if set,
// else the single KeyEnv. At least one entry is guaranteed after Validate.
func (p *Provider) AllKeyEnvs() []string {
	if len(p.KeyEnvs) > 0 {
		return p.KeyEnvs
	}
	if p.KeyEnv != "" {
		return []string{p.KeyEnv}
	}
	return nil
}

// QuarantineCfg holds per-failure-class cool-off durations for a provider's
// key pool. Zero values fall back to DefaultQuarantine.
type QuarantineCfg struct {
	Auth      Duration `yaml:"auth"`
	RateLimit Duration `yaml:"rate_limit"`
	Transient Duration `yaml:"transient"`
}

// DefaultQuarantine mirrors the step cooldown pattern: 401/403 = long (key is
// dead or quota gone), 429 = rate limit (recovers quickly), 5xx = hiccup.
var DefaultQuarantine = QuarantineCfg{
	Auth:      Duration{24 * time.Hour},
	RateLimit: Duration{time.Minute},
	Transient: Duration{time.Minute},
}

// Duration is a time.Duration that unmarshals from YAML strings like "60s".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"60s\": %w", err)
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

type Chain struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
}

type Step struct {
	Provider string         `yaml:"provider"`
	Model    string         `yaml:"model"`
	Params   map[string]any `yaml:"params,omitempty"`
}

// ParamsExcluded lists request-body fields that step params may never set.
// model is rewritten by the gateway; stream is always the client's call;
// messages would replace the conversation wholesale.
var ParamsExcluded = map[string]bool{"model": true, "stream": true, "messages": true}

// ValidateStepParams checks a step's params map: non-empty keys, safe field
// names (alphanumeric + underscore), and no excluded fields. Values are
// intentionally unconstrained — the gateway doesn't know model catalogs and
// providers decide whether to accept or ignore a field.
func ValidateStepParams(params map[string]any) error {
	for k := range params {
		if k == "" {
			return errors.New("step params: empty key")
		}
		if ParamsExcluded[k] {
			return fmt.Errorf("step params: %q is not allowed (reserved by the gateway)", k)
		}
		for _, r := range k {
			if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				return fmt.Errorf("step params: key %q must be alphanumeric + underscore", k)
			}
		}
	}
	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	providers := make(map[string]bool, len(c.Providers))
	for _, p := range c.Providers {
		if p.Name == "" {
			return errors.New("provider with empty name")
		}
		if providers[p.Name] {
			return fmt.Errorf("duplicate provider %q", p.Name)
		}
		providers[p.Name] = true
		if p.Type == "" {
			return fmt.Errorf("provider %q: missing type", p.Name)
		}
		if p.Type != "openai" && p.Type != "gemini" {
			return fmt.Errorf("provider %q: unknown type %q", p.Name, p.Type)
		}
		if p.BaseURL == "" {
			return fmt.Errorf("provider %q: missing base_url", p.Name)
		}
		if p.KeyEnv == "" && len(p.KeyEnvs) == 0 {
			return fmt.Errorf("provider %q: missing key_env or key_envs", p.Name)
		}
		if p.KeyEnv != "" && len(p.KeyEnvs) > 0 {
			return fmt.Errorf("provider %q: set either key_env or key_envs, not both", p.Name)
		}
		for _, env := range p.AllKeyEnvs() {
			if env == "" {
				return fmt.Errorf("provider %q: empty key env name", p.Name)
			}
		}
		switch p.Rotation {
		case "", "round_robin", "least_used", "sequential":
		default:
			return fmt.Errorf("provider %q: unknown rotation %q (round_robin, least_used, sequential)", p.Name, p.Rotation)
		}
		if p.Quarantine.Auth.Duration < 0 || p.Quarantine.RateLimit.Duration < 0 || p.Quarantine.Transient.Duration < 0 {
			return fmt.Errorf("provider %q: quarantine durations must be >= 0", p.Name)
		}
	}
	if len(c.Chains) == 0 {
		// chains are optional: clients create their own per use case
		return nil
	}
	chainNames := make(map[string]bool, len(c.Chains))
	for _, ch := range c.Chains {
		if ch.Name == "" {
			return errors.New("chain with empty name")
		}
		if chainNames[ch.Name] {
			return fmt.Errorf("duplicate chain %q", ch.Name)
		}
		chainNames[ch.Name] = true
		if len(ch.Steps) == 0 {
			return fmt.Errorf("chain %q: no steps", ch.Name)
		}
		for _, s := range ch.Steps {
			if !providers[s.Provider] {
				return fmt.Errorf("chain %q: unknown provider %q", ch.Name, s.Provider)
			}
			if s.Model == "" {
				return fmt.Errorf("chain %q: step for %q has no model", ch.Name, s.Provider)
			}
			if err := ValidateStepParams(s.Params); err != nil {
				return fmt.Errorf("chain %q: step for %q: %w", ch.Name, s.Provider, err)
			}
		}
	}
	if c.DefaultChain != "" && !chainNames[c.DefaultChain] {
		return fmt.Errorf("default_chain %q not found", c.DefaultChain)
	}
	return nil
}
