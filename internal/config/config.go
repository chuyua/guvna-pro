package config

import (
	"errors"
	"fmt"
	"os"

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
	Name    string   `yaml:"name"`
	Type    string   `yaml:"type"`
	BaseURL string   `yaml:"base_url"`
	KeyEnv  string   `yaml:"key_env"`
	Models  []string `yaml:"models"`
}

type Chain struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
}

type Step struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
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
		if p.KeyEnv == "" {
			return fmt.Errorf("provider %q: missing key_env", p.Name)
		}
	}
	if len(c.Chains) == 0 {
		return errors.New("no chains defined")
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
		}
	}
	if c.DefaultChain == "" {
		return errors.New("default_chain is required")
	}
	if !chainNames[c.DefaultChain] {
		return fmt.Errorf("default_chain %q not found", c.DefaultChain)
	}
	return nil
}
