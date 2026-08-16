package router

import (
	"fmt"
	"regexp"
)

var chainNameRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidateChainName checks the chain-name format: lowercase letters, digits,
// hyphens between segments (same rule opencode skills use), max 64 chars.
func ValidateChainName(name string) error {
	if name == "" || len(name) > 64 {
		return fmt.Errorf("chain name must be 1-64 chars, got %q", name)
	}
	if !chainNameRe.MatchString(name) {
		return fmt.Errorf("chain name %q invalid: lowercase letters, digits, hyphens only", name)
	}
	return nil
}
