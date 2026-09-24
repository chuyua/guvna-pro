// Package vision holds the image-input capability registry used by the /v1/auto
// selector. Like ctxsize, no upstream publishes a machine-readable
// vision-capability catalog we can read unauthenticated at request time, so the
// table is a local, hand-maintained list of known text-only models.
//
// The registry stores the negative list, not the positive one: unknown models
// resolve to true (optimistic) on purpose. An upstream that cannot take images
// fails the request with a 400 immediately, and the gateway's health layer then
// kills the lying model fast — a cheap, self-correcting lie. Defaulting to
// false instead would 404 every image request the moment the pool is all
// unfamiliar models, starving it exactly when the daily discover script has
// outpaced this file.
package vision

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Registry maps a model id to whether it accepts image input. Absent entries
// mean "assume yes"; the map only carries known-false entries.
type Registry struct {
	Models map[string]bool `yaml:"models"`
}

// loadBuiltIn returns the compiled-in table. It is the floor: an on-disk
// registry may add or override entries but never removes built-in ones, so a
// partial external file cannot blind the selector.
func loadBuiltIn() *Registry {
	return &Registry{Models: builtIn.Models}
}

// readFile is a variable so tests can avoid touching the filesystem.
var readFile = os.ReadFile

// Load reads an optional registry YAML file over the built-in table. A missing
// or empty path yields the built-in table. A missing file is not an error; a
// malformed one is.
func Load(path string) (*Registry, error) {
	base := loadBuiltIn()
	if path == "" {
		return base, nil
	}
	data, err := readFile(path)
	if err != nil {
		return base, nil
	}
	var r Registry
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	// Merge rather than reassign: the external entries must survive, and the
	// built-ins must never be dropped by a partial external file. Reassigning
	// r.Models = base.Models would throw the file's additions away.
	ext := r.Models
	r.Models = make(map[string]bool, len(builtIn.Models)+len(ext))
	for k, v := range builtIn.Models {
		r.Models[k] = v
	}
	for k, v := range ext {
		r.Models[k] = v
	}
	return &r, nil
}

// Has reports whether a model is believed to accept image input. Lookups try
// the full upstream id then the tail after any namespace slash, so
// "nvidia/deepseek-ai/deepseek-v4-flash" and "deepseek-v4-flash" both resolve.
// An unknown model returns true — see the package comment for why the default
// leans optimistic.
func (r *Registry) Has(model string) bool {
	if r == nil {
		return true
	}
	for _, key := range candidateKeys(model) {
		if v, ok := r.Models[key]; ok {
			return v
		}
	}
	return true
}

func candidateKeys(model string) []string {
	parts := splitSlash(model)
	if len(parts) == 0 {
		return nil
	}
	return []string{model, parts[len(parts)-1]}
}

func splitSlash(s string) []string {
	var out []string
	cur := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			out = append(out, s[cur:i])
			cur = i + 1
		}
	}
	out = append(out, s[cur:])
	return out
}
