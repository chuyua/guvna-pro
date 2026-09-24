// Package ctxsize holds the model-context-length registry used by the /v1/auto
// selector. Neither upstream publishes a machine-readable catalog we can read
// unauthenticated at request time — AMD's /v1/models returns "Login required"
// and NVIDIA's requires a bearer key we deliberately keep inside the gateway —
// so context sizes are a local, hand-maintained table.
//
// Entries are conservative lower bounds on purpose: an underestimate filters a
// candidate out that would have worked (a missed opportunity), while an
// overestimate sends a request the upstream rejects. Both are recoverable by the
// chain, but the second one wastes a key's quota, so entries lean small.
//
// Unknown models resolve to DefaultContext, not 0. Treating "unknown" as zero
// would make every non-registry model fail a min_context filter, silently
// starving the auto pool whenever an upstream ships a new free model — the
// daily discover script outpaces this file.
package ctxsize

import (
	"os"

	"gopkg.in/yaml.v3"
)

// DefaultContext applies to a model with no registry entry.
const DefaultContext = 8192

// Registry maps a model id to its context length in tokens.
type Registry struct {
	DefaultContext int            `yaml:"default_context"`
	Models         map[string]int `yaml:"models"`
}

// loadBuiltIn returns the compiled-in table. It is the floor: an on-disk
// registry may add or override entries but never removes built-in ones, so a
// partial external file cannot blind the selector.
func loadBuiltIn() (*Registry, error) {
	return &Registry{DefaultContext: DefaultContext, Models: builtIn.Models}, nil
}

// readFile is a variable so tests can avoid touching the filesystem.
var readFile = os.ReadFile

// Load reads an optional registry YAML file over the built-in table. A missing
// or empty path yields the built-in table. A missing file is not an error; a
// malformed one is.
func Load(path string) (*Registry, error) {
	base, err := loadBuiltIn()
	if err != nil {
		return nil, err
	}
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
	if r.DefaultContext <= 0 {
		r.DefaultContext = base.DefaultContext
	}
	// Merge rather than reassign: the external entries must survive, and the
	// built-ins must never be dropped by a partial external file. Reassigning
	// r.Models = base.Models would throw the file's additions away.
	ext := r.Models
	r.Models = make(map[string]int, len(builtIn.Models)+len(ext))
	for k, v := range builtIn.Models {
		r.Models[k] = v
	}
	for k, v := range ext {
		r.Models[k] = v
	}
	return &r, nil
}

// For returns the registered context length for a model, or DefaultContext.
// Lookups try the full upstream id then the tail after any namespace slash, so
// "nvidia/deepseek-ai/deepseek-v4-flash" and "deepseek-v4-flash" both resolve.
func (r *Registry) For(model string) int {
	if r == nil {
		return DefaultContext
	}
	dflt := r.DefaultContext
	if dflt <= 0 {
		dflt = DefaultContext
	}
	for _, key := range candidateKeys(model) {
		if n, ok := r.Models[key]; ok && n > 0 {
			return n
		}
	}
	return dflt
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
