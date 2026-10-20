package chains

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/creamy-ghost/guvna/internal/router"
	"gopkg.in/yaml.v3"
)

type fileFormat struct {
	Version int         `yaml:"version,omitempty"`
	Chains  []fileChain `yaml:"chains"`
}

type fileStep struct {
	Provider string         `yaml:"provider"`
	Model    string         `yaml:"model"`
	Params   map[string]any `yaml:"params,omitempty"`
}

type fileChain struct {
	Name  string     `yaml:"name"`
	Steps []fileStep `yaml:"steps"`
}

// Store persists runtime chains to <data-dir>/chains.yaml with atomic rewrites.
type Store struct {
	mu   sync.Mutex // serializes snapshots and writes for this Store instance
	path string
}

func New(dataDir string) *Store {
	return &Store{path: filepath.Join(dataDir, "chains.yaml")}
}

// Load reads runtime chains and applies them to the router.
// Conflicts with existing (config-defined) chains are skipped with a warning.
func (s *Store) Load(r *router.Router) error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read chains: %w", err)
	}
	var f fileFormat
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse chains.yaml: %w", err)
	}
	for _, ch := range f.Chains {
		steps := make([]router.Step, 0, len(ch.Steps))
		for _, st := range ch.Steps {
			steps = append(steps, router.Step{Provider: st.Provider, Model: st.Model, Params: st.Params})
		}
		if err := r.AddChain(ch.Name, steps); err != nil {
			fmt.Printf("chains: skipping %q from chains.yaml: %v\n", ch.Name, err)
		}
	}
	return nil
}

// Save writes the current runtime chains atomically (tmp file + rename).
func (s *Store) Save(r *router.Router) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Take the snapshot inside the write lock so an older queued save cannot
	// overwrite a newer snapshot after it has already reached disk.
	runtime := r.RuntimeChains()
	f := fileFormat{Version: 2, Chains: make([]fileChain, 0, len(runtime))}
	names := make([]string, 0, len(runtime))
	for name := range runtime {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		steps := runtime[name]
		ch := fileChain{Name: name, Steps: make([]fileStep, 0, len(steps))}
		for _, st := range steps {
			ch.Steps = append(ch.Steps, fileStep{Provider: st.Provider, Model: st.Model, Params: st.Params})
		}
		f.Chains = append(f.Chains, ch)
	}
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal chains: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("chains dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create chains temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write chains: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close chains: %w", err)
	}
	// Rename replaces the existing file without deleting it first, including
	// on Windows. If replacement fails, the original remains in place.
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("rename chains: %w", err)
	}
	return nil
}
