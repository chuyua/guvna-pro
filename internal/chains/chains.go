package chains

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/creamy-ghost/guvna/internal/router"
	"gopkg.in/yaml.v3"
)

// ErrPersistence distinguishes storage failures from chain validation errors.
var ErrPersistence = errors.New("persist runtime chains")

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
	mu      sync.Mutex // serializes snapshots and writes for this Store instance
	path    string
	dormant map[string]fileChain // records not currently accepted by the router
}

func New(dataDir string) *Store {
	return &Store{path: filepath.Join(dataDir, "chains.yaml")}
}

// Load reads runtime chains and applies them to the router. Invalid chains stay
// dormant in the store so later writes cannot erase temporarily unavailable
// providers. Existing (config-defined) chains take precedence over file records.
func (s *Store) Load(r *router.Router) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.dormant = nil
			return nil
		}
		return fmt.Errorf("read chains: %w", err)
	}
	var f fileFormat
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse chains.yaml: %w", err)
	}
	existing := make(map[string]bool)
	for _, name := range r.ChainNames() {
		existing[name] = true
	}
	dormant := make(map[string]fileChain)
	for _, ch := range f.Chains {
		if existing[ch.Name] {
			fmt.Printf("chains: skipping %q from chains.yaml: chain already exists\n", ch.Name)
			continue
		}
		steps := make([]router.Step, 0, len(ch.Steps))
		for _, st := range ch.Steps {
			steps = append(steps, router.Step{Provider: st.Provider, Model: st.Model, Params: st.Params})
		}
		if err := r.AddChain(ch.Name, steps); err != nil {
			dormant[ch.Name] = ch
			fmt.Printf("chains: keeping %q dormant from chains.yaml: %v\n", ch.Name, err)
			continue
		}
		existing[ch.Name] = true
		delete(dormant, ch.Name)
	}
	s.dormant = dormant
	return nil
}

// Save writes runtime and dormant chains atomically (tmp file + rename).
func (s *Store) Save(r *router.Router) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Take the snapshot inside the write lock so an older queued save cannot
	// overwrite a newer snapshot after it has already reached disk.
	return s.persist(r.RuntimeChains())
}

// AddChain saves a candidate snapshot before the router publishes the chain.
// All Store operations acquire Store.mu before Router.mu; persistence callbacks
// never reenter the router, avoiding the inverse lock order.
func (s *Store) AddChain(r *router.Router, name string, steps []router.Step) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.AddChainPersisted(name, steps, s.persist)
}

// RemoveChain saves a candidate snapshot before the router removes the chain.
func (s *Store) RemoveChain(r *router.Router, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.RemoveChainPersisted(name, s.persist)
}

func (s *Store) persist(runtime map[string][]router.Step) error {
	if err := s.writeSnapshot(runtime); err != nil {
		return fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	// A successfully persisted runtime chain replaces a dormant record with the
	// same name. Retire it only after the write succeeds so failures lose nothing.
	for name := range runtime {
		delete(s.dormant, name)
	}
	return nil
}

// writeSnapshot requires Store.mu. Its input is an isolated candidate or a
// committed router snapshot; it must not acquire any router lock.
func (s *Store) writeSnapshot(runtime map[string][]router.Step) error {
	f := fileFormat{Version: 2, Chains: make([]fileChain, 0, len(runtime)+len(s.dormant))}
	names := make([]string, 0, len(runtime)+len(s.dormant))
	for name := range s.dormant {
		names = append(names, name)
	}
	for name := range runtime {
		if _, dormant := s.dormant[name]; !dormant {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		ch := s.dormant[name]
		if steps, active := runtime[name]; active {
			ch = fileChain{Name: name, Steps: make([]fileStep, 0, len(steps))}
			for _, st := range steps {
				ch.Steps = append(ch.Steps, fileStep{Provider: st.Provider, Model: st.Model, Params: st.Params})
			}
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
