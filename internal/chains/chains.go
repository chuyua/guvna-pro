package chains

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
	loadErr error                // prevents writes after an unprotected load failure
}

func New(dataDir string) *Store {
	return &Store{path: filepath.Join(dataDir, "chains.yaml")}
}

// Load reads runtime chains and applies them to the router. Invalid chains stay
// dormant in the store so later writes cannot erase temporarily unavailable
// providers. Existing (config-defined) chains take precedence over file records.
// Malformed files are moved aside before writes are allowed; other load failures
// block persistence until a successful reload protects the original data.
func (s *Store) Load(r *router.Router) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.loadErr = nil
			return nil
		}
		s.loadErr = fmt.Errorf("read chains: %w", err)
		return s.loadErr
	}
	var f fileFormat
	if err := yaml.Unmarshal(data, &f); err != nil {
		backup, backupErr := s.backupCorrupt()
		if backupErr != nil {
			s.loadErr = fmt.Errorf("parse chains.yaml: %w; preserve original: %w", err, backupErr)
			return s.loadErr
		}
		s.loadErr = nil
		return fmt.Errorf("parse chains.yaml: %w (original preserved at %s)", err, backup)
	}
	s.loadErr = nil
	existing := make(map[string]string)
	for _, ch := range r.ListChains() {
		existing[ch.Name] = ch.Source
	}
	dormant := make(map[string]fileChain)
	for _, ch := range f.Chains {
		if source, exists := existing[ch.Name]; exists {
			if source == "config" {
				dormant[ch.Name] = ch
				fmt.Printf("chains: keeping %q dormant from chains.yaml: config chain takes precedence\n", ch.Name)
			}
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
		existing[ch.Name] = "runtime"
		delete(dormant, ch.Name)
	}
	s.dormant = dormant
	return nil
}

// backupCorrupt reserves a unique name before moving the original, so an older
// backup is never overwritten. Store.mu must be held by the caller.
func (s *Store) backupCorrupt() (string, error) {
	pattern := filepath.Base(s.path) + ".corrupt-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-*"
	backup, err := os.CreateTemp(filepath.Dir(s.path), pattern)
	if err != nil {
		return "", fmt.Errorf("reserve corrupt backup: %w", err)
	}
	name := backup.Name()
	if err := backup.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("close corrupt backup: %w", err)
	}
	if err := os.Rename(s.path, name); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("move corrupt chains to %s: %w", name, err)
	}
	return name, nil
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

// ListChains returns active and dormant chains with isolated step parameters.
// A config chain and its shadowed file record have the same name but different
// sources. An active runtime chain supersedes a dormant record of the same name.
func (s *Store) ListChains(r *router.Router) []router.ChainInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := r.ListChains()
	runtime := make(map[string]bool)
	for _, ch := range out {
		if ch.Source == "runtime" {
			runtime[ch.Name] = true
		}
	}
	for name, ch := range s.dormant {
		if runtime[name] {
			continue
		}
		steps := make([]router.Step, 0, len(ch.Steps))
		for _, st := range ch.Steps {
			step := router.Step{Provider: st.Provider, Model: st.Model}
			if st.Params != nil {
				step.Params = cloneParam(st.Params).(map[string]any)
			}
			steps = append(steps, step)
		}
		out = append(out, router.ChainInfo{Name: name, Source: "dormant", Steps: steps})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Source < out[j].Source
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// cloneParam copies the containers produced by YAML decoding, including invalid
// params retained in dormant records that have never passed router validation.
func cloneParam(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			return v
		}
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = cloneParam(item)
		}
		return out
	case map[any]any:
		if v == nil {
			return v
		}
		out := make(map[any]any, len(v))
		for key, item := range v {
			out[key] = cloneParam(item)
		}
		return out
	case []any:
		if v == nil {
			return v
		}
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneParam(item)
		}
		return out
	default:
		return value
	}
}

// RemoveChain saves a candidate snapshot before removing a runtime or dormant
// record. Removing a dormant record never changes a config chain of that name.
func (s *Store) RemoveChain(r *router.Router, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.dormant[name]; exists {
		// Keep the original map intact for rollback. Store.mu prevents readers
		// from observing the candidate before persistence has succeeded.
		original := s.dormant
		candidate := make(map[string]fileChain, len(original)-1)
		for key, ch := range original {
			if key != name {
				candidate[key] = ch
			}
		}
		s.dormant = candidate
		runtime := r.RuntimeChains()
		var err error
		if _, active := runtime[name]; active {
			err = r.RemoveChainPersisted(name, s.persist)
		} else {
			err = s.persist(runtime)
		}
		if err != nil {
			s.dormant = original
		}
		return err
	}
	return r.RemoveChainPersisted(name, s.persist)
}

func (s *Store) persist(runtime map[string][]router.Step) error {
	if s.loadErr != nil {
		return fmt.Errorf("%w: writes blocked until chains reload succeeds: %w", ErrPersistence, s.loadErr)
	}
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
