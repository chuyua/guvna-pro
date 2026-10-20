package chains

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/creamy-ghost/guvna/internal/router"
	"gopkg.in/yaml.v3"
)

func TestConcurrentSavePersistsFinalMutations(t *testing.T) {
	r := newRouter()
	s := New(t.TempDir())
	const workers = 64
	run := func(mutate func(int) error) {
		t.Helper()
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if err := mutate(i); err != nil {
					errs <- err
					return
				}
				errs <- s.Save(r)
			}(i)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	add := func(name string) error {
		return r.AddChain(name, []router.Step{{Provider: "groq", Model: "test-model"}})
	}
	run(func(i int) error { return add(fmt.Sprintf("chain-%02d", i)) })
	run(func(i int) error {
		if i%2 == 0 {
			return r.RemoveChain(fmt.Sprintf("chain-%02d", i))
		}
		return add(fmt.Sprintf("added-%02d", i))
	})
	loaded := newRouter()
	if err := s.Load(loaded); err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.RuntimeChains(), r.RuntimeChains(); !reflect.DeepEqual(got, want) {
		t.Fatalf("saved state differs from final router state: got %#v, want %#v", got, want)
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var saved fileFormat
	if err := yaml.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(saved.Chains))
	for _, chain := range saved.Chains {
		names = append(names, chain.Name)
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("chains not saved in stable name order: %v", names)
	}
	assertNoTempFiles(t, s)
}

func TestSaveRenameFailureCleansTemp(t *testing.T) {
	s := New(t.TempDir())
	// A non-empty directory cannot be replaced by the completed temp file.
	if err := os.Mkdir(s.path, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(s.path, "keep")
	if err := os.WriteFile(marker, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(newRouter()); err == nil {
		t.Fatal("expected rename failure")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "original" {
		t.Fatalf("original target changed: data=%q, err=%v", data, err)
	}
	assertNoTempFiles(t, s)
}

func assertNoTempFiles(t *testing.T, s *Store) {
	t.Helper()
	matches, err := filepath.Glob(s.path + ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary chain files leaked: %v", matches)
	}
}
