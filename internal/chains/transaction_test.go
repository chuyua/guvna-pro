package chains

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/creamy-ghost/guvna/internal/router"
)

func TestPersistedMutationFailureAndRetry(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "add", true: "remove"}[remove], func(t *testing.T) {
			r, s := newRouter(), New(t.TempDir())
			steps := []router.Step{{Provider: "groq", Model: "shared"}}
			for _, name := range []string{"older", "newer"} {
				if err := s.AddChain(r, name, steps); err != nil {
					t.Fatal(err)
				}
			}
			beforeRuntime, beforeList := r.RuntimeChains(), r.ListChains()
			originalPath := s.path
			beforeDisk, err := os.ReadFile(originalPath)
			if err != nil {
				t.Fatal(err)
			}
			// A regular file cannot become a directory, on any supported OS.
			// The existing saved file stays intact throughout this failed write.
			s.path = filepath.Join(originalPath, "chains.yaml")
			mutate := func() error {
				if remove {
					return s.RemoveChain(r, "older")
				}
				return s.AddChain(r, "added", steps)
			}
			if err := mutate(); !errors.Is(err, ErrPersistence) {
				t.Fatalf("storage error not classified: %v", err)
			}
			if !reflect.DeepEqual(r.RuntimeChains(), beforeRuntime) || !reflect.DeepEqual(r.ListChains(), beforeList) {
				t.Fatal("failed write changed committed chains")
			}
			if name, _, err := r.Resolve("shared"); err != nil || name != "newer" {
				t.Fatalf("failed write changed model owner: name=%q err=%v", name, err)
			}
			if name, _, err := r.Resolve("older"); err != nil || name != "older" {
				t.Fatalf("failed write removed an existing chain: name=%q err=%v", name, err)
			}
			if _, _, err := r.Resolve("added"); !errors.Is(err, router.ErrNotFound) {
				t.Fatalf("failed write published a new chain: %v", err)
			}
			afterDisk, err := os.ReadFile(originalPath)
			if err != nil || !bytes.Equal(afterDisk, beforeDisk) {
				t.Fatalf("failed write changed saved bytes: err=%v", err)
			}
			s.path = originalPath
			if err := mutate(); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			loaded := newRouter()
			if err := s.Load(loaded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(loaded.RuntimeChains(), r.RuntimeChains()) {
				t.Fatal("successful retry left disk and router inconsistent")
			}
			wantOwner := "newer"
			if !remove {
				wantOwner = "added"
			}
			if name, _, err := r.Resolve("shared"); err != nil || name != wantOwner {
				t.Fatalf("wrong model owner after retry: name=%q err=%v", name, err)
			}
			assertNoTempFiles(t, s)
		})
	}
}

func TestPersistedMutationValidationDoesNotWrite(t *testing.T) {
	r, s := newRouter(), New(t.TempDir())
	steps := []router.Step{{Provider: "groq", Model: "m"}}
	if err := s.AddChain(r, "existing", steps); err != nil {
		t.Fatal(err)
	}
	// If validation accidentally reaches persistence, it reports ErrPersistence.
	s.path = filepath.Join(s.path, "chains.yaml")
	for _, mutate := range []func() error{
		func() error { return s.AddChain(r, "Bad_Name", steps) },
		func() error { return s.AddChain(r, "empty", nil) },
		func() error { return s.AddChain(r, "existing", steps) },
		func() error { return s.RemoveChain(r, "missing") },
		func() error { return s.RemoveChain(r, "from-config") },
	} {
		if err := mutate(); err == nil || errors.Is(err, ErrPersistence) {
			t.Fatalf("validation error misclassified: %v", err)
		}
	}
}

func TestConcurrentPersistedMutationsAndSave(t *testing.T) {
	r, s := newRouter(), New(t.TempDir())
	const workers = 32
	steps := []router.Step{{Provider: "groq", Model: "shared"}}
	run := func(mutate func(int) error) {
		t.Helper()
		start := make(chan struct{})
		errs := make(chan error, workers*2)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(2)
			go func(i int) {
				defer wg.Done()
				<-start
				errs <- mutate(i)
			}(i)
			go func() {
				defer wg.Done()
				<-start
				errs <- s.Save(r)
			}()
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
	run(func(i int) error { return s.AddChain(r, fmt.Sprintf("chain-%02d", i), steps) })
	run(func(i int) error {
		if i%2 == 0 {
			return s.RemoveChain(r, fmt.Sprintf("chain-%02d", i))
		}
		return s.AddChain(r, fmt.Sprintf("added-%02d", i), steps)
	})
	loaded := newRouter()
	if err := s.Load(loaded); err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.RuntimeChains(), r.RuntimeChains(); !reflect.DeepEqual(got, want) || len(got) != workers {
		t.Fatalf("concurrent transactions lost changes: got=%v want=%v", got, want)
	}
	assertNoTempFiles(t, s)
}
