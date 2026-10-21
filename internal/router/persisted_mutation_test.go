package router

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestPersistedMutationFailurePreservesRouterState(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "add", true: "remove"}[remove], func(t *testing.T) {
			r := New(&config.Config{
				Providers: []config.Provider{{Name: "a"}},
				Chains:    []config.Chain{{Name: "configured", Steps: []config.Step{{Provider: "a", Model: "shared"}}}},
			})
			for _, name := range []string{"older", "newer"} {
				if err := r.AddChain(name, []Step{{Provider: "a", Model: "shared"}}); err != nil {
					t.Fatal(err)
				}
			}
			beforeRuntime, beforeList := r.RuntimeChains(), r.ListChains()
			beforeOwners := append([]string(nil), r.modelIndex["shared"]...)
			failure := errors.New("storage unavailable")
			called := false
			persist := func(candidate map[string][]Step) error {
				called = true
				if _, ok := candidate["configured"]; ok {
					t.Error("candidate includes a config chain")
				}
				if _, ok := candidate["older"]; ok == remove {
					t.Errorf("wrong candidate for remove=%v: %+v", remove, candidate)
				}
				if !remove {
					if _, ok := candidate["added"]; !ok {
						t.Error("candidate omits the new chain")
					}
				}
				if !reflect.DeepEqual(r.runtimeChainsLocked(), beforeRuntime) || !reflect.DeepEqual(r.modelIndex["shared"], beforeOwners) {
					t.Error("router changed before persistence completed")
				}
				// Mutating an isolated candidate must not corrupt committed chains.
				candidate["newer"][0].Model = "candidate-only"
				return failure
			}
			var err error
			if remove {
				err = r.RemoveChainPersisted("older", persist)
			} else {
				err = r.AddChainPersisted("added", []Step{{Provider: "a", Model: "shared"}}, persist)
			}
			if !called || !errors.Is(err, failure) {
				t.Fatalf("persistence failure not returned: called=%v err=%v", called, err)
			}
			if !reflect.DeepEqual(r.RuntimeChains(), beforeRuntime) || !reflect.DeepEqual(r.ListChains(), beforeList) || !reflect.DeepEqual(r.modelIndex["shared"], beforeOwners) {
				t.Fatal("failed persistence changed router state or model ownership order")
			}
			assertModelChain(t, r, "shared", "newer", "a")
		})
	}
}

func TestPersistedMutationHoldsReadersUntilCommit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "failure"}[fail], func(t *testing.T) {
			r := New(&config.Config{Providers: []config.Provider{{Name: "a"}}})
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			failure := errors.New("failed persistence")
			go func() {
				done <- r.AddChainPersisted("pending", []Step{{Provider: "a", Model: "m"}}, func(map[string][]Step) error {
					close(entered)
					<-release
					if fail {
						return failure
					}
					return nil
				})
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("persistence callback did not start")
			}
			// Public lookups all take this read lock, so none can observe a
			// pending chain or a stale snapshot across the commit boundary.
			if r.mu.TryRLock() {
				r.mu.RUnlock()
				close(release)
				t.Fatal("pending mutation did not hold the router lock")
			}
			close(release)
			select {
			case err := <-done:
				if fail && !errors.Is(err, failure) || !fail && err != nil {
					t.Fatalf("unexpected result: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("mutation did not release the router lock")
			}
			_, _, err := r.Resolve("pending")
			if fail && !errors.Is(err, ErrNotFound) || !fail && err != nil {
				t.Fatalf("wrong committed lookup: %v", err)
			}
		})
	}
}
