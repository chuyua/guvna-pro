package selector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/router"
)

func cacheCandidates(model string) []Candidate {
	return []Candidate{
		{Chain: "a", Step: router.Step{Provider: "a", Model: model}},
		{Chain: "b", Step: router.Step{Provider: "b", Model: "y"}},
	}
}

func TestDecideNoDeciderWithCache(t *testing.T) {
	for _, warm := range []bool{false, true} {
		cache, mu := &Cache{}, &sync.Mutex{}
		cands := cacheCandidates("x")
		if warm {
			Decide(cands, func(context.Context) (map[string]int, error) {
				return map[string]int{"a/x": 0}, nil
			}, nil, time.Minute, mu, cache)
		}
		got, _, source := Decide(cands, nil, []string{"b", "a"}, time.Minute, mu, cache)
		if source != "decider_unavailable" || len(got) != 2 || got[0].Step.Provider != "b" {
			t.Fatalf("warm=%v: source=%q order=%v; want static fallback", warm, source, got)
		}
		if len(cache.inflight) != 0 {
			t.Fatal("missing decider must not leave an inflight call")
		}
	}
}

func TestDecideSharesSuccessAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			cache, mu := &Cache{}, &sync.Mutex{}
			cands := cacheCandidates("x")
			var calls atomic.Int32
			entered, release := make(chan struct{}), make(chan struct{})
			decider := func(context.Context) (map[string]int, error) {
				if calls.Add(1) == 1 {
					close(entered)
				}
				<-release
				if fail {
					return nil, errors.New("local mock failure")
				}
				return map[string]int{"b/y": 0, "a/x": 1}, nil
			}
			const n = 16
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, _, source := Decide(cands, decider, []string{"a", "b"}, time.Minute, mu, cache)
					wantHead := "b"
					if fail {
						wantHead = "a"
						if source != "decider_unavailable" {
							t.Errorf("failed call source=%q", source)
						}
					} else if source != "decider" && source != "decider_shared" && source != "decider_cached" {
						t.Errorf("successful call source=%q", source)
					}
					if len(got) != 2 || got[0].Step.Provider != wantHead {
						t.Errorf("order=%v; want head %s", got, wantHead)
					}
				}()
			}
			<-entered
			close(release)
			wg.Wait()
			if calls.Load() != 1 || len(cache.inflight) != 0 {
				t.Fatalf("calls=%d inflight=%d; want 1 and 0", calls.Load(), len(cache.inflight))
			}
			// Expire without sleeping: both positive and negative entries must
			// allow a new request, and the completed call must not stay stuck.
			mu.Lock()
			cache.at = time.Now().Add(-2 * time.Minute)
			mu.Unlock()
			Decide(cands, decider, nil, time.Minute, mu, cache)
			if calls.Load() != 2 || len(cache.inflight) != 0 {
				t.Fatalf("after expiry calls=%d inflight=%d", calls.Load(), len(cache.inflight))
			}
		})
	}
}

func TestDecideFailedRankingIsNeverCachedAsSuccess(t *testing.T) {
	for _, nilSuccess := range []bool{false, true} {
		cache, mu := &Cache{}, &sync.Mutex{}
		calls := 0
		decider := func(context.Context) (map[string]int, error) {
			calls++
			if nilSuccess {
				return nil, nil
			}
			return map[string]int{"b/y": 0}, errors.New("partial result")
		}
		for i := 0; i < 2; i++ {
			got, _, source := Decide(cacheCandidates("x"), decider, []string{"a", "b"}, time.Minute, mu, cache)
			if source != "decider_unavailable" || got[0].Step.Provider != "a" {
				t.Fatalf("nilSuccess=%v call=%d source=%q order=%v", nilSuccess, i, source, got)
			}
		}
		if calls != 1 {
			t.Fatalf("nilSuccess=%v calls=%d; want negative cache", nilSuccess, calls)
		}
	}
}

func TestDecideDifferentCandidateSetsDoNotShareInflight(t *testing.T) {
	cache, mu := &Cache{}, &sync.Mutex{}
	entered := make(chan string, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for _, model := range []string{"x", "z"} {
		wg.Add(1)
		go func(model string) {
			defer wg.Done()
			got, _, _ := Decide(cacheCandidates(model), func(context.Context) (map[string]int, error) {
				entered <- model
				<-release
				return map[string]int{"a/" + model: 0}, nil
			}, []string{"b", "a"}, time.Minute, mu, cache)
			if got[0].Step.Model != model {
				t.Errorf("model=%s got=%v; candidate ranking leaked", model, got)
			}
		}(model)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case model := <-entered:
			seen[model] = true
		case <-time.After(2 * time.Second):
			close(release)
			wg.Wait()
			t.Fatal("different candidates were merged or blocked behind the decider lock")
		}
	}
	close(release)
	wg.Wait()
	if len(seen) != 2 || len(cache.inflight) != 0 {
		t.Fatalf("sets=%v inflight=%d", seen, len(cache.inflight))
	}
}
