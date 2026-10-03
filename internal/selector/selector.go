// Package selector picks the model order for an /v1/auto request.
//
// Two layers, deliberately separate:
//
//  1. Eligibility is mechanical. A step is a candidate when its provider/model
//     is not in health cool-off and its registered context length meets the
//     caller's min/max context filter. Nothing is invented here.
//
//  2. Ordering among the eligible candidates is decided by a small LLM call, so
//     the priority is a judgement about the health state at request time rather
//     than a static PROVIDE_ORDER that only reflects whatever the fleet looked
//     like when someone last edited a script. When the decider is missing,
//     unavailable, or answers nonsense, ordering falls back to that static
//     priority — the decider must never be a reason for /v1/auto to fail.
//
// The decider is an ordinary provider step that runs with auto-routing disabled,
// so it cannot recurse into /v1/auto. It is invoked at most once per
// DecideInterval, not once per request: the ordering only needs to be current
// enough to notice a provider going down, and that window is far wider than a
// single request's latency budget.
package selector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/router"
)

// Candidate is one routable provider/model pair with the facts a caller needs.
type Candidate struct {
	Chain   string
	Step    router.Step
	Context int
	Down    bool
}

// Decided records where one candidate landed and why, for logging and for the
// /admin/status surface.
type Decided struct {
	Chain   string `json:"chain"`
	Step    string `json:"step"`
	Context int    `json:"context"`
	Score   int    `json:"score"`
	Reason  string `json:"reason"`
}

// Eligible returns the candidate steps across all chains whose model context
// meets [minCtx, maxCtx]. Excluded steps are reported too — a caller that
// filtered to zero candidates needs to know that it asked for something that
// does not exist, not that it was unlucky.
//
// The same provider/model pair may appear on more than one chain (a chain
// named for a family and a provider-prefixed passthrough of the same step).
// The pair is deduplicated on first chain, so one hung upstream cannot be tried
// three times to make up for three chains that mention it.
func Eligible(chains []router.ChainInfo, minCtx, maxCtx int, ctx ForContext, hlth *health.Tracker) (in []Candidate, out []Decided) {
	seen := make(map[string]bool)
	for _, c := range chains {
		for _, st := range c.Steps {
			k := st.Provider + "/" + st.Model
			if seen[k] {
				continue
			}
			seen[k] = true
			can := Candidate{Chain: c.Name, Step: st, Context: ctx(st.Model)}
			if maxCtx > 0 && can.Context > maxCtx {
				out = append(out, Decided{Chain: c.Name, Step: k, Context: can.Context, Reason: "context_above_max"})
				continue
			}
			if minCtx > 0 && can.Context < minCtx {
				out = append(out, Decided{Chain: c.Name, Step: k, Context: can.Context, Reason: "context_below_min"})
				continue
			}
			if hlth != nil && hlth.IsDown(st.Provider, st.Model) {
				can.Down = true
				out = append(out, Decided{Chain: c.Name, Step: k, Context: can.Context, Reason: "cool_off"})
				continue
			}
			in = append(in, can)
		}
	}
	return in, out
}

// ForContext resolves a model to a context length.
type ForContext func(model string) int

// Decide orders eligible candidates. An empty decider closure, or a decider that
// fails, yields the static priority order — the decider is an advisory layer.
// Candidates the decider did not mention keep a neutral score and sort after
// the ranked ones, before the unranked tail.
func Decide(cands []Candidate, decider func(ctx context.Context) (map[string]int, error), order []string, interval time.Duration, mu *sync.Mutex, cache *Cache) ([]Candidate, []Decided, string) {
	ranked, source, err := decideOrder(cands, decider, interval, mu, cache)
	if err != nil {
		source = "decider_unavailable"
		ranked = nil
	}
	return FallbackOrder(cands, order, ranked), trace(cands, ranked, source), source
}

// trace builds a Decided row per candidate, naming the ordering source so a log
// line can say whether the order came from the decider or from the fallback.
func trace(cands []Candidate, ranked map[string]int, source string) []Decided {
	out := make([]Decided, 0, len(cands))
	for _, c := range cands {
		step := c.Step.Provider + "/" + c.Step.Model
		sc, ok := scoreOf(step, ranked)
		reason := source
		if !ok {
			sc = 1 << 30
			if source == "decider" || source == "decider_cached" {
				reason = "not_ranked_by_decider"
			}
		}
		out = append(out, Decided{Chain: c.Chain, Step: step, Context: c.Context, Score: sc, Reason: reason})
	}
	return out
}

// rank sorts a copy of cands without disturbing the caller's slice.
func FallbackOrder(cands []Candidate, order []string, ranked map[string]int) []Candidate {
	prio := priorityMap(order)
	cp := make([]Candidate, len(cands))
	copy(cp, cands)
	sort.SliceStable(cp, func(i, j int) bool {
		si, oi := scoreOf(cp[i].Step.Provider+"/"+cp[i].Step.Model, ranked)
		sj, oj := scoreOf(cp[j].Step.Provider+"/"+cp[j].Step.Model, ranked)
		// A step the decider ranked always sorts before one it did not mention;
		// comparing scores directly would put an unranked step (score 0) ahead of
		// the decider's top pick.
		if oi != oj {
			return oi && !oj
		}
		if oi {
			return si < sj
		}
		pi := prio[cp[i].Step.Provider]
		pj := prio[cp[j].Step.Provider]
		if pi != pj {
			return pi < pj
		}
		return cp[i].Chain < cp[j].Chain
	})
	return cp
}

func scoreOf(step string, ranked map[string]int) (int, bool) {
	if ranked == nil {
		return 0, false
	}
	sc, ok := ranked[step]
	if ok {
		return sc, true
	}
	// The decider may return the bare chain name rather than "provider/model".
	parts := strings.SplitN(step, "/", 2)
	if len(parts) == 2 {
		if sc, ok := ranked[parts[1]]; ok {
			return sc, true
		}
	}
	return 0, false
}

func priorityMap(order []string) map[string]int {
	m := make(map[string]int, len(order))
	for i, n := range order {
		if _, ok := m[n]; !ok {
			m[n] = i
		}
	}
	return m
}

// decideOrder returns a decider ranking, cached for the interval. A failed
// decider is cached too, so a wedged decider costs one call per interval rather
// than one per request. The mutex is only held while reading or writing the
// cache, never across the decider call itself — that call can take seconds.
func decideOrder(cands []Candidate, decider func(ctx context.Context) (map[string]int, error), interval time.Duration, mu *sync.Mutex, cache *Cache) (map[string]int, string, error) {
	if len(cands) <= 1 {
		return nil, "single_candidate", nil
	}
	if decider == nil {
		return nil, "", fmt.Errorf("decider not configured")
	}
	var key string
	if mu != nil && cache != nil {
		key = candidatesKey(cands)
		mu.Lock()
		// Two cached states: a ranking, or a decider call that failed. The
		// second one matters — with it a wedged decider costs one call per
		// interval instead of one per request, which would turn the
		// decider's 20s into the latency of every /v1/auto call.
		hit := cache.key == key && time.Since(cache.at) < interval
		if hit && cache.order != nil {
			order := cache.order
			mu.Unlock()
			return order, "decider_cached", nil
		}
		if hit && cache.order == nil {
			mu.Unlock()
			return nil, "", fmt.Errorf("decider cached failure")
		}
		// A live call for the same candidate set is already in flight: wait
		// for it instead of issuing a duplicate decider request — but only
		// briefly. The leader may be a background refresh whose decider call
		// runs to the client's full 90s header timeout; a request that waits
		// unconditionally then blows past its own patience (observed: 60s
		// client disconnects while every candidate was still queued). After
		// the wait budget the caller takes the static fallback order; when
		// the leader lands it writes the cache and later requests share it.
		if call, ok := cache.inflight[key]; ok {
			mu.Unlock()
			timer := time.NewTimer(DefaultShareWait)
			select {
			case <-call.done:
				timer.Stop()
			case <-timer.C:
				return nil, "", fmt.Errorf("decider call in flight, share wait expired")
			}
			if call.err != nil {
				return nil, "decider_shared", call.err
			}
			return call.order, "decider_shared", nil
		}
		if cache.inflight == nil {
			cache.inflight = make(map[string]*decideCall)
		}
		call := &decideCall{done: make(chan struct{})}
		cache.inflight[key] = call
		mu.Unlock()

		ranked, err := decider(context.Background())
		if err == nil && ranked == nil {
			err = fmt.Errorf("decider returned no ranking")
		}
		if err != nil {
			ranked = nil
		}
		mu.Lock()
		// err is nil iff ranked is non-nil, so one store covers both states.
		cache.key, cache.order, cache.at = key, ranked, time.Now()
		delete(cache.inflight, key)
		mu.Unlock()
		call.order, call.err = ranked, err
		close(call.done)
		if err != nil {
			return nil, "", err
		}
		return ranked, "decider", nil
	}
	ranked, err := decider(context.Background())
	if err == nil && ranked == nil {
		err = fmt.Errorf("decider returned no ranking")
	}
	if err != nil {
		return nil, "", err
	}
	return ranked, "decider", nil
}

func candidatesKey(cands []Candidate) string {
	var b strings.Builder
	for _, c := range cands {
		fmt.Fprintf(&b, "%s/%s|", c.Step.Provider, c.Step.Model)
	}
	return b.String()
}

type Cache struct {
	key   string
	order map[string]int
	at    time.Time
	// inflights merges concurrent cold-cache misses on the same candidate
	// set into one decider call. Without it, N simultaneous /v1/auto
	// requests on a cold or expired cache fire N decider calls — paid
	// tokens and rate-limit pressure for an identical answer.
	inflight map[string]*decideCall
}

type decideCall struct {
	done  chan struct{}
	order map[string]int
	err   error
}

// DefaultShareWait bounds how long a request may block on another caller's
// in-flight decider call. It mirrors DefaultDeciderRequestWait: a wait that
// exceeds the client's patience is worse than the static fallback order the
// caller gets instead.
const DefaultShareWait = 10 * time.Second

// ParseRanking reads a decider answer of the form {"order":["amd/X","nvidia/Y"]}.
// Anything unparsable yields an error; the caller then falls back. Positions are
// 1-based in the decider's head but are stored as 0-based scores here.
//
// Thinking models (glm-5.3, deepseek, ...) put their output in reasoning_content
// and leave content null. ParseRanking checks both fields: content first, then
// reasoning_content, so a thinking decider works without special configuration.
func ParseRanking(body []byte) (map[string]int, error) {
	var resp struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("no choices")
	}
	content := strings.TrimSpace(resp.Choices[0].Message.Content)
	if content == "" || content == "null" {
		content = strings.TrimSpace(resp.Choices[0].Message.ReasoningContent)
	}
	var want struct {
		Order []string `json:"order"`
	}
	if err := json.Unmarshal([]byte(content), &want); err != nil {
		return nil, err
	}
	out := make(map[string]int, len(want.Order))
	for i, s := range want.Order {
		out[strings.TrimSpace(s)] = i
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty order")
	}
	return out, nil
}
