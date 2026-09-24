package selector

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/router"
)

func TestParseRanking(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"{\"order\":[\"amd/X\",\"nvidia/Y\"]}"}}]}`)
	ranked, err := ParseRanking(body)
	if err != nil {
		t.Fatalf("ParseRanking: %v", err)
	}
	if ranked["amd/X"] != 0 || ranked["nvidia/Y"] != 1 {
		t.Fatalf("ranked = %v; want amd/X=0, nvidia/Y=1", ranked)
	}
}

func TestParseRankingRejectsGibberish(t *testing.T) {
	for _, body := range []string{
		`{"choices":[{"message":{"content":"the best one is amd"}}]}`,
		`{"choices":[]}`,
		`not json`,
		`{"choices":[{"message":{"content":"{\"order\":[]}"}}]}`,
	} {
		if _, err := ParseRanking([]byte(body)); err == nil {
			t.Fatalf("ParseRanking(%q) accepted; want error", body)
		}
	}
}

func TestEligibleFiltersByContextAndDedupes(t *testing.T) {
	chains := []router.ChainInfo{
		{Name: "big", Steps: []router.Step{{Provider: "amd", Model: "Huge"}}},
		{Name: "big-dup", Steps: []router.Step{{Provider: "amd", Model: "Huge"}}},
		{Name: "small", Steps: []router.Step{{Provider: "kira", Model: "Tini"}}},
		{Name: "mid", Steps: []router.Step{{Provider: "nvidia", Model: "Okay"}}},
	}
	ctx := func(m string) int {
		return map[string]int{"Huge": 100000, "Tini": 100, "Okay": 512}[m]
	}
	in, out := Eligible(chains, 512, 0, ctx, nil)
	if len(in) != 2 {
		t.Fatalf("eligible = %d, want 2: %+v", len(in), in)
	}
	// amd/Huge must appear once, not twice, even though two chains list it.
	n := 0
	for _, c := range in {
		if c.Step.Provider == "amd" && c.Step.Model == "Huge" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("amd/Huge appears %d times; want 1 (dedupe)", n)
	}
	if len(out) != 1 || out[0].Reason != "context_below_min" {
		t.Fatalf("excluded = %+v; want one context_below_min", out)
	}
}

func TestEligibleSkipsCoolOff(t *testing.T) {
	hlth := health.New()
	for i := 0; i < health.FailureThreshold; i++ {
		hlth.Mark("amd", "Dead", 503, nil)
	}
	if !hlth.IsDown("amd", "Dead") {
		t.Fatal("setup: step should be down after threshold failures")
	}
	chains := []router.ChainInfo{{Name: "c", Steps: []router.Step{{Provider: "amd", Model: "Dead"}}}}
	in, out := Eligible(chains, 0, 0, func(string) int { return 100 }, hlth)
	if len(in) != 0 || len(out) != 1 || out[0].Reason != "cool_off" {
		t.Fatalf("in=%+v out=%+v; want cool_off exclusion", in, out)
	}
}

func TestRankPutsDecidedBeforeUnranked(t *testing.T) {
	cands := []Candidate{
		{Chain: "a", Step: router.Step{Provider: "z", Model: "Unranked"}, Context: 100},
		{Chain: "b", Step: router.Step{Provider: "a", Model: "Ranked"}, Context: 100},
	}
	// The decider names only "a/Ranked". The unranked step must not slip ahead
	// of it just because it has no score.
	ranked := map[string]int{"a/Ranked": 0}
	got := FallbackOrder(cands, nil, ranked)
	if got[0].Step.Provider != "a" {
		t.Fatalf("rank order = %v, %v; ranked step must come first",
			got[0].Step.Provider+"/"+got[0].Step.Model, got[1].Step.Provider+"/"+getStepModel(got[1]))
	}
}

func getStepModel(c Candidate) string { return c.Step.Model }

func TestDecideFallsBackWhenNoDecider(t *testing.T) {
	cands := []Candidate{
		{Chain: "x", Step: router.Step{Provider: "kira", Model: "K"}, Context: 100},
		{Chain: "y", Step: router.Step{Provider: "amd", Model: "A"}, Context: 100},
	}
	got, decided, source := Decide(cands, nil, []string{"amd", "nvidia", "kira"}, 0, nil, nil)
	if source != "decider_unavailable" {
		t.Fatalf("source = %q, want decider_unavailable", source)
	}
	if len(got) != 2 {
		t.Fatalf("got %d candidates; want all of them (fallback, not empty)", len(got))
	}
	if got[0].Step.Provider != "amd" {
		t.Fatalf("fallback head = %s, want amd (static priority)", got[0].Step.Provider)
	}
	_ = decided
}

func TestDecideUsesRanking(t *testing.T) {
	cands := []Candidate{
		{Chain: "x", Step: router.Step{Provider: "amd", Model: "Slow"}, Context: 100},
		{Chain: "y", Step: router.Step{Provider: "nvidia", Model: "Fast"}, Context: 100},
	}
	orderJSON, _ := json.Marshal(map[string]any{"order": []string{"nvidia/Fast", "amd/Slow"}})
	// message.content is a JSON *string* in the OpenAI shape, with the ranking
	// object encoded inside it. Embedding the object directly would not parse.
	respJSON, _ := json.Marshal(map[string]any{
		"choices": []map[string]any{{"message": map[string]any{"content": string(orderJSON)}}},
	})
	decider := func(ctx context.Context) (map[string]int, error) {
		return ParseRanking(respJSON)
	}
	got, _, source := Decide(cands, decider, []string{"amd", "nvidia"}, 0, nil, nil)
	if source != "decider" {
		t.Fatalf("source = %q, want decider", source)
	}
	if got[0].Step.Provider != "nvidia" {
		t.Fatalf("head = %s, want nvidia (decider overrode static amd-first)", got[0].Step.Provider)
	}
}

func TestDecideSingleCandidateSkipsDecider(t *testing.T) {
	called := false
	cands := []Candidate{{Chain: "x", Step: router.Step{Provider: "amd", Model: "A"}, Context: 100}}
	got, _, source := Decide(cands, func(ctx context.Context) (map[string]int, error) {
		called = true
		return nil, nil
	}, nil, 0, nil, nil)
	if source != "single_candidate" {
		t.Fatalf("source = %q, want single_candidate", source)
	}
	if called {
		t.Fatal("decider called for a single candidate; it should be skipped")
	}
	if len(got) != 1 {
		t.Fatalf("got %d candidates; want 1", len(got))
	}
}

func TestDecideCachesResult(t *testing.T) {
	calls := 0
	decider := func(ctx context.Context) (map[string]int, error) {
		calls++
		return map[string]int{"a/A": 0}, nil
	}
	cands := []Candidate{
		{Chain: "x", Step: router.Step{Provider: "a", Model: "A"}, Context: 100},
		{Chain: "y", Step: router.Step{Provider: "b", Model: "B"}, Context: 100},
	}
	cache := &Cache{}
	mu := &sync.Mutex{}
	for i := 0; i < 3; i++ {
		_, _, src := Decide(cands, decider, nil, time.Minute, mu, cache)
		if src != "decider" && src != "decider_cached" {
			t.Fatalf("call %d source = %q", i, src)
		}
	}
	if calls != 1 {
		t.Fatalf("decider called %d times over 3 Decide calls; want 1 (cached)", calls)
	}
}

// TestDecideCachesDeciderFailure is the case the live gateway hit: a wedged
// decider must cost one call per interval, not one per request. Without this,
// every /v1/auto request pays the decider's 20s timeout before falling back.
func TestDecideCachesDeciderFailure(t *testing.T) {
	calls := 0
	decider := func(ctx context.Context) (map[string]int, error) {
		calls++
		return nil, errors.New("upstream timeout")
	}
	cands := []Candidate{
		{Chain: "x", Step: router.Step{Provider: "a", Model: "A"}, Context: 100},
		{Chain: "y", Step: router.Step{Provider: "b", Model: "B"}, Context: 100},
	}
	cache := &Cache{}
	mu := &sync.Mutex{}
	for i := 0; i < 3; i++ {
		got, _, src := Decide(cands, decider, []string{"a", "b"}, time.Minute, mu, cache)
		if src != "decider_unavailable" {
			t.Fatalf("call %d source = %q", i, src)
		}
		// Fallback must still produce every candidate, in provider order.
		if len(got) != 2 {
			t.Fatalf("call %d returned %d candidates; want 2", i, len(got))
		}
		if got[0].Step.Provider != "a" {
			t.Fatalf("call %d head = %q; want static provider order", i, got[0].Step.Provider)
		}
	}
	if calls != 1 {
		t.Fatalf("decider called %d times over 3 Decide calls; want 1 (negative cache)", calls)
	}
}
