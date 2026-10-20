package selector

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/creamy-ghost/guvna/internal/router"
	"sync"
	"testing"
	"time"
)

func TestRankingWrappedAndTruncated(t *testing.T) {
	for _, tc := range []struct {
		content, finish string
		ok              bool
	}{
		{"```json\n{\"order\":[\"p/B\",\"p/A\"]}\n```", "stop", true},
		{"Thinking... final answer: {\"order\":[\"p/B\",\"p/A\"]}", "stop", true},
		{"The user wants a ranking", "length", false},
		{"{\"order\":[\"p/B\",\"p/A\"]}", "length", false},
		{"{\"order\":[\"p/B\",", "stop", false},
		{"{\"order\":[\"p/B\"]} garbage", "stop", false},
		{"{\"broken\":{\"order\":[\"p/B\"]}}", "stop", false},
	} {
		body, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"finish_reason": tc.finish, "message": map[string]any{"content": tc.content}}}})
		_, err := ParseRanking(body)
		if (err == nil) != tc.ok {
			t.Errorf("%q: %v", tc.content, err)
		}
	}
}

func TestLastGoodRankingSurvivesFailureAndPoolChange(t *testing.T) {
	cands := []Candidate{{Step: router.Step{Provider: "p", Model: "A"}}, {Step: router.Step{Provider: "p", Model: "B"}}}
	var mu sync.Mutex
	var cache Cache
	good := func(context.Context) (map[string]int, error) { return map[string]int{"p/B": 0, "p/A": 1}, nil }
	Decide(cands, good, []string{"p"}, time.Minute, &mu, &cache)
	cache.at = time.Now().Add(-2 * time.Minute)
	changed := []Candidate{{Step: router.Step{Provider: "p", Model: "A"}}, {Step: router.Step{Provider: "q", Model: "New"}}}
	bad := func(context.Context) (map[string]int, error) { return nil, errors.New("truncated") }
	ordered, _, source := Decide(changed, bad, []string{"q", "p"}, time.Minute, &mu, &cache)
	if source != "decider_stale" || ordered[0].Step.Model != "A" || len(ordered) != 2 {
		t.Fatalf("%s %+v", source, ordered)
	}
	_, _, source = Decide(changed, bad, nil, time.Minute, &mu, &cache)
	if source != "decider_stale" {
		t.Fatalf("cached failure lost last good: %s", source)
	}
	cache.at = time.Now().Add(-2 * time.Minute)
	cache.goodAt = time.Now().Add(-2 * time.Hour)
	_, _, source = Decide(changed, bad, nil, time.Minute, &mu, &cache)
	if source != "decider_unavailable" {
		t.Fatalf("stale advice not expired: %s", source)
	}
}
