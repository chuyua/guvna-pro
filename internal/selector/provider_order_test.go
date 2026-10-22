package selector

import (
	"reflect"
	"testing"

	"github.com/creamy-ghost/guvna/internal/router"
)

func TestProviderPriorityFormsAndUnlistedTail(t *testing.T) {
	cands := []Candidate{
		{Chain: "a-unlisted", Step: router.Step{Provider: "z", Model: "m"}},
		{Chain: "b-other", Step: router.Step{Provider: "a", Model: "other"}},
		{Chain: "c-b", Step: router.Step{Provider: "b", Model: "m"}},
		{Chain: "d-exact", Step: router.Step{Provider: "a", Model: "vendor/m"}},
	}
	for _, tc := range []struct {
		name   string
		order  []string
		ranked map[string]int
		want   []string
	}{
		{"bare", []string{"a", "b"}, nil, []string{"a/other", "a/vendor/m", "b/m", "z/m"}},
		{"exact", []string{"a/vendor/m", "b/m"}, nil, []string{"a/vendor/m", "b/m", "z/m", "a/other"}},
		{"mixed_exact_override", []string{"a", "b", "a/vendor/m", "b"}, nil, []string{"a/other", "b/m", "a/vendor/m", "z/m"}},
		{"decider_wins", []string{"a", "b"}, map[string]int{"z/m": 0}, []string{"z/m", "a/other", "a/vendor/m", "b/m"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := FallbackOrder(cands, tc.order, tc.ranked)
			got := make([]string, len(result))
			for i, c := range result {
				got[i] = c.Step.Provider + "/" + c.Step.Model
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("order=%v want=%v", got, tc.want)
			}
			if cands[0].Step.Provider != "z" {
				t.Fatal("input mutated")
			}
		})
	}
}
