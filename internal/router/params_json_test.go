package router

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestAddChainRejectsNonJSONParamsBeforePersistence(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), map[any]any{1: "original"}} {
		r := New(testConfig())
		before := r.ListChains()
		persisted := false
		err := r.AddChainPersisted("invalid-params", []Step{{Provider: "groq", Model: "original", Params: map[string]any{"custom": value}}}, func(map[string][]Step) error {
			persisted = true
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), `step for "groq"`) || !strings.Contains(err.Error(), "cannot be encoded as JSON") {
			t.Fatalf("invalid runtime params accepted: %v", err)
		}
		if persisted || !reflect.DeepEqual(before, r.ListChains()) {
			t.Fatal("invalid runtime params reached persistence or changed routing")
		}
	}
}
