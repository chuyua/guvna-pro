package server

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/selector"
)

// estimateAutoContext is a local heuristic, not a model tokenizer. Counting the
// serialized body includes message and tool schema overhead. ASCII uses a
// cautious three bytes per token; non-ASCII uses its full UTF-8 byte count to
// avoid applying English text ratios to multilingual prompts.
func estimateAutoContext(body []byte) int {
	ascii, nonASCII := 0, 0
	for _, b := range body {
		if b < 0x80 {
			ascii++
		} else {
			nonASCII++
		}
	}
	input := (ascii+2)/3 + nonASCII
	if input < 1 {
		input = 1
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return input
	}
	maxInt := int(^uint(0) >> 1)
	output := int64(0)
	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		var n int64
		if json.Unmarshal(fields[field], &n) == nil && n > output {
			output = n
		}
	}
	if output > int64(maxInt-input) {
		return maxInt
	}
	return input + int(output)
}

func fitAutoContext(cands []selector.Candidate, required int) ([]selector.Candidate, []selector.Decided) {
	kept := make([]selector.Candidate, 0, len(cands))
	var excluded []selector.Decided
	for _, c := range cands {
		if c.Context < required {
			excluded = append(excluded, selector.Decided{Chain: c.Chain, Step: c.Step.Provider + "/" + c.Step.Model, Context: c.Context, Reason: "request_above_context"})
			continue
		}
		kept = append(kept, c)
	}
	return kept, excluded
}

// Non-positive values explicitly disable the Auto total budget. Invalid values
// retain the default, matching other duration configuration fallbacks.
func autoTotalBudget() time.Duration {
	if value := strings.TrimSpace(os.Getenv("GUVNA_AUTO_TOTAL_BUDGET")); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return 45 * time.Second
}
