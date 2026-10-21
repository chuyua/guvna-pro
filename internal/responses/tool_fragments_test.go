package responses

import "testing"

func TestStreamPreservesMultipleToolDeltasAndSplitNames(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	s, err := NewStream(rec, fl, &Request{}, Served{})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_","type":"function","function":{"name":"get_","arguments":"{\"b\":"}},{"index":0,"id":"first","type":"function","function":{"name":"a","arguments":"{\"a\":"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"id":"second","function":{"name":"b","arguments":"2}"}}]}}]}`,
	} {
		if err := s.Chunk(mustChunk(t, raw)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Finish("tool_calls", nil); err != nil {
		t.Fatal(err)
	}
	var completed map[string]any
	deltas := 0
	for _, event := range parseSSE(t, rec.Body.String()) {
		if event.typ() == "response.function_call_arguments.delta" {
			deltas++
		}
		if event.typ() == "response.completed" {
			completed = event.response()
		}
	}
	if deltas != 4 || completed == nil {
		t.Fatalf("lost tool delta: count=%d response=%v", deltas, completed)
	}
	output := completed["output"].([]any)
	if len(output) != 2 {
		t.Fatalf("output = %v", output)
	}
	byID := map[string]map[string]any{}
	for _, raw := range output {
		call := raw.(map[string]any)
		byID[call["call_id"].(string)] = call
	}
	if byID["first"]["arguments"] != `{"a":1}` || byID["call_second"]["name"] != "get_b" || byID["call_second"]["arguments"] != `{"b":2}` {
		t.Fatalf("tool fragments lost: %v", output)
	}
}
