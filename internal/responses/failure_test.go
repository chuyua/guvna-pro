package responses

import (
	"bufio"
	"strings"
	"testing"
)

func TestStreamFailureKeepsIDAndAbandonsPartialItems(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	s, err := NewStream(rec, fl, &Request{}, Served{ChatModel: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Chunk(mustChunk(t, `{"choices":[{"delta":{"content":"partial"}}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail("upstream interrupted"); err != nil {
		t.Fatal(err)
	}
	var createdID, failedID string
	var terminals int
	sc := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		data := parseData(t, strings.TrimPrefix(line, "data: "))
		resp, ok := data["response"].(map[string]any)
		if !ok {
			continue
		}
		if data["type"] == "response.created" {
			createdID, _ = resp["id"].(string)
		}
		if data["type"] == "response.failed" {
			failedID, _ = resp["id"].(string)
			terminals++
			if resp["status"] != "failed" || resp["error"] == nil {
				t.Fatalf("failed response=%v", resp)
			}
		}
	}
	if createdID == "" || failedID != createdID || terminals != 1 {
		t.Fatalf("IDs=%q/%q terminals=%d", createdID, failedID, terminals)
	}
	body := rec.Body.String()
	if strings.Contains(body, "response.completed") || strings.Contains(body, "response.output_item.done") || strings.Count(body, "data: [DONE]") != 1 {
		t.Fatalf("invalid failure terminal: %s", body)
	}
}
