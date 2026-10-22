package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatusPreservesJSONAndDisplaysTelemetryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"uptime":"1h","chains":[],"steps":[],"keys":[{"env":"KEY_A"}],"future_field":{"kept":true},"usage_error":"persisted usage unavailable","telemetry":{"flushes_ok":2,"flushes_failed":3,"retried_events":4,"dropped_events":5,"buffer_len":6,"inflight_events":7}}`)
	}))
	defer server.Close()
	args := []string{"--url", server.URL, "--token", "dummy"}
	output, err := captureCLIOutput(t, func(c command) error { return c.statusCmd(append(args, "--json")) })
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"telemetry", "usage_error", "keys", "future_field"} {
		if _, ok := raw[field]; !ok {
			t.Fatalf("field %s lost from JSON", field)
		}
	}
	output, err = captureCLIOutput(t, func(c command) error { return c.statusCmd(args) })
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"persisted usage unavailable", "buffer 6", "in-flight 7", "dropped 5", "2 ok / 3 failed", "retained for retry 4"} {
		if !strings.Contains(output, text) {
			t.Fatalf("missing %q in %s", text, output)
		}
	}
}
