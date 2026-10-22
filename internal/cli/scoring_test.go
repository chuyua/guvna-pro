package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestScoringUsesLocalSocketAndPreservesJSON(t *testing.T) {
	const since = "2026-10-21T01:02:03.000000001Z"
	const until = "2026-10-21T02:02:03.000000001Z"
	var requests atomic.Int32
	socket := cliTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/admin/auto/scoring" || r.Header.Get("Authorization") != "Bearer local-admin" || r.URL.Query().Get("since") != since || r.URL.Query().Get("until") != until {
			t.Errorf("wrong scoring request: %s auth=%q", r.URL, r.Header.Get("Authorization"))
		}
		io.WriteString(w, `{"mode":"shadow","future_field":true,"auto_requests":{"since":"2026-10-21T01:02:03.000000001Z","until":"2026-10-21T02:02:03.000000001Z","truncated":true,"policies":[{"mode":"shadow","policy":"decider","requests":4,"successes":2,"failures":1,"canceled":1,"attempts":6,"success_rate":0.6666667,"first_attempt_success_rate":0.3333333,"fallback_rate":0.3333333,"p95_latency_ms":87}]},"provider_health":{"truncated":true,"providers":[{"provider":"sample","attempts":6,"successes":2,"upstream_failures":1,"network_failures":1,"auth_failures":1,"rate_limited":1,"request_errors":0,"other_failures":0,"health_samples":5,"service_success_rate":0.4,"p95_latency_ms":70}]},"provider_overview":[{"provider":"sample","observation_status":"observed","tracked_models":2,"down_models":1},{"provider":"unused","observation_status":"no_data","tracked_models":0,"down_models":0}],"current_model_health":[{"provider":"sample","model":"one","failures":3,"down":true,"down_for_ns":1000000000}],"telemetry":{"buffer_len":2,"inflight_events":3,"dropped_events":4,"flushes_ok":5,"flushes_failed":6,"retried_events":7}}`)
	}))
	t.Setenv("GUVNA_ADMIN_SOCKET", socket)
	t.Setenv("GUVNA_ADMIN_KEY", "local-admin")
	args := []string{"status", "--scoring", "--since", since, "--until", until}
	var out bytes.Buffer
	if err := Run(append(args, "--json"), &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &raw); err != nil || raw["future_field"] == nil {
		t.Fatalf("raw fields lost: %s err=%v", out.Bytes(), err)
	}
	out.Reset()
	if err := Run(args, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"shadow", "decider", "33.3%", "66.7%", "sample", "40.0%", "unused: no_data", "1/2 tracked models cooling down", "cooldown 1s", "dropped 4", "retained for retry 7", "request samples truncated", "provider samples truncated", "unexecuted policy"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in %s", want, out.String())
		}
	}
	if requests.Load() != 2 {
		t.Fatalf("unexpected request count: %d", requests.Load())
	}
}

func TestCLIRejectsRemoteURLAndInvalidObservationWindow(t *testing.T) {
	var remoteCalls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
	}))
	defer remote.Close()
	socket, requests := cliTestGateway(t)
	t.Setenv("GUVNA_URL", remote.URL)
	t.Setenv("GUVNA_ADMIN_SOCKET", socket)
	t.Setenv("GUVNA_ADMIN_KEY", "local-admin")
	for _, args := range [][]string{
		{"status", "--url", remote.URL},
		{"status", "--scoring", "--since", "invalid"},
		{"status", "--until", "2026-10-21T00:00:00Z"},
	} {
		if err := Run(args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted invalid flags: %v", args)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("invalid flags sent request: %+v", request)
	default:
	}
	if err := Run([]string{"chains", "--json"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if remoteCalls.Load() != 0 {
		t.Fatal("GUVNA_URL caused a remote management request")
	}
	if request := <-requests; request.path != "/v1/chains" {
		t.Fatalf("local management changed: %+v", request)
	}
}
