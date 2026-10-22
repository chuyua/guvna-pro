package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/selector"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

func shadowTestServer(t *testing.T, upstream string) (*Server, map[string]string, []selector.Candidate) {
	t.Helper()
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: upstream, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: upstream, KeyEnv: "B"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}, {Provider: "b", Model: "m"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	env := map[string]string{"A": "dummy", "B": "dummy", "GUVNA_PROVIDE_ORDER": "a b", "GUVNA_AUTO_SCORE_MIN_SAMPLES": "2"}
	s.env = func(k string) string { return env[k] }
	for i := 0; i < 2; i++ {
		for _, e := range []telemetry.Event{{Provider: "a", Model: "m", Status: 500, LatencyMS: 1000}, {Provider: "b", Model: "m", Status: 200, LatencyMS: 20}} {
			e.RequestID = s.newRequestID()
			e.Attempt = 1
			e.Ts = time.Now()
			s.tm.Record(e)
		}
	}
	if err := s.tm.Flush(); err != nil {
		t.Fatal(err)
	}
	cands := []selector.Candidate{{Chain: "main", Step: router.Step{Provider: "a", Model: "m"}}, {Chain: "main", Step: router.Step{Provider: "b", Model: "m"}}}
	return s, env, cands
}

func TestAutoShadowModesAndSampleGate(t *testing.T) {
	s, env, cands := shadowTestServer(t, "http://unused.invalid")
	r := withRequestID(httptest.NewRequest("POST", "/v1/auto", nil), "request-correlation")
	for _, tc := range []struct {
		mode, approved, wantFirst, wantReason string
		applied                               bool
	}{
		{"", "", "a", "", false},
		{"shadow", "", "a", "shadow_only", false},
		{"telemetry", "", "a", "manual_comparison_required", false},
		{"telemetry", "true", "b", "operator_approved", true},
		{"decider", "true", "a", "", false},
	} {
		env["GUVNA_AUTO_ORDER_MODE"], env["GUVNA_AUTO_TELEMETRY_APPROVED"] = tc.mode, tc.approved
		got, source := s.applyAutoScoring(r, cands, "decider_cached", false)
		if got[0].Step.Provider != tc.wantFirst || (source == "telemetry") != tc.applied {
			t.Fatalf("mode=%s order=%v source=%s", tc.mode, candidateNames(got), source)
		}
		if tc.wantReason != "" {
			state := &s.autoScoring
			comparison := state.recent[(state.next-1+autoComparisonLimit)%autoComparisonLimit]
			if comparison.Reason != tc.wantReason || !comparison.Disagrees || comparison.Applied != tc.applied || comparison.RequestID != "request-correlation" {
				t.Fatalf("comparison=%+v", comparison)
			}
		}
	}
	env["GUVNA_AUTO_ORDER_MODE"] = "telemetry"
	unknown := append(append([]selector.Candidate{}, cands...), selector.Candidate{Step: router.Step{Provider: "unknown", Model: "m"}})
	if got, source := s.applyAutoScoring(r, unknown, "decider", false); source != "decider" || got[0].Step.Provider != "a" {
		t.Fatal("undersampled candidate bypassed activation gate")
	}
	env["GUVNA_PROVIDE_ORDER"] = "b/vendor/m a nonsense"
	if got := s.providerOrder(); !reflect.DeepEqual(got, []string{"b/vendor/m", "a"}) {
		t.Fatalf("provider/model entries rejected: %v", got)
	}
}

func TestAutoShadowPreservesServingOrderAndAdminQuery(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer up.Close()
	s, env, _ := shadowTestServer(t, up.URL)
	env["GUVNA_AUTO_ORDER_MODE"] = "shadow"
	r := httptest.NewRequest("POST", "/v1/auto", strings.NewReader(`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`))
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || w.Header().Get("X-Guvna-Step") != "a/m" {
		t.Fatalf("shadow changed serving route: status=%d step=%s", w.Code, w.Header().Get("X-Guvna-Step"))
	}
	for _, tc := range []struct {
		url, token string
		status     int
	}{{"/admin/auto/scoring", "client-1", 401}, {"/admin/auto/scoring?since=invalid", "admin-secret", 400}, {"/admin/auto/scoring?since=2020-01-01T00:00:00Z", "admin-secret", 400}, {"/admin/auto/scoring", "admin-secret", 200}} {
		r := httptest.NewRequest("GET", tc.url, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s status=%d want=%d body=%s", tc.url, w.Code, tc.status, w.Body)
		}
		if tc.status == 200 {
			var result struct {
				Comparisons []autoComparison            `json:"comparisons"`
				Metrics     telemetry.CandidateSnapshot `json:"metrics"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Comparisons) != 1 || !result.Comparisons[0].Disagrees || result.Comparisons[0].Applied || len(result.Metrics.Candidates) != 2 {
				t.Fatalf("query=%s", w.Body)
			}
		}
	}
}

func TestAutoScoringCacheAndBoundedComparisons(t *testing.T) {
	s, env, cands := shadowTestServer(t, "http://unused.invalid")
	env["GUVNA_AUTO_ORDER_MODE"] = "shadow"
	r := withRequestID(httptest.NewRequest("POST", "/v1/auto", nil), "cached")
	s.applyAutoScoring(r, cands, "decider", false)
	if _, err := s.tm.DB().Exec("DROP TABLE requests"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < autoComparisonLimit+2; i++ {
		s.applyAutoScoring(r, cands, "decider", false)
	}
	state := &s.autoScoring
	if state.count != autoComparisonLimit {
		t.Fatalf("comparison ring grew beyond cap: %d", state.count)
	}
	if got := state.recent[(state.next-1+autoComparisonLimit)%autoComparisonLimit]; got.Reason != "shadow_only" {
		t.Fatalf("cache miss triggered DB read on request: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, source := s.applyAutoScoring(r.WithContext(ctx), cands, "decider", false); source != "decider" {
		t.Fatal("canceled request changed policy")
	}
}

func TestTelemetryCostRankAndTruncatedGate(t *testing.T) {
	cands := []selector.Candidate{{Chain: "a", Step: router.Step{Provider: "a", Model: "m"}}, {Chain: "b", Step: router.Step{Provider: "b", Model: "m"}}, {Chain: "c", Step: router.Step{Provider: "c", Model: "m"}}}
	snapshot := telemetry.CandidateSnapshot{Candidates: []telemetry.CandidateStat{{Provider: "a", Model: "m", FirstAttempts: 100, ExpectedCostMS: 100, P95LatencyMS: 10}, {Provider: "b", Model: "m", FirstAttempts: 100, ExpectedCostMS: 50, P95LatencyMS: 1000}}}
	got, ready := telemetryCandidateOrder(cands, []string{"a", "b", "c"}, snapshot, false, 100)
	if ready || !reflect.DeepEqual(candidateNames(got), []string{"b/m", "a/m", "c/m"}) {
		t.Fatalf("unknown tail or cost/p rank wrong: %v ready=%v", candidateNames(got), ready)
	}
	snapshot.Truncated = true
	if _, ready := telemetryCandidateOrder(cands[:2], nil, snapshot, false, 100); ready {
		t.Fatal("truncated data activated telemetry")
	}
	if _, ready := telemetryCandidateOrder(cands[:2], nil, snapshot, true, 100); ready {
		t.Fatal("non-stream data ranked streams")
	}
}
