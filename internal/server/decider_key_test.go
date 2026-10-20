package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
)

const validDeciderBody = `{"choices":[{"message":{"content":"{\"order\":[\"a/m\"]}"}}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`

func TestDeciderUpstreamFailuresQuarantineAndRecordKey(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		truncated bool
		class     keypool.Class
	}{
		{"unauthorized", 401, `{"error":"unauthorized"}`, false, keypool.ClassAuth},
		{"forbidden", 403, `{"error":"forbidden"}`, false, keypool.ClassAuth},
		{"rate_limited", 429, `{"error":"rate limited"}`, false, keypool.ClassRateLimit},
		{"server_error", 503, `{"error":"unavailable"}`, false, keypool.ClassTransient},
		{"invalid_json", 200, `{broken`, false, keypool.ClassTransient},
		{"invalid_ranking", 200, `{"choices":[{"message":{"content":"not a ranking"}}]}`, false, keypool.ClassTransient},
		{"error_in_2xx", 200, `{"error":"broken","choices":[{"message":{"content":"{\"order\":[\"a/m\"]}"}}]}`, false, keypool.ClassTransient},
		{"truncated", 200, `{"choices":`, true, keypool.ClassTransient},
		{"bad_request", 400, `{"error":"bad request"}`, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if tc.truncated {
					w.Header().Set("Content-Length", "1000")
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.decideProvider, s.decideModel = "a", "m"
			if _, err := s.callDecider(nil, context.Background(), nil); err == nil {
				t.Fatal("bad decider response accepted")
			}
			ks := s.pools["a"].Keys()[0]
			wantFailures := 0
			if tc.class != "" {
				wantFailures = 1
			}
			if ks.Failures != wantFailures || ks.Reason != string(tc.class) || ks.Requests != 1 || hits.Load() != 1 {
				t.Fatalf("key=%+v calls=%d", ks, hits.Load())
			}
			if tc.class != "" {
				if _, err := s.callDecider(nil, context.Background(), nil); err != errNoAvailableKeys || hits.Load() != 1 {
					t.Fatalf("isolated key reused: err=%v calls=%d", err, hits.Load())
				}
			}
			if len(s.hlth.Snapshot()) != 0 {
				t.Fatal("advisory decider changed model health")
			}
		})
	}
}

func TestDeciderNetworkFailureRecordsKeyOnce(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer up.Close()
	s := runtimeServer(t, up.URL)
	s.decideProvider, s.decideModel = "a", "m"
	if _, err := s.callDecider(nil, context.Background(), nil); err == nil {
		t.Fatal("network failure accepted")
	}
	ks := s.pools["a"].Keys()[0]
	if ks.Failures != 1 || ks.Reason != string(keypool.ClassTransient) || ks.Requests != 1 {
		t.Fatalf("key=%+v", ks)
	}
}

func TestDeciderCancellationIsNeutralAndOwnDeadlineIsFailure(t *testing.T) {
	for _, phase := range []string{"headers", "body", "own_deadline"} {
		t.Run(phase, func(t *testing.T) {
			started := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if phase == "body" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.decideProvider, s.decideModel = "a", "m"
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := parent
			if phase == "own_deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(parent, 50*time.Millisecond)
				defer stop()
			}
			done := make(chan error, 1)
			go func() { _, err := s.callDecider(parent, ctx, nil); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("decider did not start")
			}
			if phase != "own_deadline" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancellation accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("decider did not stop")
			}
			ks := s.pools["a"].Keys()[0]
			want := 0
			if phase == "own_deadline" {
				want = 1
			}
			if ks.Failures != want || ks.Requests != int64(want) {
				t.Fatalf("key=%+v", ks)
			}
		})
	}
}

func TestDeciderValidRankingRecoversKeyAndRecordsUsage(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, validDeciderBody) }))
	defer up.Close()
	s := runtimeServer(t, up.URL)
	s.decideProvider, s.decideModel = "a", "m"
	seedRecoveredKey(t, s)
	ranked, err := s.callDecider(nil, context.Background(), nil)
	if err != nil || len(ranked) != 1 {
		t.Fatalf("ranking=%v err=%v", ranked, err)
	}
	ks := s.pools["a"].Keys()[0]
	if ks.Failures != 0 || ks.Requests != 1 || ks.TokensIn != 2 || ks.TokensOut != 3 {
		t.Fatalf("key=%+v", ks)
	}
}

func TestDeciderRotatesAfterQuarantine(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, validDeciderBody)
	}))
	defer up.Close()
	s := runtimeServer(t, up.URL)
	s.decideProvider, s.decideModel = "a", "m"
	p, err := keypool.New("a", []string{"A", "A2"}, "round_robin", config.QuarantineCfg{})
	if err != nil {
		t.Fatal(err)
	}
	s.pools["a"] = p
	s.env = func(string) string { return "dummy" }
	if _, err := s.callDecider(nil, context.Background(), nil); err == nil {
		t.Fatal("401 accepted")
	}
	if _, err := s.callDecider(nil, context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ks := p.Keys()
	if ks[0].Failures != 1 || ks[0].Requests != 1 || ks[1].Failures != 0 || ks[1].Requests != 1 || hits.Load() != 2 {
		t.Fatalf("keys=%+v calls=%d", ks, hits.Load())
	}
}
