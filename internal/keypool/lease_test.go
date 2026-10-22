package keypool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

type leaseClock struct{ tick atomic.Int64 }

func leasePool(t *testing.T, keys ...string) (*Pool, *leaseClock) {
	t.Helper()
	clock := &leaseClock{}
	clock.tick.Store(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC).UnixNano())
	p, err := NewWithClock("p", keys, "", config.QuarantineCfg{}, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	return p, clock
}

func (c *leaseClock) now() time.Time      { return time.Unix(0, c.tick.Load()) }
func (c *leaseClock) add(d time.Duration) { c.tick.Add(int64(d)) }

func acquireLease(t *testing.T, p *Pool) *Lease {
	t.Helper()
	l := p.Acquire(nil)
	if l == nil || l.Env() == "" || !l.Available() {
		t.Fatal("expected an available admitted lease")
	}
	return l
}

// Keep the audit's unbound API regression: one batch of already-admitted
// failures must not amplify the active quarantine even for legacy callers.
func TestReviewLateFailureSameQuarantine(t *testing.T) {
	p, _ := leasePool(t, "K")
	for i := 0; i < 8; i++ {
		if p.Pick() != "K" {
			t.Fatal("admission")
		}
	}
	for i := 0; i < 8; i++ {
		p.Mark("K", ClassRateLimit)
	}
	st := p.Keys()[0]
	if st.Failures != 1 || st.DownFor != time.Minute {
		t.Fatalf("late failures escalated same quarantine: %+v", st)
	}
}

func TestLeaseConcurrentFailureCohortCountsOnce(t *testing.T) {
	for _, class := range []Class{ClassRateLimit, ClassTransient, ClassAuth} {
		t.Run(string(class), func(t *testing.T) {
			p, _ := leasePool(t, "K")
			leases := make([]*Lease, 64)
			for i := range leases {
				leases[i] = acquireLease(t, p)
			}
			var wg sync.WaitGroup
			var applied atomic.Int64
			for _, lease := range leases {
				wg.Add(1)
				go func(l *Lease) {
					defer wg.Done()
					if l.Failure(class) != "" {
						applied.Add(1)
					}
					p.Record(l.Env(), 1, 2) // stale penalties must not discard usage
				}(lease)
			}
			wg.Wait()
			st := p.Keys()[0]
			if applied.Load() != 1 || st.Failures != 1 || st.DownFor != p.durationFor(class) {
				t.Fatalf("cohort applied=%d state=%+v", applied.Load(), st)
			}
			if st.Requests != 64 || st.TokensIn != 64 || st.TokensOut != 128 {
				t.Fatalf("lost attempted usage: %+v", st)
			}
			if p.Acquire(nil) != nil {
				t.Fatal("quarantined key admitted")
			}
		})
	}
}

func TestLeaseFencesExpiredAndRecoveredWindows(t *testing.T) {
	p, clock := leasePool(t, "K")
	failed := acquireLease(t, p)
	oldSuccess, oldFailure, oldAuth := acquireLease(t, p), acquireLease(t, p), acquireLease(t, p)
	failed.Failure(ClassRateLimit)
	clock.add(time.Minute) // exact boundary is selectable, but old leases stay stale
	if oldFailure.Available() || oldSuccess.Available() {
		t.Fatal("expiry revived old admissions")
	}
	oldSuccess.Success()
	if p.Keys()[0].Failures != 1 {
		t.Fatal("old success reset an expired but unrecovered key")
	}
	recovered := acquireLease(t, p)
	recovered.Success()
	oldFailure.Failure(ClassTransient)
	oldAuth.Failure(ClassAuth)
	if st := p.Keys()[0]; st.Failures != 0 || st.State != "healthy" {
		t.Fatalf("stale failure poisoned recovery: %+v", st)
	}
	if !acquireLease(t, p).Available() {
		t.Fatal("recovered key unavailable")
	}
}

func TestLeaseFailureFromEarlierWindowCannotAlterNewWindow(t *testing.T) {
	p, clock := leasePool(t, "K")
	initial := acquireLease(t, p)
	oldAuth := acquireLease(t, p)
	oldFailure := acquireLease(t, p)
	initial.Failure(ClassRateLimit)
	clock.add(time.Minute)
	newFailure := acquireLease(t, p)
	newAuth := acquireLease(t, p)
	newFailure.Failure(ClassTransient)
	before := p.Keys()[0]
	oldAuth.Failure(ClassAuth)
	oldFailure.Failure(ClassRateLimit)
	if after := p.Keys()[0]; after != before {
		t.Fatalf("old cohort changed later window: before=%+v after=%+v", before, after)
	}
	if got := newAuth.Failure(ClassAuth); got != ClassAuth {
		t.Fatalf("current cohort auth evidence ignored: %q", got)
	}
	if st := p.Keys()[0]; st.Failures != 2 || st.Reason != string(ClassAuth) {
		t.Fatalf("auth upgrade counted another outage: %+v", st)
	}
}

func TestLeaseAuthUpgradeOnceWithoutStretching(t *testing.T) {
	for _, initial := range []Class{ClassRateLimit, ClassTransient} {
		t.Run(string(initial), func(t *testing.T) {
			p, clock := leasePool(t, "K")
			failure, auth, lateAuth, lateTransient := acquireLease(t, p), acquireLease(t, p), acquireLease(t, p), acquireLease(t, p)
			failure.Failure(initial)
			clock.add(15 * time.Second)
			if got := auth.Failure(ClassAuth); got != ClassAuth {
				t.Fatalf("auth upgrade = %q", got)
			}
			until := p.keys[0].downUntil
			if st := p.Keys()[0]; st.Failures != 1 || st.Reason != string(ClassAuth) || st.DownFor != 24*time.Hour {
				t.Fatalf("upgrade state = %+v", st)
			}
			clock.add(45 * time.Second)
			lateAuth.Failure(ClassAuth)
			lateTransient.Failure(ClassTransient)
			if st := p.Keys()[0]; p.keys[0].downUntil != until || st.Failures != 1 || st.Reason != string(ClassAuth) {
				t.Fatalf("later failures stretched/downgraded auth: %+v", st)
			}
		})
	}
}

func TestLeaseSerialExpiredFailuresGrowAndRecoveryResets(t *testing.T) {
	p, clock := leasePool(t, "K")
	for failures := 0; failures < 20; failures++ {
		lease := acquireLease(t, p)
		if got := lease.Failure(ClassRateLimit); got != ClassRateLimit {
			t.Fatalf("failure %d not applied", failures+1)
		}
		want := time.Minute
		for i := 0; i < failures && want < 24*time.Hour; i++ {
			want = min(2*want, 24*time.Hour)
		}
		st := p.Keys()[0]
		if st.Failures != failures+1 || st.DownFor != want {
			t.Fatalf("failure %d state=%+v want=%s", failures+1, st, want)
		}
		clock.add(want)
	}
	acquireLease(t, p).Success()
	if st := p.Keys()[0]; st.Failures != 0 || st.State != "healthy" {
		t.Fatalf("not recovered: %+v", st)
	}
	acquireLease(t, p).Failure(ClassRateLimit)
	if st := p.Keys()[0]; st.Failures != 1 || st.DownFor != time.Minute {
		t.Fatalf("fresh failure retained old multiplier: %+v", st)
	}
}

func TestLeaseCompletionIdempotentNeutralAndIndependent(t *testing.T) {
	p, _ := leasePool(t, "A", "B")
	a := p.Acquire(map[string]bool{"B": true})
	b := p.Acquire(map[string]bool{"A": true})
	if a.Env() != "A" || b.Env() != "B" || p.Acquire(map[string]bool{"A": true, "B": true}) != nil {
		t.Fatal("exclusion/selection mismatch")
	}
	a.Release()
	a.Failure(ClassAuth)
	a.Success()
	if a.Available() || p.Keys()[0].Failures != 0 {
		t.Fatal("released lease changed key")
	}
	b.Failure("") // a caller/network-neutral outcome also terminates the lease
	b.Failure(ClassAuth)
	if b.Available() || p.Keys()[1].Failures != 0 {
		t.Fatal("neutral failure did not release the lease")
	}
	a = p.Acquire(map[string]bool{"B": true})
	a.Failure(ClassRateLimit)
	a.Failure(ClassAuth)
	a.Success()
	if st := p.Keys()[0]; st.Failures != 1 || st.Reason != string(ClassRateLimit) {
		t.Fatalf("duplicate completion changed key: %+v", st)
	}
	if l := p.Acquire(nil); l == nil || l.Env() != "B" {
		t.Fatal("one key quarantine affected independent key")
	}
	var none *Lease
	none.Release()
	none.Success()
	if none.Env() != "" || none.Available() || none.Failure(ClassAuth) != "" {
		t.Fatal("nil lease is not neutral")
	}
}

func TestLegacyMarkAuthUpgradeDoesNotRepeat(t *testing.T) {
	p, clock := leasePool(t, "K")
	p.Mark("K", ClassTransient)
	clock.add(time.Second)
	p.Mark("K", ClassAuth)
	until := p.keys[0].downUntil
	clock.add(time.Second)
	p.Mark("K", ClassAuth)
	p.Mark("K", ClassRateLimit)
	if st := p.Keys()[0]; st.Failures != 1 || p.keys[0].downUntil != until || st.Reason != string(ClassAuth) {
		t.Fatalf("legacy mark amplified current auth window: %+v", st)
	}
}
