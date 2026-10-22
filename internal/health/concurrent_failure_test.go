package health

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentFailuresAdvanceAdmissionEpochOnce(t *testing.T) {
	for _, initial := range []int{0, FailureThreshold - 1} {
		t.Run(time.Duration(initial).String(), func(t *testing.T) {
			tkr := fakeTracker(t, time.Unix(1000, 0))
			for i := 0; i < initial; i++ {
				tkr.Mark("p", "m", 503, nil)
			}
			permits := make([]*Permit, 64)
			for i := range permits {
				var ok bool
				permits[i], ok = tkr.TryAcquire("p", "m")
				if !ok {
					t.Fatal("healthy traffic must remain concurrent")
				}
			}
			var wg sync.WaitGroup
			for _, p := range permits {
				wg.Add(1)
				go func(p *Permit) { defer wg.Done(); p.Failure(503, nil) }(p)
			}
			wg.Wait()
			st := tkr.Snapshot()[0]
			if st.Failures != initial+1 || st.Down != (initial+1 >= FailureThreshold) {
				t.Fatalf("concurrent outage counted repeatedly: %+v", st)
			}
			if st.Down && st.DownFor != CooldownBase {
				t.Fatalf("initial cooldown = %s, want %s", st.DownFor, CooldownBase)
			}
		})
	}
}

func TestSerialPermitFailuresAcrossCooldownWindows(t *testing.T) {
	tkr := fakeTracker(t, time.Unix(1000, 0))
	for i := 0; i < FailureThreshold; i++ {
		p, ok := tkr.TryAcquire("p", "m")
		if !ok {
			t.Fatalf("serial request %d not admitted", i+1)
		}
		p.Failure(503, nil)
	}
	for _, want := range []time.Duration{CooldownBase, 2 * CooldownBase, 4 * CooldownBase, 8 * CooldownBase, CooldownMax, CooldownMax} {
		if st := tkr.Snapshot()[0]; !st.Down || st.DownFor != want {
			t.Fatalf("state = %+v, want cooldown %s", st, want)
		}
		if _, ok := tkr.TryAcquire("p", "m"); ok {
			t.Fatal("admitted request within cooldown")
		}
		advance(tkr, want)
		p, ok := tkr.TryAcquire("p", "m")
		if !ok {
			t.Fatal("did not admit probe at exact expiry")
		}
		if _, ok := tkr.TryAcquire("p", "m"); ok {
			t.Fatal("admitted a second recovery probe")
		}
		p.Failure(503, nil)
	}
	advance(tkr, CooldownMax)
	p, ok := tkr.TryAcquire("p", "m")
	if !ok {
		t.Fatal("missing recovery probe")
	}
	p.Success()
	if st := tkr.Snapshot()[0]; st.Down || st.Failures != 0 {
		t.Fatalf("probe success did not reset: %+v", st)
	}
}

func TestDelayedSuccessCannotEraseBelowThresholdFailure(t *testing.T) {
	tkr := fakeTracker(t, time.Unix(1000, 0))
	oldSuccess, _ := tkr.TryAcquire("p", "m")
	failure, _ := tkr.TryAcquire("p", "m")
	failure.Failure(503, nil)
	oldSuccess.Success()
	if st := tkr.Snapshot()[0]; st.Failures != 1 || st.Down {
		t.Fatalf("old success erased a more recent failure: %+v", st)
	}
	// A newly admitted healthy result may reset the failure count.
	newSuccess, _ := tkr.TryAcquire("p", "m")
	newSuccess.Success()
	if st := tkr.Snapshot()[0]; st.Failures != 0 || st.Down {
		t.Fatalf("fresh success failed to recover: %+v", st)
	}
}

func TestDirectFailureInvalidatesActiveRecoveryProbe(t *testing.T) {
	tkr := expiredTracker(t)
	p, _ := tkr.TryAcquire("p", "m")
	tkr.Mark("p", "m", 503, nil)
	p.Success()
	if st := tkr.Snapshot()[0]; !st.Down || st.Failures != FailureThreshold+1 {
		t.Fatalf("old probe cleared direct failure: %+v", st)
	}
	advance(tkr, 2*CooldownBase)
	next, ok := tkr.TryAcquire("p", "m")
	if !ok {
		t.Fatal("direct failure leaked the old probing flag")
	}
	next.Release()
}

func TestAttributed422HasPermanentCooldown(t *testing.T) {
	tkr := fakeTracker(t, time.Unix(1000, 0))
	p, _ := tkr.TryAcquire("p", "m")
	p.Failure(422, nil)
	if st := tkr.Snapshot()[0]; !st.Down || st.Failures != FailureThreshold || st.DownFor != PermanentCooldown {
		t.Fatalf("attributed incompatibility = %+v", st)
	}
}
