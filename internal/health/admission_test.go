package health

import (
	"sync"
	"testing"
	"time"
)

func expiredTracker(t *testing.T) *Tracker {
	t.Helper()
	tkr := fakeTracker(t, time.Now())
	for i := 0; i < FailureThreshold; i++ {
		tkr.Mark("p", "m", 500, nil)
	}
	advance(tkr, CooldownBase)
	return tkr
}

func TestHalfOpenAdmitsOneOf64AndSuccessRestoresTraffic(t *testing.T) {
	tkr := expiredTracker(t)
	var wg sync.WaitGroup
	permits := make(chan *Permit, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p, ok := tkr.TryAcquire("p", "m"); ok {
				permits <- p
			}
		}()
	}
	wg.Wait()
	if len(permits) != 1 {
		t.Fatalf("admissions = %d, want 1", len(permits))
	}
	if !tkr.IsDown("p", "m") {
		t.Fatal("active probe must be unavailable to other callers")
	}
	(<-permits).Success()
	for i := 0; i < 64; i++ {
		p, ok := tkr.TryAcquire("p", "m")
		if !ok {
			t.Fatal("successful probe must restore normal traffic")
		}
		p.Release()
	}
	if ss := tkr.Snapshot()[0]; ss.Down || ss.Failures != 0 {
		t.Fatalf("not recovered: %+v", ss)
	}
}

func TestHalfOpenFailureRestartsCooldown(t *testing.T) {
	tkr := expiredTracker(t)
	p, _ := tkr.TryAcquire("p", "m")
	p.Failure(500, nil)
	p.Release()
	if _, ok := tkr.TryAcquire("p", "m"); ok {
		t.Fatal("failed probe must restart cooldown")
	}
	if d := tkr.Snapshot()[0].DownFor; d != 2*CooldownBase {
		t.Fatalf("cooldown = %v", d)
	}
	advance(tkr, 2*CooldownBase)
	p, ok := tkr.TryAcquire("p", "m")
	if !ok {
		t.Fatal("must admit probe at cooldown boundary")
	}
	p.Release()
}

func TestReleaseAbandonsProbeWithoutChangingFailures(t *testing.T) {
	tkr := expiredTracker(t)
	for i := 0; i < 2; i++ {
		p, ok := tkr.TryAcquire("p", "m")
		if !ok {
			t.Fatal("abandoned probe leaked")
		}
		p.Release()
		p.Release()
	}
	if n := tkr.Snapshot()[0].Failures; n != FailureThreshold {
		t.Fatalf("release recorded failure: %d", n)
	}
}

func TestOldInflightCannotClearOrFailNewProbe(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	oldSuccess, _ := tkr.TryAcquire("p", "m")
	oldFailure, _ := tkr.TryAcquire("p", "m")
	for i := 0; i < FailureThreshold; i++ {
		tkr.Mark("p", "m", 500, nil)
	}
	// Even before a probe starts, an old successful request cannot clear down.
	oldSuccess.Success()
	if !tkr.IsDown("p", "m") {
		t.Fatal("old success cleared cooldown")
	}
	advance(tkr, CooldownBase)
	probe, _ := tkr.TryAcquire("p", "m")
	oldFailure.Failure(500, nil)
	if _, ok := tkr.TryAcquire("p", "m"); ok {
		t.Fatal("old result cleared active probe")
	}
	if n := tkr.Snapshot()[0].Failures; n != FailureThreshold {
		t.Fatalf("old result changed new epoch: %d", n)
	}
	probe.Release()
	next, ok := tkr.TryAcquire("p", "m")
	if !ok {
		t.Fatal("next probe blocked")
	}
	probe.Success() // stale completion after release cannot consume the next lease
	if _, ok := tkr.TryAcquire("p", "m"); ok {
		t.Fatal("old release cleared next probe")
	}
	next.Success()
}

func TestIsDownDoesNotCreateOrAcquireState(t *testing.T) {
	tkr := New()
	tkr.IsDown("p", "missing")
	if len(tkr.Snapshot()) != 0 {
		t.Fatal("read-only IsDown created state")
	}
	tkr = expiredTracker(t)
	for i := 0; i < 64; i++ {
		if tkr.IsDown("p", "m") {
			t.Fatal("expired cooldown still down")
		}
	}
	p, ok := tkr.TryAcquire("p", "m")
	if !ok {
		t.Fatal("read-only filtering acquired probe")
	}
	p.Release()
}
