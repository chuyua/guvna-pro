package health

import (
	"errors"
	"testing"
	"time"
)

func fakeTracker(t *testing.T, start time.Time) *Tracker {
	t.Helper()
	tkr := New()
	now := start
	tkr.now = func() time.Time { return now }
	return tkr
}

func advance(tkr *Tracker, d time.Duration) {
	base := tkr.now()
	tkr.now = func() time.Time { return base.Add(d) }
}

func TestMarkDownAfterThreshold(t *testing.T) {
	start := time.Now()
	tkr := fakeTracker(t, start)

	for i := 0; i < FailureThreshold-1; i++ {
		tkr.Mark("p", "m", 429, nil)
		if tkr.IsDown("p", "m") {
			t.Fatalf("down before threshold reached (failure %d)", i+1)
		}
	}
	tkr.Mark("p", "m", 429, nil)
	if !tkr.IsDown("p", "m") {
		t.Fatal("expected down after threshold failures")
	}
}

func TestSuccessResetsState(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	for i := 0; i < FailureThreshold; i++ {
		tkr.Mark("p", "m", 0, errors.New("boom"))
	}
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: step down")
	}
	tkr.MarkSuccess("p", "m")
	if tkr.IsDown("p", "m") {
		t.Fatal("success must reset failure state")
	}
}

func TestCoolOffExpires(t *testing.T) {
	start := time.Now()
	tkr := fakeTracker(t, start)
	for i := 0; i < FailureThreshold; i++ {
		tkr.Mark("p", "m", 502, nil)
	}
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: step down")
	}
	advance(tkr, CooldownBase)
	if tkr.IsDown("p", "m") {
		t.Fatal("step must be retried after cool-off expires")
	}
}

func TestCooldownGrowsExponentially(t *testing.T) {
	start := time.Now()
	tkr := fakeTracker(t, start)

	failUntil := func(n int) {
		for i := 0; i < n; i++ {
			tkr.Mark("p", "m", 500, nil)
		}
	}

	// First outage: 3 failures -> 1 minute cool-off.
	failUntil(FailureThreshold)
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: down after threshold")
	}
	advance(tkr, CooldownBase+time.Second)
	if tkr.IsDown("p", "m") {
		t.Fatal("first cool-off should have expired")
	}

	// Second outage (5 consecutive failures total): cool-off doubles to 4
	// minutes, measured from the new failure time.
	failUntil(2)
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: down after second outage")
	}
	advance(tkr, CooldownBase+time.Second)
	if !tkr.IsDown("p", "m") {
		t.Fatal("cool-off should double with consecutive failures")
	}
	advance(tkr, 3*CooldownBase)
	if tkr.IsDown("p", "m") {
		t.Fatal("doubled cool-off should now be expired")
	}
}

func TestCooldownCapped(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	for i := 0; i < 30; i++ {
		tkr.Mark("p", "m", 500, nil)
	}
	if d := tkr.Snapshot()[0].DownFor; d > CooldownMax {
		t.Fatalf("cool-off %v exceeds cap %v", d, CooldownMax)
	}
}

func TestIsolationBetweenSteps(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	for i := 0; i < FailureThreshold; i++ {
		tkr.Mark("p", "broken", 500, nil)
	}
	if !tkr.IsDown("p", "broken") {
		t.Fatal("broken model should be down")
	}
	if tkr.IsDown("p", "healthy") {
		t.Fatal("healthy model of same provider must not be affected")
	}
}

func TestSingleBadRequestMarksDown(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	tkr.Mark("p", "m", 400, nil)
	if !tkr.IsDown("p", "m") {
		t.Fatal("single 400 must mark step down immediately")
	}
	ss := tkr.Snapshot()
	if len(ss) != 1 {
		t.Fatalf("expected 1 step in snapshot, got %d", len(ss))
	}
	if !ss[0].Down {
		t.Fatal("snapshot must report Down=true after single 400")
	}
	if d := ss[0].DownFor; d <= 0 || d > PermanentCooldown {
		t.Fatalf("cool-off %v, want (0, %v]", d, PermanentCooldown)
	}
}

func TestSingleNotFoundMarksDown(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	tkr.Mark("p", "m", 404, nil)
	if !tkr.IsDown("p", "m") {
		t.Fatal("single 404 must mark step down immediately")
	}
}

func TestPermanentCooldownExpires(t *testing.T) {
	start := time.Now()
	tkr := fakeTracker(t, start)
	tkr.Mark("p", "m", 400, nil)
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: step down")
	}
	advance(tkr, PermanentCooldown)
	if tkr.IsDown("p", "m") {
		t.Fatal("step must be retried after permanent cool-off expires")
	}
}

func TestSingleRateLimitNotDown(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	tkr.Mark("p", "m", 429, nil)
	if tkr.IsDown("p", "m") {
		t.Fatal("single 429 must not mark step down before threshold")
	}
}

func TestSuccessResetsPermanentDown(t *testing.T) {
	tkr := fakeTracker(t, time.Now())
	tkr.Mark("p", "m", 404, nil)
	if !tkr.IsDown("p", "m") {
		t.Fatal("precondition: step down")
	}
	tkr.MarkSuccess("p", "m")
	if tkr.IsDown("p", "m") {
		t.Fatal("success must reset permanent down state")
	}
	if ss := tkr.Snapshot(); ss[0].Down || ss[0].Failures != 0 {
		t.Fatalf("snapshot not reset: %+v", ss[0])
	}
}
