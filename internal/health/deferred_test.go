package health

import (
	"testing"
	"time"
)

func TestDeferredFailureConfirmOnceAndReleaseProbe(t *testing.T) {
	h := fakeTracker(t, time.Now())
	h.Mark("p", "m", 400, nil)
	advance(h, PermanentCooldown)
	p, ok := h.TryAcquire("p", "m")
	if !ok {
		t.Fatal("probe unavailable")
	}
	d := p.DeferFailure(400, nil)
	next, ok := h.TryAcquire("p", "m")
	if !ok {
		t.Fatal("deferred blame retained probe")
	}
	next.Release()
	d.Confirm() // next probe has a newer generation
	if h.IsDown("p", "m") {
		t.Fatal("stale blame overwrote newer probe")
	}

	h = New()
	p, _ = h.TryAcquire("p", "m")
	d = p.DeferFailure(400, nil)
	d.Confirm()
	d.Confirm()
	p.Failure(500, nil)
	if st := h.Snapshot()[0]; !st.Down || st.Failures != FailureThreshold {
		t.Fatalf("state=%+v", st)
	}
}

func TestDeferredFailureCannotOverwriteNewFailureOrSuccess(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "success"}[success], func(t *testing.T) {
			h := New()
			p, _ := h.TryAcquire("p", "m")
			d := p.DeferFailure(400, nil)
			newer, _ := h.TryAcquire("p", "m")
			if success {
				newer.Success()
			} else {
				newer.Failure(503, nil)
			}
			d.Confirm()
			st := h.Snapshot()[0]
			want := 1
			if success {
				want = 0
			}
			if st.Failures != want || st.Down {
				t.Fatalf("state=%+v", st)
			}
		})
	}
}
