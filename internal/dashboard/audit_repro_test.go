package dashboard

import "testing"

func TestAuditLogsOverflowClamp(t *testing.T) {
	gw := &fakeGateway{logs: `{"lines":["a"]}`}
	_, _, ds := newTestDashboard(t, gw)
	c := login(t, ds.URL)
	cget(t, c, ds.URL+"/partial/logs?n=9999")
	got := gw.lastReq.URL.Query().Get("n")
	t.Logf("requested=9999 forwarded=%s expected=512", got)
	if got != "512" {
		t.Fatalf("overflow should clamp to 512, got %s", got)
	}
}
