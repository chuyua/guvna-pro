package outcome

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestClassifyStatuses(t *testing.T) {
	cases := []struct {
		status int
		want   Outcome
	}{
		{400, ModelIncompatible},
		{422, ModelIncompatible},
		{401, Auth},
		{403, Auth},
		{429, RateLimit},
		{500, UpstreamTransient},
		{502, UpstreamTransient},
		{503, UpstreamTransient},
		{404, CallerError},
		{408, CallerError},
		{200, ""},
	}
	for _, tc := range cases {
		if got := Classify(tc.status, nil); got != tc.want {
			t.Errorf("Classify(%d) = %s, want %s", tc.status, got, tc.want)
		}
	}
}

func TestClassifyErrors(t *testing.T) {
	if got := Classify(0, &net.DNSError{Err: "no such host", IsNotFound: true}); got != LocalNetwork {
		t.Errorf("dns error = %s, want local_network", got)
	}
	if got := Classify(0, errors.New("dial tcp: connection refused")); got != LocalNetwork {
		t.Errorf("dial error = %s, want local_network", got)
	}
	if got := Classify(0, context.Canceled); got != Canceled {
		t.Errorf("canceled = %s, want canceled", got)
	}
	if got := Classify(0, context.DeadlineExceeded); got != UpstreamTransient {
		t.Errorf("deadline = %s, want upstream_transient", got)
	}
}

func TestKeyAndModelMapping(t *testing.T) {
	if KeyClass(Auth) != "auth" || KeyClass(RateLimit) != "rate_limit" {
		t.Fatalf("key class mapping wrong: %q/%q", KeyClass(Auth), KeyClass(RateLimit))
	}
	if KeyClass(UpstreamTransient) != "" || KeyClass(LocalNetwork) != "" || KeyClass(ModelIncompatible) != "" || KeyClass(CallerError) != "" || KeyClass(Canceled) != "" {
		t.Fatal("neutral outcomes must not quarantine a key")
	}
	if !ModelFailure(LocalNetwork) || !ModelFailure(UpstreamTransient) || !ModelFailure(RateLimit) {
		t.Fatal("network/upstream failures must count for model health")
	}
	if ModelFailure(Auth) || ModelFailure(CallerError) || ModelFailure(ModelIncompatible) || ModelFailure(Canceled) {
		t.Fatal("neutral outcomes must not count for model health")
	}
	if !Retryable(Auth) || !Retryable(RateLimit) || !Retryable(UpstreamTransient) || !Retryable(LocalNetwork) {
		t.Fatal("auth/rate_limit/upstream/local_network must be retryable")
	}
	if Retryable(CallerError) || Retryable(ModelIncompatible) || Retryable(Canceled) {
		t.Fatal("caller/incompatible/canceled must not retry")
	}
}
