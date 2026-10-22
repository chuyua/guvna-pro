package main

import (
	"errors"
	"strings"
	"testing"
)

func TestFinishTelemetryErrorVisibleToCaller(t *testing.T) {
	var result error
	finishTelemetry(func() error { return nil }, &result)
	if result != nil {
		t.Fatalf("successful close changed result: %v", result)
	}
	original := errors.New("listener failed")
	failure := errors.New("SQLite write failed")
	result = original
	calls := 0
	finishTelemetry(func() error {
		calls++
		return failure
	}, &result)
	if calls != 1 || !errors.Is(result, original) || !errors.Is(result, failure) || !strings.Contains(result.Error(), "telemetry close") {
		t.Fatalf("telemetry close failure was lost: result=%v calls=%d", result, calls)
	}
}
