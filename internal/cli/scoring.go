package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/creamy-ghost/guvna/internal/telemetry"
)

type scoringResponse struct {
	Mode               string                           `json:"mode"`
	AutoRequests       telemetry.AutoRequestSnapshot    `json:"auto_requests"`
	ProviderHealth     telemetry.ProviderHealthSnapshot `json:"provider_health"`
	CurrentModelHealth []step                           `json:"current_model_health"`
	ProviderOverview   []struct {
		Provider          string `json:"provider"`
		ObservationStatus string `json:"observation_status"`
		TrackedModels     int    `json:"tracked_models"`
		DownModels        int    `json:"down_models"`
	} `json:"provider_overview"`
	Telemetry telemetry.FlushStats `json:"telemetry"`
}

func (c command) scoringStatus(cf commonFlags, since, until string) error {
	query := url.Values{}
	for name, value := range map[string]string{"since": since, "until": until} {
		if value != "" {
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return fmt.Errorf("invalid %s timestamp: %w", name, err)
			}
			query.Set(name, value)
		}
	}
	path := "/admin/auto/scoring"
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	var raw json.RawMessage
	if err := cf.get(path, &raw); err != nil {
		return err
	}
	if cf.json {
		return c.printJSON(raw)
	}
	var report scoringResponse
	if err := json.Unmarshal(raw, &report); err != nil {
		return err
	}
	fmt.Fprintf(c.out, "AUTO OBSERVATIONS (mode %s, completed in [%s, %s))\n", report.Mode,
		report.AutoRequests.Since.Format(time.RFC3339Nano), report.AutoRequests.Until.Format(time.RFC3339Nano))
	fmt.Fprintf(c.out, "%-10s %-10s %-8s %8s %8s %8s %8s %9s %10s %10s %10s\n", "MODE", "POLICY", "RELAY", "REQUESTS", "FAILED", "CANCELED", "ATTEMPTS", "SUCCESS", "FIRST OK", "FALLBACK", "P95 MS")
	for _, policy := range report.AutoRequests.Policies {
		fmt.Fprintf(c.out, "%-10s %-10s %-8s %8d %8d %8d %8d %8.1f%% %9.1f%% %9.1f%% %10d\n", policy.Mode, policy.Policy, relayLabel(policy.Stream),
			policy.Requests, policy.Failures, policy.Canceled, policy.Attempts, 100*policy.SuccessRate, 100*policy.FirstAttemptSuccessRate, 100*policy.FallbackRate, policy.P95LatencyMS)
	}
	if len(report.AutoRequests.Policies) == 0 {
		fmt.Fprintln(c.out, "no_data")
	}
	if report.AutoRequests.Truncated {
		fmt.Fprintln(c.out, "request samples truncated; this is a bounded observation")
	}
	fmt.Fprintln(c.out, "\nPROVIDER OBSERVATIONS (passive attempts; network faults may be local)")
	fmt.Fprintf(c.out, "%-14s %-8s %8s %8s %8s %8s %8s %8s %8s %8s %10s %8s\n", "PROVIDER", "RELAY", "ATTEMPTS", "SUCCESS", "UPSTREAM", "NETWORK", "AUTH", "LIMIT", "REQUEST", "OTHER", "SERVICE OK", "P95 MS")
	for _, provider := range report.ProviderHealth.Providers {
		availability := "unknown"
		if provider.ServiceSuccessRate != nil {
			availability = fmt.Sprintf("%.1f%%", 100**provider.ServiceSuccessRate)
		}
		fmt.Fprintf(c.out, "%-14s %-8s %8d %8d %8d %8d %8d %8d %8d %8d %10s %8d\n", provider.Provider, relayLabel(provider.Stream),
			provider.Attempts, provider.Successes, provider.UpstreamFailures, provider.NetworkFailures, provider.AuthFailures,
			provider.RateLimited, provider.RequestErrors, provider.OtherFailures, availability, provider.P95LatencyMS)
	}
	for _, provider := range report.ProviderOverview {
		fmt.Fprintf(c.out, "  %s: %s; currently %d/%d tracked models cooling down\n", provider.Provider, provider.ObservationStatus, provider.DownModels, provider.TrackedModels)
	}
	if len(report.ProviderOverview) == 0 && len(report.ProviderHealth.Providers) == 0 {
		fmt.Fprintln(c.out, "no_data")
	}
	if report.ProviderHealth.Truncated {
		fmt.Fprintln(c.out, "provider samples truncated; this is a bounded observation")
	}
	fmt.Fprintln(c.out, "\nCURRENT MODEL COOLDOWNS (separate from the observation window)")
	for _, model := range report.CurrentModelHealth {
		state := "not cooling"
		if model.Down {
			state = fmt.Sprintf("cooldown %v", time.Duration(model.DownFor))
		}
		fmt.Fprintf(c.out, "  %s/%s: %s (%d consecutive failures)\n", model.Provider, model.Model, state, model.Failures)
	}
	fmt.Fprintf(c.out, "\nTELEMETRY\n  buffer %d   in-flight %d   dropped %d\n", report.Telemetry.BufferLen, report.Telemetry.InflightEvents, report.Telemetry.DroppedEvents)
	fmt.Fprintf(c.out, "  flushes %d ok / %d failed   retained for retry %d\n", report.Telemetry.FlushesOK, report.Telemetry.FlushesFailed, report.Telemetry.RetriedEvents)
	fmt.Fprintln(c.out, "Shadow ordering differences do not establish outcomes for the unexecuted policy.")
	return nil
}

func relayLabel(stream bool) string {
	if stream {
		return "stream"
	}
	return "buffered"
}
