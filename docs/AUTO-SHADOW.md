# Auto ordering experiments

The existing LLM decider remains the default. This experiment compares it with
local observations without sending duplicate inference requests to candidates.
It does not infer how an untried candidate would have answered.

## Modes and rollback

| Setting | Default | Meaning |
| --- | --- | --- |
| `GUVNA_AUTO_ORDER_MODE` | `decider` | `decider`, `shadow`, or `telemetry`; invalid values use `decider` |
| `GUVNA_AUTO_SCORE_WINDOW` | `1h` | Positive Go duration, at most 7 days |
| `GUVNA_AUTO_SCORE_MIN_SAMPLES` | `100` | Minimum observed first attempts for **every** currently eligible candidate |
| `GUVNA_AUTO_TELEMETRY_APPROVED` | `false` | Operator affirmation that a separate policy comparison supports switching |

In `shadow`, the serving order is exactly the decider/static fallback order.
The gateway computes a telemetry order and records differences. In `telemetry`,
that alternative order is used only when the operator affirmation is true,
every eligible candidate meets the sample threshold for the current stream mode,
and the bounded snapshot is not truncated. Otherwise execution retains the
decider order. The decider path and its cache remain available in every mode;
this experiment does not remove its API cost. Returning the setting to `decider`
restores the original ordering. Environment changes normally require restarting
the service, according to how its process environment is managed.

No automatic promotion occurs. First collect shadow observations, then compare
actual policy cohorts or a controlled replay with comparable request mix,
models, credentials and time ranges. Require better first-attempt success and
fallback rates without unacceptable p95 latency before affirming a switch.
The minimum sample setting is a data floor, not a statistical significance test.

## Score and metrics

The lower shadow score sorts first:

```text
p = (successful_attempts + 1) / (all_attempts + 2)
mean_cost_ms = sum(all_attempt_latency_ms) / all_attempts
score_ms = max(1, mean_cost_ms) / p
```

Cost here means observed elapsed milliseconds, including failed attempts;
it is not token cost or provider pricing. The 1ms floor handles timestamps
rounded below a millisecond. Add-one probability smoothing avoids division by
zero. p95 is a report metric and is not the cost numerator. Equal scores use
observed fallback rate, then static priority. Unknown or undersampled candidates
stay in static order after sufficiently observed candidates in the shadow list;
they also prevent activation of the telemetry policy. There is no time decay,
quota tier or local rate limiter in this experiment.

Metrics are grouped by provider/model and stream flag, using persisted events
with nonempty `request_id` and positive `attempt`. Legacy and canceled rows are
excluded. A successful attempt has a 2xx status and empty failure class.

- `first_attempt_success_rate`: successful first attempts divided by observed
  first attempts for that candidate, counting each request/candidate once.
- `fallback_rate`: fraction of observed request/candidate pairs followed by a
  different candidate in the same request. Retrying another key of the same
  candidate alone is not fallback.
- `p95_latency_ms`: nearest-rank 95th percentile over all observed attempts,
  including failed attempts. Streaming latency includes the observed relay.

These conditional observations depend on which candidates the current policy
tries. A fallback candidate sees a different workload from the first choice.
The attempt schema does not distinguish API protocol or prompt class, so calls
to the same model through different endpoints can share a group. Window-edge
requests and capped samples can be incomplete. None of these metrics proves
that the unexecuted shadow policy would outperform the decider.

## Inspection and bounds

Use the admin Bearer credential to request:

```text
GET /admin/auto/scoring
GET /admin/auto/scoring?since=2026-10-21T00:00:00Z&until=2026-10-21T01:00:00Z
```

The response includes `metrics`, the requested mode and sample threshold, and
`comparisons` containing request IDs, both orders, disagreement, application
status and a reason. `since` is inclusive; `until` is exclusive. Both are
RFC3339 timestamps and the interval must be positive and no longer than 7 days.
Metric queries read at most the newest 100000 matching rows and expose
`truncated`; the admin query has a 1s deadline. Routing uses a 15s cache with
a 100ms query deadline and retains the decider on storage errors.

Comparison records are an in-memory ring of the latest 512 selections and are
lost on restart. They are also logged. Request routing adds no synchronous
SQLite writes for comparisons. The metrics themselves come from the existing
persisted telemetry flush. Shadow records contain candidate identifiers and
ordering metadata, not prompts, response content or credential values.

`GUVNA_PROVIDE_ORDER` accepts bare providers and exact `provider/model` entries.
Model names may contain further slashes. Exact entries override a bare entry
for that candidate; the first occurrence of a duplicate entry wins. Unlisted
candidates follow configured entries, with the existing chain-name tie break.
Decider rankings still take precedence over static priority.
