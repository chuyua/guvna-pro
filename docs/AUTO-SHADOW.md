# Passive Auto and provider observations

The existing LLM decider remains the default. Normal Auto requests now produce a
completed request summary in every ordering mode. Provider observations come
from real attempt telemetry across the model routes. Collection and inspection
send no new inference requests, probes or randomized traffic. They do not infer
how an untried candidate would have answered.

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

## Actual Auto request results

Each invocation of the Auto handler (`/v1/auto` and Chat `model:auto`) records one
summary after completion, including selection errors and cancellation. Rejected
authentication and body-read errors before entering that handler are outside
this collection scope. A summary stores request ID, configured `mode`, actual
`policy` (`decider`, `static`, `telemetry`, or `none` before selection), original
decider source, stream flag, actual attempt and distinct candidate counts,
terminal status/class, and elapsed time. It stores no prompts, response content
or credential values.

`auto_requests.policies` groups actual requests by mode, policy and stream flag:

- `requests`, `successes`, `failures`, `canceled`, `attempts` and
  `no_attempt_requests` describe whole requests, not just individual attempts.
- `first_attempt_success_rate` requires a validated successful completion with
  exactly one actual upstream attempt.
- `fallback_rate` counts requests which actually tried more than one distinct
  provider/model. Rotating keys or retrying the same candidate is not fallback.
- Rates use `requests - canceled` as their denominator. Selection failures
  remain unsuccessful requests. Canceled requests are reported separately.
- `p95_latency_ms` is the nearest-rank p95 of completed, noncanceled requests,
  including failed requests. Elapsed time includes decision waits, retries and
  the complete response body/stream, rather than just response headers.

Summary windows use completion time in `[since, until)`. A request which starts
before the window but completes within it is included as a whole request. No
outcome is fabricated for the unexecuted shadow order: its success rate remains
unknown. Comparing different time periods can reflect changes in workload or
provider conditions; these observations alone do not prove a better policy.

## Passive provider health

`provider_health.providers` groups real attempt rows by provider and stream flag.
Rows without request IDs or positive attempt metadata are legacy rows and do not
participate. Failure counters are separate:

| Counter | Meaning |
| --- | --- |
| `successes` | Complete validated 2xx response |
| `upstream_failures` | 5xx, unusable 2xx or gateway timeout |
| `network_failures` | DNS, connection or transport failure; may be local |
| `auth_failures` | 401/403 credential issue |
| `rate_limited` | 429 capacity/quota limit |
| `request_errors` | Caller error or model/request incompatibility |
| `canceled` / `other_failures` | Explicit cancellation / unrecognized class |

`health_samples = successes + upstream_failures + network_failures + rate_limited`.
`service_success_rate = successes / health_samples`, or `null` without such
evidence. Authentication and request errors are not counted as provider service
failures. This is observed availability along the request path, not a diagnosis
of a provider-wide outage. `p95_latency_ms` includes failed noncanceled attempts;
`last_attempt_at` identifies the latest observation. Existing attempt telemetry
generally omits canceled attempts; the request summaries still expose Auto
cancellations.

`provider_overview` includes configured providers with no observations as
`observation_status: no_data`. `current_model_health` and each provider's
`down_models` describe the current in-memory model breaker state separately from
the historical window. One cooled model does not establish that every model at
the provider is down.

## Local inspection and bounds

On the gateway host, use the existing admin credential with the local CLI:

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
guvna status --scoring
guvna status --scoring --since 2026-10-21T00:00:00Z --until 2026-10-21T01:00:00Z --json
```

The command connects only to the filesystem socket. Specify `--socket` when the
service uses a custom data directory; see [CLI.md](CLI.md). There are no public
`/admin/*` or `/v1/chains` management routes, including on the model API port
forwarded by Cloudflare Tunnel. The management socket must not be proxied.

The report includes `auto_requests`, `provider_health`, `provider_overview`,
`current_model_health`, `observed_at` and `telemetry` flush/queue/drop counters.
It retains `metrics`, the requested mode and sample threshold, and
`comparisons` containing request IDs, both orders, disagreement, application
status and a reason. `since` is inclusive; `until` is exclusive. Both are
RFC3339Nano timestamps and the interval must be positive and no longer than 7 days.
Queries read bounded samples of at most 100000 rows and expose `truncated`;
the provider query may conservatively report truncation when precision boundary
filtering reaches its scan cap. All diagnostic queries share a 1s deadline and
report storage errors explicitly. Routing uses a 15s cache with
a 100ms query deadline and retains the decider on storage errors.

New summaries use the same bounded FIFO and transactional background flush as
attempt events (30s in the service). Recently completed traffic may still be
queued; inspect `telemetry` before interpreting an empty or incomplete report.
Failed batches are retained for retry where capacity permits; dropped counters
cover both event kinds. A process crash can lose the current buffered interval.

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
