# Upstream reliability

Non-streaming Chat, Responses, Auto and Embeddings buffer and validate each successful upstream body before committing a client response. Invalid or truncated 2xx bodies can use the next configured step. If no step serves a valid result, the gateway returns a 502 instead of forwarding a broken success. This validation checks protocol structure; it cannot judge the usefulness of an answer.

An upstream may already have generated or billed a request before a body fails. Retrying another step before client output can therefore duplicate generation or billing. Once streaming headers or events have been sent, the gateway never switches providers or combines their answers. A later failure leaves HTTP status 200 in place and emits a Chat error event or a Responses `response.failed` event.

## Endings and tool calls

By default, Chat and Responses streams require a recognized `finish_reason`. `[DONE]` alone is insufficient; a recognized finish followed by EOF is accepted. Responses maps `length` and `content_filter` to `response.incomplete`, preserving the distinction between transport success and completed generation.

Chat relays track endings separately for every observed choice. For a request with `n > 1`, each requested choice index must be present and finish; one finished choice cannot hide an unfinished or omitted choice. Invalid or duplicate indices, conflicting endings and payloads appended after a choice has ended are rejected. Usage-only frames remain supported. The nonstandard-ending opt-out applies only to a single-choice stream, not to missing or unfinished choices in a multiple-choice result.

Stream-only aggregation preserves all observed choices, ordered by choice index. Each choice keeps its own content, reasoning, finish reason and tool calls; tool calls are ordered by their index within that choice. Every observed choice must finish, and an effective request count of `n > 1` also requires the requested choices to be present. When the effective count is omitted or `1`, providers returning extra choices remain compatible, but every returned choice must be complete. Malformed/error payloads or incomplete choices fail before client output and can use the next configured step.

Non-streaming Chat and Auto apply the same count contract before committing output: `n > 1` requires exactly the requested choices with unique indices from `0` through `n - 1`. Missing choices or missing, negative, duplicate or out-of-range indices make the response unusable and allow fallback. All three Chat transport forms use the effective `n` for the current candidate: an explicit client field takes precedence, while a chain step's `params.n` fills an absent field. Each candidate resolves its defaults independently, so one candidate's count cannot carry into the next. Responses retains its single-output translation contract.

Tool calls must have an ID, a function name, type `function`, and valid JSON arguments when a complete call is reported. Responses streams ending with `length` or `content_filter` can retain partial calls as incomplete output. Stream-only aggregation rejects incomplete calls before committing output. The Responses translation path still maps only the first Chat choice; it does not expose multiple Chat choices as multiple Responses outputs.

Embeddings must contain nonempty finite numeric vectors or valid base64 float32 bytes in every data entry. All vectors in one response must use the same dimension and representation. For recognized nonempty text, text-batch, token-array and token-batch inputs, the response must contain exactly one vector per input. Every batch entry needs a unique integer index from `0` through `data.length - 1`; out-of-order entries are accepted and forwarded unchanged. A single entry may omit its index for compatibility, but any provided index must be valid.

The effective positive `dimensions` and explicit `encoding_format` (`float` or `base64`) constrain each candidate's response. Client fields take precedence over chain defaults, including explicit null or zero; defaults are resolved independently for each candidate. If the format is absent, either supported representation is accepted. Unrecognized or invalid request values remain the upstream's responsibility. Validation does not assume a model's default dimension or judge vector quality. An incomplete batch, invalid index or mismatched shape/encoding fails before output, permits the next configured candidate, and records a transient failure for the attempted upstream.

## Limits

| Environment variable | Default | Scope |
|---|---|---|
| `GUVNA_MAX_REQUEST_BYTES` | `33554432` (32 MiB) | Incoming Chat, Responses, Auto, Embeddings and `POST /v1/chains` request body |
| `GUVNA_REQUEST_READ_TIMEOUT` | `30s` | Incoming model and chain-creation request body read only |
| `GUVNA_STEP_BUDGET` | `180s` | One ordinary non-streaming step, including retries, headers and body |
| `GUVNA_REQUEST_BUDGET` | `300s` | Ordinary non-streaming candidate walk |
| `GUVNA_STREAM_IDLE_TIMEOUT` | `120s` | No complete upstream line during a streaming relay; also caps a streaming error-body read |
| `GUVNA_STREAM_MAX_DURATION` | `10m` | Active Chat, Responses or Auto relay, measured from successful upstream headers regardless of progress |
| `GUVNA_MAX_BODY_BYTES` | `33554432` (32 MiB) | Buffered success/error bodies, including the decider response |
| `GUVNA_REQUIRE_STREAM_TERMINATOR` | `true` | Ordinary single-choice Chat relay finish check; `false` permits nonstandard endings, including incomplete results |

The duration settings in the table accept Go durations; `0` disables that duration limit. Both byte settings must be positive integers. These limits are loaded when the server starts. SSE lines and joined event data retain their fixed 8 MiB cap. Stream-only aggregation additionally applies `GUVNA_MAX_BODY_BYTES` independently to consumed SSE input and the final JSON response. Input includes SSE framing, so even an eventual small response can exhaust the input budget. Reaching the input cap probes for excess bytes rather than treating truncation as EOF.

Aggregation charges escaped text, tool arguments and JSON structure before retaining each fragment. Per-event choice/tool structure checks run before decoding large typed slices, including duplicate or case-insensitive JSON fields. These bounds do not impose a fixed process memory ceiling. A limit failure closes the upstream body before client output and follows the existing candidate fallback path; exhausted fallback retains the existing 502 response, with the specific limit failure logged.

Auto retains `GUVNA_AUTO_STEP_BUDGET=20s` and `GUVNA_AUTO_TOTAL_BUDGET=45s`. Its total budget is measured from handler entry and covers the decider call, shared decision waits and the candidate walk. The step budget applies to non-streaming candidates, including stream-only aggregation. Streaming setup and failed responses share the remaining total budget; successful upstream headers stop that setup timer, and the active relay then uses the independent idle and maximum-duration limits. A non-positive Auto total budget disables it; the Auto step budget requires a positive duration and otherwise retains its default.

The idle limit measures upstream line activity: upstream comment lines count, while downstream keep-alives do not. The maximum duration is a wall-clock deadline that upstream activity cannot extend. Expiry emits a Chat `upstream_stream_timeout` error or a Responses `response.failed` event, records one transient step failure and closes the upstream body; it never switches providers after committing a stream. Setting `GUVNA_STREAM_MAX_DURATION=0` disables only this hard deadline, leaving the idle limit in effect.

Caller cancellation stops backoff and further candidates without adding a health/key failure or recording success. An earlier upstream failure remains recorded. Gateway upstream budgets count as transient step failures. The Auto total budget deducts time spent reading and parsing the incoming request; incoming body reads also have the separate limits below.

## Incoming request bodies

Chat, Responses, Auto, Embeddings and `POST /v1/chains` share one bounded body reader. A declared `Content-Length` above the limit is rejected before reading; streamed or chunked bodies are checked against the bytes actually read. An exactly-at-limit body is allowed. An oversized body returns `413 request_too_large`; a gateway body-read timeout returns `408 request_timeout`. These failures occur before upstream selection or runtime-chain changes and do not add model-health or key failures. Caller cancellation remains neutral. Chain creation requires one complete JSON value; trailing JSON or garbage is rejected before changing a route.

The read timeout covers only incoming body consumption. A successful read stops and joins its cancellation callback, then clears the native read deadline before generation begins. It does not cap upstream generation or an active response stream. Chat requests using `model:auto` pass the already-read body to Auto instead of reading it again. Failed HTTP/1 reads close the connection and keep an expired read deadline so the server cannot block while draining the unread body; HTTP/2 read deadlines apply to that request's stream.

Standard HTTP/1 and HTTP/2 writers use `http.ResponseController.SetReadDeadline`. A wrapping writer should expose `Unwrap` so the controller can reach its transport. For custom writers without read-deadline support, cancellation closes the body and relies on the `http.Request.Body` contract that `Close` unblocks `Read`. These limits do not cover request headers. Management is CLI-only; there are no browser form routes.

## Auto context selection

For requests without images, Auto first applies the configured model-context filters, then checks each eligible candidate against a local estimate of the current request plus explicitly requested output. The estimate includes the serialized request's message/tool structure: roughly three ASCII bytes per token and one token per non-ASCII UTF-8 byte. It reserves the larger positive value of `max_tokens` and `max_completion_tokens`, with overflow protection. If every eligible candidate is too small, Auto returns `400 context_length_exceeded` before calling a candidate.

This is a heuristic, not a model tokenizer: it can overestimate or underestimate usage. It does not predict an unspecified output allowance or model-specific image token costs, and image requests skip this estimate. Unknown models keep the existing 8192-token registry default. The service's default minimum-context preference can still degrade to a smaller pool when no registered model meets it, but those text-only candidates must also pass the current request estimate. Explicit client context bounds remain hard filters. Upstream context validation remains authoritative.

## HTTP failure attribution

All four execution surfaces use `outcome.Classify` for one mutually exclusive failure class. A real HTTP status takes precedence over an attached descriptive error. Caller cancellation/deadlines and gateway budget expiry carry distinct classified errors.

| Outcome | Model health | Key state |
|---|---|---|
| Successful validated `2xx` | Reset on completed relay | Healthy |
| `400`, `422` (`model_incompatible`), text request | Neutral | Neutral |
| `400`, `422`, image request | Deferred compatibility cool-off after another candidate serves a valid result | Neutral |
| `401`, `403` (`auth`) | Neutral | Auth quarantine |
| Other `4xx` except `429` (`caller_error`) | Neutral | Neutral |
| `429` (`rate_limit`) | One failed step after credential attempts fail | Rate-limit quarantine |
| `5xx`, unusable success bodies and gateway timeouts (`upstream_transient`) | One failed step | Neutral |
| DNS, dial/connect failures including dial timeouts (`local_network`) | One failed step | Neutral |
| Caller cancellation/deadline or downstream disconnect | Neutral | Neutral |

Image compatibility applies to the effective Chat payload, including translated Responses image input. Embeddings have no image input contract. Service failures are not evidence that a credential is defective: they release its attempt lease without changing its failure count or quarantine. Only a validated completed success resets prior key failures. A whole step counts at most one model failure even when several attempts were made. A recovered attempt can complete without charging a model failure. Error responses retain the upstream status/body on exhausted fallback. The advisory decider uses the same key attribution, including malformed rankings, but does not change serving-model health.

Image rejection is deferred until a later candidate completes a structurally valid body or stream and its downstream writes succeed. An all-failure walk, cancellation, timeout, malformed success, truncated stream or downstream disconnect does not confirm earlier image blame. Deferral releases a recovery probe immediately; its generation-bound token cannot overwrite a newer failure or recovery.

## Retry budgets and backoff

Credential failover and transient retries have separate bounds. Each step tries a different usable credential after `401`, `403` or `429`, at most once per configured credential; `GUVNA_RETRIES=0` does not disable credential failover. DNS/connection failures and `5xx` retry the same healthy key up to `GUVNA_RETRIES` extra times (default `2`). Each retry acquires a fresh lease, retains backoff and consumes the shared retry budget. If another request quarantines that key, an unused healthy credential may be selected instead. The gateway never bypasses quarantine to force a retry. `GUVNA_RETRIES=0` disables these service/network retries even with multiple healthy keys. Invalid bodies and active stream failures retain their existing fallback/termination behavior rather than starting another credential attempt.

`GUVNA_RETRY_BUDGET` caps extra attempts across the entire candidate walk (default `16`; `0` permits only each initial attempt until a retry would be needed). Exhausting this shared budget stops the walk and returns the last available upstream response, or 503 when no HTTP response exists. It does not cancel a selected successful stream. Existing step/request deadlines independently cap time spent connecting, waiting, retrying and reading non-streaming bodies.

For `429` and `5xx`, a valid `Retry-After` in integer seconds or HTTP-date takes precedence. The wait is capped by `GUVNA_RETRY_MAX_WAIT` (default `30s`). Otherwise Full Jitter samples a delay from `[0, min(base * 2^(attempt-1), max_wait))`, with `GUVNA_RETRY_BASE=250ms` by default. Both durations must be positive. Waiting honors caller cancellation and the existing step deadline immediately. A key is rechecked after the wait in case another request quarantined it. The last bounded upstream error body is preserved if all usable credentials disappear during backoff.

Switching to a different credential after `401` or `403` is immediate and still consumes the shared retry budget. Rate-limit failover retains backoff: because `Retry-After` has no portable credential scope, a `429` hint is conservatively treated as provider-wide even when switching keys. No scope is inferred from vendor-specific rate-limit headers.

## Attempt telemetry

Every real upstream attempt has one persisted row, with a request ID shared across the whole candidate walk (including Chat `model:auto`), a one-based attempt number restarting for each candidate, `latency_ms` measured from that actual upstream call through its body/stream completion, and `failure_class`. Successful rows have an empty failure class. Retried failures are recorded immediately; the selected terminal relay is recorded once with token usage. No extra terminal row duplicates the last retry. Cancellation is neutral and does not erase previous upstream failures. Key usage counts actual attempts, including failed credential attempts; secret key values are never persisted.

`/admin/status` exposes buffer lifecycle counters under `telemetry`: `flushes_ok`, `flushes_failed`, `retried_events`, `dropped_events`, `buffer_len` and `inflight_events`. If persisted aggregates are unavailable, status still returns health and telemetry diagnostics with `usage_error`; zero usage values in that response must not be interpreted as a successful aggregate read. New attempt columns migrate existing SQLite databases without removing old rows. Buffer durability remains process-local; process termination can lose unflushed events.

## Recovery admission and key exhaustion

`IsDown` is a read-only health query. The Chat, Responses, Auto and Embeddings execution walks use `TryAcquire` instead: after a provider/model cool-off expires, only one request owns its recovery probe. Other requests skip to the next candidate immediately. A successful probe resets model health; a failed probe restarts cool-off. Cancellation, unavailable keys, non-health HTTP outcomes and downstream disconnects release the permit without manufacturing a health failure. A generation token prevents older results from clearing a newer probe or its cool-off.

The key pool returns no key while all credentials remain quarantined. That step makes no upstream call, does not retry an empty credential and does not count another model/key failure. If every candidate is unavailable, the walk returns 503; if a real upstream response was already obtained, it preserves that result. Keys become selectable at the exact expiry boundary. A delayed success cannot clear a key that is still quarantined by another request. Retries recheck the selected key after backoff and can select another available key if it was quarantined during the wait.

The advisory Auto decider follows the same key availability and quarantine rules. Auth, rate-limit, server and invalid-success failures update its key state; DNS/connection failures and caller cancellation are neutral for credentials. Valid rankings record successful key usage. The decider still does not change candidate model health and falls back to the existing static ordering when unavailable. Canceled decisions do not populate the ranking failure cache, and a canceled shared waiter does not cancel another request's leader decision.

Half-open admission is per provider/model, not a provider-wide circuit breaker or a per-key single-flight limit. Healthy traffic remains concurrent.

## Runtime chain persistence

Each `Store` serializes the complete snapshot-to-replacement operation. Saves use unique temporary files in the target directory and stable chain ordering; failed replacements clean up the temporary file and leave the original target intact. Router input and returned snapshots copy JSON/YAML parameter containers so external mutation does not race with persistence.

HTTP runtime-chain creation and deletion persist the proposed snapshot before publishing the router change. A failed save returns HTTP 500 without changing either the live chain map or model-alias resolution; the previous file remains intact, and the same operation can be retried. Validation, duplicate-name conflicts and config-chain protection retain their existing 400/409/404 responses. Concurrent mutations through the shared `Store` are serialized so successful changes cannot overwrite each other.

These guarantees apply to one shared `Store` instance. Multiple processes or independently constructed stores writing the same path are not coordinated. No delete-before-rename workaround is used. This is not an fsync-based crash or power-loss durability guarantee; on Windows a target held without delete sharing can still reject replacement, in which case the API reports failure and preserves the original file.

## Verification

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./internal/chains ./internal/router ./internal/health ./internal/keypool ./internal/server ./internal/responses -count=1
```

PowerShell users can run `./Validate.ps1 -Race`. If a Windows MinGW installation under a non-ASCII path fails to locate `crt2.o` or `crtbegin.o`, `./Validate.ps1 -Race -ShortToolchainPaths` temporarily sets the GCC search paths using the installed directory's short path. It restores the process environment on exit and does not change system settings. This workaround requires Windows short-path support and an installed GCC toolchain.

The regression tests use local simulated upstreams and dummy keys. They cover malformed/empty 2xx, truncated and stalled bodies, body limits, fallback, cancellation during body/backoff, downstream write failure, invalid SSE endings, idle and hard stream deadlines, oversized SSE lines/events, and multiple fragmented tool calls. Multiple-choice tests check interleaved choices, isolated tool indices, missing/unfinished choices, invalid indices and fallback before output. Count-contract tests also check non-streaming responses, chain-default counts, explicit client precedence and candidate-default isolation across non-streaming, streaming and aggregated Chat/Auto paths. Auto tests check request-context fit, explicit output reservation, image-estimate bypass, decider/shared-wait deadlines and cancellation-cache attribution. HTTP status tests cover ten route forms, key retries and neutral half-open release.

Embedding contract tests cover recognized input forms, missing/extra vectors, batch indices, inconsistent/requested dimensions, numeric/base64 encoding and finite values. Local upstream tests verify fallback before output, exhausted-chain 502 responses, per-step defaults, explicit client precedence, health/key attribution and neutral caller cancellation.

Incoming-reader tests cover declared and actual size limits, exact-limit bodies, disabled read timeouts, caller cancellation/deadlines, wrapped writers, real HTTP/1 and HTTP/2 slow uploads, long responses and connection reuse after successful reads. Route-level fault injection checks zero upstream calls and neutral health/key state across ten route forms, real network 408 responses on all four model endpoints and chain creation, and single-read Auto alias handling that charges upload time to the total budget. Chain-route tests cover rejected input without mutations, exact-limit JSON, trailing data, persistence failures and successful retries.

Recovery tests cover 64 concurrent admissions across ten route forms, probe release and old results, all-key quarantine, decider key results, key quarantine during backoff, 64 concurrent saves, snapshot isolation, and failed replacement cleanup. A Windows-only test verifies preservation of a locked target file.

Provider-wide isolation, exact model tokenization, request-header limits and multiple-choice Responses translation remain outside this batch. The existing named chains, selection architecture and gateway deployment architecture are retained.
