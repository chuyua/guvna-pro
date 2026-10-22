# API key attempt leases

Serving and decider attempts should select a key with `Pool.Acquire(excluded)`.
Selection and the key's current generation are captured under one lock. A
healthy key can have multiple leases; leases do not serialize its traffic.

The lease API is:

```go
lease := pool.Acquire(excluded) // nil when no selectable key remains
keyEnv := lease.Env()
if !lease.Available() { // recheck after a cancellable backoff
    lease.Release()
    // Acquire again; an expired quarantine cannot revive an old lease.
}

// Complete once after the real attempt, including any successful response body:
lease.Failure(keypool.ClassRateLimit) // returns the class only when applied
// OR lease.Success()
// OR lease.Release() // cancellation, caller error or abandoned selection
```

All lease methods accept nil. `Failure("")` also completes the lease neutrally.
`Failure`, `Success` and `Release` are terminal and idempotent. A second retry
needs a new lease, even when it reuses the same credential. Request and token
accounting stays separate in `Pool.Record`; every attempted request can count
usage even when its late result no longer changes isolation.

## Failure generations

An accepted failure increments the key's generation and establishes one
quarantine window. Other results admitted in the same generation cannot add
more failures, prolong the window or clear it. This remains true after expiry:
only an attempt acquired for the current generation can establish a new
window or confirm recovery. A recovery reset advances generation again, so
an earlier failure cannot poison a recovered key.

Consecutive failures from genuine later attempts retain the configured
exponential durations and existing 24-hour maximum. The defaults remain
auth 24 hours, rate limit 1 minute and transient 1 minute; this fix adds no
new timeout policy, limiter or Retry-After parser.

## Authentication upgrade

A concurrent 401/403 is stronger credential evidence than a rate limit or
transient failure. An auth result from the exact admission cohort that
established the **current active** non-auth window may upgrade that window
once. The upgrade preserves its failure count and never shortens the existing
deadline; it applies the auth duration to that outage. Repeated auth results
do not refresh the deadline. Auth results from an earlier expired window or
from before a recovery are stale and cannot upgrade a newer window.

## Compatibility API

`Pick`, `PickExcluding`, `Available`, `Mark` and `MarkSuccess` remain available.
The unbound `Mark` suppresses repeated failures during an active quarantine,
except the single auth upgrade described above. At expiry a new `Mark` can
increase the failure count normally.

Because these legacy methods carry no admission token, they cannot distinguish
a delayed result from a genuine attempt after expiry. Keep them for direct
administration and compatibility; execution paths must use leases to obtain
the full cross-window stale-result protection. `MarkSuccess` retains its
active-quarantine guard, while `Lease.Success` additionally checks generation.

## Verification

The keypool tests use an atomic fake clock. They cover 64 concurrent results,
the original eight-result audit regression, exact expiry boundaries, serial
backoff to the existing cap, recovered/new-window fencing, one-time auth
upgrades, cancellation/duplicate completion, exclusions, independent keys and
usage accounting. No real upstream or credentials are involved.
