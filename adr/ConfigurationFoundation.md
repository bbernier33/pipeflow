# ADR-032: Additive Configuration Foundation

Date: 2026-08-31

## Decision

Begin the post-v1 roadmap with an optional configuration layer for existing
execution policies. `Pipeline.WithConfig` returns a detached configured copy;
the original v1 definition and active executions are never mutated.

The first supported hierarchy is:

```text
Core default < YAML default < Pipeline < Stage < Step < explicit Go
```

Pipeline and Stage timeouts; Step timeout, retry, polling schedule, and
rate-limit policies; Parallel failure policy; and Background failure policy
participate. Names are valid identifiers because v1 validation requires
uniqueness in their scopes. Unknown fields, paths, durations, and invalid
policies fail before execution.

`EffectiveConfig` exposes typed resolved settings and their source. It contains
configuration only—never callbacks, credentials, Context values, or flowing
business payloads.

## Completed v1.1 scope

Nested scopes use explicit `subflows`, `stages`, `parallels`, and `branches`
paths instead of flattened names. Polling predicates and conditions remain Go
code. `WithPollPredicate` separates a Go predicate from a YAML-owned polling
schedule.

Anonymous `ConcurrentSteps` groups keep worker count and failure policy in Go
because positional YAML keys are not stable identifiers; their contained Steps
remain configurable using validated Stage-scoped names.

v1.1 adds no hot reload, environment-variable expansion, secrets,
Worker/Stream modes, Source/Sink roles, telemetry, recovery, circuit breaker,
or idempotency behavior. Active-run configuration is immutable.
