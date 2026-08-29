# ADR-024: Run-Scoped Rate Limiting and Throttling

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

`WithRateLimit(RateLimitPolicy)` applies optional token-bucket rate and
concurrency limits to Step action invocations:

```go
type RateLimitPolicy struct {
    Key           string
    MaxCalls      int
    Interval      time.Duration
    MaxConcurrent int
}
```

`MaxCalls` requires a positive `Interval` and establishes the token-bucket
capacity and initial burst. `MaxConcurrent` establishes a semaphore bound.
At least one limit must be configured. Negative values and incomplete rate
settings are validation errors.

Every retry attempt and polling operation acquires capacity immediately before
calling user code. Conditional skips do not acquire capacity. Token and
concurrency waits observe the active Go context, so existing Pipeline, Stage,
Step, polling, and parent deadlines remain authoritative. Attempt timing
includes throttling waits.

Limiters live in run-scoped Context metadata shared by branch Contexts. A
non-empty `Key` shares a limiter across Steps in that execution. An empty key
uses Step identity. Every new Pipeline run resets limiter state. Equal keyed
policies compose; conflicting keyed policies are rejected before execution,
including policies declared in structured branches and nested Subflows.

## Retry-After

Errors may implement `RetryAfterError`, exposing `RetryAfter() time.Duration`.
Pipeflow discovers the interface with `errors.As`. A positive duration becomes
the minimum retry delay and postpones new acquisitions from the same limiter.
Existing retry eligibility, maximum attempts, error wrapping, cancellation,
and `AttemptReport.RetryDelay` behavior remain unchanged.

## Boundaries

This limiter is in-memory and execution-scoped. This phase adds no distributed
quota service, persistent or cross-run rate state, HTTP header parsing,
provider-specific adapters, dynamic reconfiguration, circuit breaker,
adaptive algorithm, metrics exporter, or general resilience framework.
