# ADR-009: Core Execution Semantics

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

Pipeflow executes pipelines, stages, and stage items in declaration order.
Sequential execution does not start another unit after the Go context is
cancelled. A failure returns a nil flow output and prevents later sequential
work from running.

Successful step values follow the value-flow rules documented by the public
Step API. A retrying step publishes only its successful result. Failed attempt
values are discarded, and retry exhaustion stops the flow.

`ConcurrentSteps` gives every child the same input and returns successful
results in declaration order, regardless of completion order.

`WaitAll` is the default. It waits for every started child and joins failures in
declaration order. The group does not expose partial results on failure.

`FailFast` cancels its child context after the first observed error. It waits
for already-running children, does not start queued bounded work after
cancellation, and returns the first observed non-cancellation error. Errors
caused only by cancellation are still returned; they are not treated as
success. User functions must cooperate with `context.Context` to stop running
work early.

A positive worker limit bounds simultaneously running children. Zero retains
the existing unlimited behavior; Phase 19 makes negative limits invalid.

## Explicit Boundaries

- Pipeflow does not forcibly stop user goroutines.
- Partial concurrent results are not exposed on failure.
- Skipped-work reporting and structured execution errors belong to later
  roadmap phases.
- Deterministic result ordering does not imply deterministic goroutine start or
  completion ordering.
