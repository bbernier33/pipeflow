# Core Timeout, Configuration, and Idempotency Correctness

## Status

Accepted as a Core correctness blocker before further v3.x readiness work.

## Timeout arbitration

An action result is accepted only when its completion instant is before the
active context deadline. Checking `Context.Err` alone is insufficient because
the runtime timer callback may be scheduled slightly after the deadline. Once
the deadline has elapsed, a late successful result is discarded and normal
timeout reporting and Recovery policy apply.

This retains synchronous ordinary-function execution. Pipeflow does not spawn
and abandon every timed Step merely to return early; functions should continue
to observe cancellation through the legacy context-aware adapter when needed.

## Configuration flags

`WithCondition` configures only a condition. `WithRateLimit` marks the rate
limit as an explicit Go override. Effective configuration therefore reports the
correct source and YAML rate-limit configuration is not accidentally masked by
an unrelated condition.

## Idempotency finalization

Idempotency `Complete` and `Release` are cleanup/finalization calls. They run
with cancellation detached from the completed attempt but under a finite
deadline. The default is 30 seconds; `WithFinalizationTimeout` returns a guard
copy with an application-specific bound.

The store call runs behind a buffered result channel so a store that ignores
context cannot block Pipeline completion forever. Timeout is reported through
`ErrIdempotencyFinalizationTimeout`, the Step error, observation, and
`IdempotencyReport` store-failure outcome. Go cannot terminate an uncooperative
goroutine; store adapters must still honor their context to release resources.
