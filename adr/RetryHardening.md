# ADR-015: Retry Hardening

- **Status:** Accepted
- **Date:** 2026-08-26

## Decision

`WithRetry` accepts a run-local `RetryPolicy` with a maximum attempt count,
base delay, fixed or exponential backoff, an optional maximum delay, symmetric
jitter, and an optional error predicate.

`MaxAttempts` includes the initial execution. A missing policy executes once,
and a non-positive maximum is normalized to one. Fixed backoff is the zero
value. Exponential backoff doubles the base delay after each failed attempt;
duration overflow saturates instead of wrapping. Jitter is clamped to the
range `[0, 1]` and applied before the final maximum-delay cap.

`RetryIf` receives the Step's ordinary returned error. Returning false ends the
Step with that error immediately. Context cancellation and deadline errors are
never retried and bypass the predicate. Backoff waits select on the execution
context, so cancellation and Step, Stage, or Pipeline timeouts interrupt them.

When polling and retry are combined, every poll owns a fresh attempt sequence.
Retry exhaustion preserves the existing structured `ExecutionError`, including
its final attempt number.

Every attempted invocation remains visible in `StepReport.Attempts` and, for a
polling Step, its `PollReport.Attempts`. `AttemptReport.RetryDelay` records the
actual selected delay when another attempt will follow. Reports do not retain
inputs, outputs, or predicate decisions.

## Boundaries

This phase does not add hooks, circuit breakers, rate limiting, persistence,
metrics exporters, distributed retry coordination, or user-supplied random
sources. Jitter is intentionally nondeterministic; tests assert its bounds
rather than a particular random value.
