# ADR-013: Execution Timeouts

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

Pipeflow supports total execution timeouts at Pipeline, Stage, and Step scope:

```go
NewStep(name, fn, WithTimeout(duration))
NewStage(name, items...).WithTimeout(duration)
NewPipeline(name, stages...).WithTimeout(duration)
```

Non-positive durations disable the timeout at that scope. Nested timeouts use
standard Go context deadline inheritance, so the earliest parent or child
deadline wins.

A Step timeout covers the entire Step execution, including all attempts and
retry delays. It is not reset per attempt. A Stage timeout covers all Stage
items. A Pipeline timeout covers the complete run.

When a deadline expires, Pipeflow propagates `context.DeadlineExceeded` through
`ExecutionError`. Active report units become `timeout`; later pending work
becomes `skipped`; the enclosing Pipeline and RunReport become `timeout`.
Ordinary parent cancellation remains `cancelled`.

Timeout errors are not retried. If user code returns after its deadline with a
nil error, Pipeflow checks the context after return and rejects that late
success. The same boundary check applies to custom StageItem implementations.

## Cooperative Limitation

Pipeflow cannot forcibly terminate an executing Go function or goroutine. User
functions should accept or otherwise use `context.Context` when prompt timeout
return is required. An uncooperative function delays the caller until it
returns, after which the scope is still reported as timed out.

## Live State

Before expiry, `Execution.Current()` and `Execution.Report()` show normal
running state and advancing durations. These APIs remain observation-only;
timeout configuration and cancellation control do not flow through snapshots.

## Boundaries

This phase does not add per-attempt timeouts, dynamic deadline mutation,
individual Step cancellation controls, timeout callbacks, persistence,
metrics, or forced goroutine termination.
