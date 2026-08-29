# ADR-012: Run-Scoped Live Execution State

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

Phase 4 reuses Phase 3 timing and reporting rather than introducing another
state store. `Pipeline.Start` returns `*Execution`, a run-scoped handle for an
asynchronously executing Pipeline.

The handle exposes read-only inspection:

```go
Status() Status
Current() CurrentExecution
Report() RunReport
Done() <-chan struct{}
Wait() (output any, report RunReport, err error)
```

`Report` returns a deep snapshot. Durations for running Runs, Stages, Steps, and
Attempts are calculated at snapshot time and therefore advance while work is
active. Mutating a returned snapshot cannot mutate live execution state.

`Current` contains every active Stage and Step. It is intentionally plural at
each level because concurrent execution can have multiple active Steps. An
active Step includes its current attempt number and attempt duration.

Configuration, argument, and setup failures are returned synchronously from
`Start`; no Execution is created. Once Start succeeds, execution outcomes are
obtained through `Wait` or the final Report. `Done` closes after the final
result is safely published.

Pipeline carries no mutable global run state. Every Start call receives an
independent recorder and completion handle. Concurrent snapshot reads are
serialized by the run-local recorder mutex.

## Boundaries

The caller owns cancellation through `context.Context`. This phase does not add
forced termination, report persistence, serialization, metrics exporters, UI,
HTTP endpoints, stuck-step policy, or Phase 12 background-task orchestration.
