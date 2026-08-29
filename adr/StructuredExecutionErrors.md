# ADR-010: Structured Execution Errors

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

Errors produced during execution are represented by `*ExecutionError`:

```go
type ExecutionError struct {
    Pipeline string
    Stage    string
    Step     string
    Attempt  int
    Err      error
}
```

Each execution boundary adds the location it owns. A Step records its name and
attempt, its Stage adds the stage name, and its Pipeline adds the pipeline name.
Cancellation at a broader boundary contains only the location known there.

`ExecutionError` implements `error` and `Unwrap() error`. Callers use
`errors.Is` for causes and `errors.As` for structured details. Direct equality
with the original business error is not guaranteed after wrapping.

Retry exhaustion reports the final attempted execution number. Cancellation
before an attempt or during its retry delay reports the attempt at which
cancellation was observed.

For `WaitAll`, every joined child error is independently enriched. Declaration
order remains intact, and `errors.Is` can find every underlying cause.

Pipeline failure hooks receive the same fully enriched error returned by
`Pipeline.Run`.

## Boundaries

Pre-run API, configuration, and validation errors are plain errors because no
execution location or attempt exists. This phase does not add error codes,
serialization, RunReport retention, panic recovery, or execution history.
