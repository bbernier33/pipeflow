# ADR-029: Unified Panic Recovery

Date: 2026-08-29

## Decision

Every invocation of application code owned by Pipeflow uses one failure rule:

```text
panic -> recover -> PanicError(value, stack) -> structured location -> normal failure policy
```

The boundary includes Step actions, conditions, polling predicates, retry
filters and retry-after callbacks, metadata extractors, custom `StageItem`
implementations, lifecycle and legacy Pipeline hooks, logger calls, managed
background functions, and finalizers.

`PanicError` contains the original panic value and the stack captured where the
panic crosses the Pipeflow boundary. The engine attaches its normal
`ExecutionError`, `BackgroundError`, or `CleanupError` wrapper. Consequently
callers can inspect both the panic and its Pipeline/Stage/Step/Attempt/Poll or
operational location with `errors.As`.

Panic recovery does not introduce a separate policy system. A recovered panic
is handled like an error at that boundary: Step retry policy may retry it,
fail-fast policy cancels sibling work, background fatality policy remains in
force, and cancellation/timeout status continues to use the existing rules.
Lifecycle callback panics fail the execution because callback delivery is part
of the synchronous boundary contract.

All registered finalizers execute after work stops even when work or an
observer panics. Finalizer panics become named cleanup failures and do not stop
the remaining finalizers. A panic from the final lifecycle notification is
also reported, although cleanup has already completed by that point.

Reports store errors and execution facts only; they do not retain business
payloads.

## Boundaries

Pipeflow can recover only panics that cross a goroutine stack it owns. If an
application callback launches its own goroutine, that application owns panic
handling inside the goroutine. This phase adds no configurable panic modes,
serialization, sinks, exporters, persistence, UI behavior, or workflow/DAG
semantics.
