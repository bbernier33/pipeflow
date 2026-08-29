# ADR-017: Finalization and Cleanup

- **Status:** Accepted
- **Date:** 2026-08-26

## Decision

`Pipeline.Finally(name, finalizer)` registers a named cleanup callback. A
Pipeline value copies its finalizer configuration, and callbacks execute once
per run in reverse registration order, matching Go's `defer` ordering.

Finalizers run after Pipeline work stops but before the recorder, legacy
Pipeline hooks, and generic terminal lifecycle events receive the final
outcome. They run for success, error, cancellation, timeout, and recovered
panics. Every registered finalizer runs even when another finalizer fails or
panics.

The callback receives a context produced with `context.WithoutCancel` from the
execution context. Values are preserved while the execution cancellation and
deadline are removed, allowing cleanup after an expired timeout. Because Core
does not impose a universal cleanup timeout, callbacks that perform external
work should derive their own bounded context.

`Finalization` is an immutable view of the pre-cleanup Run ID, Pipeline name,
status, and error. All callbacks receive the same view; one callback cannot
change what later callbacks observe.

A returned cleanup error is wrapped in a named `CleanupError`. Execution and
cleanup errors are combined with `errors.Join`, so each cause remains
discoverable through `errors.Is` and `errors.As`. Cleanup failure changes a
successful run to failed and suppresses its otherwise successful output.

`RunReport.Cleanups` records finalizers in execution order with name, status,
start/end time, duration, and error. It retains no resource or business
payload. Pipeline duration includes cleanup time.

Panics in Step actions, retry predicates, and polling predicates are recovered
as `PanicError` with a stack trace. Finalizer panics are likewise recovered,
wrapped as cleanup errors, and do not skip remaining cleanup. Lifecycle hooks
remain required to be non-panicking; hook panic recovery is not a stable part
of this contract.

## Boundaries

This phase does not add dynamic cleanup registration, resource ownership
graphs, asynchronous cleanup, global cleanup workers, cleanup retry policies,
default cleanup deadlines, persistence, or external cleanup orchestration.
