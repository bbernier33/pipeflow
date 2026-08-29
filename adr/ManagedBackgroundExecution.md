# ADR-020: Managed Background Execution

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

`Pipeline.WithBackground` configures named support work that runs concurrently
with the main Stage flow. A Background receives only the execution's standard
Go context and returns an error; it has no input, output, or role in business
value flow.

Pipeflow owns the complete worker lifecycle. Configured workers start in
declaration order before Stage execution. On main completion, error,
cancellation, timeout, or fatal Background failure, Pipeflow cancels the shared
execution context and waits for every worker before finalization and cleanup.
Worker goroutines never outlive the completed execution.

`BackgroundFatal` is the zero-value default. Its failure cancels main execution
and returns a named `BackgroundError`. Multiple fatal failures are joined in
declaration order. `BackgroundNonFatal` records failure without cancelling or
failing an otherwise successful Pipeline.

A context cancellation or deadline returned because Pipeflow stopped a worker
after successful main execution is normalized to completed. Parent
cancellation and timeout remain visible. A context error returned while the
worker context was still active is treated as an actual worker failure.

Panics are recovered as `PanicError` and follow the configured fatality policy.
All worker status, timing, failure policy, and error facts are retained in
`RunReport.Backgrounds`. `Execution.Current().Backgrounds` provides read-only
live state. Lifecycle hooks expose Background start and terminal boundaries.

Background callbacks must cooperate with context cancellation. Pipeflow waits
for them but does not forcibly terminate goroutines.

## Ordering

```text
PipelineStarted
BackgroundStarted...
Stage execution
cancel Backgrounds
wait for Background terminal events
Pipeline finalizers
PipelineCompleted | PipelineFailed
PipelineFinalized
```

## Boundaries

This phase does not add detached daemons, dynamic worker registration, worker
restarts, worker result flow, mutable runtime controls, worker-specific retry
policies, warning sinks, or persistence. Those require separate policies or
external integration through lifecycle/report APIs.
