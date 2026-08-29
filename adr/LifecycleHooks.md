# ADR-016: Lifecycle Events and Hooks

- **Status:** Accepted
- **Date:** 2026-08-26

## Decision

`Pipeline.WithLifecycleHook` registers a generic `LifecycleHook` that observes
Pipeline, Stage, and Step boundaries for each run. `LifecycleEvent` contains
only execution facts: event type, Run ID, Pipeline/Stage/Step identity, status,
occurrence time, and an optional error. Flowing business values and execution
control are intentionally absent.

The stable event vocabulary is:

```text
PipelineStarted
  StageStarted
    StepStarted
    StepCompleted | StepFailed
  StageCompleted | StageFailed
PipelineCompleted | PipelineFailed
PipelineFinalized
```

Hooks are synchronous and execute in registration order for an individual
event. The engine does not advance past a boundary until its callbacks return.
Concurrent Steps can invoke the same hook concurrently. Ordering across sibling
Steps is therefore unspecified, but every Step's start event happens before its
own terminal event. Pipeline and Stage terminal events happen after all child
terminal events.

Callbacks have no error return and cannot replace the flowing business value.
As of Phase 21, hook panics are recovered as `PanicError` values and fail the
owning execution boundary. Hook implementations are still expected to be
brief, concurrency-safe, and non-panicking.

`PipelineFinalized` is emitted after the Pipeline terminal event on every
ordinary engine return, including failures, cancellation, and timeout. It is a
notification, not a cleanup facility. Registered finalizers are the cleanup
mechanism.

The existing `OnStarted`, `OnCompleted`, and `OnFailed` Pipeline callbacks are
preserved. Generic Pipeline boundary events run before their corresponding
legacy callback, and `PipelineFinalized` runs after it.

Lifecycle hook configuration is copied by `WithLifecycleHook`; applications
should finish configuring a Pipeline before starting concurrent executions.

## Boundaries

This phase does not add asynchronous delivery, event buffering, serialization,
sinks, metrics exporters, persistence, cleanup registration, retries for hook
delivery, or hook-driven cancellation and mutation.
