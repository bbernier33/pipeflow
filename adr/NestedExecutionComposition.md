# ADR-023: Nested Execution Composition

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

Pipeflow provides `NewSubflow(name, stages...)` for reusable, named sequential
Stage groups. A Subflow is a `StageItem`: its input enters its first Stage,
nested Stage outputs flow in declaration order, and its final output becomes
the containing Stage's next value.

A Subflow is part of its parent execution. It shares the Run ID, Pipeflow
Context, Go context, cancellation, lifecycle dispatcher, and value stream. It
does not invoke `Pipeline.Run`, create a child execution handle, or establish a
separate execution-wide state boundary. Definitions are immutable after
construction and may be reused safely across runs or positions.

Static value-flow validation crosses both Subflow boundaries. Runtime errors
retain their nested Stage and Step location and add the containing Subflow
name. Cancellation, timeout, panic recovery, retries, polling, conditions,
Parallel groups, and ConcurrentSteps retain their existing semantics inside
nested Stages.

## Observation

`StageReport.Subflows` contains declaration-ordered `SubflowReport` values.
Each report records name, status, timing, structured error, and its nested
Stage reports. This hierarchy is available in live report snapshots and is
recursively cloned so callers cannot mutate recorder state.

`Execution.Current()` exposes running Subflows and active nested Stages,
Steps, Parallels, Branches, and further Subflows. It remains read-only.

Lifecycle observation adds `SubflowStarted`, `SubflowCompleted`, and
`SubflowFailed`. Events generated inside a Subflow identify the immediate
containing Subflow through `LifecycleEvent.Subflow`.

## Boundaries

Subflows do not own backgrounds, finalizers, Pipeline hooks, independent
timeouts, run IDs, execution handles, or configuration inheritance. This phase
adds no arbitrary dependency edges, implicit fan-out, DAG scheduling, dynamic
graph mutation, child-run cancellation API, or automatic data merge. Explicit
existing composition primitives remain the only source of concurrency.
