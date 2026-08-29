# ADR-018: Context Concurrency Hardening

- **Status:** Accepted
- **Date:** 2026-08-26

## Decision

Pipeflow `Context` remains optional shared execution-wide state. Its values map,
logger reference, status, timing, and compatibility location fields are all
accessed under synchronization.

`Set` and `Get` protect the map container. Values are stored and returned as-is;
Pipeflow does not copy arbitrary Go values, so callers must synchronize mutable
objects placed in the Context. Compound read-modify-write operations are not
made atomic by separate `Get` and `Set` calls.

Logger calls made through a Context are serialized by a wrapper. This protects
non-thread-safe logger implementations from concurrent Pipeflow calls without
changing the public `Logger` interface.

A supplied Context can belong to only one running Pipeline execution.
Overlapping reuse fails before execution with `ErrContextInUse` and a zero
report. The Context can be reused after the first run, including after cleanup
has finished. Shared values persist across sequential reuse; lifecycle status,
timing, and compatibility location fields are reset.

`CurrentStage` and `CurrentStep` remain for source compatibility but are
deprecated. `CurrentStep` cannot faithfully represent `ConcurrentSteps`, and
neither scalar is the canonical observation API. `Execution.Current()` is the
run-scoped structured source of active Stage and Step state.

## Concurrency Contract

- Concurrent `Set`, `Get`, and metadata reads do not race.
- Concurrent Steps may share one Context within a run.
- Calls through `Context.Logger()` are serialized.
- One Context cannot represent overlapping runs.
- Configuration and control remain outside Context.

## Boundaries

This phase does not add atomic mutation helpers, typed Context keys, deep value
copying, per-Step child contexts, or mutable execution controls. Phase 13 adds
shallow value-map snapshots specifically for structured Parallel Branches.
