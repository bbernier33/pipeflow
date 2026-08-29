# ADR-025: Complete Live Progress State

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

Phase 4 introduced `Execution.Current()` and live `RunReport` snapshots early
because later concurrency, timeout, polling, background, and Subflow work
needed observation. `Current()` intentionally contains only active work. That
projection cannot by itself render a full operational view containing
completed, running, and pending siblings.

## Decision

`Execution.State()` returns an `ExecutionState` snapshot containing Run ID,
Pipeline name/status/duration, backgrounds, and the complete declared execution
hierarchy in declaration order. Stage state recursively includes Steps,
Parallels, Branches, and Subflows.

Each unit contains only identity, status, and duration. A running Step also
contains its active poll number, attempt number, and attempt duration. Once an
attempt or poll finishes it is no longer represented as active; full history
remains the responsibility of `Execution.Report()`.

`State()` is derived from the existing mutex-protected run recorder. It creates
new slices and values for every call, adds no second mutable state store, and
never retains or exposes flowing business payloads.

The observation APIs therefore have distinct roles:

- `Status()` returns one run status.
- `Current()` returns active work only.
- `State()` returns the complete lightweight execution topology.
- `Report()` returns detailed execution facts and history.

## Boundaries

Live state remains read-only and run-scoped. It cannot change worker counts,
retry policy, rate limits, deadlines, or individual Step cancellation. This
phase adds no polling endpoint, push subscription, serialization, persistence,
metrics exporter, UI component, stuck-step detector, or orchestration control
protocol.
