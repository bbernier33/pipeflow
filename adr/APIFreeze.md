# ADR-031: Core v1 API Freeze

Date: 2026-08-29

## Decision

Freeze the current exported Pipeflow Core API for v1 after documenting its
primary and compatibility surfaces. Phase 23 introduces no new execution
feature and does not redesign working signatures.

Ordinary Go functions remain the default Step API. Pipeline, Stage, Step,
Parallel, Branch, Subflow, and ConcurrentSteps remain composition primitives,
not graph nodes. `Execution` and payload-free reports remain the observation
surface; Context remains the shared-data surface.

The old Pipeline-only hooks and Context execution-status accessors are retained
and marked deprecated. Removing them immediately would create compatibility
cost without simplifying the execution engine. New code uses lifecycle hooks
and run-scoped Execution/report snapshots.

An external-package compile contract exercises the intended public
constructors, methods, callback interfaces, policy types, reports, states,
errors, and constants. This complements behavioral tests: compilation detects
signature drift while existing tests protect semantics.

## Compatibility rule

Within v1, exported identifiers and existing signatures are not removed,
renamed, or incompatibly changed. Additive APIs remain possible when necessary,
but Core growth should prefer separate packages and composition. Invalid input
may be rejected earlier when required for safety or correctness.

## Boundaries

This phase does not promise stable internals, exact concurrent scheduling, log
wording, timing values, or formatting beyond documented error identity and
location. It adds no serialization, exporters, persistence, metrics, UI,
transport, domain toolbox, or DAG/workflow behavior.
