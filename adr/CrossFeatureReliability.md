# ADR-030: Cross-feature Reliability Matrix

Date: 2026-08-29

## Decision

Phase 22 adds no execution capability or public API. It establishes
cross-feature tests for the seams most likely to fail as Pipeflow's production
core grows:

- retry attempts waiting on a rate limiter remain bounded by Step timeout;
- a panic in a fail-fast Parallel branch is structured, cancels siblings, and
  is not returned before sibling shutdown;
- parent cancellation stops managed Background work before finalizers execute;
- an unknown custom `StageItem` output is checked at the conditional Subflow's
  runtime type boundary with its nested location preserved;
- `RetryAfter` delay inside polling remains cancellation-aware and produces a
  completed first Poll plus a timed-out second Poll;
- Parallel Branch Context maps remain isolated while fail-fast cancellation is
  propagated;
- panic from asynchronous `Start` work closes `Done` and surfaces consistently
  from `Wait`, `Status`, and report snapshots.

The tests assert output/error semantics, structured locations, terminal state,
nested reports, cleanup ordering, and cancellation observation. Timing checks
use cancellation-driven synchronization instead of narrow elapsed-time bounds
where practical.

## Boundaries

This phase does not add policies, callbacks, execution controls, persistence,
serialization, exporters, metrics, UI behavior, DAG semantics, or payloads in
reports. Runtime changes are permitted only when an interaction test exposes a
violation of an already documented contract.
