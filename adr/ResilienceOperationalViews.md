# Resilience Operational Views

## Decision

The Core observation contract includes explicit `recovery`, `circuit`, and
`idempotency` scopes. Their locations carry only operational classifications:
Recovery name/attempt/decision, Circuit dependency/state/probe disposition,
and Idempotency guard/outcome. Stable idempotency keys, business values, and
error messages are never emitted.

`obs.Collector` derives three cumulative, read-only views:

- Recovery activations, completion/failure counts, and decision totals grouped
  by Pipeline, Stage, Step, and Recovery name.
- Circuit event, failure, probe, and short-circuit counts grouped by logical
  dependency, plus the latest observed state.
- Idempotency execution, duplicate, releasable-failure, and store-failure
  counts grouped by guard name.

These are derived views over raw observation evidence. They do not participate
in Retry, Recovery, Circuit, or Idempotency decisions and cannot mutate their
state. Observation failures remain isolated from execution.

## Limitations

Counts are process-local and cover only evidence received by the Collector.
The latest Circuit state is the latest observed execution outcome, not a
replacement for `CircuitBreaker.Snapshot`. Persistent history, rates/windows,
alerts, cross-process aggregation, and remote control remain deferred.
