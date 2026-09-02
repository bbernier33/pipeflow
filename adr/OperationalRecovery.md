# Operational Recovery

## Decision

Pipeflow v2 begins with opt-in, Step-owned Operational Recovery. A Recovery
Stage receives a metadata-only `Failure`, runs through normal Stage/Step
machinery, and must finish by returning one of four `RecoveryDecision` values.

`RecoveryRetryStep` starts a fresh normal execution of the owning Step. The
original Step remains the only producer of business output. Recovery cannot
manufacture or inject a replacement value.

Recovery activations are bounded by `RecoveryPolicy.MaxAttempts`; each
activation may also have a timeout. Normal Step retries are exhausted before
Recovery runs. A recovered retry receives a fresh normal retry and Step-timeout
budget. Parent cancellation is terminal and never starts Recovery.

## Reporting and errors

`StepReport.Recoveries` preserves the original failure, decision, timing, and
nested Stage/Step reports without retaining input or output values.
`RecoveryError` preserves ordinary Go error identity through `errors.Is` and
`errors.As`. Lifecycle and observation events identify Recovery without
carrying business data.

`RecoveryFailStep`, `RecoveryFailStage`, and `RecoveryFailPipeline` are distinct
control decisions and remain inspectable. In the current finite sequential
engine all three terminate the run because there is no continue-after-failure
policy. This release does not add fallback, DAG, or workflow semantics.

## Concurrency

Recovery is run-scoped. Concurrent failures may run concurrent Recovery
Stages. Pipeflow protects its reports and observation dispatch, but application
objects mutated by Recovery must follow normal Go synchronization rules. Shared
single-flight recovery is intentionally deferred.

## Deferred

Circuit breakers, idempotency guards, automatic payload access, automatic
failure merging, persistence, exporters, and distributed coordination are not
part of this slice.
