# Circuit Breaker / Dependency Guard

## Decision

Pipeflow models a circuit as an explicit, concurrency-safe object associated
with a logical dependency identity. Applications share the same
`*CircuitBreaker` across every Step and Pipeline that uses that dependency.
There is no hidden global registry.

The state machine is `CLOSED -> OPEN -> HALF_OPEN -> CLOSED`. Qualifying
failures inside a rolling observation window open the circuit. Once the open
duration expires, a bounded number of probes may execute. A qualifying probe
failure reopens the circuit; a successful probe closes it. Completion from a
stale concurrent generation cannot overwrite a newer state transition.

## Failure classification

A failure predicate is mandatory. Pipeflow does not assume that business,
validation, cancellation, or other errors indicate an unhealthy dependency.
Predicate panics use the normal `PanicError` policy.

The breaker admits and observes one complete Step execution. Normal retries
and Operational Recovery happen inside that admission. Only the terminal
outcome is classified, preventing individual retry attempts from inflating the
shared failure count. An open-circuit rejection never invokes the Step or its
Recovery Stage.

## Observation and concurrency

`CircuitBreaker.Snapshot` provides read-only live state across runs.
`StepReport.Circuit` records dependency identity, state before and after the
execution, whether it was a half-open probe, and whether it was short-circuited.
Neither surface retains business payloads.

State is in-process and mutex protected. It is intentionally not durable or
distributed. The application owns the lifetime and sharing topology of each
breaker.

## Deferred

Distributed state, persistence, automatic dependency registries, exporters,
manual state mutation, and Idempotency Guard are outside this slice.
