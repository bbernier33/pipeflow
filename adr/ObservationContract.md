# ADR-033: Observation Contract

## Decision

Pipeflow Core exposes an opt-in, in-process `Observer` contract through
`Pipeline.WithObserver`. The contract has three raw families:

- `TraceEvent`: timestamped transitions through Pipeline, Stage, Step, Attempt,
  Parallel, Branch, Subflow, and Background scopes;
- `MetricSample`: raw execution count and duration samples;
- `ProfileSample`: elapsed time attributed to a hierarchy location.

Trace is the source execution fact. Metrics and profile samples are emitted from
the same transition, so Core does not maintain a competing telemetry recorder.
Rates, percentiles, history, health, snapshots, and explanations are derived by
consumers rather than represented as additional Core primitives.

## Safety boundary

Observation is disabled by default. No dispatcher or start-time map is allocated
without an observer. Observer calls are serialized because parallel Pipeflow work
may produce facts concurrently. Observers should return promptly.

An observer panic is recovered and cannot fail, cancel, or otherwise change a
Pipeline run. Telemetry contains execution identity, status, timestamps,
durations, numeric samples, and an error's Go type/panic classification. It does
not contain flowing values, error messages, credentials, or report payloads.

## Deliberate limits

- The contract is in-process; transports, exporters, persistence, and dashboards
  belong outside Core.
- Core emits raw samples, not aggregates such as p95 or retry rate.
- Runtime CPU/memory/GC correlation can be performed by a consumer using event
  timestamps; automatic runtime sampling is not part of v1.2.
- Recovery, Circuit, and Idempotency use explicit observation scopes now that
  those Core++ primitives exist. Their locations contain classifications only,
  never business values, idempotency keys, or error messages.
- Observation is synchronous and failure-isolated. A slow observer can add
  latency, so buffering belongs in an adapter whose loss/backpressure policy is
  explicit.

## Lifecycle hooks versus observers

Lifecycle hooks remain control-sensitive extension points: their panic fails the
owning boundary. Observers are operational evidence consumers and cannot affect
execution outcome. Applications should select the API matching that distinction.
