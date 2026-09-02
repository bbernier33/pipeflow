# Observation Tool Foundation

## Decision

The Observation / Operations Tool begins as the separate `pipeflow/obs`
package. Core does not import it. `obs.Collector` implements the existing
`pipeflow.Observer` contract, and `obs.Source` exposes immutable operational
snapshots to future transports and clients.

This first slice uses an in-process boundary. It derives:

- current and recently completed run views;
- explainable pipeline health from observed run evidence;
- errors grouped by Pipeline, Stage, Step, and error class;
- metric counts and sums grouped by structural location;
- cumulative and maximum profile time by structural location;
- bounded recent Trace, Metric, and Profile evidence.

The collector is safe for concurrent use by many Pipelines and applications.
Snapshots are detached copies and cannot affect collection or execution.

## Retention and data boundary

Raw evidence and completed run history are bounded in memory. Aggregate views
retain structural dimensions observed during the collector's lifetime. The
collector stores only the payload-free Core telemetry types: names, IDs,
statuses, timestamps, durations, counts, and error classifications. It never
receives flowing values or error messages.

Health is deliberately conservative in this slice: active work is `BUSY`, any
observed failed run is `DEGRADED`, successful history without failures is
`HEALTHY`, and insufficient evidence is `UNKNOWN`. More sophisticated
time-window policies belong to a later derived-analysis slice.

## Deferred

HTTP/WebSocket/TCP transport, authentication, persistent history, runtime
resource sampling, queue/worker adapters, resilience-specific views, effective
configuration, explain analysis, TUI, multi-process discovery, Prometheus, and
OpenTelemetry exporters are not included here.
