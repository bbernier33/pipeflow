# Readiness Flow Pressure and Backpressure

## Status

Accepted for the seventh v3.x Production Readiness increment.

## Decision

`readiness.PressurePlan` drives the real Pipeflow Worker runtime with a finite
item set and its configured worker, queue buffer, and max-in-flight bounds. It
does not emulate a second queue or scheduler.

Submission latency is measured against an explicit backpressure threshold while
a read-only sampler records maximum queue depth, in-flight work, and active
workers. Intake is then closed and every accepted Work handle is joined before
Worker shutdown. Completion order is derived from RunReport end times.

Each Work output exists only long enough to evaluate the existing Scenario
expectations. Retained evidence includes the full payload-free RunReport,
submission latency/error, counts, bounds, completion order, drain state, and
final Worker status.

Built-in expectations cover observed blocking, graceful drain, queue bounds,
and preserved completion order. Submission timeout and input errors are reported
as rejected work; accepted work is never silently discarded.

## Boundaries

Single-worker ordering is expected naturally. Concurrent completion remains
unordered unless an application deliberately implements ordering. Lossy
backpressure, durable queues, distributed producers, Stream windows, and
protocol-specific disconnect/reconnect behavior remain outside this increment.
