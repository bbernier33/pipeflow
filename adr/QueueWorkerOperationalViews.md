# Queue and Worker Operational Views

## Decision

`obs.Collector.TrackWorker` explicitly registers a named Core `Worker` for
read-only operational snapshots. Registration returns an idempotent removal
function. Observation does not discover workers globally and neither Pipeline
nor Worker gains mutable observation state.

Each `Collector.Snapshot` reads the Worker's existing concurrency-safe,
payload-free `State` and derives two views:

- Worker: Pipeline, lifecycle status, configured concurrency, active and
  in-flight counts, and submitted/completed/failed counters.
- Queue: depth, buffer capacity, max-in-flight bound, in-flight count, and
  current buffer utilization.

Queue utilization is `depth / buffer capacity`. It is zero for an unbuffered
Worker because there is no storage capacity to occupy. Queue depth is a
point-in-time value, not a durable accounting record.

## Boundaries

The views never expose queued inputs or completed outputs. They cannot submit,
cancel, close, resize, or otherwise control a Worker. Tracking starts and ends
only through the explicit registration API; no goroutine or polling lifecycle
is created by Observation.

This slice covers Pipeflow's in-process Worker runtime. Generic queue-provider
adapters, broker lag/partition concepts, durable history, alerting, and remote
control require separate contracts and remain deferred.
