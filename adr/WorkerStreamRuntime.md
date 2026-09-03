# ADR-035: Worker and Stream Runtime

## Decision

Pipeflow v1.4 adds two run-scoped, in-memory runtimes around the unchanged finite
Pipeline engine:

- `Pipeline.StartWorker` accepts explicit submissions. Each accepted `Work`
  executes `RunWithReport` and owns a unique Run ID and RunReport.
- `Pipeline.StartStream` reads values from an application/adapter channel,
  submits them through the same Worker machinery, and publishes transient
  `StreamResult` values.

Normal Step functions are unchanged across finite, Worker, and Stream use.
Worker/Stream are lifecycle profiles, not alternate Step engines.

## Bounds and backpressure

`Workers` bounds active Pipeline runs, `Buffer` bounds queued work, and optional
`MaxInFlight` bounds accepted active-plus-queued work. Submission blocks when a
bound is reached. There is no lossy policy in v1.4 and no silent drop behavior.

Queues are process memory only. Pipeflow makes no durability, replay,
at-least-once, or exactly-once promise. Durable ingress and checkpointing belong
to adapters/external infrastructure.

## Shutdown

`Close` stops intake and drains accepted work. `Cancel` cancels active work and
completes queued handles with cancellation. Parent cancellation has the same
immediate-cancellation behavior. All accepted Work handles reach completion.

## Failure domains

One Worker run failure is returned by `Work.Wait` and does not stop the Worker.
Stream defaults to `StreamContinue`; `StreamStop` cancels the runtime after an
item failure. Runtime validation/cancellation is distinct from item failure and
is returned by runtime `Wait`.

## Ordering and payload boundary

One Stream worker publishes results in input order. Multiple workers publish in
completion order and make no ordering guarantee. Advanced ordered concurrency,
batching, and windows are deferred because each needs explicit latency/memory
semantics rather than a cosmetic setting.

RunReport and runtime state remain payload-free. `Work.Wait` and `StreamResult`
return business output only to the caller that explicitly owns that result.

## Source and Sink relationship

Input channels are supplied by application/toolbox adapters. Source and Sink
roles remain semantic Step boundary metadata. Protocol connection management,
acknowledgement, durable queueing, and storage-specific batching remain outside
Core.
