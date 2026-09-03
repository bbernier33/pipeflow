# Runtime Resource Correlation

## Decision

The Observation collector retains a bounded timeline of `ResourceSample`
values alongside Trace, Metric, and Profile evidence. `CaptureRuntime` records
one synchronous standard-library Go runtime sample. `RecordResource` accepts a
timestamped sample supplied by an application or external sampler.

Standard samples include goroutine and cgo-call counts; heap allocation,
in-use, object, stack, allocation/free, and next-GC values; GC cycle and pause
information; and GC CPU fraction. The collector derives the latest value and
changes across its retained window. Samples are ordered by timestamp and the
oldest samples are discarded at `ResourceCapacity`.

`CorrelateResources` is a pure operation that associates each Trace event with
the newest sample at or before the event, subject to an explicit freshness
limit. This is temporal evidence only. It does not assert that a Pipeline,
Stage, or Step caused a memory, goroutine, or GC change.

## Lifecycle and cost boundary

Sampling is caller-scheduled. Observation creates no ticker or goroutine and
does not sample inside Pipeflow execution automatically. Applications should
choose a cadence appropriate for their overhead and diagnostic needs rather
than calling `runtime.ReadMemStats` on every execution event.

The standard library does not expose reliable process/system CPU percentage,
resident-set size, or host pressure. Pipeflow does not fabricate these values;
platform or provider adapters can record them in a later slice. Persistent
History naturally stores resource samples present in each snapshot.
