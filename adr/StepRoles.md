# ADR-034: Source and Sink Step Roles

## Decision

Pipeflow adds three semantic `StepRole` values: `normal`, `source`, and `sink`.
`NewSourceStep` and `NewSinkStep` construct the same concrete `*Step` used by
`NewStep`; they do not introduce another execution path, function adapter, or
hidden hierarchy.

Roles appear in `Describe`, `StepReport`, live `Execution` state, lifecycle
events, observation locations, and effective configuration. YAML may assign a
role to a normal Step. A role selected by a Go role constructor wins over YAML,
following the existing configuration precedence rules.

Roles do not constrain function signatures or alter value flow. In particular,
a sink using `func(T) error` retains the existing pass-through semantics, and
the same ordinary function remains reusable under any role.

## Queue boundary

The v1.3 roadmap discusses source/sink buffers, workers, batching,
backpressure, ordering, and drain behavior. Those settings require a lifecycle
that owns multiple values. A finite Step invocation owns one input/output and
cannot honestly apply queue backpressure or drain semantics.

Therefore v1.3 does not expose inert queue settings or a visible `QueueStep`.
The internal queue and its bounded-blocking default will arrive with v1.4
Worker/Stream execution, where cancellation, closure, failure, ordering, and
drain ownership can be specified and tested end to end. The role metadata added
here is the stable selection point for those future role-oriented defaults.

## Consequences

- Existing Steps retain identical behavior and default to `normal`.
- Source/Sink improve readability and operational classification immediately.
- Protocol and storage behavior remains application/toolbox code.
- v1.4 can add queue policy around the existing Step boundary without changing
  business function signatures or creating another engine.
