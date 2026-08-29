# ADR-026: Small Result Metadata

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

Steps accept `WithResultMetadata`, and Stages expose a value-returning
`WithResultMetadata` method. Both accept `func() ResultMetadata` or
`func(T) ResultMetadata` extractors. Step extractors receive the final
successful Step result after retry and polling policies complete. Stage
extractors receive the final Stage output.

An extractor runs exactly once after successful work. It does not alter the
flowing value. Failed and conditionally skipped work does not invoke its
extractor or retain metadata. Extractor input compatibility participates in
static flow validation where possible and uses the existing runtime type check
otherwise. Invalid metadata and recovered extractor panics fail the owning
execution unit through normal structured errors.

`StepReport.Metadata` and `StageReport.Metadata` contain cloned metadata maps.
Report snapshots clone them again, so callers cannot mutate recorder state.
Nested Stage and branch Step reports follow the same rule. Live `State()` and
`Current()` omit metadata and remain lightweight progress views.

## Size and Type Boundary

`ResultMetadata` is `map[string]any` at the API boundary for ergonomic named
facts, but accepted values are deliberately constrained. A map may contain at
most 64 entries. Keys must be non-empty and no longer than 128 bytes. Values
may be nil, strings up to 4096 bytes, booleans, signed or unsigned integers,
floating-point values, `time.Duration`, or `time.Time`.

Slices, arrays, maps, pointers, channels, functions, complex values, and
arbitrary structs are rejected. This prevents the metadata facility from
becoming implicit business-payload retention.

## Separation

- Flow data continues through ordinary Go function returns.
- Result metadata is a small set of operational facts derived from success.
- RunReport remains the structured execution-fact hierarchy and carries the
  metadata alongside status and timing.
- Stage business-result retention remains unimplemented and separate.

## Boundaries

This phase adds no automatic aggregation or inheritance, metadata mutation API,
cross-Step communication, search or indexing, serialization, persistence,
sink, metrics exporter, UI convention, or large retained result mechanism.
