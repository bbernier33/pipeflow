# Pipeflow v1 API Stability

V3.0.0 ships under `github.com/bbernier33/pipeflow/v3`. The document below records
the historical v1 compatibility contract. The `/v3` migration is an explicit Go
module/type boundary; see [MIGRATING_V3.md](MIGRATING_V3.md). Existing Core
execution semantics are retained in v3, with the additive resilience,
observation, and readiness capabilities described in [RELEASE_NOTES.md](RELEASE_NOTES.md).

Pipeflow Core's public API is frozen for v1 as of Phase 23. The supported
surface is the exported Go API documented by `go doc`, with the compatibility
exceptions below.

## Primary v1 API

- composition: `NewPipeline`, `NewStage`, `NewStep`, `NewParallel`,
  `NewBranch`, `NewSubflow`, and `NewConcurrentSteps`;
- execution: `Pipeline.Run`, `RunWithReport`, and `Start`, plus the read-only
  `Execution` handle;
- shared execution data: `Context.Set`, `Get`, and `Logger`;
- policies: Step options, Pipeline/Stage timeouts, retries, polling,
  conditions, rate limiting, failure policies, managed backgrounds, and
  finalizers;
- observation: lifecycle hooks, structured errors, reports, live state,
  result metadata, validation, and definition introspection;
- extension: `StageItem`, `Logger`, `RetryAfterError`, and callback types.

Minor v1 releases may add exported identifiers or optional struct fields but
will not remove or rename exported identifiers, change existing signatures,
reinterpret existing constants, or change documented execution semantics.
Security and correctness fixes may make invalid configurations fail earlier.

## Compatibility API

The original `PipelineHook`, `PipelineEvent`, and `Pipeline.OnStarted`,
`OnCompleted`, and `OnFailed` APIs remain callable in v1 but are deprecated in
favor of `WithLifecycleHook`.

Execution status and timing methods on `Context` also remain callable but are
deprecated. `Execution.Current`, `Execution.State`, `Execution.Report`, and
`RunReport` are the concurrency-safe observation APIs. `Context.Set`, `Get`,
and `Logger` remain primary APIs.

`Pipeline.Run`, `RunWithReport`, and `Start` retain the compatibility argument
form `(*Context, input)` in addition to zero or one ordinary input. New code
should omit Pipeflow Context unless shared run-wide data or a custom logger is
needed.

## Explicitly not frozen

Unexported implementation details, report formatting text beyond documented
error identity/location, log message wording, scheduling order between
concurrent siblings, and elapsed durations are not API contracts.

Future domain packages, adapters, serialization, exporters, persistence, UI,
and workflow/DAG facilities are outside the Core v1 freeze.
