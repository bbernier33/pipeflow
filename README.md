# Pipeflow

Pipeflow is a lightweight, composable pipeline execution library for Go.

It provides reusable execution primitives for structuring backend processing as pipelines, stages, and steps while keeping execution concerns separate from business logic.

Pipeflow Core v1.0 is stable and intended for reuse across backend services,
data processing, IoT, realtime applications, game backends, and other Go
workloads.

Additive v2 Core++ resilience and Observation tooling are implemented on the
development branch, and the first v3 Production Readiness increment is now in
progress. These additions are unreleased; the v1 API remains frozen.

## Project Status

Pipeflow Core v1.0.0 is released. Its public execution API and documented
semantics are frozen for the v1 release line. See
[API_STABILITY.md](API_STABILITY.md) for compatibility guarantees and the
small retained deprecated surface.

The Core is production-oriented: cancellation, timeouts, retries, polling,
rate limits, structured concurrency, cleanup, panic recovery, live state, and
payload-free reports have race and cross-feature interaction coverage.

Contributors working on concurrency should run `go test -race ./...`. Windows
setup and the validated MSYS2 UCRT64 command are documented in
[Windows Race Testing](adr/RaceTestingWindows.md).

## Installation

```bash
go get github.com/bbernier33/pipeflow@v1.0.0
```

## Motivation

Many backend applications eventually need the same execution infrastructure:

```text
Input
  │
  ▼
Gather
  │
  ▼
Normalize
  │
  ▼
Process
  │
  ▼
Store
  │
  ▼
Output
```

Without a shared execution layer, projects repeatedly implement:

- execution ordering
- lifecycle tracking
- error propagation
- cancellation
- retries
- concurrency
- logging
- timeouts
- execution policies
- observability hooks

Pipeflow aims to provide these capabilities as reusable Go primitives so applications can focus on their domain logic.

## Operational Recovery (v2 development)

Operational Recovery runs only after a Step's normal retry/polling behavior is
exhausted. It receives failure metadata, repairs execution conditions, and
returns an explicit control decision. It never replaces the flowing business
value.

```go
fetch := pipeflow.NewStep("fetch", fetchOrders).WithRecovery(
    pipeflow.NewRecoveryStage("refresh credentials",
        pipeflow.NewStep("refresh", func(f pipeflow.Failure) error {
            return refreshCredentials(f.Err)
        }),
        pipeflow.NewStep("decide", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) {
            return pipeflow.RecoveryRetryStep, nil
        }),
    ),
    pipeflow.RecoveryPolicy{MaxAttempts: 1, Timeout: 30 * time.Second},
)
```

Recovery is bounded and run-scoped. Concurrent recoveries do not single-flight;
shared application state must use normal Go synchronization. See
[Operational Recovery](adr/OperationalRecovery.md).

### Circuit Breaker / Dependency Guard

Share an explicit breaker wherever Steps call the same logical dependency:

```go
provider, err := pipeflow.NewCircuitBreaker("orders-api", pipeflow.CircuitBreakerPolicy{
    FailureThreshold:  5,
    ObservationWindow: time.Minute,
    OpenDuration:      30 * time.Second,
    HalfOpenMaxProbes: 1,
    IsFailure: func(err error) bool {
        return errors.Is(err, ErrProviderUnavailable)
    },
})

fetch.WithCircuitBreaker(provider)
update.WithCircuitBreaker(provider)
```

The predicate is required: Pipeflow never assumes ordinary business errors
mean that a dependency is unhealthy. `provider.Snapshot()` exposes read-only
live state, and each protected Step records a payload-free circuit report. See
[Circuit Breaker](adr/CircuitBreaker.md).

### Idempotency Guard

Idempotency Guard prevents a completed side effect from being repeated when
the application supplies a stable key and an atomic store:

```go
guard, err := pipeflow.NewIdempotencyGuard(
    "payments",
    paymentStore,
    func(payment Payment) string { return payment.ID },
)

charge := pipeflow.NewStep("charge", func(payment Payment) error {
    return gateway.Charge(payment)
}).WithIdempotencyGuard(guard)
```

Guarded Steps must be pass-through functions. A duplicate-completed execution
is skipped and its input continues flowing. One claim surrounds retries and
Recovery. The built-in memory store is process-local; durable or distributed
guarantees come from application-provided stores. Pipeflow does not promise
exactly-once execution. See [Idempotency Guard](adr/IdempotencyGuard.md).

## Observation / Operations Tool (v2.x development)

The separate `obs` package consumes Pipeflow telemetry without participating
in execution:

```go
collector := obs.NewCollector(obs.Options{
    TraceCapacity:    5_000,
    RunCapacity:      500,
    ResourceCapacity: 1_000,
})

pipeline = pipeline.WithObserver(collector)
snapshot := collector.Snapshot()
```

Long-lived Core workers can be added explicitly to the same read-only view:

```go
worker, err := pipeline.StartWorker(ctx, pipeflow.WorkerOptions{
    Workers: 4,
    Buffer:  100,
})
if err != nil {
    log.Fatal(err)
}
untrack, err := collector.TrackWorker("orders-primary", worker)
if err != nil {
    log.Fatal(err)
}
defer untrack()
```

`Snapshot().Workers` reports status, concurrency, active/in-flight work, and
outcome counters. `Snapshot().Queues` reports depth, capacity, utilization,
and max-in-flight state. These views contain no queued values and provide no
control over the Worker.

Resilience telemetry is also derived automatically from observed executions:

```go
snapshot := collector.Snapshot()
for _, circuit := range snapshot.Circuits {
    log.Printf("dependency=%s state=%s short_circuits=%d",
        circuit.Dependency, circuit.State, circuit.ShortCircuits)
}
```

`Snapshot().Recoveries` summarizes activations, failures, and decisions;
`Snapshot().Circuits` summarizes dependency state, failures, probes, and
short-circuits; `Snapshot().Idempotency` summarizes executions, duplicates,
releasable failures, and store failures. Stable keys and business values are
never emitted.

Pipeline topology and resolved execution policy can be registered explicitly:

```go
untrackPipeline, err := collector.TrackPipeline(configuredPipeline)
if err != nil {
    log.Fatal(err)
}
defer untrackPipeline()

definition := collector.Snapshot().Definitions[0]
```

Each definition contains the callback-free `Describe()` tree. Pipelines built
with `WithConfig` also contain their detached `EffectiveConfig()` values and
winning configuration sources; unconfigured Pipelines expose topology with a
nil effective configuration. This surface is observation-only and cannot
change an active Pipeline.

Derived analysis remains a pure view over a snapshot:

```go
analysis, err := obs.Analyze(collector.Snapshot(), obs.AnalysisOptions{})
if err != nil {
    log.Fatal(err)
}
if analysis.PrimaryBottleneck != nil {
    log.Printf("%s: %s",
        analysis.PrimaryBottleneck.Code,
        analysis.PrimaryBottleneck.Summary)
}
```

Findings carry stable codes, severity, structural location, and numeric
evidence. Default rules cover queue pressure, worker saturation, concentrated
Step time, Pipeline failure rate, Recovery activity, Circuit state, and
Idempotency anomalies. Empty findings mean no configured threshold was crossed,
not proof that the application is healthy.

Runtime correlation is explicit and caller-scheduled:

```go
// Call from an application-owned ticker or at an incident boundary.
sample := collector.CaptureRuntime()

correlations, err := obs.CorrelateResources(
    collector.Snapshot(),
    obs.CorrelationOptions{MaxSampleAge: 30 * time.Second},
)
```

The standard sample includes heap/allocation counters, goroutines, cgo calls,
stack use, and GC activity. `RecordResource` accepts equivalent samples from
external process/system samplers. Correlation associates each execution event
with its latest preceding fresh sample; it describes timing, not causation.
Pipeflow does not invent process CPU percentages unavailable from Go's standard
library.

Operational history is opt-in and recorded outside execution:

```go
store, err := history.Open("./pipeflow-history", history.Options{
    MaxSnapshots: 10_000,
    MaxAge:       7 * 24 * time.Hour,
    Sync:         true,
})
if err != nil {
    log.Fatal(err)
}

// The application chooses the recording interval or incident boundaries.
if err := store.Append(collector.Snapshot()); err != nil {
    log.Printf("record observation history: %v", err)
}
```

Records are immutable, versioned, payload-free snapshot files. Writes use a
temporary file and atomic rename; count/age retention is explicit. The store
has no background goroutine and is never an execution dependency.

The initial in-process collector provides bounded recent telemetry, run and
pipeline views, explainable basic health, structurally grouped error classes,
metric aggregates, and profile totals. Snapshots are detached and read-only;
the collector never receives business payloads or error messages.

An optional standard-library HTTP transport exposes those snapshots without
giving the transport control over execution:

```go
handler, err := obshttp.NewHandler(collector, obshttp.Options{
    Authorize: func(r *http.Request) bool {
        return r.Header.Get("Authorization") == "Bearer "+token
    },
    History: store,
})
if err != nil {
    log.Fatal(err)
}

// Mount handler in the application's existing HTTP server.
mux.Handle("/operations/", http.StripPrefix("/operations", handler))
```

The handler serves `GET`/`HEAD` on `/healthz`, `/v1/health`, `/v1/workers`,
`/v1/queues`, `/v1/resilience`, `/v1/config`, `/v1/explain`, `/v1/resources`,
`/v1/dashboard`, optional `/v1/history`, and `/v1/snapshot`.
History accepts RFC3339 `from`/`to`, `limit` (maximum 1000), and `order=asc|desc`.
Responses are versioned, payload-free JSON with caching
disabled. The embedding application owns authentication policy, TLS, bind
address, server lifecycle, and request logging. Persistent history,
exporters, streaming, and multi-application aggregation remain later v2.x
slices. See [Observation Tool Foundation](adr/ObservationToolFoundation.md),
[Queue and Worker Operational Views](adr/QueueWorkerOperationalViews.md), and
[Resilience Operational Views](adr/ResilienceOperationalViews.md), and
[Persistent Observation History](adr/PersistentObservationHistory.md), and
[Effective Configuration View](adr/EffectiveConfigurationView.md), and
[Explain and Bottleneck Analysis](adr/ExplainBottleneckAnalysis.md), and
[Runtime Resource Correlation](adr/RuntimeResourceCorrelation.md), and
[Observation TUI](adr/ObservationTUI.md), and
[Observation HTTP Transport](adr/ObservationHTTPTransport.md).

Run the read-only terminal dashboard against a mounted Observation handler:

```sh
PIPEFLOW_OBS_TOKEN=secret go run ./cmd/pipeflow-obs \
  -url http://127.0.0.1:8080/operations
```

Use `-once -color=false` for scripts and captured output. Interactive mode
refreshes every two seconds by default and exits cleanly on Ctrl+C/SIGTERM.
The TUI cannot submit work, cancel executions, or modify configuration.

## Production Readiness Tool (v3.x development)

The additive `readiness` package starts the v3 tool with an explicit
Arrange-Act-Assert scenario model. A scenario executes the real Pipeline and
checks its transient output, execution error, and payload-free `RunReport`.

```go
result := readiness.NewScenario("safe sample", &pipeline).
    Arrange("use representative input and a fake sink").
    WithInput(sample).
    Expect(
        readiness.RunSucceeds(),
        readiness.Expect("output accepted", func(got readiness.Observation) readiness.Assessment {
            if accepted(got.Output) {
                return readiness.Passed("safe sink accepted the output")
            }
            return readiness.Failed("safe sink rejected the output")
        }),
    ).
    Run(context.Background())
```

Every scenario declares at least one expectation. Execution failure is evidence,
not an automatic scenario failure, because an injected failure may be precisely
the expected behavior. Checks return `PASS`, `FAIL`, or `REVIEW`; overall
precedence is `FAIL`, then `REVIEW`, then `PASS`. Retained results do not contain
business output.

Contract and topology readiness is available before execution:

```go
contracts := readiness.InspectContractsInput(&pipeline, sample)
for _, boundary := range contracts.Boundaries {
    fmt.Printf("%s (%s) -> %s (%s): %s\n",
        boundary.Producer.Name, boundary.Producer.Type,
        boundary.Consumer.Name, boundary.Consumer.Type,
        boundary.Status)
}
```

Known compatible boundaries produce `PASS`, known incompatible boundaries
produce `FAIL`, and types that cannot be established before execution produce
`REVIEW`. Pass-through Steps preserve the identity of the value's actual
producer. The underlying Core `InspectFlow` methods expose structured endpoints
and Go type names without functions or payloads, so tools never need to parse
validation error strings.

Safe end-to-end verification composes contract inspection with a real Scenario:

```go
e2e := readiness.NewEndToEnd(
    readiness.NewScenario("sample order", &pipeline).
        WithInput(sample).
        Expect(readiness.RunSucceeds(), outputWasCaptured),
    readiness.SampleIngress("order fixture"),
    readiness.SafeSink("in-memory capture"),
)
result := e2e.Run(context.Background())
```

The verifier rejects missing, duplicate, or unsupported boundary declarations
and stops before execution when contract preflight fails. Its payload-free
coverage lists every reported Pipeline, Stage, Step, Parallel, Branch, and
Subflow path with status, duration, attempts, and error evidence. The Scenario's
expectations remain authoritative, allowing an expected injected failure to be
a passing end-to-end test.

Type-aware mutation plans exercise plausible malformed payloads without changing
business code:

```go
mutations := readiness.NewMutationPlan(
    readiness.NewScenario("provider mutations", &pipeline).
        Expect(payloadRemainsControlled),
    map[string]any{"provider": "Clover", "items": []any{"one", "two"}},
    readiness.WithMutationMaxCases(50),
)
result := mutations.Run(context.Background())
```

The deterministic generator applies one change per case: zero/nil/empty values,
string truncation and case variants, primitive type changes through interface
fields, missing or extra map keys, and empty/reordered/duplicated slices. It
walks exported struct fields and nested collections without modifying the sample.
Depth and case limits prevent accidental explosion. Generated values are
transient; retained mutation results contain only paths, kinds, type names,
Scenario evidence, and payload-free reports.

Semantic mutation, injection, load, chaos, suites, and aggregate readiness
verdicts remain later v3.x increments. See [Readiness Scenario Model](adr/ReadinessScenarioModel.md)
and [Readiness Contract and Topology Inspection](adr/ReadinessContractTopology.md),
[Readiness End-to-End Verification](adr/ReadinessEndToEnd.md), and
[Readiness Type-Aware Payload Mutation](adr/ReadinessPayloadMutation.md).

## Design Goals

Pipeflow should be:

- simple to understand
- embedded directly into Go applications
- composable
- explicit rather than magical
- concurrency-aware
- cancellation-aware
- easy to test
- independent of external infrastructure
- suitable as a reusable backend execution foundation

## Architecture

The core execution model is:

```text
Pipeline
│
├── Context
│
└── Stage
    │
    ├── Step
    │
    ├── Step
    │
    └── ConcurrentSteps
        ├── Step
        ├── Step
        └── Step
```

A `Pipeline` executes stages sequentially.

A `Stage` executes its `StageItem`s sequentially.

A `StageItem` can currently be:

- `Step`
- `ConcurrentSteps`
- `Parallel`
- `Subflow`

`ConcurrentSteps` allows multiple independent steps to execute concurrently within an otherwise sequential stage.

## Example

```go
pipeline := pipeflow.NewPipeline(
    "Orders",
    pipeflow.NewStage(
        "Prepare",
        pipeflow.NewStep("Load", func() (Order, error) {
            return loadOrder()
        }),
        pipeflow.NewStep("Audit", func(order Order) error {
            return auditOrder(order)
        }),
        pipeflow.NewStep("Price", func(order Order) (Invoice, error) {
            return priceOrder(order)
        }),
    ),
)

invoice, err := pipeline.Run(context.Background())
```

Step return values flow naturally through the pipeline: a step's output is the
next step's input, a stage's final output is the next stage's input, and the
last stage's output is returned by `Pipeline.Run`.

Ordinary step functions can use these signatures:

```go
func() error
func() (T, error)
func(T) error
func(T) (U, error)
```

An error-only function preserves the value already in the flow. This makes
validation, logging, persistence, and other side effects natural pass-through
steps. Function errors stop sequential execution and remain discoverable with
`errors.Is` after Pipeflow adds execution-location information.

Pipeflow validates statically knowable adjacent Step types before execution.
The actual initial input is checked during preflight, before a Run ID is
created, hooks fire, or workload code begins.

Call `pipeline.Validate()` explicitly when configuration-time validation is
useful. Use `pipeline.ValidateInput(input)` when the initial value is already
available. `Run` and `Start` perform both structure and input-aware validation
automatically.

## Pre-execution Validation

Validation recursively covers Pipeline, Stage, ConcurrentSteps, Parallel,
Branch, and Subflow definitions. It checks:

- ordinary Step signatures and policy callback signatures;
- statically knowable adjacent value types and the actual initial input;
- nil execution units;
- empty and duplicate identifiers within their immediate scope;
- negative worker limits and invalid failure policies;
- retry, polling, condition, rate-limit, timeout, and result-metadata policy
  configuration already defined by those APIs;
- conflicting shared rate-limit policies.

```go
if err := pipeline.Validate(); err != nil {
    // Definition is invalid independently of an initial value.
}

if err := pipeline.ValidateInput(order); err != nil {
    // The definition or initial value flow is invalid.
}
```

`Validate()` deliberately leaves a first typed consumer unresolved when no
input type is knowable. `ValidateInput()` resolves that boundary, including Go
nil assignability. Legacy `any` StageItems make subsequent flow dynamically
typed, so compatibility after them remains a runtime check.

Names are unique only within the scope where they identify report and
operational children: sibling Stages, top-level Steps within a Stage, sibling
Parallels/Subflows, Branches within a Parallel, and Steps within a Branch. A
Subflow definition can therefore be reused in different parent Stages.

Validation returns before execution and produces no RunReport or lifecycle
events. It does not invoke business functions, predicates, metadata
extractors, hooks, backgrounds, or finalizers.

Zero continues to mean unlimited workers. Negative worker counts are invalid.
Existing documented non-positive timeout disabling and retry/polling defaults
remain compatible.

See [ADR-027](adr/PipelineValidation.md) for the Phase 19 contract.

## Pipeline Introspection

`Describe()` exposes the declared execution structure without starting a run:

```go
description := pipeline.Describe()

fmt.Println(description.Kind, description.Name)
for _, stage := range description.Children {
    fmt.Println(stage.Kind, stage.Name)
}

fmt.Println(description)
```

The final line renders a Unicode tree:

```text
Invoice Processing
├── Ingest
│   ├── Read
│   └── Parse
├── Normalize
│   ├── Clean
│   └── Deduplicate
└── Output
    └── Store
```

`Description` is a uniform recursive node with `Kind`, `Name`, `Type`, and
`Children`. Stable kinds identify Pipelines, Stages, Steps, ConcurrentSteps,
Parallels, Branches, Subflows, custom StageItems, and invalid nil items.
Custom StageItems expose only their Go type name because Pipeflow does not know
their internal structure.

Every call builds a fresh tree. Mutating it cannot change the Pipeline.
Introspection is definition-only and contains no function values, policy
callbacks, flowing data, result metadata, RunReport facts, live status, or
execution controls. It is safe to call on an invalid definition, allowing
tools to display structure before separately calling `Validate()`.

Pipeflow provides the structure and compact text rendering only. Mermaid,
documentation generators, CLI styling, serialization, and visual tools remain
downstream concerns.

See [ADR-028](adr/PipelineIntrospection.md) for the Phase 20 contract.

## Context

Pipeflow provides an optional shared execution `Context` across the pipeline.

It supports shared values and compatibility execution metadata such as:

- execution status
- execution duration

Go's standard `context.Context` is propagated separately through execution for cancellation and deadlines.

Simple pipelines do not need to create one:

```go
output, err := pipeline.Run(goCtx)
output, err := pipeline.Run(goCtx, input)
```

Code that needs execution-wide shared values, lifecycle state, or a custom
logger can provide a Pipeflow context using the compatibility form:

```go
output, err := pipeline.Run(goCtx, pipeflowCtx, input)
```

The two contexts have different responsibilities:

```text
context.Context
    cancellation
    deadlines
    request-scoped Go operations

pipeflow.Context
    shared pipeline values
    compatibility execution metadata
    logging
```

`Context.Set` and `Context.Get` are safe for concurrent access by
`ConcurrentSteps`. The map container is protected; if a stored value is itself
mutable, callers remain responsible for synchronizing that value. Logger calls
obtained through one Context are serialized, allowing ConcurrentSteps to share
a logger safely.

A supplied Pipeflow Context is owned by one active execution at a time.
Overlapping use returns `ErrContextInUse`; sequential reuse remains supported.
This avoids presenting one ambiguous status and timing record for several
runs.

`Context.CurrentStage` and `Context.CurrentStep` are deprecated compatibility
methods. A scalar current Step cannot describe parallel execution. Use the
run-scoped, read-only `Execution.Current()` API for structured live state.

See [ADR-018](adr/ContextConcurrency.md) for the Phase 10 contract.

## Cancellation

Pipeline execution accepts Go `context.Context`.

The same context is propagated through:

```text
Pipeline
  ↓
Stage
  ↓
StageItem
  ↓
Step / ConcurrentSteps
```

This allows steps and other execution primitives to cooperate with cancellation and deadlines using standard Go semantics.

The original context-aware step adapter remains available for infrastructure
code that genuinely needs both contexts:

```go
step := pipeflow.NewStep(
    "Process",
    func(
        goCtx context.Context,
        ctx *pipeflow.Context,
        input any,
    ) (any, error) {
        // business logic
        return input, nil
    },
)
```

Cancellation is cooperative. Pipeflow does not forcibly terminate running goroutines.

## Concurrent Execution

Independent steps can execute concurrently using `ConcurrentSteps`.

```go
group := pipeflow.NewConcurrentSteps(
    []*pipeflow.Step{
        stepA,
        stepB,
        stepC,
    },
)
```

Results preserve step declaration order rather than completion order.

### Failure Policies

Concurrent groups support configurable failure behavior.

#### WaitAll

`WaitAll` is the default.

All steps are allowed to complete even when one or more steps fail.

Multiple errors are collected and returned together.

```go
group := pipeflow.NewConcurrentSteps(
    []*pipeflow.Step{
        stepA,
        stepB,
        stepC,
    },
    pipeflow.WithFailurePolicy(pipeflow.WaitAll),
)
```

Conceptually:

```text
A ─────────► success
B ──► error
C ─────────────► success
                     │
                     ▼
                wait for all
                     │
                     ▼
                return error
```

#### FailFast

`FailFast` cancels sibling work when the first step fails.

```go
group := pipeflow.NewConcurrentSteps(
    []*pipeflow.Step{
        stepA,
        stepB,
        stepC,
    },
    pipeflow.WithFailurePolicy(pipeflow.FailFast),
)
```

Conceptually:

```text
A ─────────────►
B ──► error
C ─────────────►
       │
       ▼
 cancel siblings
```

Cancellation remains cooperative: already-running goroutines must observe their `context.Context` to stop early.

### Bounded Concurrency

Concurrent execution can optionally be limited using `WithMaxWorkers`.

```go
group := pipeflow.NewConcurrentSteps(
    []*pipeflow.Step{
        stepA,
        stepB,
        stepC,
        stepD,
    },
    pipeflow.WithMaxWorkers(2),
)
```

With a worker limit of `2`, Pipeflow executes at most two steps from the concurrent group at the same time.

```text
A ────────►
B ─────►
              C ───────►
              D ─────►
```

A positive worker limit provides bounded concurrency.

When `WithMaxWorkers(...)` is not configured, or is configured as zero,
concurrency remains unlimited. Negative worker counts are validation errors.

Worker capacity is acquired before starting additional step goroutines. This prevents large concurrent groups from creating an unbounded number of goroutines waiting for execution capacity.

Queued work respects Go context cancellation.

When bounded concurrency is combined with `FailFast`, a step failure cancels the concurrent group and prevents work still waiting for capacity from starting.

Result ordering remains based on step declaration order regardless of worker limits or execution order.

## Structured Parallel Branches

`Parallel` runs named sequential branches from the same flowing input and then
joins their final values:

```go
providers := pipeflow.NewParallel("providers", []pipeflow.Branch{
    pipeflow.NewBranch("gmail",
        pipeflow.NewStep("find", findGmailAccount),
        pipeflow.NewStep("load", loadGmailMessages),
    ),
    pipeflow.NewBranch("microsoft",
        pipeflow.NewStep("find", findMicrosoftAccount),
        pipeflow.NewStep("load", loadMicrosoftMessages),
    ),
})

join := pipeflow.NewStep("join", func(results pipeflow.ParallelResults) ([]Message, error) {
    gmail := results[0].Value.([]Message)
    microsoft := results[1].Value.([]Message)
    return append(gmail, microsoft...), nil
})
```

Each Branch is an ordinary sequential value flow. Every Branch receives the
same Parallel input, and its final Step output becomes that Branch's result.
An empty Branch passes the input through unchanged.

`ParallelResults` contains `BranchResult{Name, Value}` entries in Branch
declaration order, regardless of completion order. This deterministic value is
the Parallel item's output and can flow into the next ordinary Step.

`WaitAll` is the default. It waits for every Branch and joins failures in
declaration order. `WithParallelFailurePolicy(FailFast)` cancels sibling
Branches cooperatively and waits for started work to stop. The first observed
non-cancellation error remains primary.

When any Branch fails, the Parallel output is nil. Successful sibling payloads
are deliberately discarded rather than exposed as implicit partial results.
Payload-free progress remains available in `RunReport` and `Execution.Current`.

Reports use an explicit `StageReport.Parallels -> ParallelReport.Branches ->
BranchReport.Steps` hierarchy. Errors include Parallel and Branch identity,
and lifecycle events expose corresponding boundaries.

At Parallel start, Pipeflow takes one shallow snapshot of the shared Context
value map and gives each Branch its own Context initialized from that same
snapshot. Branch Steps may safely use `Set` and `Get`, and sequential Steps in
one Branch see that Branch's mutations. Siblings and the shared root Context do
not see those mutations, and Pipeflow performs no automatic merge.

Isolation stops at the map boundary. Pointers, maps, slices, and other
referenced objects stored as values remain shared references and follow normal
Go concurrency rules. Branch outputs through `ParallelResults` are the explicit
business-data join; Context is not a hidden sibling communication channel.

This is structured fork/join composition only. Parallel groups cannot depend
on sibling outputs, create edges, or address arbitrary earlier nodes.

See [ADR-019](adr/StructuredParallelExecution.md) for the Phase 11 contract.
See [ADR-021](adr/ScopedBranchState.md) for the Phase 13 Context contract.

## Managed Background Execution

Pipeline-owned support work can run alongside the main Stage flow:

```go
pipeline = pipeline.WithBackground("lease renewal", func(ctx context.Context) error {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            if err := renewLease(ctx); err != nil {
                return err
            }
        case <-ctx.Done():
            return ctx.Err()
        }
    }
})
```

Background work starts before Stage execution, receives the execution's Go
context, and does not participate in value flow. When main execution stops,
Pipeflow cancels every Background and waits for clean shutdown before running
Pipeline finalizers.

Fatal failure is the default and cancels main execution:

```go
pipeline = pipeline.WithBackground("heartbeat", heartbeat)
```

A support process whose failure should only be reported can be non-fatal:

```go
pipeline = pipeline.WithBackground(
    "telemetry",
    publishTelemetry,
    pipeflow.WithBackgroundFailurePolicy(pipeflow.BackgroundNonFatal),
)
```

Fatal errors are wrapped as `BackgroundError` and combined in declaration
order. Non-fatal failures appear in `RunReport.Backgrounds` but do not replace
the flowing value or fail an otherwise successful run. Background panics become
`PanicError` values.

Cancellation returned while Pipeflow is stopping a Background after successful
main execution is normalized to `completed`. Parent cancellation and deadlines
remain `cancelled` or `timeout`. `Execution.Current().Backgrounds` exposes only
read-only live state.

Background functions must cooperate with context cancellation. Pipeflow waits
for every worker and cannot forcibly stop a goroutine that ignores its context.
This API does not create detached daemons, mutate workers through `Execution`,
or feed background results into Pipeline value flow.

See [ADR-020](adr/ManagedBackgroundExecution.md) for the Phase 12 contract.

## Execution Semantics

Sequential execution follows declaration order:

```text
Pipeline stages -> Stage items -> Steps
```

Each successful value becomes the next sequential input. When a Step or Stage
fails, its parent returns a nil output and no later sequential work starts.
Retrying Steps expose only the eventual successful value; failed-attempt values
never enter the flow.

Cancellation is checked before starting each Pipeline Stage and Stage item.
Running user functions are not forcibly stopped. Context-aware functions must
cooperate with Go's `context.Context` when early termination is required.

Concurrent groups use these deterministic rules:

- every child receives the same group input;
- successful results are returned in declaration order, not completion order;
- `WaitAll` waits for all started work and joins errors in declaration order;
- `FailFast` cancels siblings on the first observed error and preserves the
  first observed non-cancellation failure as the primary error;
- bounded work still queued after fail-fast cancellation is not started;
- already-running work is awaited under both policies;
- partial results are not returned when the group fails.

See [ADR-009](adr/ExecutionSemantics.md) for the complete Phase 1 contract.

## Structured Execution Errors

Failures that occur during execution return `*pipeflow.ExecutionError` values:

```go
type ExecutionError struct {
    Pipeline string
    Stage    string
    Parallel string
    Branch   string
    Step     string
    Attempt  int
    Err      error
}
```

Use standard Go error inspection:

```go
output, err := pipeline.Run(context.Background())
if err != nil {
    if errors.Is(err, context.Canceled) {
        // The original cause remains discoverable.
    }

    var executionErr *pipeflow.ExecutionError
    if errors.As(err, &executionErr) {
        log.Printf(
            "pipeline=%s stage=%s step=%s attempt=%d: %v",
            executionErr.Pipeline,
            executionErr.Stage,
            executionErr.Step,
            executionErr.Attempt,
            executionErr.Err,
        )
    }
}
```

Errors from concurrent `WaitAll` execution remain joined with `errors.Join`,
and every branch error carries its own location. `errors.Is` searches every
cause, while `errors.As` returns the first matching structured error in
declaration order.

Configuration and pre-run validation failures are not `ExecutionError` values
because execution did not begin.

See [ADR-010](adr/StructuredExecutionErrors.md) for the complete contract.

## Run Reports

Use `RunWithReport` when execution facts are needed:

```go
output, report, err := pipeline.RunWithReport(context.Background(), input)

fmt.Println(report.RunID)
fmt.Println(report.Status)
fmt.Println(report.Duration)

for _, stage := range report.Stages {
    fmt.Println(stage.Name, stage.Status, stage.Duration)
    for _, step := range stage.Steps {
        fmt.Println(step.Name, step.Status, step.Duration)
        for _, attempt := range step.Attempts {
            fmt.Println(attempt.Attempt, attempt.Status, attempt.Duration)
        }
    }
}
```

`Pipeline.Run` remains the simplest API and executes through the same reporting
path while discarding the returned report.

Every started run receives a random 128-bit hexadecimal Run ID. Reports contain
start/end timestamps, durations, final status, structured errors, stages,
steps, and retry attempts. Status values are:

- `pending`
- `running`
- `completed`
- `failed`
- `skipped`
- `cancelled`
- `timeout`

Stages and Steps that are not started after a failure or cancellation are
reported as `skipped` with zero timestamps and duration. A Go deadline maps to
`timeout`; ordinary context cancellation maps to `cancelled`. When joined
errors include a business failure, the enclosing execution remains `failed`.

Reports intentionally do not contain flowing business values. Pipeflow does
not serialize, persist, publish, or render reports; callers decide what to do
with the returned structured data.

See [ADR-011](adr/RunReports.md) for the complete Phase 3 contract.

## Result Metadata

Successful Steps and Stages may attach small scalar execution facts to their
reports without changing normal value flow:

```go
step := pipeflow.NewStep("import", importRecords,
    pipeflow.WithResultMetadata(func(result ImportResult) pipeflow.ResultMetadata {
        return pipeflow.ResultMetadata{
            "records_processed": result.Count,
            "items_rejected":    result.Rejected,
            "source":            "gmail",
        }
    }),
)

stage := pipeflow.NewStage("ingest", step).
    WithResultMetadata(func(result ImportResult) pipeflow.ResultMetadata {
        return pipeflow.ResultMetadata{"records_processed": result.Count}
    })
```

Extractors accept `func() ResultMetadata` or `func(T) ResultMetadata`. A Step
extractor receives its final successful flowing result after retries and
polling. A Stage extractor receives its final Stage output. Extractors run once
and do not wrap, replace, retain, or otherwise change that value.

Metadata is available through `StepReport.Metadata` and
`StageReport.Metadata`. Skipped or failed work records none. Extractor input
types are validated statically where possible and dynamically for legacy
adapters. Invalid metadata or extractor panics fail the owning Step or Stage
through normal structured errors.

To keep this distinct from business payload retention, metadata is limited to
64 entries. Keys are non-empty and at most 128 bytes. Values may be nil,
strings up to 4096 bytes, booleans, integer or floating-point scalars,
`time.Duration`, or `time.Time`. Slices, maps, pointers, arbitrary structs, and
other payload-shaped values are rejected.

The separation remains explicit:

- flow data moves through ordinary Go return values;
- result metadata contains small operational facts;
- `RunReport` contains execution history plus those facts;
- `State()` remains a lightweight payload- and metadata-free progress view.

This phase does not introduce retained Stage business results, metadata sinks,
serialization, automatic aggregation, indexing, metrics, or cross-Step
communication.

See [ADR-026](adr/ResultMetadata.md) for the Phase 18 contract.

## Live Execution State

`Pipeline.Start` begins execution asynchronously and returns a read-only,
run-scoped handle:

```go
execution, err := pipeline.Start(context.Background(), input)
if err != nil {
    // Invalid configuration and setup errors are returned synchronously.
}

fmt.Println(execution.Status())

current := execution.Current()
fmt.Println(current.Pipeline, current.Status, current.Duration)
for _, stage := range current.Stages {
    fmt.Println(stage.Name, stage.Status, stage.Duration)
    for _, step := range stage.Steps {
        fmt.Println(
            step.Name,
            step.Status,
            step.Duration,
            step.Attempt,
            step.AttemptDuration,
        )
    }
}

state := execution.State()
for _, stage := range state.Stages {
    // Includes completed, running, and pending stages.
    fmt.Println(stage.Name, stage.Status, stage.Duration)
}

snapshot := execution.Report()
output, finalReport, err := execution.Wait()
```

The handle provides:

- `Status()` for the run's current status;
- `Current()` for currently active Stages and Steps;
- `State()` for the complete payload-free execution topology and status;
- `Report()` for an immutable point-in-time `RunReport` snapshot;
- `Done()` for integration with `select`;
- `Wait()` for the final output, report, and error.

Running durations are calculated when a snapshot is requested, using the
timing data already collected for RunReport. Multiple concurrent Steps may be
returned by `Current()`; Pipeflow does not reduce parallel execution to a
misleading scalar "current step."

`State()` complements that active-only projection. It returns every declared
Stage and its Steps, Parallels, Branches, and nested Subflows in declaration
order, including completed and pending siblings. It also contains Run ID,
Pipeline status, backgrounds, advancing durations, and the active poll/retry
attempt. Completed attempts are deliberately not presented as active; their
history remains available in `Report()`.

`State()` contains no flowing values, retained results, errors, hooks, or
control methods. Each call constructs an independent snapshot from the same
mutex-protected recorder used by `Report()`.

`Pipeline` stores no mutable current or last run. Each call to `Start` owns its
recorder, result, and completion signal. The caller owns the supplied Go
context and can cancel it normally.

This is live inspection of one Pipeline execution, not metrics exporting,
persistence, UI rendering, or managed background-task orchestration.

See [ADR-012](adr/LiveExecutionState.md) for the Phase 4 contract.
See [ADR-025](adr/LiveProgressState.md) for the Phase 17 full-state contract.


## Retry Policies

Steps can optionally retry failed executions using `WithRetry`.

```go
step := pipeflow.NewStep(
    "Fetch Users",
    fetchUsers,
    pipeflow.WithRetry(pipeflow.RetryPolicy{
        MaxAttempts: 3,
        Delay:       500 * time.Millisecond,
        Backoff:     pipeflow.ExponentialBackoff,
        MaxDelay:    5 * time.Second,
        Jitter:      0.2,
        RetryIf: func(err error) bool {
            return errors.Is(err, errTemporarilyUnavailable)
        },
    }),
)
```

`FixedBackoff` is the default. `ExponentialBackoff` doubles `Delay` after each
failed attempt. `MaxDelay` caps the final delay, including jitter. `Jitter` is
a symmetric fraction from `0` to `1`; for example, `0.2` varies a delay by up
to 20 percent in either direction. `RetryIf`, when present, can stop retrying
an error immediately. Context cancellation and deadline errors are never
retried and do not invoke the predicate.

`MaxAttempts` counts the initial call, not only retries. Steps without a retry
policy execute once. Each poll has an independent attempt sequence when
polling and retry are combined.

Retry delays cooperate with Go context cancellation. `AttemptReport` exposes
the selected `RetryDelay` for every failed attempt followed by a retry, so
backoff behavior remains observable without retaining business payloads.

See [ADR-015](adr/RetryHardening.md) for the Phase 7 contract.

## Rate Limiting and Throttling

Rate limiting is an optional Step invocation policy:

```go
pipeflow.NewStep("charge", charge,
    pipeflow.WithRateLimit(pipeflow.RateLimitPolicy{
        Key:           "payments-api",
        MaxCalls:      10,
        Interval:      time.Second,
        MaxConcurrent: 2,
    }),
)
```

`MaxCalls` and `Interval` configure a token-bucket rate with an initial burst
up to `MaxCalls`. `MaxConcurrent` bounds simultaneously executing actions.
Either limit may be used independently. Waiting for a token or concurrency
slot observes the Step's Go context, timeout, and parent cancellation.

The limiter applies to every actual action invocation, including retries and
polls. A conditionally skipped Step consumes no capacity. The Step's attempt
duration includes any throttling wait, so existing live state and reports show
time spent waiting without adding business payloads.

Limiters are execution-scoped. A non-empty `Key` shares capacity among all
Steps using the same policy in one run, including Parallel branches and nested
Subflows. Without a key, capacity belongs to that Step definition within the
run. State is reset for every Pipeline execution. Conflicting policies using
the same key are rejected by `Validate`.

API-client errors can optionally implement:

```go
type RetryAfterError interface {
    error
    RetryAfter() time.Duration
}
```

A positive retry-after duration postpones the shared limiter and becomes the
minimum retry delay. It is discovered through `errors.As`, so normal wrapping
continues to work. Pipeflow does not parse HTTP headers itself.

This phase does not add distributed coordination, persistent quotas, circuit
breakers, adaptive rates, HTTP middleware, or provider-specific behavior.

See [ADR-024](adr/RateLimiting.md) for the Phase 16 contract.

## Timeouts

Timeouts can be applied at Step, Stage, and Pipeline scope:

```go
pipeline := pipeflow.NewPipeline(
    "orders",
    pipeflow.NewStage(
        "process",
        pipeflow.NewStep(
            "call service",
            callService,
            pipeflow.WithTimeout(2*time.Second),
        ),
    ).WithTimeout(10*time.Second),
).WithTimeout(30 * time.Second)
```

Each timeout is a total budget for its scope:

- Step timeout includes every retry attempt and retry delay;
- Stage timeout includes all Stage items;
- Pipeline timeout includes the complete Pipeline run;
- nested scopes inherit the earliest active deadline;
- a non-positive duration disables that scope's timeout.

Pipeflow implements timeouts using Go context deadlines. Cancellation is
cooperative while user code is running: context-aware functions should stop
when `ctx.Done()` closes. Pipeflow checks the deadline again after a function
or custom StageItem returns, so work that ignores cancellation cannot report a
late success, but Pipeflow cannot forcibly terminate that work while it runs.

Timeout failures:

- unwrap to `context.DeadlineExceeded`;
- carry normal `ExecutionError` location and attempt information;
- mark active Attempt, Step, Stage, Pipeline, and RunReport state as `timeout`;
- mark work that never started as `skipped`;
- remain distinct from parent `context.Canceled`, which is `cancelled`.

`Execution.Current()` remains observation-only while a timeout approaches. It
does not mutate deadlines or cancel individual Steps.

See [ADR-013](adr/Timeouts.md) for the Phase 5 contract.

## Polling / Wait Until

Polling repeats a successful Step operation until an ordinary Go predicate is
satisfied:

```go
waitForJob := pipeflow.NewStep(
    "wait for job",
    fetchJob,
    pipeflow.WithPolling(pipeflow.PollPolicy{
        Every:    time.Second,
        MaxPolls: 20,
        Timeout:  time.Minute,
        Until: func(job Job) bool {
            return job.Ready
        },
    }),
)
```

The operation and predicate have separate meanings:

```text
operation error
    -> retry policy

operation success + predicate false
    -> wait, then poll again

operation success + predicate true
    -> value continues through the Pipeline
```

Supported predicate forms are:

```go
func() bool
func(T) bool
```

The predicate input is validated against the Step's output before execution
where possible and at runtime for dynamically typed legacy adapters.

`MaxPolls` and `Timeout` are independent limits. A non-positive `MaxPolls`
allows polling until its predicate, context, or timeout stops it. A
non-positive polling timeout disables that specific deadline. Negative
intervals are treated as zero. Poll timeout inherits the earliest active
Step, Stage, Pipeline, or parent deadline.

If the predicate remains false through `MaxPolls`, the Step returns
`ErrPollLimitExceeded`, discoverable with `errors.Is`. Cancellation and timeout
retain their normal `cancelled` and `timeout` states.

`StepReport.Polls` records per-poll timing, completion, attempts, and errors.
`StepReport.Attempts` remains the flattened attempt history for compatibility.
`Execution.Current()` exposes the active poll and attempt numbers without
providing mutation or control methods.

See [ADR-014](adr/Polling.md) for the Phase 6 contract.

## Conditional Steps

`WithCondition` runs a Step only when an ordinary Go predicate returns true:

```go
pipeflow.NewStep(
    "send confirmation",
    func(order Order) error { return send(order) },
    pipeflow.WithCondition(func(order Order) bool { return order.Ready }),
)
```

Supported predicate forms are `func() bool` and `func(T) bool`. Predicate input
types are validated against statically known flowing types before execution
and checked again at runtime when the prior type is dynamic.

The condition is evaluated once, before polling and retry attempts. When it is
false, the action is not called and the existing value passes through
unchanged. This also means a conditional transformer can have two possible
flow types: its input when skipped and its output when run. If those types
differ, Pipeflow defers validation of following Steps to runtime and returns a
clear type error if the selected path is incompatible.

An intentionally skipped Step has `StatusSkipped`, zero timing, and no attempt
or poll history in `RunReport`. It emits `StepSkipped`, without emitting
`StepStarted` or a completed/failed Step event. Parent cancellation and timeout
are checked before the predicate and still win over conditional skipping.

Conditions are deliberately Step-local. This phase does not add an expression
language, rule engine, else branches, conditional Stages, or hidden value
communication.

See [ADR-022](adr/ConditionalExecution.md) for the Phase 14 contract.

## Reusable Subflows

`Subflow` composes a named sequential group of Stages inside a parent Stage:

```go
prepare := pipeflow.NewSubflow("prepare-order",
    pipeflow.NewStage("normalize", normalizeStep),
    pipeflow.NewStage("validate", validateStep),
)

pipeline := pipeflow.NewPipeline("orders",
    pipeflow.NewStage("process",
        prepare,
        pipeflow.NewStep("persist", persist),
    ),
)
```

The value entering a Subflow becomes its first Stage input. Each nested Stage
passes its output to the next, and the final nested Stage output returns to the
containing Stage. The same immutable Subflow definition can be reused.

Subflows share the parent run's Run ID, Pipeflow Context, Go context,
cancellation, and value stream. They do not create child Pipeline executions
and cannot own backgrounds, finalizers, Pipeline hooks, or independent
execution handles. Existing `Parallel` and `ConcurrentSteps` groups may still
be used explicitly inside their nested Stages.

`RunReport` records `StageReport.Subflows`, each containing a `SubflowReport`
with status, timing, structured error, and nested Stage reports. `Current()`
exposes running Subflows and their active nested work. Lifecycle hooks receive
`SubflowStarted`, `SubflowCompleted`, or `SubflowFailed`; events produced inside
the group carry its name in `LifecycleEvent.Subflow`.

Subflows may be nested, but execution remains hierarchical and sequential.
They add no graph edges, dependencies, implicit parallelism, cross-run state,
or child-run control surface.

See [ADR-023](adr/NestedExecutionComposition.md) for the Phase 15 contract.

## Lifecycle Hooks

Pipeflow provides synchronous, observation-only lifecycle hooks for Pipeline,
Stage, Parallel, Branch, and Step boundaries:

```go
pipeline = pipeline.WithLifecycleHook(func(event pipeflow.LifecycleEvent) {
    fmt.Printf("%s %s/%s: %s\n",
        event.Type, event.Stage, event.Step, event.Status)
})
```

Events include the Run ID, Pipeline/Stage/Parallel/Branch/Step names, status,
occurrence time, and an error where applicable. They never contain flowing
business values and provide no execution-control methods.

Lifecycle event types are:

- `PipelineStarted`, `PipelineCompleted`, `PipelineFailed`, `PipelineFinalized`
- `StageStarted`, `StageCompleted`, `StageFailed`
- `SubflowStarted`, `SubflowCompleted`, `SubflowFailed`
- `ParallelStarted`, `ParallelCompleted`, `ParallelFailed`
- `BranchStarted`, `BranchCompleted`, `BranchFailed`
- `BackgroundStarted`, `BackgroundCompleted`, `BackgroundFailed`
- `StepStarted`, `StepCompleted`, `StepFailed`, `StepSkipped`

Hooks execute in registration order for each event. They execute before the
engine advances beyond that lifecycle boundary. Concurrent Steps may invoke
hooks concurrently, so callback-owned state must be concurrency-safe;
cross-Step ordering is intentionally unspecified, while each Step's start
always precedes its terminal event.

`PipelineFinalized` follows the completed or failed Pipeline event for every
normal return, including cancellation and timeout. It is observational rather
than a cleanup mechanism. Hooks cannot return errors. A hook panic is recovered
as a `PanicError` and fails the owning execution boundary; registered
finalizers still run.

The original Pipeline-specific hooks remain available for compatibility:

Available pipeline hooks:

- `OnStarted`
- `OnCompleted`
- `OnFailed`

Example:

```go
pipeline.OnCompleted(func(event pipeflow.PipelineEvent) {
    fmt.Printf(
        "%s completed in %s\n",
        event.Name,
        event.Context.Duration(),
    )
})
```

See [ADR-016](adr/LifecycleHooks.md) for the Phase 8 contract.

## Finalization and Cleanup

Register named Pipeline finalizers for operational cleanup:

```go
pipeline = pipeline.Finally("stop worker", func(ctx context.Context, result pipeflow.Finalization) error {
    cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    return stopWorker(cleanupCtx)
})
```

Finalizers run synchronously in reverse registration order after Pipeline work
stops and before the final Pipeline report and lifecycle events. They run after
success, failure, cancellation, timeout, and recovered user-code panics.

The cleanup context preserves parent values but deliberately detaches from the
execution cancellation and deadline. This lets cleanup proceed after timeout;
external calls should establish their own finite cleanup deadline as shown
above.

`Finalization` contains the Run ID, Pipeline name, and execution status/error
before cleanup failures are added. Every finalizer receives that same immutable
outcome. All finalizers run even when one returns an error or panics.

Cleanup errors are wrapped as `CleanupError` and joined with the original error
using `errors.Join`, preserving `errors.Is` and `errors.As`. A cleanup failure
turns an otherwise successful run into a failed run and suppresses its output.
`RunReport.Cleanups` records each finalizer in execution order with status and
timing, but no business payloads.

Panics from Step actions and their retry/polling callbacks become `PanicError`
values with a captured stack. Finalizer panics also become cleanup errors and
do not prevent remaining finalizers from running.

See [ADR-017](adr/Finalization.md) for the Phase 9 contract.

## Panic Policy

When Pipeflow invokes user code, a panic is recovered at the nearest
Pipeflow-owned execution boundary and converted to `*PanicError`. The error
retains the panic value and a Go stack trace. It is then wrapped by the same
structured location errors used for ordinary failures, so Pipeline, Stage,
Step, Parallel Branch, Subflow, Poll, Attempt, Background, and Cleanup location
remains available through `errors.As`.

This rule covers actions, conditions, polling predicates, retry callbacks,
metadata extractors, custom Stage items, lifecycle and legacy hooks, managed
background work, log callbacks, and finalizers. Normal policies still apply:
Step panics may be retried, fail-fast work cancels its siblings, non-fatal
background work remains non-fatal, and every registered finalizer runs.

The final `RunReport` records the failure but never retains the flowing business
value. Goroutines created directly by application callbacks are outside a
Pipeflow-owned boundary; applications remain responsible for panic handling in
those goroutines.

See [ADR-029](adr/PanicRecovery.md) for the Phase 21 contract.

## Cross-feature Reliability

Pipeflow's core interaction suite verifies that execution policies compose at
their boundaries, including retry with rate-limit waiting and timeout,
fail-fast Parallel cancellation after panic, managed Background shutdown before
finalization, runtime type errors inside conditional Subflows, polling with
`RetryAfter` and timeout, scoped Branch Context cancellation, and asynchronous
panic reporting through `Execution.Wait`.

These are reliability tests of the existing public contracts; Phase 22 adds no
new execution modes or public API. See
[ADR-030](adr/CrossFeatureReliability.md) for the tested invariants.

## V1 Core Capabilities

- Pipeline → Stage → StageItem execution model
- Sequential pipeline execution
- Concurrent step execution
- Concurrent failure policies
  - `WaitAll`
  - `FailFast`
- Deterministic concurrent result ordering
- Shared execution context
- Shared data propagation
- Go `context.Context` propagation
- Cancellation support
- Configurable step retry policies
- Cancellation-aware retry delays
- Pipeline, Stage, and Step lifecycle hooks
- LIFO Pipeline finalization and cleanup
- Structured parallel branches and deterministic joins
- Managed Pipeline-owned background execution
- Scoped, isolated Parallel Branch Context values
- Conditional Step execution with pass-through skipping
- Reusable sequential Subflows with nested reports
- Run-scoped Step rate and concurrency limiting
- Complete payload-free live execution state snapshots
- Small scalar Step and Stage result metadata
- Recursive input-aware pre-execution validation
- Programmatic Pipeline definition introspection
- Consistent panic recovery with stacks and structured execution locations
- Cross-feature execution policy reliability coverage
- Execution status
- Execution timing
- Logging
- Error propagation

## V1 Release

The current stable release is [`v1.0.0`](https://github.com/bbernier33/pipeflow/releases/tag/v1.0.0).
Core v1 is complete: future compatible v1 releases may add APIs, while breaking
changes require a new major version. Streaming, transport adapters, ETL, AI,
and other domain capabilities can build around the frozen Core rather than
expanding its execution model.

## V1.1 Configuration

V1.1 adds an optional, immutable configuration layer
for existing execution policies. Zero-configuration Go remains unchanged, and
explicit Go options always win:

```go
cfg, err := pipeflow.ParseConfigYAML(data)
if err != nil {
    return err
}

configured, err := pipeline.WithConfig(cfg)
if err != nil {
    return err
}

effective, _ := configured.EffectiveConfig()
```

```yaml
defaults:
  step:
    retry:
      max_attempts: 3
      backoff: exponential
pipelines:
  orders:
    timeout: 30s
    stages:
      process:
        steps:
          charge:
            timeout: 5s
```

The schema covers Pipeline/Stage timeouts; Step timeout, retry, polling schedule,
and rate limiting; Parallel failure policy; and Background failure policy.
Nested Subflows and Parallel Branches use explicit paths. Conditions and
polling predicates remain Go code—use `WithPollPredicate` when YAML owns the
polling schedule. See [ADR-032](adr/ConfigurationFoundation.md).

## Observation (v1.2)

Attach an observer when an application needs structured in-process telemetry:

```go
pipeline = pipeline.WithObserver(pipeflow.ObserverFuncs{
    Trace: func(event pipeflow.TraceEvent) {
        // Timestamped Pipeline/Stage/Step/Attempt execution path.
    },
    Metric: func(sample pipeflow.MetricSample) {
        // Raw execution counts and durations; aggregate in the consumer.
    },
    Profile: func(sample pipeflow.ProfileSample) {
        // Elapsed time attributed to an execution location.
    },
})
```

Observation is off by default and contains no flowing business values or error
messages. Observer panics are isolated from execution, callbacks are serialized,
and consumers should return promptly. Transports, exporters, persistence,
dashboards, health/history views, and runtime sampling remain outside Core. See
[ADR-033](adr/ObservationContract.md).

## Source and Sink Steps (v1.3)

Source and Sink roles make application boundaries explicit while retaining the
same Step engine and ordinary function signatures:

```go
pipeline := pipeflow.NewPipeline("orders",
    pipeflow.NewStage("flow",
        pipeflow.NewSourceStep("load", loadOrder),
        pipeflow.NewStep("price", priceOrder),
        pipeflow.NewSinkStep("store", storeInvoice),
    ),
)
```

`StepRoleNormal`, `StepRoleSource`, and `StepRoleSink` appear in descriptions,
reports, live state, lifecycle events, observation data, and effective YAML
configuration. Roles do not change value flow: a `func(T) error` Sink remains a
pass-through Step.

Queue buffering, batching, workers, backpressure, ordering, and drain semantics
begin with Worker/Stream execution in v1.4, where a runtime actually owns
multiple values. v1.3 intentionally adds no inert buffer settings and no visible
QueueStep. See [ADR-034](adr/StepRoles.md).

## Worker and Stream Runtime (v1.4)

Run the same finite Pipeline repeatedly without changing its Step functions:

```go
worker, err := pipeline.StartWorker(ctx, pipeflow.WorkerOptions{
    Workers:     4,
    Buffer:      100,
    MaxInFlight: 104,
})

work, err := worker.Submit(ctx, input)
output, report, err := work.Wait()

worker.Close() // stop intake and drain accepted work
err = worker.Wait()
```

Or connect an application/adapter input channel as a continuous Stream:

```go
stream, err := pipeline.StartStream(ctx, inputs, pipeflow.StreamOptions{
    Workers:       4,
    Buffer:        100,
    MaxInFlight:   104,
    FailurePolicy: pipeflow.StreamContinue,
})

for result := range stream.Results() {
    // result.Output, result.Report, result.Err
}
```

Queues are bounded and block upstream when full. Worker run failures and Stream
item failures are isolated; Streams continue by default. `Close` drains,
`Cancel` stops immediately, one Stream worker preserves order, and concurrent
workers emit completion order. Runtime queues are memory-only and provide no
durability or exactly-once guarantee. See [ADR-035](adr/WorkerStreamRuntime.md).

## Long-Term Direction

After the production core is stabilized, Pipeflow is intended to grow through modular capabilities such as:

```text
Pipeflow
├── Core
├── Stream
├── Routing
├── Sources / Sinks
├── Resilience
├── Metrics / Tracing
├── ETL
├── FP
├── AI
├── Validation
├── Cache
├── Scheduler
├── Collections
├── Graph
└── Adapters
    ├── MQTT
    ├── WebSocket
    ├── HTTP
    ├── Kafka
    ├── RabbitMQ
    ├── SQL
    ├── Redis
    └── Filesystem
```

The core should remain domain-neutral while addons and adapters provide specialized processing and integration capabilities.

## Philosophy

Pipeflow favors explicit, readable execution over configuration-heavy or magical abstractions.

The library should define **how work executes** while leaving business logic in the hands of the application.

Execution behavior such as concurrency, cancellation, retries, ordering, failures, and resource limits should be explicit and predictable.
