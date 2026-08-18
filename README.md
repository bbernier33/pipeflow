# Pipeflow

Pipeflow is a lightweight, composable pipeline execution library for Go.

It provides reusable execution primitives for structuring backend processing as pipelines, stages, and steps while keeping execution concerns separate from business logic.

Pipeflow is being developed toward a production-ready v1.0 core intended for reuse across backend services, data processing, IoT, realtime applications, game backends, and other Go workloads.

## Project Status

Pipeflow is an early-stage open-source library under active development.

The library is functional, but the public API may change between minor releases while the project approaches its first stable `v1.0.0` release.

Pipeflow is not yet recommended as a production dependency.

## Installation

```bash
go get github.com/bbernier33/pipeflow
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

`ConcurrentSteps` allows multiple independent steps to execute concurrently within an otherwise sequential stage.

## Example

```go
pipeline := pipeflow.NewPipeline(
    "Example",
    pipeflow.NewStage(
        "Gather",

        pipeflow.NewConcurrentSteps(
            []*pipeflow.Step{
                pipeflow.NewStep("Users", fetchUsers),
                pipeflow.NewStep("Orders", fetchOrders),
                pipeflow.NewStep("Products", fetchProducts),
            },
        ),

        pipeflow.NewStep("Merge", mergeResults),
    ),
)
```

A merge step is not required after `ConcurrentSteps`.

The concurrent group waits according to its configured execution policy and returns its outputs. Subsequent aggregation or transformation is performed explicitly by later steps only when required by the application.

## Context

Pipeflow provides a shared execution `Context` across the pipeline.

It supports shared values and execution metadata such as:

- execution status
- current stage
- current step
- execution duration

Go's standard `context.Context` is propagated separately through execution for cancellation and deadlines.

Conceptually:

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
    execution metadata
    lifecycle state
    logging
```

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

Step actions receive both contexts:

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

When `WithMaxWorkers(...)` is not configured, or the configured value is less than or equal to zero, concurrency remains unlimited to preserve the default behavior of `ConcurrentSteps`.

Worker capacity is acquired before starting additional step goroutines. This prevents large concurrent groups from creating an unbounded number of goroutines waiting for execution capacity.

Queued work respects Go context cancellation.

When bounded concurrency is combined with `FailFast`, a step failure cancels the concurrent group and prevents work still waiting for capacity from starting.

Result ordering remains based on step declaration order regardless of worker limits or execution order.


## Retry Policies

Steps can optionally retry failed executions using `WithRetry`.

```go
step := pipeflow.NewStep(
    "Fetch Users",
    fetchUsers,
    pipeflow.WithRetry(pipeflow.RetryPolicy{
        MaxAttempts: 3,
        Delay:       500 * time.Millisecond,
    }),
)
```

Steps without a retry policy execute once.

Retry delays cooperate with Go context cancellation.

## Lifecycle Hooks

Pipeflow provides pipeline lifecycle hooks that allow applications to react to execution without modifying business logic.

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

## Current Features

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
- Pipeline lifecycle hooks
- Execution status
- Execution timing
- Logging
- Error propagation

## Roadmap to v1.0

Pipeflow is currently strengthening its core execution guarantees before the public API is stabilized.

### Completed

- [x] Core sequential execution
- [x] Shared execution context
- [x] Execution lifecycle
- [x] Pipeline lifecycle hooks
- [x] Concurrent step execution
- [x] Retry policies
- [x] Go context / cancellation
- [x] Concurrent execution failure policies
- [x] Bounded concurrency / worker limits


### Production Core

- [ ] Step, stage, and pipeline timeouts
- [ ] Structured execution errors
- [ ] Stage and step lifecycle hooks
- [ ] Context concurrency hardening
- [ ] Race/stress/reliability testing
- [ ] Performance benchmarks
- [ ] Public API stability review
- [ ] Typed-core / generics evaluation

### v1.0

`v1.0.0` will represent a stable, documented, production-ready Pipeflow execution core.

Streaming, transport adapters, ETL, AI, and other domain capabilities will build on this foundation rather than being requirements for core v1.0.

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
