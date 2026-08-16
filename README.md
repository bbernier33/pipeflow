# Pipeflow

Pipeflow is a lightweight pipeline architecture library for Go.

The goal is to make it easier to structure processing applications as a clear sequence of stages and steps, while keeping execution concerns separate from business logic.

Pipeflow is also a long-term learning project for exploring Go through the development of a real reusable library.

## Project Status

Pipeflow is an early-stage open-source library under active development.

The public API is evolving and may change between minor releases while the project approaches its first stable (v1.0.0) release.

The library is functional but is not yet recommended for production workloads.

## Installation

```bash
go get github.com/bbernier33/pipeflow
```

## Motivation

Many applications follow a similar structure:

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
```

Without a shared structure, each project tends to reimplement:

execution order
lifecycle tracking
error handling
cancellation
logging
retries
concurrency

Pipeflow aims to provide that reusable structure so applications can focus on their domain logic.

## Design goals

Pipeflow should be:

- simple to understand
- embedded directly into Go applications
- strongly typed
- easy to test
- composable
- explicit rather than magical
- useful without requiring external infrastructure

## Non-Goals

Pipeflow is not intended to become:

- a distributed workflow platform
- a scheduler
- a replacement for Airflow or Temporal
- a database-backed orchestration system
- a visual workflow designer

The project should remain focused on in-process application pipelines.

## Architecture

Pipeflow is organized as:

Pipeline
│
├── Context
│
└── Stages
    ├── StageItem
    │   ├── Step
    │   └── ConcurrentSteps
    │          ├── Step
    │          ├── Step
    │          └── Step
    └── ...

A pipeline executes stages in order.

A stage contains one or more StageItems.

StageItems may be individual Steps or ConcurrentSteps groups.

```go
pipeline := pipeflow.NewPipeline(
    "Example",
    pipeflow.NewStage(
        "Gather",

        pipeflow.NewConcurrentSteps(
            pipeflow.NewStep("Users", fetchUsers),
            pipeflow.NewStep("Orders", fetchOrders),
            pipeflow.NewStep("Products", fetchProducts),
        ),

        pipeflow.NewStep("Merge", mergeResults),
    ),
)
```

The execution context exposes runtime metadata such as:

- execution status
- current stage
- current step
- execution duration


## Hooks

Pipeflow provides lifecycle hooks that allow applications to react to pipeline execution without modifying business logic.

```go
pipeline.OnCompleted(func(event pipeflow.PipelineEvent) {
    fmt.Printf(
        "%s completed in %s\n",
        event.Name,
        event.Context.Duration(),
    )
})
```

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


## Current Features

- Sequential pipeline execution
- Concurrent step execution
- Configurable step retry policies
- Pipeline → Stage → StageItem architecture
- Shared execution context
- Shared data propagation
- Pipeline lifecycle hooks
- Execution timing
- Logging
- Early error propagation
- Cancellation support using Go `context.Context`


## Roadmap

### Completed

- [x] Sequential execution
- [x] Concurrent step execution
- [x] Shared execution context
- [x] Logging
- [x] Execution lifecycle
- [x] Pipeline lifecycle hooks
- [x] Retry policies
- [x] Cancellation with Go `context.Context`

### Planned

- [ ] Metrics
- [ ] Typed pipelines (Generics)


## Philosophy

Pipeflow favors explicit, readable workflows over configuration-heavy or magical abstractions.

The library aims to provide reusable execution primitives while leaving business logic entirely in the hands of the application.