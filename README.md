# Pipeflow

Pipeflow is a lightweight pipeline architecture library for Go.

The goal is to make it easier to structure processing applications as a clear sequence of stages and steps, while keeping execution concerns separate from business logic.

Pipeflow is also a long-term learning project for exploring Go through the development of a real reusable library.

## Project status

Pipeflow is currently in the early design and learning phase.

The API is not stable yet, and the project is not ready for production use.


## Installation

```bash
go get github.com/bbernier33/pipeflow


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

simple to understand
embedded directly into Go applications
strongly typed
easy to test
composable
explicit rather than magical
useful without requiring external infrastructure

## Non-Goals

Pipeflow is not intended to become:

a distributed workflow platform
a scheduler
a replacement for Airflow or Temporal
a database-backed orchestration system
a visual workflow designer

The project should remain focused on in-process application pipelines.

## Architecture

The initial model is:

Pipeline
│
├── Context
│   ├── Shared Values
│   ├── Logger
│   ├── Status
│   ├── Current Stage
│   ├── Current Step
│   └── Timing
│
└── Stages
    ├── Step
    ├── Step
    └── ...

A pipeline executes stages in order.

A stage contains one or more steps.

Steps operate on shared, strongly typed pipeline data.

```go
ctx := pipeflow.NewContext()

pipeline := pipeflow.NewPipeline(
    "Example",
    pipeflow.NewStage(
        "Stage One",
        pipeflow.NewStep("Print", func(ctx *pipeflow.Context, input any) (any, error) {
            fmt.Println("Hello Pipeflow")
            return input, nil
        }),
    ),
)

_, err := pipeline.Run(ctx, nil)
if err != nil {
    log.Fatal(err)
}

fmt.Println(ctx.Status())
fmt.Println(ctx.Duration())
```
The execution context exposes runtime metadata such as:

- execution status
- current stage
- current step
- execution duration

## Current Features

- Sequential pipeline execution
- Pipeline → Stage → Step architecture
- Shared execution context
- Data propagation between stages and steps
- Pluggable logger interface
- Default logger implementation
- Execution lifecycle tracking
  - Pending
  - Running
  - Completed
  - Failed
- Execution timing
  - Start time
  - End time
  - Duration

## Roadmap

### Completed

- [x] Sequential execution
- [x] Shared execution context
- [x] Data propagation
- [x] Logging
- [x] Execution lifecycle
- [x] Execution timing

### Planned

- [ ] Hooks / Events
- [ ] Retry policies
- [ ] Parallel execution
- [ ] Cancellation
- [ ] Metrics
- [ ] Typed pipelines (Generics)


## Philosophy

Pipeflow favors explicit, readable workflows over configuration-heavy or magical abstractions.

The library aims to provide reusable execution primitives while leaving business logic entirely in the hands of the application.