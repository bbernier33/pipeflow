# Pipeflow

Pipeflow is a lightweight pipeline architecture library for Go.

The goal is to make it easier to structure processing applications as a clear sequence of stages and steps, while keeping execution concerns separate from business logic.

Pipeflow is also a long-term learning project for exploring Go through the development of a real reusable library.

## Project status

Pipeflow is currently in the early design and learning phase.

The API is not stable yet, and the project is not ready for production use.

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

Design goals

Pipeflow should be:

simple to understand
embedded directly into Go applications
strongly typed
easy to test
composable
explicit rather than magical
useful without requiring external infrastructure
Non-goals

Pipeflow is not intended to become:

a distributed workflow platform
a scheduler
a replacement for Airflow or Temporal
a database-backed orchestration system
a visual workflow designer

The project should remain focused on in-process application pipelines.

Planned architecture

The initial model is:

Pipeline
  │
  ├── Stage
  │     ├── Step
  │     └── Step
  │
  └── Stage
        └── Step

A pipeline executes stages in order.

A stage contains one or more steps.

Steps operate on shared, strongly typed pipeline data.