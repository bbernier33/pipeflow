# ADR-007: Cancellation with `context.Context`

- **Status:** Accepted
- **Date:** 2026-08-15

## Context

Pipeflow currently supports:

- sequential pipeline execution
- concurrent step execution
- retries
- execution status and timing
- shared Pipeflow context

As execution becomes more complex, callers need a way to stop work that is no longer needed.

Examples include:

- an HTTP request being cancelled
- a timeout expiring
- a CLI process being interrupted
- a parent worker shutting down
- retry delays that should stop early

Go already provides `context.Context` for cancellation, deadlines, and request lifetime.

Pipeflow also has its own `Context`, which stores execution-specific information such as:

- shared values
- logger
- status
- timing
- current stage
- current step

These two responsibilities should remain separate.

## Decision

Pipeflow will use Go's standard `context.Context` for cancellation and deadlines.

Pipeflow's existing `*pipeflow.Context` will remain responsible for Pipeflow execution metadata and shared values.

Execution methods will receive both contexts.

Conceptually:

```go
Run(
    ctx context.Context,
    pipeCtx *pipeflow.Context,
    input any,
)
```

The Go context controls execution lifetime.

The Pipeflow context stores execution state.

## Cancellation Behavior

Before starting new work, Pipeflow checks whether the Go context has been cancelled.

If cancellation has occurred:

- no new stage item should start
- the pipeline returns the context error
- the pipeline status becomes failed
- `context.Canceled` or `context.DeadlineExceeded` is propagated to the caller

Running user code is not forcibly terminated.

Cancellation is cooperative. Step implementations must either:

- check `ctx.Done()`
- call APIs that support `context.Context`
- return normally

## Retry Behavior

Retry delays must be cancellable.

Instead of using an unconditional:

```go
time.Sleep(delay)
```

Pipeflow waits for either:

- the retry delay to finish
- the context to be cancelled

If cancellation occurs during the delay, no further retry attempt is started.

## ConcurrentSteps

All steps inside `ConcurrentSteps` receive the same Go context.

Cancellation is propagated to all concurrent steps.

Pipeflow waits for already-running goroutines to finish before returning.

No new work is started after cancellation is detected.

## Scope

This ADR includes:

- cancellation propagation
- deadline propagation
- cancellation-aware retry delays
- cancellation checks before starting new work
- support inside sequential and concurrent execution

The following are intentionally excluded:

- forced termination of running goroutines
- automatic per-step timeouts
- cancellation policies
- partial-result recovery
- cleanup hooks

These may be introduced separately in future ADRs.

## Consequences

### Positive

- Uses Go's standard cancellation model
- Integrates naturally with HTTP, database, and worker code
- Retry delays can stop immediately
- Concurrent execution can share the same cancellation signal
- Pipeflow execution metadata remains separate from request lifetime

### Negative

- Execution method signatures become larger
- Existing Pipeflow APIs must change
- User step functions must accept `context.Context`
- Cancellation depends on user code cooperating with the context

## Rationale

Pipeflow should not implement a custom cancellation system.

`context.Context` is the standard Go mechanism for propagating cancellation and deadlines across API boundaries.

Keeping it separate from `pipeflow.Context` preserves a clear responsibility split:

```text
context.Context
└── cancellation
└── deadlines
└── request lifetime

pipeflow.Context
└── shared values
└── logging
└── status
└── timing
└── execution metadata
```