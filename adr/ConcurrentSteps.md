# ADR-005: Introduce ConcurrentSteps for Concurrent Step Execution

- **Status:** Accepted
- **Date:** 2026-07-25

## Context

Pipeflow currently executes every stage sequentially. While this behavior is predictable and easy to understand, many workloads contain independent operations that do not rely on one another.

Examples include:

- Multiple API requests
- Database queries
- File processing
- AI inference
- Web scraping

Executing these operations sequentially increases the overall execution time even though they can safely execute concurrently.

The library requires a simple, explicit mechanism for running independent steps concurrently while preserving the sequential pipeline model.

---

## Decision

Pipeflow introduces **ConcurrentSteps**, a new stage item that executes multiple steps concurrently using Go goroutines.

A stage remains sequential, but a concurrent step group may be inserted anywhere within the stage.

ConcurrentSteps is responsible only for concurrent execution and synchronization. It returns the ordered outputs of its contained steps. Any aggregation or transformation is left to subsequent steps.

Example:

```go
pipeflow.NewStage(
    "Gather Data",

    pipeflow.NewConcurrentSteps(
        pipeflow.NewStep("Fetch Users", fetchUsers),
        pipeflow.NewStep("Fetch Orders", fetchOrders),
        pipeflow.NewStep("Fetch Products", fetchProducts),
    ),

    pipeflow.NewStep("Merge Results", mergeResults),
)
```

Each step inside the concurrent group:

- receives the same input
- executes independently
- runs in its own goroutine

The concurrent group waits until every step has completed before execution continues.

---

## Output

The output of a concurrent group is an ordered `[]any`.

Results preserve declaration order, regardless of execution order.

Example:

```go
[]any{
    users,
    orders,
    products,
}
```

---

## Error Handling

If one or more concurrent steps fail:

- all running steps are allowed to complete
- execution waits for every goroutine
- errors are returned using `errors.Join(...)`
- the stage terminates without executing the next stage item

---

## Scope

This ADR introduces only concurrent step execution.

The following features are intentionally excluded:

- Concurrent stages
- Nested concurrent groups
- Pipeline concurrency
- Cancellation
- Concurrency limits
- Dependency graphs (DAGs)

These capabilities may be introduced through future ADRs.

---

## Consequences

### Positive

- Reduces execution time for independent work
- Preserves the existing sequential pipeline model
- Keeps concurrency explicit rather than automatic
- Aligns with Go's concurrency model using goroutines

### Negative

- Introduces synchronization complexity
- Requires thread-safe access to shared context
- Adds concurrent execution paths to the pipeline engine

---

## Rationale

Concurrency is introduced as an explicit stage construct rather than automatic optimization.

This approach:

- keeps execution predictable
- makes concurrent execution visible in pipeline definitions
- avoids hidden scheduling decisions
- provides a foundation for future concurrency features without changing the existing pipeline model