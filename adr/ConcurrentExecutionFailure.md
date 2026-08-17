ADR-008: Concurrent Execution Failure Policies
Status: Accepted
Date: 2026-08-16
Context

ConcurrentSteps currently starts all contained steps concurrently, waits for every step to finish, and joins any returned errors.

This behavior is predictable and useful for workloads where all concurrent work should be allowed to complete, but it is not appropriate for every backend workload.

Examples:

ETL jobs may want to collect all failures before returning.
API aggregation may want to stop as soon as one required dependency fails.
Game or realtime processing may want sibling work cancelled immediately after a critical failure.
IoT workloads may prefer to finish independent work even when one operation fails.

Pipeflow therefore needs explicit concurrent failure policies instead of one hardcoded behavior.

Decision

ConcurrentSteps will support two failure policies:

WaitAll
FailFast

WaitAll remains the default to preserve existing behavior and backwards compatibility.

WaitAll

All concurrent steps are allowed to finish, even if one or more fail.

After every step completes:

successful outputs remain available internally
all errors are collected
multiple errors are combined using errors.Join(...)
the concurrent group returns an error if any step failed

Conceptually:

Step A ─────────► success
Step B ──► error
Step C ─────────────► success
                         │
                         ▼
                    wait for all
                         │
                         ▼
                  return joined error
FailFast

All concurrent steps start with a child context.Context.

When the first step returns an error:

the concurrent group cancels the child context
sibling steps receive the cancellation signal
no additional work should be started by the group
already-running goroutines are not forcibly terminated
cooperative steps may stop early by observing context.Context

Conceptually:

Step A ─────────────►
Step B ──► error
Step C ─────────────►
          │
          ▼
      cancel group

Fail-fast cancellation is cooperative.

Pipeflow does not forcibly terminate goroutines.

Running user code must either:

observe ctx.Done()
call APIs that support context.Context
or return normally
Default Behavior

The default failure policy is:

WaitAll

This preserves the behavior of existing ConcurrentSteps usage.

A caller must explicitly request fail-fast behavior.

Potential API direction:

pipeflow.NewConcurrentSteps(
    stepA,
    stepB,
    stepC,
    pipeflow.WithFailurePolicy(pipeflow.FailFast),
)

The exact constructor/options API is an implementation detail and may evolve without changing this architectural decision.

Error Behavior
WaitAll

If multiple steps fail, all errors are returned using:

errors.Join(...)
FailFast

The first non-cancellation step error is treated as the primary failure.

Cancellation errors produced by sibling steps as a consequence of fail-fast cancellation should not replace the original triggering error.

The implementation may retain additional sibling errors for diagnostics, but the triggering failure remains the primary cause.

Result Behavior

Result ordering remains deterministic.

Outputs correspond to declaration order, not completion order.

Failure policy does not change result ordering semantics.

Partial-result exposure is not defined by this ADR and will be handled separately.

Scope

This ADR defines only concurrent failure behavior.

Included:

WaitAll
FailFast
default policy
sibling cancellation semantics
error behavior
compatibility with Go context.Context

Not included:

partial-result policies
worker limits
completion-order results
retry policy changes
per-step timeouts
stream-level failure policies
dead-letter handling

These concerns will be addressed separately.

Consequences
Positive
Concurrent behavior becomes explicit and configurable.
Existing users keep current behavior by default.
Pipeflow can support different production workloads without custom concurrency code.
Fail-fast integrates naturally with Go cancellation.
Error behavior becomes more predictable.
Negative
ConcurrentSteps gains additional execution-policy complexity.
Fail-fast depends on user code cooperating with cancellation.
Concurrent error handling requires care to preserve the triggering error.
Additional tests are required for cancellation races and sibling failures.
Rationale

Concurrent workloads have different failure requirements.

Hardcoding one behavior would force applications to work around Pipeflow rather than configure it.

Providing a small set of explicit execution policies keeps the runtime predictable while preserving the principle that concurrency semantics should never be hidden or magical.
