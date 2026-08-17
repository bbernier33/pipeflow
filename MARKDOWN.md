# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [v0.7.0] - 2026-08-17

### Added

#### Concurrent Execution Failure Policies

- Added configurable failure policies for `ConcurrentSteps`.
- Added `FailurePolicy`.
- Added `WaitAll` failure policy.
- Added `FailFast` failure policy.
- Added `WithFailurePolicy(...)` concurrent-step option.
- Added fail-fast sibling cancellation using Go `context.Context`.
- Added tests covering concurrent failure-policy behavior.

### Changed

- `WaitAll` is the default `ConcurrentSteps` failure policy, preserving previous concurrent execution behavior.
- `NewConcurrentSteps(...)` now accepts a `[]*Step` followed by optional `ConcurrentStepsOption` values.
- Concurrent result ordering remains based on step declaration order rather than completion order.

### Behavior

#### WaitAll

- All concurrent steps are allowed to finish.
- Errors from failed steps are collected.
- Multiple errors are combined using `errors.Join(...)`.

#### FailFast

- The first step failure triggers cancellation of the concurrent group.
- Sibling steps receive the cancellation signal through `context.Context`.
- Running goroutines are not forcibly terminated; cancellation remains cooperative.
- The original triggering failure remains the primary failure rather than being replaced by sibling cancellation errors.


---

## [v0.6.0] - 2026-08-15

### Added

#### Cancellation Support

- Added Go `context.Context` to pipeline execution.
- Added cancellation context propagation through `Pipeline`, `Stage`, `StageItem`, `ConcurrentSteps`, and `Step`.
- Updated step actions to receive `context.Context`.
- Added cancellation-aware retry delays.
- Added cancellation checks before retry attempts.
- Added support for propagating `context.Canceled` and `context.DeadlineExceeded`.

---

## [v0.5.0] - 2026-08-01

### Added

#### Step Retry Policies

- Added configurable retry policies for steps.
- Added `RetryPolicy` with configurable `MaxAttempts` and `Delay`.
- Added `WithRetry` step option.
- Added fixed-delay retries for failed step executions.
- Added retry lifecycle logging.
- Added tests for retry success, exhaustion, and immediate success.

### Changed

- Updated `Step` execution to support optional retry policies.
- Steps without a retry policy continue to execute once by default.

---

## [v0.4.0] - 2026-07-25

### Added

#### Concurrent Execution

- Added `ConcurrentSteps` execution primitive.
- Added concurrent step execution within a stage.
- Added comprehensive `ConcurrentSteps` test suite.

### Changed

- Introduced the `StageItem` abstraction.
- Updated `Stage` to execute `StageItem`s instead of only `Step`s.
- Updated `NewStep()` to return `*Step`.

---

## [v0.3.0] - 2026-07-25

### Added

#### Pipeline Lifecycle Hooks

- Added pipeline lifecycle hooks for execution events.
- Added `OnStarted` hook.
- Added `OnCompleted` hook.
- Added `OnFailed` hook.
- Added `PipelineEvent` for hook callbacks.
- Support multiple hook handlers per lifecycle event.
- Integrated hooks into the pipeline execution lifecycle.

---

## [v0.2.0] - 2026-07-25

### Added

#### Execution Context

- Added shared execution `Context`.
- Added key/value storage shared across all stages and steps.
- Added configurable logger support.
- Added default logger implementation.

#### Execution Lifecycle

- Added execution status tracking.
    - `Pending`
    - `Running`
    - `Completed`
    - `Failed`
- Added execution timing.
    - Start time
    - End time
    - Duration
- Added current stage tracking.
- Added current step tracking.

---

## [v0.1.0] - 2026-07-24

### Added

#### Core Pipeline Engine

- Sequential pipeline execution
- Concurrent step execution
- Pipeline → Stage → StageItem architecture
- Shared execution context
- Shared data propagation
- Pipeline lifecycle hooks
- Execution timing
- Logging
- Early error propagation
