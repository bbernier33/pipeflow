# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- Initial Pipeflow release.
- Sequential pipeline execution.
- Stage abstraction.
- Step abstraction.
- Shared data propagation between stages.
- Pipeline builder API.
- Error propagation and early termination.
- Initial test suite.