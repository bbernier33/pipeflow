# ADR-014: Polling / Wait Until

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

Polling is an optional Step execution policy configured with `WithPolling` and
`PollPolicy`:

```go
type PollPolicy struct {
    Every    time.Duration
    MaxPolls int
    Timeout  time.Duration
    Until    any
}
```

`Until` accepts an ordinary `func() bool` or `func(T) bool`. Predicate types are
validated against statically known Step output types before execution and
against dynamic values at invocation time where necessary.

Polling is distinct from retry. Each poll runs the Step operation. If the
operation fails, the existing retry policy applies within that poll. If the
operation succeeds but the predicate is false, the poll is complete and the
next poll begins after `Every`. A satisfying operation output becomes the
Step's normal flowing output.

`MaxPolls <= 0` means no count limit. `Timeout <= 0` disables the polling-only
deadline. `Every < 0` is normalized to zero. Polling observes the earliest
parent, Pipeline, Stage, Step, or polling deadline.

Exhausting MaxPolls returns `ErrPollLimitExceeded`. Timeout and cancellation
use standard Go context errors. All errors retain structured execution
location; polling errors additionally record the poll number.

## Reporting

`StepReport.Polls` contains `PollReport` values with poll number, status,
completion flag, timing, attempts, and structured error. The existing
`StepReport.Attempts` remains a flattened history. `CurrentStep` exposes the
active Poll and Attempt numbers. Reports contain no polled business values.

## Boundaries

This phase does not add predicate expression languages, YAML predicates,
dynamic polling control, progressive/backoff intervals, jitter, result
retention, polling callbacks, or per-poll payloads. Live execution state
remains observation-only.
