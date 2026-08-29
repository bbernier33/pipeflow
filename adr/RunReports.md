# ADR-011: Run IDs and Structured Run Reports

- **Status:** Accepted
- **Date:** 2026-08-25

## Decision

`Pipeline.RunWithReport` returns `(output any, report RunReport, err error)`.
`Pipeline.Run` remains compatible and delegates to the same execution path.
The Pipeline does not retain a mutable last report, allowing independent runs
without pipeline-level report state.

Every execution that starts receives a random 128-bit hexadecimal Run ID and a
canonical in-memory report. The report hierarchy is:

```text
RunReport
└── StageReport
    └── StepReport
        ├── AttemptReport
        └── PollReport
            └── AttemptReport
```

Structured parallel execution adds this hierarchy beneath a Stage:

```text
StageReport
└── ParallelReport
    └── BranchReport
        └── StepReport
            ├── AttemptReport
            └── PollReport
```

Managed background work is recorded in declaration order under
`RunReport.Backgrounds`. It contains execution status and timing but no worker
payloads.

Each level records status, start/end time, duration, and an error where
applicable. Reports use the structured errors introduced in ADR-010. Attempt
history records every invoked attempt in numerical order, including failed
attempts preceding eventual success. An attempt followed by a retry also
records the selected retry delay.

Polling Steps additionally record per-poll facts. The Step-level Attempts
slice remains a flattened compatibility view of attempts across all polls.

The final statuses are `completed`, `failed`, `skipped`, `cancelled`, and
`timeout`; `pending` and `running` are also defined execution states. Work not
started because an earlier unit failed or cancellation occurred is `skipped`.
Work intentionally omitted by a false Step condition is also `skipped`; it has
zero timing values and no attempt or poll history. `context.DeadlineExceeded` is `timeout`,
and `context.Canceled` is `cancelled`. A joined error containing any business
failure is `failed` rather than being downgraded to cancellation.

Concurrent Step reports remain in declaration order. The run-local recorder is
mutex-protected so concurrent children can record timing and attempts safely.

## Boundaries

Reports never retain Step input, output, or other flowing business payloads.
Phase 18 permits only explicitly extracted, size- and type-bounded scalar
result metadata on successful Step and Stage reports.
This phase does not add serialization, persistence, sinks, metrics exporters,
UI rendering, live progress APIs, panic recovery, or result metadata.

Configuration and validation failures occur before a run starts and therefore
return a zero `RunReport`.
