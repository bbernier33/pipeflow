# ADR-019: Structured Parallel Execution

- **Status:** Accepted
- **Date:** 2026-08-26

## Decision

`NewParallel` creates a named structured fork/join Stage item containing named
Branches. A Branch is a sequential list of Steps and uses the same ordinary
value-flow rules as a Stage. Every Branch receives the same input value; an
empty Branch passes that value through.

After every Branch succeeds, Parallel returns `ParallelResults`, a
declaration-ordered slice of `BranchResult{Name, Value}`. Completion order never
changes join order. A following ordinary Step performs any domain-specific
aggregation, keeping Core free of generic merge rules.

`WaitAll` is the default failure policy. It waits for every Branch and joins
errors in Branch declaration order. `FailFast` cancels sibling Branch contexts
on the first observed error, waits for all started Branches, and preserves the
first observed non-cancellation failure. Cancellation remains cooperative.

Any Branch failure makes the Parallel output nil. Successful sibling payloads
are discarded; Phase 11 does not expose implicit partial business results.
Reports still show which Branches and Steps completed, failed, or were
cancelled.

Run reports contain `ParallelReport -> BranchReport -> StepReport`. The live
execution snapshot mirrors this structure. Structured errors add Parallel and
Branch identity, and lifecycle hooks expose Parallel and Branch boundaries.
Reports and events do not retain business payloads.

Phase 13 refines Context behavior: Branches inherit independent shallow
snapshots of the run Context value map while sharing execution metadata and the
logger. No Branch value-map mutations are merged.

## Boundaries

Parallel is lexical fork/join composition, not a DAG. Branches cannot depend on
sibling outputs, add arbitrary edges, address previous nodes, dynamically
schedule graph nodes, or continue independently after the join. This phase does
not add nested Pipelines, conditional routing, background execution, branch
state isolation, partial-result recovery, or branch worker limits.
