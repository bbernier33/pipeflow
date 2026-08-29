# ADR-021: Scoped Branch State

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

At each `Parallel` invocation, Pipeflow snapshots the shared Context value map
once before launching Branch goroutines. Every Branch receives a distinct child
Context initialized from that common snapshot.

The snapshot is shallow. Map entries are copied, so `Set` operations and key
replacement remain Branch-local. Referenced values—including pointers, maps,
slices, channels, and objects containing mutable state—retain their ordinary Go
identity. Applications must synchronize those shared objects normally.

Sequential Steps within one Branch receive the same child Context and therefore
see earlier mutations from that Branch. Sibling Branches never see those
value-map mutations, and the parent Context is unchanged after the join.

Branch Contexts share the run's synchronized logger and execution metadata.
Status and timing therefore continue to describe the owning Pipeline run rather
than becoming misleading independent Branch executions. Structured Branch
status remains available through reports and `Execution.Current()`.

Pipeflow performs no implicit merge, conflict detection, or last-writer-wins
behavior. Business data crosses the join explicitly through
`ParallelResults`. Context does not provide hidden Branch-output communication.

## Contract

```text
shared Context values at Parallel start
├── shallow copy -> Branch A Context -> local mutations only
└── shallow copy -> Branch B Context -> local mutations only

join -> ParallelResults
     -> no Context merge
```

## Boundaries

This phase does not add merge callbacks, exported Context cloning, deep-copy
reflection, conflict resolution, sibling lookup, nested scopes, transactional
state, or automatic promotion of Branch values to shared execution state.
