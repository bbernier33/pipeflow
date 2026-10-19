# Readiness Type-Aware Payload Mutation

## Status

Accepted for the fourth v3.x Production Readiness increment.

## Decision

The `readiness` package generates deterministic, single-change payload
mutations from a representative Go value. Mutations are selected according to
the value's actual shape: zero and nil values, string case/truncation/empty
forms, empty/reordered/duplicated slices, missing/extra string map keys, nested
field changes, and plausible primitive type changes through interface values.

Generation is bounded by configurable depth and case count. Map traversal is
sorted, exported struct fields are traversed, and the original sample is never
modified. Each generated `Mutation` temporarily carries its input value.

`MutationPlan` composes this generator with an existing Scenario and executes
one real Pipeline run per mutation. Retained results contain only path, kind,
type names, Scenario checks, and the normal payload-free RunReport. They do not
retain mutated business values.

## Deferred

The generator does not guess domain semantics. UUID versions, identifiers,
money, timestamps, acceptance/rejection classification, and persisted
regression specifications belong to semantic mutation. Multi-change and random
chaos combinations remain later milestones.
