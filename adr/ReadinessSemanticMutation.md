# Readiness Semantic Mutation

## Status

Accepted for the fifth v3.x Production Readiness increment.

## Decision

Domain semantics remain application-owned. A `SemanticCase` names a mutated
input and path, then declares expected ACCEPT, REJECT, or UNSPECIFIED behavior.
`SemanticPlan` executes each case through the real Pipeline Scenario and compares
the expectation with observed completion or rejection.

UNSPECIFIED always produces REVIEW. Developers can turn an exploratory result
into a deterministic regression specification by committing an explicit ACCEPT
or REJECT expectation in ordinary Go code.

An optional payload-free evidence check can verify why a case was accepted or
rejected using its Scenario result and RunReport. This prevents a case from
passing solely because an unrelated failure happened to reject it. Evidence
check panics and invalid verdicts become failures.

Mutated inputs exist only inside the plan. `SemanticCaseResult` retains the case
name, path, expected and observed behavior, verdict, evidence check, and normal
payload-free Scenario evidence; it does not expose the input.

## Consequences

Pipeflow does not guess that a string is a UUID, timestamp, provider identifier,
or monetary value. Applications can encode those meanings without adding domain
concepts to Core or the readiness package. Specification serialization and
external persistence remain deferred.
