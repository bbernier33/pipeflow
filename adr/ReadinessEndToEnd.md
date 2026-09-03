# Readiness End-to-End Verification

## Status

Accepted for the third v3.x Production Readiness increment.

## Decision

`readiness.EndToEnd` composes the existing Scenario and contract inspection
models. Before execution it requires exactly one named sample ingress and at
least one named safe sink, then performs contract preflight. A failing contract
or unsafe/incomplete boundary declaration prevents execution.

The real Pipeline executes once through the Scenario. Its explicit expectations
remain authoritative, so an expected execution failure can pass. The result
retains the normal payload-free RunReport and derives ordered coverage entries
for Pipeline, Stage, Step, Parallel, Branch, and Subflow paths, including status,
duration, attempts, and structured errors.

## Safety boundary

This increment supports only caller-controlled sample ingress and sinks where
production writes cannot occur. Pipeflow cannot prove that an arbitrary Go
function is safe; the declaration is an explicit application-owned assertion,
which should be paired with Scenario expectations that inspect the safe sink.

Real ingress, staging output, gated production boundaries, and promotion levels
remain deferred to the progressive production integration milestone.
