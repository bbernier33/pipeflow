# Readiness Scenario Model

## Status

Accepted for the first v3.x Production Readiness increment.

## Decision

Production-readiness testing lives in the additive `readiness` package. A
`Scenario` names an arrangement, executes a real `Pipeline`, and evaluates one
or more explicit expectations against an `Observation` containing the output,
`RunReport`, and execution error.

Execution errors are evidence rather than automatic scenario failures. This is
necessary because a correctly handled injected failure can be the behavior a
scenario expects. Every scenario must therefore declare at least one
expectation; `RunSucceeds` covers the normal success case.

Checks return PASS, FAIL, or REVIEW. Overall precedence is FAIL, then REVIEW,
then PASS. Check panics and invalid verdicts become deterministic failures.

Business output is available only while checks execute. The retained `Result`
stores the existing payload-free `RunReport`, check summaries, timing, verdict,
and tool errors. It does not retain the business output.

## Deferred

Suites, aggregate readiness policy, contract reports, injection, mutation,
load, pressure, resilience-specific checks, chaos, persistence, CLI, and final
READY classification remain later v3.x increments.
