# Recovery readiness testing

The readiness package exposes `ExpectRecovery(path, RecoveryExpectation)` as an
ordinary Scenario expectation. It inspects existing Step and Recovery reports;
Core owns execution, retry, remediation limits, timeouts, and cleanup.

The expectation checks the exact number and ordering of recovery activations.
Each activation may constrain its name, status, decision, original failure, and
recovery failure. Error identity uses `errors.Is`. Optional Step attempt count
and terminal status verify the original operation reran after a RETRY_STEP.
An empty activation list explicitly asserts that recovery did not run.

Paths include the Pipeline and containing Stages, Subflows, Parallel groups,
and Branches. Missing or ambiguous paths fail rather than matching an unrelated
Step by name. ConcurrentSteps use their existing Stage-level Step report paths.
Names containing slashes can produce ambiguous paths, which are rejected.

Applications compose these checks with controlled function injection and normal
Scenario checks for business output, nested recovery Steps, failure scope,
skipped work, and finalization. A completed recovery Stage can choose FAIL_STEP;
its completed status describes remediation execution, not successful business
processing. `RunSucceeds` should be added when pipeline continuation is required.

Tests cover successful repair after exhausted retries, all terminal decisions,
repair failure, recovery timeout, persistent failure with bounded remediation,
parent cancellation with cleanup, nested paths, and mismatched evidence.
No business payload or second runtime is introduced.
