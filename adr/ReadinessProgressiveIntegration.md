# Progressive production integration

`Integration` combines an existing Scenario, an explicit integration level,
declared boundaries, and an immutable name-and-kind allowlist. It reuses the E2E
contract preflight, Scenario execution, coverage, timing, and RunReport.

The five supported combinations are:

| Level | Ingress | Output |
| --- | --- | --- |
| SafeSample (1) | Sample | Safe capture/fake |
| RealIngress (2) | External source in safe environment | Safe capture/fake |
| StagingOutput (3) | Sample | Development/staging |
| SafeEnvironment (4) | External source in safe environment | Development/staging |
| Production (5) | Declared sample/real/production | Declared safe/staging/production |

Every verification requires exactly one ingress and at least one sink. Level 5
additionally requires at least one production boundary. Production declarations
are rejected at earlier levels, even if the allowlist contains a grant.

Sample ingress and safe capture sinks need no grants. Every external, staging,
or production boundary requires its exact name AND kind in IntegrationPolicy.
Missing grants, invalid topology, and level mismatches fail before Pipeline
execution. Contracts are checked before business functions can run. Existing
EndToEnd retains its safe-sample-only contract.

Applications explicitly choose each level; success does not automatically
promote or authorize subsequent runs. Expectations remain authoritative, with
normal PASS/FAIL/REVIEW semantics, including intentionally expected failures.
No aggregate READY verdict or automatic production write is introduced.

Boundary declarations attest to application wiring. Arbitrary Go functions
cannot be sandboxed or inspected for the true remote destination. The caller
must wire fixtures, adapters, permissions, and destinations to match declarations,
and supply explicit application policy for any real production verification.
The tool gates declared boundaries but does not provision or switch credentials.
Report names and arrangements must avoid secrets and business payloads.
