# Untested-path detection

`DetectUntestedPaths` compares the current Pipeline topology with a collection
of terminal RunReports. Each declared Step implicitly requires an executed
attempt. Skipped, pending, absent, and merely running Steps do not satisfy that
requirement. ConcurrentSteps are flattened consistently with Core report paths;
Subflow and Parallel/Branch paths remain scoped.

Applications explicitly declare required policy paths using CoverageRequirement.
Supported evidence includes skipped conditions, normal retry, timeout,
cancellation, Recovery decisions/timeouts, circuit opening/probes/closing/
short-circuiting, and idempotency completion/duplicate/in-progress/release.
Describe does not expose all dormant policies, so the tool cannot automatically
infer every configured branch. Declare the meaningful variants in the test
specification, including critical remediation and duplicate-handling paths.

Missing required evidence produces REVIEW. Critical gaps produce FAIL. Unknown
requirements, ambiguous paths, foreign reports, and unfinished run snapshots
produce FAIL with a tool error. Custom StageItems produce an explicit REVIEW
warning because their internal topology is opaque. Results contain metadata
only; no report or payload is retained.

PASS describes coverage, not correctness: a failed attempt still exercises an
execution path. Compose this assessment with Scenario assertions and eventual
aggregate readiness policy. The caller must supply evidence from the same
Pipeline revision; RunReports do not contain a definition fingerprint. Remote
boundary promotion and integration-level coverage are not inferred from Step
reports. No hidden policy branch is claimed covered from mere successful flow.
