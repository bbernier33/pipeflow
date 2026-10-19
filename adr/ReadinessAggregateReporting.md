# Aggregate readiness reporting

`BuildReadinessReport` consumes already-executed readiness result values and
applies an explicit ReadinessPolicy. It performs no execution or promotion.
Supported evidence includes contracts, Scenarios, E2E, payload/semantic mutation,
load, pressure, chaos, integration, and coverage results. Scenario-based recovery,
circuit, idempotency, and injection categories are explicitly caller-labelled;
the label is not proof of adequate assertions.

The default policy requires contracts, E2E, and coverage. Applications may
specify a nonempty Required category list for their own requirements. No
evidence is never READY. Invalid/duplicate policy categories, empty/duplicate
evidence names, unsupported result types, and mismatched category/type pairs
produce NOT READY.

Status precedence is:

1. NOT READY for failed evidence, missing required categories, critical gaps,
   tool errors, or warnings when WarningsAsFailures is set.
2. REVIEW REQUIRED for unresolved evidence or noncritical coverage gaps.
3. READY WITH WARNINGS for accepted warning messages.
4. READY when declared requirements have passed without warnings or reviews.

AllowNonCriticalGaps explicitly converts coverage-only noncritical gaps into
warnings. Critical gaps, unknown custom-StageItem warnings, and failed evidence
cannot be downgraded through this setting. Ordinary failed assertions remain
failures even if a parent result claims PASS. Expected Pipeline failures remain
governed by Scenario expectations. Semantic mutation retains its explicit
accept/reject classification, including expected rejections.

The aggregate stores detached metadata summaries, checks, run IDs, review items,
warnings, gaps, and tool errors. Load/pressure measurements and chaos seeds are
rendered in human summaries. Raw RunReports remain in original evidence; no
business inputs, outputs, callback functions, or pipelines are retained.
String renders a deterministic human report; serialization, persistence,
exporters, UI, and automatic suite scheduling are outside this increment.

READY means the declared conditions were tested successfully. It does not prove
all production requirements or authorize production access. Supply evidence from
the same application revision and explicitly require the categories and policy
branches relevant to that application. Category completeness alone does not
prove that all production integration levels were exercised.
