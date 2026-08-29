# ADR-027: Recursive Pre-execution Validation

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

`Pipeline.Validate()` validates structure, policies, and statically knowable
value flow without requiring an initial value. `Pipeline.ValidateInput(input)`
performs the same pass with the actual initial Go value. `Run`,
`RunWithReport`, and `Start` call input-aware validation before allocating a
recorder, marking Pipeflow Context state as running, or invoking lifecycle and
workload code.

Standalone Stage and Subflow definitions expose matching `Validate` and
`ValidateInput` methods. Direct `Run` calls use the input-aware form.

Validation recursively checks:

- Step action, condition, polling, metadata, retry, and rate-limit
  configuration errors;
- adjacent typed value flow, including ConcurrentSteps' `[]any` output;
- initial input type and nil assignability;
- nil StageItems and Steps;
- negative ConcurrentSteps worker counts;
- invalid concurrent and Parallel failure policies;
- conflicting named rate-limit policies;
- empty and duplicate operational identifiers within their parent scope;
- background and finalizer identity and background policy configuration.

Duplicate checks are scoped. Stage names are unique within a Pipeline or
Subflow. Top-level Step names, Parallel names, and Subflow names are each
unique among siblings in a Stage. Branch names are unique within a Parallel,
and Step names are unique within a Branch. Definitions remain reusable in
different parent scopes.

## Dynamic Boundaries

Legacy StageItems and legacy Step adapters intentionally make subsequent flow
types unknown. Conditional transformers whose executed and skipped types
differ have the same boundary. Validation resumes only where a type becomes
statically known again; otherwise existing runtime assignability checks return
structured errors.

`Validate()` cannot reject an unknown initial input expected by the first
consumer. `ValidateInput()` can. An untyped nil input is accepted only by Go
nilable parameter types.

## Compatibility

Zero remains the default unlimited worker count. Negative worker counts become
configuration errors. Existing documented normalization and disabling rules
for retry counts/delays, polling limits/intervals, and non-positive timeouts are
preserved.

## Boundaries

Validation does not invoke actions, predicates, metadata extractors, hooks,
backgrounds, or finalizers. It adds no schema language, serialization,
configuration loader, DAG analysis, side-effect detection, business-rule
validation, or guarantee about dynamically typed user code.
