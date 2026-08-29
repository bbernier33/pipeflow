# ADR-022: Conditional Step Execution

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

Conditional execution is an optional Step policy configured with
`WithCondition`. It accepts an ordinary `func() bool` or `func(T) bool`.
Pipeflow validates typed predicate inputs against statically known flow where
possible and performs the normal runtime assignability check otherwise.

The predicate is evaluated exactly once before the Step action, retries, and
polling. A true result follows normal Step execution. A false result skips the
action and passes the current flowing value through unchanged. Parent context
cancellation or timeout is checked before and immediately after predicate
evaluation, so cancellation is not disguised as a skip.

A skipped Step is recorded explicitly as `StatusSkipped`, with zero start/end
times, zero duration, no error, and no attempt or poll reports. It emits a
`StepSkipped` lifecycle event and does not emit `StepStarted`, `StepCompleted`,
or `StepFailed`. Reports and lifecycle events continue to exclude business
payloads.

## Flow Validation

For a pass-through Step, conditional execution does not change its static flow
type. A conditional transformer whose input and output types differ has two
possible results: the input type when skipped and the output type when run.
The existing linear validator represents that following flow as unknown rather
than inventing a union type. Downstream input compatibility is then enforced
at runtime with the existing structured Step error location.

## Boundaries

This decision adds no rule or expression language, else branch, automatic
branching, Stage or Pipeline conditions, dynamic policy mutation, retained
condition result, or cross-Step communication. Structured branches remain the
explicit composition mechanism when both outcomes need work.
