package readiness

import (
	"errors"
	"fmt"

	"github.com/bbernier33/pipeflow/v3"
)

// IdempotencyExpectation specifies exact guard, claim, and outcome evidence.
// Empty Claim matches an empty reported Claim (for example, duplicate work).
// Status, Error, and StoreError are optional terminal/error identity constraints.
type IdempotencyExpectation struct {
	Guard      string
	Claim      pipeflow.IdempotencyClaimState
	Outcome    pipeflow.IdempotencyOutcome
	Status     pipeflow.Status
	Error      error
	StoreError error
}

// ExpectIdempotency checks a complete Step report path using the standard
// Scenario expectation model. Missing evidence or missing/ambiguous paths fail.
// Guard reports contain no idempotency keys or business values. Side-effect
// counts and backing-store durability require application-specific checks.
func ExpectIdempotency(path string, expected IdempotencyExpectation) Expectation {
	return Expect("idempotency at "+path, func(observed Observation) Assessment {
		matches := reportSteps(observed.Report)[path]
		if len(matches) != 1 {
			return Failed(fmt.Sprintf("expected one Step at %q, found %d", path, len(matches)))
		}
		step := matches[0]
		got := step.Idempotency
		if got == nil {
			return Failed("Step has no idempotency report")
		}
		if got.Guard != expected.Guard || got.Claim != expected.Claim || got.Outcome != expected.Outcome {
			return Failed(fmt.Sprintf("guard/claim/outcome: got %q/%q/%q, want %q/%q/%q", got.Guard, got.Claim, got.Outcome, expected.Guard, expected.Claim, expected.Outcome))
		}
		if expected.Status != "" && step.Status != expected.Status {
			return Failed(fmt.Sprintf("Step status: got %s, want %s", step.Status, expected.Status))
		}
		if expected.Error != nil && !errors.Is(step.Error, expected.Error) {
			return Failed(fmt.Sprintf("Step error does not match %v", expected.Error))
		}
		if expected.StoreError != nil && !errors.Is(got.Error, expected.StoreError) {
			return Failed(fmt.Sprintf("store error does not match %v", expected.StoreError))
		}
		return Passed(fmt.Sprintf("guard %q: claim=%s, outcome=%s", got.Guard, got.Claim, got.Outcome))
	})
}
