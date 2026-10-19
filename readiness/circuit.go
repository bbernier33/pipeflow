package readiness

import (
	"errors"
	"fmt"

	"github.com/bbernier33/pipeflow"
)

// CircuitExpectation specifies exact circuit evidence for one Step invocation.
// Circuit fields, including Probe and ShortCircuited, are all compared exactly.
// Status and Error are optional constraints on the Step's terminal outcome.
type CircuitExpectation struct {
	Circuit pipeflow.CircuitReport
	Status  pipeflow.Status
	Error   error
}

// ExpectCircuit checks circuit evidence at a complete Step report path. Missing
// circuit evidence and missing or ambiguous paths fail. Snapshots describe the
// state observed by that invocation, not the breaker's current global state.
func ExpectCircuit(path string, expected CircuitExpectation) Expectation {
	return Expect("circuit at "+path, func(observed Observation) Assessment {
		matches := reportSteps(observed.Report)[path]
		if len(matches) != 1 {
			return Failed(fmt.Sprintf("expected one Step at %q, found %d", path, len(matches)))
		}
		step := matches[0]
		if step.Circuit == nil {
			return Failed("Step has no circuit report")
		}
		if *step.Circuit != expected.Circuit {
			return Failed(fmt.Sprintf("circuit evidence: got %+v, want %+v", *step.Circuit, expected.Circuit))
		}
		if expected.Status != "" && step.Status != expected.Status {
			return Failed(fmt.Sprintf("Step status: got %s, want %s", step.Status, expected.Status))
		}
		if expected.Error != nil && !errors.Is(step.Error, expected.Error) {
			return Failed(fmt.Sprintf("Step error does not match %v", expected.Error))
		}
		return Passed(fmt.Sprintf("dependency %q: %s -> %s, probe=%t, short-circuited=%t", step.Circuit.Dependency, step.Circuit.StateBefore, step.Circuit.StateAfter, step.Circuit.Probe, step.Circuit.ShortCircuited))
	})
}
