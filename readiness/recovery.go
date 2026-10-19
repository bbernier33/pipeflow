package readiness

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bbernier33/pipeflow"
)

// RecoveryExpectation specifies the exact observed Recovery sequence for a Step.
// Runs may be empty to assert that Recovery never activated. Attempts is optional
// (zero disables the check); it counts original Step invocations across recovery.
type RecoveryExpectation struct {
	Attempts int
	Status   pipeflow.Status
	Runs     []RecoveryRunExpectation
}

// RecoveryRunExpectation describes one ordered remediation activation.
// Empty Name, Status, and Decision fields are unconstrained. OriginalError and
// Error, when supplied, are checked with errors.Is. Use a custom Scenario check
// for additional nested recovery-stage evidence.
type RecoveryRunExpectation struct {
	Name          string
	Status        pipeflow.Status
	Decision      pipeflow.RecoveryDecision
	OriginalError error
	Error         error
}

// ExpectRecovery checks a Step identified by its complete report path, such as
// pipeline/stage/step or pipeline/stage/parallel/branch/step. Ambiguous or missing
// paths fail. Paths must match report names exactly.
func ExpectRecovery(path string, expected RecoveryExpectation) Expectation {
	expected.Runs = append([]RecoveryRunExpectation(nil), expected.Runs...)
	return Expect("recovery at "+path, func(observed Observation) Assessment {
		steps := reportSteps(observed.Report)
		matches := steps[path]
		if len(matches) != 1 {
			return Failed(fmt.Sprintf("expected one Step at %q, found %d", path, len(matches)))
		}
		step := matches[0]
		if expected.Attempts < 0 {
			return Failed("attempt count cannot be negative")
		}
		if expected.Attempts > 0 && len(step.Attempts) != expected.Attempts {
			return Failed(fmt.Sprintf("Step attempts: got %d, want %d", len(step.Attempts), expected.Attempts))
		}
		if expected.Status != "" && step.Status != expected.Status {
			return Failed(fmt.Sprintf("Step status: got %s, want %s", step.Status, expected.Status))
		}
		if len(step.Recoveries) != len(expected.Runs) {
			return Failed(fmt.Sprintf("Recovery activations: got %d, want %d", len(step.Recoveries), len(expected.Runs)))
		}
		for index, want := range expected.Runs {
			got := step.Recoveries[index]
			var mismatches []string
			if want.Name != "" && want.Name != got.Name {
				mismatches = append(mismatches, "name")
			}
			if want.Status != "" && want.Status != got.Status {
				mismatches = append(mismatches, "status")
			}
			if want.Decision != "" && want.Decision != got.Decision {
				mismatches = append(mismatches, "decision")
			}
			if want.OriginalError != nil && !errors.Is(got.OriginalError, want.OriginalError) {
				mismatches = append(mismatches, "original error")
			}
			if want.Error != nil && !errors.Is(got.Error, want.Error) {
				mismatches = append(mismatches, "recovery error")
			}
			if len(mismatches) != 0 {
				return Failed(fmt.Sprintf("Recovery activation %d mismatched %s", index+1, strings.Join(mismatches, ", ")))
			}
		}
		return Passed(fmt.Sprintf("%d Recovery activations and %d Step attempts match", len(step.Recoveries), len(step.Attempts)))
	})
}

func reportSteps(report pipeflow.RunReport) map[string][]pipeflow.StepReport {
	result := make(map[string][]pipeflow.StepReport)
	add := func(path string, step pipeflow.StepReport) { result[path] = append(result[path], step) }
	var stages func(string, []pipeflow.StageReport)
	stages = func(parent string, reports []pipeflow.StageReport) {
		for _, stage := range reports {
			path := parent + "/" + stage.Name
			for _, step := range stage.Steps {
				add(path+"/"+step.Name, step)
			}
			for _, parallel := range stage.Parallels {
				for _, branch := range parallel.Branches {
					for _, step := range branch.Steps {
						add(path+"/"+parallel.Name+"/"+branch.Name+"/"+step.Name, step)
					}
				}
			}
			for _, subflow := range stage.Subflows {
				stages(path+"/"+subflow.Name, subflow.Stages)
			}
		}
	}
	stages(report.Pipeline, report.Stages)
	return result
}
