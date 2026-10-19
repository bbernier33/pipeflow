package readiness

import (
	"fmt"

	"github.com/bbernier33/pipeflow"
)

// CoverageBehavior identifies a report-observable execution path through a Step.
type CoverageBehavior string

const (
	CoverageExecuted              CoverageBehavior = "executed"
	CoverageSkipped               CoverageBehavior = "skipped"
	CoverageRetried               CoverageBehavior = "retried"
	CoverageTimeout               CoverageBehavior = "timeout"
	CoverageCancelled             CoverageBehavior = "cancelled"
	CoverageRecoveryRetryStep     CoverageBehavior = "recovery_retry_step"
	CoverageRecoveryFailStep      CoverageBehavior = "recovery_fail_step"
	CoverageRecoveryFailStage     CoverageBehavior = "recovery_fail_stage"
	CoverageRecoveryFailPipeline  CoverageBehavior = "recovery_fail_pipeline"
	CoverageRecoveryTimeout       CoverageBehavior = "recovery_timeout"
	CoverageCircuitOpened         CoverageBehavior = "circuit_opened"
	CoverageCircuitShortCircuited CoverageBehavior = "circuit_short_circuited"
	CoverageCircuitProbe          CoverageBehavior = "circuit_probe"
	CoverageCircuitClosed         CoverageBehavior = "circuit_closed"
	CoverageIdempotencyCompleted  CoverageBehavior = "idempotency_completed"
	CoverageIdempotencyDuplicate  CoverageBehavior = "idempotency_duplicate_completed"
	CoverageIdempotencyInProgress CoverageBehavior = "idempotency_in_progress"
	CoverageIdempotencyReleased   CoverageBehavior = "idempotency_released"
)

// CoverageRequirement names a required behavior. Critical gaps cause FAIL;
// otherwise a gap requires REVIEW. Every topology Step implicitly requires
// CoverageExecuted; an explicit requirement can mark that path critical.
type CoverageRequirement struct {
	Path     string
	Behavior CoverageBehavior
	Critical bool
}

// PathCoverage describes observations of a declared Step across completed runs.
type PathCoverage struct {
	Path      string
	Exercised bool
	Statuses  []pipeflow.Status
}

// CoverageGap is one required behavior that the supplied reports never exercised.
type CoverageGap struct {
	CoverageRequirement
	Reason string
}

// CoverageResult retains metadata-only coverage and requirements, never payloads.
// PASS means declared coverage is satisfied, not that execution was correct.
type CoverageResult struct {
	Verdict      Verdict
	Paths        []PathCoverage
	Requirements []CoverageRequirement
	Gaps         []CoverageGap
	Warnings     []string
	Error        error
}

// DetectUntestedPaths compares the current topology with terminal RunReports.
// Policy branches must be explicitly declared: Describe does not expose dormant
// policies. Caller-owned reports must come from the same Pipeline revision.
// Missing evidence yields REVIEW; critical gaps or invalid evidence yield FAIL.
func DetectUntestedPaths(pipeline *pipeflow.Pipeline, reports []pipeflow.RunReport, required ...CoverageRequirement) CoverageResult {
	result := CoverageResult{Verdict: VerdictFail}
	if pipeline == nil {
		result.Error = errNilPipeline
		return result
	}
	if err := pipeline.Validate(); err != nil {
		result.Error = err
		return result
	}
	definition := pipeline.Describe()
	paths, warnings := topologyStepPaths(definition)
	result.Warnings = warnings
	known := make(map[string]bool, len(paths))
	positions := make(map[CoverageRequirement]int)
	for _, path := range paths {
		if known[path] {
			result.Error = fmt.Errorf("readiness: ambiguous coverage path %q", path)
			return result
		}
		known[path] = true
		requirement := CoverageRequirement{Path: path, Behavior: CoverageExecuted}
		positions[requirement] = len(result.Requirements)
		result.Requirements = append(result.Requirements, requirement)
	}
	for _, requirement := range required {
		if !known[requirement.Path] || !validCoverageBehavior(requirement.Behavior) {
			result.Error = fmt.Errorf("readiness: invalid coverage requirement %q (%s)", requirement.Path, requirement.Behavior)
			return result
		}
		key := requirement
		key.Critical = false
		if index, exists := positions[key]; exists {
			result.Requirements[index].Critical = result.Requirements[index].Critical || requirement.Critical
		} else {
			positions[key] = len(result.Requirements)
			result.Requirements = append(result.Requirements, requirement)
		}
	}
	observations := make(map[string][]pipeflow.StepReport)
	for _, report := range reports {
		if report.RunID == "" || report.Pipeline != definition.Name || !terminalCoverageStatus(report.Status) {
			result.Error = fmt.Errorf("readiness: coverage requires terminal reports with Run IDs for pipeline %q", definition.Name)
			return result
		}
		for path, steps := range reportSteps(report) {
			if !known[path] || len(steps) != 1 {
				result.Error = fmt.Errorf("readiness: unknown or ambiguous report Step path %q", path)
				return result
			}
			observations[path] = append(observations[path], steps...)
		}
	}
	result.Verdict = VerdictPass
	if len(warnings) > 0 {
		result.Verdict = VerdictReview
	}
	for _, path := range paths {
		coverage := PathCoverage{Path: path}
		seen := make(map[pipeflow.Status]bool)
		for _, step := range observations[path] {
			if !seen[step.Status] {
				coverage.Statuses = append(coverage.Statuses, step.Status)
				seen[step.Status] = true
			}
			coverage.Exercised = coverage.Exercised || observedCoverage(step, CoverageExecuted)
		}
		result.Paths = append(result.Paths, coverage)
	}
	for _, requirement := range result.Requirements {
		covered := false
		for _, step := range observations[requirement.Path] {
			if observedCoverage(step, requirement.Behavior) {
				covered = true
				break
			}
		}
		if !covered {
			result.Gaps = append(result.Gaps, CoverageGap{CoverageRequirement: requirement, Reason: "required behavior not observed in supplied reports"})
			if requirement.Critical {
				result.Verdict = VerdictFail
			} else {
				result.Verdict = combine(result.Verdict, VerdictReview)
			}
		}
	}
	return result
}

func topologyStepPaths(root pipeflow.Description) ([]string, []string) {
	var paths []string
	var warnings []string
	var visit func(string, pipeflow.Description)
	visit = func(parent string, node pipeflow.Description) {
		path := parent
		if node.Kind != pipeflow.DescriptionConcurrent {
			if parent == "" {
				path = node.Name
			} else {
				path = parent + "/" + node.Name
			}
		}
		if node.Kind == pipeflow.DescriptionStep {
			paths = append(paths, path)
		}
		if node.Kind == pipeflow.DescriptionCustomItem {
			warnings = append(warnings, fmt.Sprintf("%s: custom StageItem %s has no inspectable Step topology", parent, node.Type))
		}
		for _, child := range node.Children {
			visit(path, child)
		}
	}
	visit("", root)
	return paths, warnings
}

func terminalCoverageStatus(status pipeflow.Status) bool {
	return status == pipeflow.StatusCompleted || status == pipeflow.StatusFailed || status == pipeflow.StatusTimeout || status == pipeflow.StatusCancelled
}

func validCoverageBehavior(behavior CoverageBehavior) bool {
	switch behavior {
	case CoverageExecuted, CoverageSkipped, CoverageRetried, CoverageTimeout, CoverageCancelled, CoverageRecoveryRetryStep, CoverageRecoveryFailStep, CoverageRecoveryFailStage, CoverageRecoveryFailPipeline, CoverageRecoveryTimeout, CoverageCircuitOpened, CoverageCircuitShortCircuited, CoverageCircuitProbe, CoverageCircuitClosed, CoverageIdempotencyCompleted, CoverageIdempotencyDuplicate, CoverageIdempotencyInProgress, CoverageIdempotencyReleased:
		return true
	default:
		return false
	}
}

func observedCoverage(step pipeflow.StepReport, behavior CoverageBehavior) bool {
	switch behavior {
	case CoverageExecuted:
		for _, attempt := range step.Attempts {
			if terminalCoverageStatus(attempt.Status) {
				return true
			}
		}
	case CoverageSkipped:
		return step.Status == pipeflow.StatusSkipped
	case CoverageRetried:
		for _, attempt := range step.Attempts {
			if attempt.Attempt > 1 && terminalCoverageStatus(attempt.Status) {
				return true
			}
		}
	case CoverageTimeout:
		if step.Status == pipeflow.StatusTimeout {
			return true
		}
		for _, attempt := range step.Attempts {
			if attempt.Status == pipeflow.StatusTimeout {
				return true
			}
		}
	case CoverageCancelled:
		return step.Status == pipeflow.StatusCancelled
	case CoverageRecoveryTimeout:
		for _, recovery := range step.Recoveries {
			if recovery.Status == pipeflow.StatusTimeout {
				return true
			}
		}
	case CoverageRecoveryRetryStep, CoverageRecoveryFailStep, CoverageRecoveryFailStage, CoverageRecoveryFailPipeline:
		decision := map[CoverageBehavior]pipeflow.RecoveryDecision{CoverageRecoveryRetryStep: pipeflow.RecoveryRetryStep, CoverageRecoveryFailStep: pipeflow.RecoveryFailStep, CoverageRecoveryFailStage: pipeflow.RecoveryFailStage, CoverageRecoveryFailPipeline: pipeflow.RecoveryFailPipeline}[behavior]
		for _, recovery := range step.Recoveries {
			if recovery.Decision == decision && recovery.Status == pipeflow.StatusCompleted && recovery.Error == nil {
				return true
			}
		}
	case CoverageCircuitOpened:
		return step.Circuit != nil && !step.Circuit.ShortCircuited && step.Circuit.StateBefore != pipeflow.CircuitOpen && step.Circuit.StateAfter == pipeflow.CircuitOpen
	case CoverageCircuitShortCircuited:
		return step.Circuit != nil && step.Circuit.ShortCircuited
	case CoverageCircuitProbe:
		return step.Circuit != nil && step.Circuit.Probe
	case CoverageCircuitClosed:
		return step.Circuit != nil && step.Circuit.Probe && step.Circuit.StateAfter == pipeflow.CircuitClosed
	case CoverageIdempotencyCompleted, CoverageIdempotencyDuplicate, CoverageIdempotencyInProgress, CoverageIdempotencyReleased:
		outcome := map[CoverageBehavior]pipeflow.IdempotencyOutcome{CoverageIdempotencyCompleted: pipeflow.IdempotencyExecuted, CoverageIdempotencyDuplicate: pipeflow.IdempotencyDuplicateCompleted, CoverageIdempotencyInProgress: pipeflow.IdempotencyDuplicateInProgress, CoverageIdempotencyReleased: pipeflow.IdempotencyFailedReleasable}[behavior]
		return step.Idempotency != nil && step.Idempotency.Outcome == outcome
	}
	return false
}
