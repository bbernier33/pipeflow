package readiness

import "github.com/bbernier33/pipeflow"

// ContractResult is a payload-free pre-execution topology and contract review.
type ContractResult struct {
	Verdict    Verdict
	Topology   pipeflow.Description
	Boundaries []pipeflow.FlowBoundary
	Error      error
}

// InspectContracts inspects contracts without assuming an initial input type.
func InspectContracts(pipeline *pipeflow.Pipeline) ContractResult {
	if pipeline == nil {
		return ContractResult{Verdict: VerdictFail, Error: errNilPipeline}
	}
	return contractResult(pipeline.InspectFlow())
}

// InspectContractsInput inspects contracts using the supplied initial value.
// A nil value is treated as a known nil input.
func InspectContractsInput(pipeline *pipeflow.Pipeline, input any) ContractResult {
	if pipeline == nil {
		return ContractResult{Verdict: VerdictFail, Error: errNilPipeline}
	}
	return contractResult(pipeline.InspectFlowInput(input))
}

func contractResult(report pipeflow.FlowReport) ContractResult {
	result := ContractResult{Verdict: VerdictPass, Topology: report.Topology, Boundaries: report.Boundaries, Error: report.Error}
	if report.Error != nil {
		result.Verdict = VerdictFail
		return result
	}
	for _, boundary := range report.Boundaries {
		if boundary.Status == pipeflow.FlowContractIncompatible {
			result.Verdict = VerdictFail
			return result
		}
		if boundary.Status == pipeflow.FlowContractUnknown {
			result.Verdict = VerdictReview
		}
	}
	return result
}
