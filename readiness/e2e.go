package readiness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bbernier33/pipeflow"
)

// BoundaryKind identifies an explicitly safe end-to-end test boundary.
type BoundaryKind string

const (
	BoundarySampleIngress BoundaryKind = "sample_ingress"
	BoundarySafeSink      BoundaryKind = "safe_sink"
)

// Boundary declares a controlled edge of an end-to-end verification.
type Boundary struct {
	Name string
	Kind BoundaryKind
}

// SampleIngress declares representative, caller-controlled input.
func SampleIngress(name string) Boundary {
	return Boundary{Name: name, Kind: BoundarySampleIngress}
}

// SafeSink declares an output boundary where production writes cannot occur.
func SafeSink(name string) Boundary {
	return Boundary{Name: name, Kind: BoundarySafeSink}
}

// CoverageNode is payload-free execution evidence for one Pipeline path.
type CoverageNode struct {
	Path     string
	Kind     pipeflow.DescriptionKind
	Name     string
	Status   pipeflow.Status
	Duration time.Duration
	Attempts int
	Error    error
}

// EndToEnd composes a Scenario with controlled ingress/output declarations.
type EndToEnd struct {
	scenario   Scenario
	boundaries []Boundary
}

// NewEndToEnd creates a safe end-to-end verification. Exactly one sample
// ingress and at least one safe sink must be declared before execution.
func NewEndToEnd(scenario Scenario, boundaries ...Boundary) EndToEnd {
	return EndToEnd{scenario: scenario, boundaries: append([]Boundary(nil), boundaries...)}
}

// EndToEndResult retains contract, scenario, boundary, and execution-path
// evidence. It does not retain the Pipeline's business output.
type EndToEndResult struct {
	Name       string
	Verdict    Verdict
	StartedAt  time.Time
	EndedAt    time.Time
	Duration   time.Duration
	Boundaries []Boundary
	Contracts  ContractResult
	Scenario   Result
	Coverage   []CoverageNode
	Error      error
}

// Run performs contract preflight and then executes the real Pipeline Scenario.
func (e EndToEnd) Run(ctx context.Context) EndToEndResult {
	started := time.Now()
	result := EndToEndResult{
		Name:       e.scenario.name,
		Verdict:    VerdictFail,
		StartedAt:  started,
		Boundaries: append([]Boundary(nil), e.boundaries...),
	}
	defer func() {
		result.EndedAt = time.Now()
		result.Duration = result.EndedAt.Sub(started)
	}()

	if err := validateBoundaries(e.boundaries); err != nil {
		result.Error = err
		return result
	}
	if e.scenario.pipeline == nil {
		result.Error = errNilPipeline
		return result
	}
	if e.scenario.hasInput {
		result.Contracts = InspectContractsInput(e.scenario.pipeline, e.scenario.input)
	} else {
		result.Contracts = InspectContracts(e.scenario.pipeline)
	}
	if result.Contracts.Verdict == VerdictFail {
		result.Error = fmt.Errorf("readiness: end-to-end contract preflight failed: %w", result.Contracts.Error)
		return result
	}

	result.Scenario = e.scenario.Run(ctx)
	result.Coverage = reportCoverage(result.Scenario.Report)
	result.Verdict = combine(result.Contracts.Verdict, result.Scenario.Verdict)
	if result.Scenario.Error != nil {
		result.Error = result.Scenario.Error
		result.Verdict = VerdictFail
	}
	return result
}

func validateBoundaries(boundaries []Boundary) error {
	var ingress int
	var sinks int
	seen := make(map[string]struct{}, len(boundaries))
	for _, boundary := range boundaries {
		if boundary.Name == "" {
			return errors.New("readiness: end-to-end boundary name cannot be empty")
		}
		if _, exists := seen[boundary.Name]; exists {
			return fmt.Errorf("readiness: duplicate end-to-end boundary %q", boundary.Name)
		}
		seen[boundary.Name] = struct{}{}
		switch boundary.Kind {
		case BoundarySampleIngress:
			ingress++
		case BoundarySafeSink:
			sinks++
		default:
			return fmt.Errorf("readiness: boundary %q has unsupported kind %q", boundary.Name, boundary.Kind)
		}
	}
	if ingress != 1 {
		return fmt.Errorf("readiness: end-to-end verification requires exactly one sample ingress, got %d", ingress)
	}
	if sinks == 0 {
		return errors.New("readiness: end-to-end verification requires at least one safe sink")
	}
	return nil
}

func reportCoverage(report pipeflow.RunReport) []CoverageNode {
	if report.RunID == "" {
		return nil
	}
	coverage := []CoverageNode{{Path: report.Pipeline, Kind: pipeflow.DescriptionPipeline, Name: report.Pipeline, Status: report.Status, Duration: report.Duration, Error: report.Error}}
	for _, stage := range report.Stages {
		coverage = appendStageCoverage(coverage, report.Pipeline, stage)
	}
	return coverage
}

func appendStageCoverage(coverage []CoverageNode, parent string, stage pipeflow.StageReport) []CoverageNode {
	path := parent + "/" + stage.Name
	coverage = append(coverage, CoverageNode{Path: path, Kind: pipeflow.DescriptionStage, Name: stage.Name, Status: stage.Status, Duration: stage.Duration, Error: stage.Error})
	for _, step := range stage.Steps {
		coverage = append(coverage, CoverageNode{Path: path + "/" + step.Name, Kind: pipeflow.DescriptionStep, Name: step.Name, Status: step.Status, Duration: step.Duration, Attempts: len(step.Attempts), Error: step.Error})
	}
	for _, parallel := range stage.Parallels {
		parallelPath := path + "/" + parallel.Name
		coverage = append(coverage, CoverageNode{Path: parallelPath, Kind: pipeflow.DescriptionParallel, Name: parallel.Name, Status: parallel.Status, Duration: parallel.Duration, Error: parallel.Error})
		for _, branch := range parallel.Branches {
			branchPath := parallelPath + "/" + branch.Name
			coverage = append(coverage, CoverageNode{Path: branchPath, Kind: pipeflow.DescriptionBranch, Name: branch.Name, Status: branch.Status, Duration: branch.Duration, Error: branch.Error})
			for _, step := range branch.Steps {
				coverage = append(coverage, CoverageNode{Path: branchPath + "/" + step.Name, Kind: pipeflow.DescriptionStep, Name: step.Name, Status: step.Status, Duration: step.Duration, Attempts: len(step.Attempts), Error: step.Error})
			}
		}
	}
	for _, subflow := range stage.Subflows {
		subflowPath := path + "/" + subflow.Name
		coverage = append(coverage, CoverageNode{Path: subflowPath, Kind: pipeflow.DescriptionSubflow, Name: subflow.Name, Status: subflow.Status, Duration: subflow.Duration, Error: subflow.Error})
		for _, nested := range subflow.Stages {
			coverage = appendStageCoverage(coverage, subflowPath, nested)
		}
	}
	return coverage
}
