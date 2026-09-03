package readiness

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbernier33/pipeflow"
)

// SemanticBehavior describes whether a domain-specific input should be
// accepted or rejected. Unspecified behavior is exploratory and requires review.
type SemanticBehavior string

const (
	SemanticUnspecified SemanticBehavior = "unspecified"
	SemanticAccept      SemanticBehavior = "accept"
	SemanticReject      SemanticBehavior = "reject"
)

// SemanticCheck verifies payload-free evidence after expected/observed behavior
// has been classified.
type SemanticCheck func(Result) Assessment

// SemanticCase is one application-defined domain mutation. Its input is held
// only by the plan and is omitted from retained results.
type SemanticCase struct {
	name     string
	path     string
	input    any
	expected SemanticBehavior
	check    SemanticCheck
}

// NewSemanticCase creates a domain-specific mutated input with no expectation.
// It will produce REVIEW until classified with ExpectBehavior.
func NewSemanticCase(name, path string, input any) SemanticCase {
	return SemanticCase{name: name, path: path, input: input, expected: SemanticUnspecified}
}

// ExpectBehavior classifies the case as accepted, rejected, or unspecified.
func (c SemanticCase) ExpectBehavior(expected SemanticBehavior) SemanticCase {
	c.expected = expected
	return c
}

// Verify adds an optional payload-free evidence check. It is useful for proving
// that rejection occurred for the intended reason.
func (c SemanticCase) Verify(check SemanticCheck) SemanticCase {
	c.check = check
	return c
}

// SemanticPlan applies reusable domain cases to a Pipeline Scenario.
type SemanticPlan struct {
	scenario Scenario
	cases    []SemanticCase
}

// NewSemanticPlan creates a semantic mutation plan.
func NewSemanticPlan(scenario Scenario, cases ...SemanticCase) SemanticPlan {
	return SemanticPlan{scenario: scenario, cases: append([]SemanticCase(nil), cases...)}
}

// SemanticCaseResult contains no mutated business input.
type SemanticCaseResult struct {
	Name     string
	Path     string
	Expected SemanticBehavior
	Observed SemanticBehavior
	Verdict  Verdict
	Check    *CheckResult
	Scenario Result
	Error    error
}

// SemanticPlanResult aggregates semantic cases using standard verdict precedence.
type SemanticPlanResult struct {
	Verdict Verdict
	Cases   []SemanticCaseResult
	Error   error
}

// Run executes semantic cases deterministically in declaration order.
func (p SemanticPlan) Run(ctx context.Context) SemanticPlanResult {
	if err := validateSemanticCases(p.cases); err != nil {
		return SemanticPlanResult{Verdict: VerdictFail, Error: err}
	}
	base := p.scenario
	base.input = nil
	base.hasInput = false
	if err := base.validate(); err != nil {
		return SemanticPlanResult{Verdict: VerdictFail, Error: fmt.Errorf("readiness: semantic plan scenario: %w", err)}
	}
	if ctx == nil {
		return SemanticPlanResult{Verdict: VerdictFail, Error: errors.New("readiness: context cannot be nil")}
	}
	result := SemanticPlanResult{Verdict: VerdictPass, Cases: make([]SemanticCaseResult, 0, len(p.cases))}
	for _, semanticCase := range p.cases {
		if err := ctx.Err(); err != nil {
			result.Verdict = VerdictFail
			result.Error = err
			return result
		}
		scenarioResult := p.scenario.WithInput(semanticCase.input).Run(ctx)
		observed := observedBehavior(scenarioResult)
		caseResult := SemanticCaseResult{
			Name: semanticCase.name, Path: semanticCase.path,
			Expected: semanticCase.expected, Observed: observed,
			Scenario: scenarioResult,
		}
		caseResult.Verdict = semanticVerdict(semanticCase.expected, observed, scenarioResult)
		if semanticCase.check != nil {
			checked := evaluateSemanticCheck(semanticCase, scenarioResult)
			caseResult.Check = &checked
			caseResult.Verdict = combine(caseResult.Verdict, checked.Verdict)
			caseResult.Error = checked.Error
		}
		result.Cases = append(result.Cases, caseResult)
		result.Verdict = combine(result.Verdict, caseResult.Verdict)
	}
	return result
}

func validateSemanticCases(cases []SemanticCase) error {
	if len(cases) == 0 {
		return errors.New("readiness: semantic plan requires at least one case")
	}
	seen := make(map[string]struct{}, len(cases))
	for _, semanticCase := range cases {
		if semanticCase.name == "" {
			return errors.New("readiness: semantic case name cannot be empty")
		}
		if semanticCase.path == "" {
			return fmt.Errorf("readiness: semantic case %q path cannot be empty", semanticCase.name)
		}
		if _, exists := seen[semanticCase.name]; exists {
			return fmt.Errorf("readiness: duplicate semantic case %q", semanticCase.name)
		}
		seen[semanticCase.name] = struct{}{}
		switch semanticCase.expected {
		case SemanticUnspecified, SemanticAccept, SemanticReject:
		default:
			return fmt.Errorf("readiness: semantic case %q has invalid expected behavior %q", semanticCase.name, semanticCase.expected)
		}
	}
	return nil
}

func observedBehavior(result Result) SemanticBehavior {
	if result.Error != nil || result.Report.Error != nil {
		return SemanticReject
	}
	if result.Report.Status == pipeflow.StatusCompleted {
		return SemanticAccept
	}
	return SemanticReject
}

func semanticVerdict(expected, observed SemanticBehavior, result Result) Verdict {
	if expected == SemanticUnspecified {
		return VerdictReview
	}
	if expected != observed {
		return VerdictFail
	}
	if expected == SemanticAccept && result.Verdict == VerdictFail {
		return VerdictFail
	}
	return VerdictPass
}

func evaluateSemanticCheck(semanticCase SemanticCase, result Result) (checked CheckResult) {
	checked.Name = semanticCase.name + " evidence"
	defer func() {
		if recovered := recover(); recovered != nil {
			checked.Verdict = VerdictFail
			checked.Observed = "semantic evidence check panicked"
			checked.Error = fmt.Errorf("readiness: semantic case %q evidence check panicked: %v", semanticCase.name, recovered)
		}
	}()
	assessment := semanticCase.check(result)
	checked.Verdict = assessment.Verdict
	checked.Observed = assessment.Observed
	if checked.Verdict != VerdictPass && checked.Verdict != VerdictFail && checked.Verdict != VerdictReview {
		checked.Verdict = VerdictFail
		checked.Error = fmt.Errorf("readiness: semantic case %q evidence check returned invalid verdict %q", semanticCase.name, assessment.Verdict)
	}
	return checked
}
