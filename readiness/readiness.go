package readiness

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/bbernier33/pipeflow"
)

// Verdict is the outcome of a check or Scenario.
type Verdict string

const (
	VerdictPass   Verdict = "pass"
	VerdictFail   Verdict = "fail"
	VerdictReview Verdict = "review"
)

// Observation is the transient evidence presented to a Check. Output may
// contain business data and is therefore never copied into Result.
type Observation struct {
	Output any
	Report pipeflow.RunReport
	Err    error
}

// Check evaluates observed execution evidence.
type Check func(Observation) Assessment

// Expectation names one behavior that a Scenario must evaluate.
type Expectation struct {
	Name  string
	Check Check
}

// Assessment is returned by a Check.
type Assessment struct {
	Verdict  Verdict
	Observed string
}

// Passed records that expected behavior occurred.
func Passed(observed string) Assessment {
	return Assessment{Verdict: VerdictPass, Observed: observed}
}

// Failed records that expected behavior did not occur.
func Failed(observed string) Assessment {
	return Assessment{Verdict: VerdictFail, Observed: observed}
}

// NeedsReview records behavior for which no deterministic expectation exists.
func NeedsReview(observed string) Assessment {
	return Assessment{Verdict: VerdictReview, Observed: observed}
}

// Expect creates a named Scenario expectation.
func Expect(name string, check Check) Expectation {
	return Expectation{Name: name, Check: check}
}

// RunSucceeds checks the ordinary successful-execution case.
func RunSucceeds() Expectation {
	return Expect("pipeline succeeds", func(observation Observation) Assessment {
		if observation.Err != nil {
			return Failed(observation.Err.Error())
		}
		if observation.Report.Status != pipeflow.StatusCompleted {
			return Failed(fmt.Sprintf("pipeline status is %s", observation.Report.Status))
		}
		return Passed("pipeline completed")
	})
}

// Scenario is one Arrange-Act-Assert production-readiness exercise.
// Construct it with NewScenario and configure it by value.
type Scenario struct {
	name         string
	arrangement  string
	pipeline     *pipeflow.Pipeline
	input        any
	hasInput     bool
	expectations []Expectation
}

// NewScenario creates a Scenario for a real Pipeline.
func NewScenario(name string, pipeline *pipeflow.Pipeline) Scenario {
	return Scenario{name: name, pipeline: pipeline}
}

// Arrange describes the production condition established by the caller.
func (s Scenario) Arrange(description string) Scenario {
	s.arrangement = description
	return s
}

// WithInput supplies the Pipeline's initial flowing value. A nil input is
// distinct from supplying no input.
func (s Scenario) WithInput(input any) Scenario {
	s.input = input
	s.hasInput = true
	return s
}

// Expect appends explicit expected behavior. Expectations run in order.
func (s Scenario) Expect(expectations ...Expectation) Scenario {
	s.expectations = append(append([]Expectation(nil), s.expectations...), expectations...)
	return s
}

// CheckResult contains one expectation's retained, payload-free result.
type CheckResult struct {
	Name     string
	Verdict  Verdict
	Observed string
	Error    error
}

// Result is the complete retained evidence for one Scenario. It intentionally
// omits the flowing business output.
type Result struct {
	Name        string
	Arrangement string
	Verdict     Verdict
	StartedAt   time.Time
	EndedAt     time.Time
	Duration    time.Duration
	Report      pipeflow.RunReport
	Checks      []CheckResult
	Error       error
}

// Run validates and executes the Scenario, then evaluates every expectation.
func (s Scenario) Run(ctx context.Context) Result {
	started := time.Now()
	result := Result{Name: s.name, Arrangement: s.arrangement, Verdict: VerdictFail, StartedAt: started}
	defer func() {
		result.EndedAt = time.Now()
		result.Duration = result.EndedAt.Sub(started)
	}()

	if err := s.validate(); err != nil {
		result.Error = err
		return result
	}
	if ctx == nil {
		result.Error = errors.New("readiness: context cannot be nil")
		return result
	}

	var output any
	var runErr error
	if s.hasInput {
		output, result.Report, runErr = s.pipeline.RunWithReport(ctx, s.input)
	} else {
		output, result.Report, runErr = s.pipeline.RunWithReport(ctx)
	}
	observation := Observation{Output: output, Report: result.Report, Err: runErr}
	result.Verdict = VerdictPass
	for _, expectation := range s.expectations {
		checkResult := evaluate(expectation, observation)
		result.Checks = append(result.Checks, checkResult)
		result.Verdict = combine(result.Verdict, checkResult.Verdict)
	}
	return result
}

func (s Scenario) validate() error {
	if s.name == "" {
		return errors.New("readiness: scenario name cannot be empty")
	}
	if s.pipeline == nil {
		return fmt.Errorf("readiness: scenario %q has a nil pipeline", s.name)
	}
	if len(s.expectations) == 0 {
		return fmt.Errorf("readiness: scenario %q must declare at least one expectation", s.name)
	}
	seen := make(map[string]struct{}, len(s.expectations))
	for _, expectation := range s.expectations {
		if expectation.Name == "" {
			return fmt.Errorf("readiness: scenario %q has an expectation with an empty name", s.name)
		}
		if expectation.Check == nil {
			return fmt.Errorf("readiness: scenario %q expectation %q has a nil check", s.name, expectation.Name)
		}
		if _, exists := seen[expectation.Name]; exists {
			return fmt.Errorf("readiness: scenario %q has duplicate expectation %q", s.name, expectation.Name)
		}
		seen[expectation.Name] = struct{}{}
	}
	if s.hasInput {
		return s.pipeline.ValidateInput(s.input)
	}
	return s.pipeline.Validate()
}

func evaluate(expectation Expectation, observation Observation) (result CheckResult) {
	result.Name = expectation.Name
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Verdict = VerdictFail
			result.Error = fmt.Errorf("readiness: expectation %q panicked: %v\n%s", expectation.Name, recovered, debug.Stack())
			result.Observed = "expectation panicked"
		}
	}()
	assessment := expectation.Check(observation)
	result.Verdict = assessment.Verdict
	result.Observed = assessment.Observed
	if result.Verdict != VerdictPass && result.Verdict != VerdictFail && result.Verdict != VerdictReview {
		result.Verdict = VerdictFail
		result.Error = fmt.Errorf("readiness: expectation %q returned invalid verdict %q", expectation.Name, assessment.Verdict)
	}
	return result
}

func combine(current, next Verdict) Verdict {
	if current == VerdictFail || next == VerdictFail {
		return VerdictFail
	}
	if current == VerdictReview || next == VerdictReview {
		return VerdictReview
	}
	return VerdictPass
}
