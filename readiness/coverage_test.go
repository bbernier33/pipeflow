package readiness_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestCoverageDetectsSkippedThenExercisedConditionalPath(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("conditional", func(int) error { return nil }, pipeflow.WithCondition(func(value int) bool { return value > 0 }))))
	_, skipped, err := p.RunWithReport(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{skipped})
	if result.Verdict != readiness.VerdictReview || len(result.Gaps) != 1 || result.Paths[0].Exercised {
		t.Fatalf("result=%+v", result)
	}
	_, executed, err := p.RunWithReport(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	covered := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{skipped, executed}, readiness.CoverageRequirement{Path: "p/s/conditional", Behavior: readiness.CoverageSkipped})
	if covered.Verdict != readiness.VerdictPass || len(covered.Gaps) != 0 {
		t.Fatalf("result=%+v", covered)
	}
}

func TestCoverageCriticalDormantRecoveryDecisionIsFailure(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("work", func() error { return nil })))
	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report}, readiness.CoverageRequirement{Path: "p/s/work", Behavior: readiness.CoverageRecoveryFailPipeline, Critical: true})
	if result.Verdict != readiness.VerdictFail || len(result.Gaps) != 1 || !result.Gaps[0].Critical {
		t.Fatalf("result=%+v", result)
	}
}

func TestCoverageNestedAndConcurrentPaths(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s",
		pipeflow.NewConcurrentSteps([]*pipeflow.Step{pipeflow.NewStep("concurrent", func() error { return nil })}),
		pipeflow.NewParallel("parallel", []pipeflow.Branch{pipeflow.NewBranch("branch", pipeflow.NewStep("leaf", func() error { return nil }))}),
		pipeflow.NewSubflow("sub", pipeflow.NewStage("inner", pipeflow.NewStep("nested", func() error { return nil }))),
	))
	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report})
	if result.Verdict != readiness.VerdictPass || len(result.Paths) != 3 {
		t.Fatalf("result=%+v", result)
	}
}

func TestCoverageRetriedIsNotJustRecoveryRerun(t *testing.T) {
	failure := errors.New("temporary")
	wrapped, err := readiness.Inject(func() error { return nil }, readiness.Fault{Calls: 1, Error: failure})
	if err != nil {
		t.Fatal(err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("work", wrapped).WithRecovery(pipeflow.NewRecoveryStage("repair", pipeflow.NewStep("retry", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil })), pipeflow.RecoveryPolicy{MaxAttempts: 1})))
	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report}, readiness.CoverageRequirement{Path: "p/s/work", Behavior: readiness.CoverageRetried}, readiness.CoverageRequirement{Path: "p/s/work", Behavior: readiness.CoverageRecoveryRetryStep})
	if result.Verdict != readiness.VerdictReview || len(result.Gaps) != 1 || result.Gaps[0].Behavior != readiness.CoverageRetried {
		t.Fatalf("result=%+v", result)
	}
}

func TestCoverageRejectsForeignOrUnfinishedEvidenceAndInvalidRequirements(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("work", func() error { return nil })))
	for _, report := range []pipeflow.RunReport{{}, {RunID: "run", Pipeline: "wrong", Status: pipeflow.StatusCompleted}, {RunID: "run", Pipeline: "p", Status: pipeflow.StatusRunning}} {
		if result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report}); result.Error == nil || result.Verdict != readiness.VerdictFail {
			t.Fatal(result)
		}
	}
	for _, requirement := range []readiness.CoverageRequirement{{Path: "p/s/missing", Behavior: readiness.CoverageExecuted}, {Path: "p/s/work", Behavior: "imaginary"}} {
		if result := readiness.DetectUntestedPaths(&p, nil, requirement); result.Error == nil {
			t.Fatal(result)
		}
	}
	if result := readiness.DetectUntestedPaths(nil, nil); result.Error == nil {
		t.Fatal(result)
	}
}

func TestCoverageMissingReportsAndSkippedFutureStepsRemainGaps(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("fail", func() error { return errors.New("failure") }), pipeflow.NewStep("future", func() error { t.Fatal("future ran"); return nil })))
	missing := readiness.DetectUntestedPaths(&p, nil)
	if len(missing.Gaps) != 2 || missing.Verdict != readiness.VerdictReview {
		t.Fatal(missing)
	}
	_, report, _ := p.RunWithReport(context.Background())
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report})
	if len(result.Gaps) != 1 || result.Gaps[0].Path != "p/s/future" {
		t.Fatalf("result=%+v", result)
	}
}

func TestCoverageRecognizesDeclaredPolicyEvidence(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("work", func() error { return nil })))
	for _, test := range []struct {
		behavior readiness.CoverageBehavior
		step     pipeflow.StepReport
	}{
		{readiness.CoverageRetried, pipeflow.StepReport{Attempts: []pipeflow.AttemptReport{{Attempt: 2, Status: pipeflow.StatusCompleted}}}},
		{readiness.CoverageTimeout, pipeflow.StepReport{Status: pipeflow.StatusTimeout}},
		{readiness.CoverageCancelled, pipeflow.StepReport{Status: pipeflow.StatusCancelled}},
		{readiness.CoverageRecoveryTimeout, pipeflow.StepReport{Recoveries: []pipeflow.RecoveryReport{{Status: pipeflow.StatusTimeout}}}},
		{readiness.CoverageRecoveryFailStep, pipeflow.StepReport{Recoveries: []pipeflow.RecoveryReport{{Status: pipeflow.StatusCompleted, Decision: pipeflow.RecoveryFailStep}}}},
		{readiness.CoverageRecoveryFailStage, pipeflow.StepReport{Recoveries: []pipeflow.RecoveryReport{{Status: pipeflow.StatusCompleted, Decision: pipeflow.RecoveryFailStage}}}},
		{readiness.CoverageRecoveryFailPipeline, pipeflow.StepReport{Recoveries: []pipeflow.RecoveryReport{{Status: pipeflow.StatusCompleted, Decision: pipeflow.RecoveryFailPipeline}}}},
		{readiness.CoverageCircuitOpened, pipeflow.StepReport{Circuit: &pipeflow.CircuitReport{StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen}}},
		{readiness.CoverageCircuitShortCircuited, pipeflow.StepReport{Circuit: &pipeflow.CircuitReport{ShortCircuited: true}}},
		{readiness.CoverageCircuitProbe, pipeflow.StepReport{Circuit: &pipeflow.CircuitReport{Probe: true}}},
		{readiness.CoverageCircuitClosed, pipeflow.StepReport{Circuit: &pipeflow.CircuitReport{Probe: true, StateAfter: pipeflow.CircuitClosed}}},
		{readiness.CoverageIdempotencyCompleted, pipeflow.StepReport{Idempotency: &pipeflow.IdempotencyReport{Outcome: pipeflow.IdempotencyExecuted}}},
		{readiness.CoverageIdempotencyDuplicate, pipeflow.StepReport{Idempotency: &pipeflow.IdempotencyReport{Outcome: pipeflow.IdempotencyDuplicateCompleted}}},
		{readiness.CoverageIdempotencyInProgress, pipeflow.StepReport{Idempotency: &pipeflow.IdempotencyReport{Outcome: pipeflow.IdempotencyDuplicateInProgress}}},
		{readiness.CoverageIdempotencyReleased, pipeflow.StepReport{Idempotency: &pipeflow.IdempotencyReport{Outcome: pipeflow.IdempotencyFailedReleasable}}},
	} {
		t.Run(string(test.behavior), func(t *testing.T) {
			step := test.step
			step.Name = "work"
			if len(step.Attempts) == 0 {
				step.Attempts = []pipeflow.AttemptReport{{Attempt: 1, Status: pipeflow.StatusCompleted}}
			}
			report := pipeflow.RunReport{RunID: "run", Pipeline: "p", Status: pipeflow.StatusCompleted, Stages: []pipeflow.StageReport{{Name: "s", Steps: []pipeflow.StepReport{step}}}}
			required := readiness.CoverageRequirement{Path: "p/s/work", Behavior: test.behavior}
			if result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report}, required); result.Verdict != readiness.VerdictPass {
				t.Fatalf("result=%+v", result)
			}
			report.Stages[0].Steps[0] = pipeflow.StepReport{Name: "work", Attempts: []pipeflow.AttemptReport{{Attempt: 1, Status: pipeflow.StatusCompleted}}}
			if result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report}, required); result.Verdict != readiness.VerdictReview || len(result.Gaps) != 1 {
				t.Fatalf("unrelated execution covered policy: %+v", result)
			}
		})
	}
}

type coverageCustomItem struct{}

func (coverageCustomItem) Run(_ context.Context, _ *pipeflow.Context, value any) (any, error) {
	return value, nil
}

func TestCoverageCustomItemsRequireReviewAndCriticalDefaultsEscalate(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", coverageCustomItem{}, pipeflow.NewStep("work", func() error { return nil })))
	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{report})
	if result.Verdict != readiness.VerdictReview || len(result.Warnings) != 1 {
		t.Fatalf("result=%+v", result)
	}
	critical := readiness.DetectUntestedPaths(&p, nil, readiness.CoverageRequirement{Path: "p/s/work", Behavior: readiness.CoverageExecuted, Critical: true})
	if critical.Verdict != readiness.VerdictFail || len(critical.Gaps) != 1 || !critical.Gaps[0].Critical {
		t.Fatalf("result=%+v", critical)
	}
}
