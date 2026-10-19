package readiness_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow/v3"
	"github.com/bbernier33/pipeflow/v3/readiness"
)

func TestRecoveryReadinessRepairRetriesOriginalFunction(t *testing.T) {
	failure := errors.New("expired credentials")
	calls, repairs := 0, 0
	wrapped, err := readiness.Inject(func(value int) (int, error) { calls++; return value * 2, nil }, readiness.Fault{Calls: 2, Error: failure})
	if err != nil {
		t.Fatal(err)
	}
	step := pipeflow.NewStep("fetch", wrapped, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 2})).WithRecovery(
		pipeflow.NewRecoveryStage("repair",
			pipeflow.NewStep("refresh", func(f pipeflow.Failure) error { repairs++; return nil }),
			pipeflow.NewStep("retry", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil }),
		), pipeflow.RecoveryPolicy{MaxAttempts: 1})
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", step))
	result := readiness.NewScenario("repair", &p).WithInput(21).Expect(
		readiness.RunSucceeds(),
		readiness.ExpectRecovery("p/s/fetch", readiness.RecoveryExpectation{Attempts: 3, Status: pipeflow.StatusCompleted, Runs: []readiness.RecoveryRunExpectation{{Name: "repair", Status: pipeflow.StatusCompleted, Decision: pipeflow.RecoveryRetryStep, OriginalError: failure}}}),
		readiness.Expect("real output", func(o readiness.Observation) readiness.Assessment {
			if o.Output != 42 || calls != 1 || repairs != 1 {
				return readiness.Failed("original function or repair did not execute exactly once")
			}
			if len(o.Report.Stages[0].Steps[0].Recoveries[0].Stages[0].Steps) != 2 {
				return readiness.Failed("missing recovery Step evidence")
			}
			return readiness.Passed("original function produced 42 after repair")
		}),
	).Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("result=%+v", result)
	}
}

func TestRecoveryReadinessTerminalDecisions(t *testing.T) {
	for _, decision := range []pipeflow.RecoveryDecision{pipeflow.RecoveryFailStep, pipeflow.RecoveryFailStage, pipeflow.RecoveryFailPipeline} {
		t.Run(string(decision), func(t *testing.T) {
			failure := errors.New("dependency down")
			step := pipeflow.NewStep("fetch", func() error { return failure }).WithRecovery(pipeflow.NewRecoveryStage("diagnose", pipeflow.NewStep("decide", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return decision, nil })), pipeflow.RecoveryPolicy{MaxAttempts: 1})
			p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", step))
			result := readiness.NewScenario("decision", &p).Expect(
				readiness.ExpectRecovery("p/s/fetch", readiness.RecoveryExpectation{Attempts: 1, Status: pipeflow.StatusFailed, Runs: []readiness.RecoveryRunExpectation{{Decision: decision, OriginalError: failure}}}),
				readiness.Expect("failure retained", func(o readiness.Observation) readiness.Assessment {
					if errors.Is(o.Err, failure) {
						return readiness.Passed("original failure retained")
					}
					return readiness.Failed("original failure lost")
				}),
			).Run(context.Background())
			if result.Verdict != readiness.VerdictPass {
				t.Fatalf("checks=%+v error=%v", result.Checks, result.Error)
			}
		})
	}
}

func TestRecoveryReadinessFailureAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			failure := errors.New("dependency down")
			repairError := errors.New("repair unavailable")
			var action any = func(pipeflow.Failure) error { return repairError }
			status := pipeflow.StatusFailed
			recoveryTimeout := time.Second
			if timeout {
				var err error
				action, err = readiness.Inject(func(context.Context, *pipeflow.Context, any) (any, error) { return nil, nil }, readiness.Fault{WaitForCancellation: true})
				if err != nil {
					t.Fatal(err)
				}
				repairError = context.DeadlineExceeded
				status = pipeflow.StatusTimeout
				recoveryTimeout = time.Millisecond
			}
			step := pipeflow.NewStep("fetch", func() error { return failure }).WithRecovery(pipeflow.NewRecoveryStage("repair", pipeflow.NewStep("attempt", action), pipeflow.NewStep("decide", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil })), pipeflow.RecoveryPolicy{Timeout: recoveryTimeout})
			p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", step))
			result := readiness.NewScenario("failed repair", &p).Expect(readiness.ExpectRecovery("p/s/fetch", readiness.RecoveryExpectation{Attempts: 1, Runs: []readiness.RecoveryRunExpectation{{Status: status, OriginalError: failure, Error: repairError}}})).Run(context.Background())
			if result.Verdict != readiness.VerdictPass {
				t.Fatalf("checks=%+v error=%v", result.Checks, result.Error)
			}
		})
	}
}

func TestRecoveryReadinessBoundsRepeatedRemediation(t *testing.T) {
	failure := errors.New("persistent failure")
	step := pipeflow.NewStep("fetch", func() error { return failure }).WithRecovery(pipeflow.NewRecoveryStage("repair", pipeflow.NewStep("decide", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil })), pipeflow.RecoveryPolicy{MaxAttempts: 2})
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewSubflow("nested", pipeflow.NewStage("inner", step))))
	run := readiness.RecoveryRunExpectation{Decision: pipeflow.RecoveryRetryStep, OriginalError: failure}
	result := readiness.NewScenario("bounded repair", &p).Expect(readiness.ExpectRecovery("p/s/nested/inner/fetch", readiness.RecoveryExpectation{Attempts: 3, Status: pipeflow.StatusFailed, Runs: []readiness.RecoveryRunExpectation{run, run}})).Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("checks=%+v error=%v", result.Checks, result.Error)
	}
}

func TestRecoveryExpectationRejectsMissingAndIncorrectEvidence(t *testing.T) {
	step := pipeflow.StepReport{Name: "step", Status: pipeflow.StatusCompleted}
	o := readiness.Observation{Report: pipeflow.RunReport{Pipeline: "p", Stages: []pipeflow.StageReport{{Name: "s", Steps: []pipeflow.StepReport{step}}}}}
	for _, expected := range []readiness.RecoveryExpectation{
		{Attempts: 1}, {Status: pipeflow.StatusFailed}, {Runs: []readiness.RecoveryRunExpectation{{Name: "never"}}},
	} {
		if got := readiness.ExpectRecovery("p/s/step", expected).Check(o); got.Verdict != readiness.VerdictFail {
			t.Fatalf("accepted missing evidence: %+v", got)
		}
	}
	if got := readiness.ExpectRecovery("p/s/missing", readiness.RecoveryExpectation{}).Check(o); got.Verdict != readiness.VerdictFail {
		t.Fatal(got)
	}
	if got := readiness.ExpectRecovery("p/s/step", readiness.RecoveryExpectation{}).Check(o); got.Verdict != readiness.VerdictPass {
		t.Fatal(got)
	}
}

func TestRecoveryExpectationDistinguishesParallelPaths(t *testing.T) {
	left := pipeflow.StepReport{Name: "fetch", Status: pipeflow.StatusCompleted}
	right := pipeflow.StepReport{Name: "fetch", Recoveries: []pipeflow.RecoveryReport{{Name: "repair", Decision: pipeflow.RecoveryFailStep, Status: pipeflow.StatusCompleted}}}
	o := readiness.Observation{Report: pipeflow.RunReport{Pipeline: "p", Stages: []pipeflow.StageReport{{Name: "s", Parallels: []pipeflow.ParallelReport{{Name: "fanout", Branches: []pipeflow.BranchReport{{Name: "left", Steps: []pipeflow.StepReport{left}}, {Name: "right", Steps: []pipeflow.StepReport{right}}}}}}}}}
	if got := readiness.ExpectRecovery("p/s/fanout/left/fetch", readiness.RecoveryExpectation{}).Check(o); got.Verdict != readiness.VerdictPass {
		t.Fatal(got)
	}
	for _, want := range []readiness.RecoveryRunExpectation{
		{Name: "wrong"}, {Status: pipeflow.StatusFailed}, {Decision: pipeflow.RecoveryRetryStep}, {OriginalError: errors.New("missing")}, {Error: errors.New("missing")},
	} {
		got := readiness.ExpectRecovery("p/s/fanout/right/fetch", readiness.RecoveryExpectation{Runs: []readiness.RecoveryRunExpectation{want}}).Check(o)
		if got.Verdict != readiness.VerdictFail {
			t.Fatalf("mismatch passed: %+v", want)
		}
	}
}

func TestRecoveryReadinessParentCancellationStillFinalizes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("dependency down")
	wrapped, err := readiness.Inject(func(context.Context, *pipeflow.Context, any) (any, error) { return nil, nil }, readiness.Fault{WaitForCancellation: true, Cancel: cancel})
	if err != nil {
		t.Fatal(err)
	}
	step := pipeflow.NewStep("fetch", func() error { return failure }).WithRecovery(pipeflow.NewRecoveryStage("repair", pipeflow.NewStep("wait", wrapped), pipeflow.NewStep("retry", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil })), pipeflow.RecoveryPolicy{MaxAttempts: 1})
	cleaned := false
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", step)).Finally("cleanup", func(context.Context, pipeflow.Finalization) error { cleaned = true; return nil })
	result := readiness.NewScenario("cancel repair", &p).Expect(
		readiness.ExpectRecovery("p/s/fetch", readiness.RecoveryExpectation{Attempts: 1, Runs: []readiness.RecoveryRunExpectation{{Status: pipeflow.StatusCancelled, Error: context.Canceled, OriginalError: failure}}}),
		readiness.Expect("cancelled and finalized", func(o readiness.Observation) readiness.Assessment {
			if cleaned && errors.Is(o.Err, context.Canceled) && o.Report.Status == pipeflow.StatusCancelled {
				return readiness.Passed("cleanup completed after cancellation")
			}
			return readiness.Failed("cancellation or cleanup missing")
		}),
	).Run(ctx)
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("checks=%+v err=%v", result.Checks, result.Error)
	}
}
