package readiness_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow/v3"
	"github.com/bbernier33/pipeflow/v3/readiness"
)

func readinessBreaker(t *testing.T, threshold int, cooldown time.Duration, failure error) *pipeflow.CircuitBreaker {
	t.Helper()
	breaker, err := pipeflow.NewCircuitBreaker("api", pipeflow.CircuitBreakerPolicy{
		FailureThreshold: threshold, ObservationWindow: time.Minute,
		OpenDuration: cooldown, HalfOpenMaxProbes: 1,
		IsFailure: func(err error) bool { return errors.Is(err, failure) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return breaker
}

func assertCircuit(t *testing.T, p *pipeflow.Pipeline, expected readiness.CircuitExpectation) {
	t.Helper()
	result := readiness.NewScenario("circuit behavior", p).Expect(readiness.ExpectCircuit("p/s/call", expected)).Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("checks=%+v err=%v", result.Checks, result.Error)
	}
}

func TestCircuitReadinessThresholdAndCallSuppression(t *testing.T) {
	failure := errors.New("api unavailable")
	breaker := readinessBreaker(t, 2, time.Minute, failure)
	var calls atomic.Int64
	fn, err := readiness.Inject(func() error { calls.Add(1); return nil }, readiness.Fault{Calls: 2, Error: failure})
	if err != nil {
		t.Fatal(err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", fn).WithCircuitBreaker(breaker)))
	assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitClosed}, Status: pipeflow.StatusFailed, Error: failure})
	assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen}, Status: pipeflow.StatusFailed, Error: failure})
	assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitOpen, StateAfter: pipeflow.CircuitOpen, ShortCircuited: true}, Status: pipeflow.StatusFailed, Error: pipeflow.ErrCircuitOpen})
	if calls.Load() != 0 {
		t.Fatalf("underlying calls=%d", calls.Load())
	}
}

func TestCircuitReadinessBusinessErrorsDoNotOpen(t *testing.T) {
	failure := errors.New("api unavailable")
	business := errors.New("invalid order")
	breaker := readinessBreaker(t, 1, time.Minute, failure)
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", func() error { return business }).WithCircuitBreaker(breaker)))
	for range 3 {
		assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitClosed}, Status: pipeflow.StatusFailed, Error: business})
	}
	if got := breaker.Snapshot(); got.QualifyingFails != 0 || got.State != pipeflow.CircuitClosed {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestCircuitReadinessProbeClosesOrReopens(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "close", false: "reopen"}[success], func(t *testing.T) {
			failure := errors.New("api unavailable")
			breaker := readinessBreaker(t, 1, time.Millisecond, failure)
			calls := 0
			p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", func() error {
				calls++
				if calls == 1 || !success {
					return failure
				}
				return nil
			}).WithCircuitBreaker(breaker)))
			assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen}, Error: failure})
			<-time.After(time.Until(breaker.Snapshot().RetryAt))
			after, status := pipeflow.CircuitClosed, pipeflow.StatusCompleted
			var wantErr error
			if !success {
				after, status, wantErr = pipeflow.CircuitOpen, pipeflow.StatusFailed, failure
			}
			assertCircuit(t, &p, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitHalfOpen, StateAfter: after, Probe: true}, Status: status, Error: wantErr})
			if calls != 2 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestCircuitReadinessHalfOpenProbeLimitAcrossRuns(t *testing.T) {
	failure := errors.New("api unavailable")
	breaker := readinessBreaker(t, 1, time.Millisecond, failure)
	open := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", func() error { return failure }).WithCircuitBreaker(breaker)))
	assertCircuit(t, &open, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen}})
	<-time.After(time.Until(breaker.Snapshot().RetryAt))
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	probe := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", func() error { close(started); <-release; return nil }).WithCircuitBreaker(breaker)))
	execution, err := probe.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	assertCircuit(t, &open, readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitHalfOpen, StateAfter: pipeflow.CircuitHalfOpen, ShortCircuited: true}, Error: pipeflow.ErrCircuitOpen})
	unblock()
	_, report, err := execution.Wait()
	if err != nil {
		t.Fatal(err)
	}
	got := readiness.ExpectCircuit("p/s/call", readiness.CircuitExpectation{Circuit: pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitHalfOpen, StateAfter: pipeflow.CircuitClosed, Probe: true}, Status: pipeflow.StatusCompleted}).Check(readiness.Observation{Report: report})
	if got.Verdict != readiness.VerdictPass {
		t.Fatal(got)
	}
}

func TestCircuitReadinessRejectsMissingAndWrongEvidence(t *testing.T) {
	actual := pipeflow.CircuitReport{Dependency: "api", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen}
	step := pipeflow.StepReport{Name: "call", Circuit: &actual, Status: pipeflow.StatusFailed}
	o := readiness.Observation{Report: pipeflow.RunReport{Pipeline: "p", Stages: []pipeflow.StageReport{{Name: "s", Steps: []pipeflow.StepReport{step}}}}}
	for _, expected := range []readiness.CircuitExpectation{
		{Circuit: pipeflow.CircuitReport{Dependency: "wrong"}},
		{Circuit: actual, Status: pipeflow.StatusCompleted},
		{Circuit: actual, Error: errors.New("missing")},
	} {
		if got := readiness.ExpectCircuit("p/s/call", expected).Check(o); got.Verdict != readiness.VerdictFail {
			t.Fatal(got)
		}
	}
	if got := readiness.ExpectCircuit("p/s/missing", readiness.CircuitExpectation{Circuit: actual}).Check(o); got.Verdict != readiness.VerdictFail {
		t.Fatal(got)
	}
	o.Report.Stages[0].Steps[0].Circuit = nil
	if got := readiness.ExpectCircuit("p/s/call", readiness.CircuitExpectation{}).Check(o); got.Verdict != readiness.VerdictFail {
		t.Fatal(got)
	}
}
