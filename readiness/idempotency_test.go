package readiness_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func readinessGuard(t *testing.T, store pipeflow.IdempotencyStore) *pipeflow.IdempotencyGuard {
	t.Helper()
	guard, err := pipeflow.NewIdempotencyGuard("payments", store, func(key string) string { return key })
	if err != nil {
		t.Fatal(err)
	}
	return guard
}

func assertIdempotency(t *testing.T, p *pipeflow.Pipeline, key string, want readiness.IdempotencyExpectation) readiness.Result {
	t.Helper()
	result := readiness.NewScenario("guard behavior", p).WithInput(key).Expect(readiness.ExpectIdempotency("p/s/charge", want)).Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("checks=%+v err=%v", result.Checks, result.Error)
	}
	return result
}

func TestIdempotencyReadinessCompletedRedeliveryAndIndependentKeys(t *testing.T) {
	guard := readinessGuard(t, pipeflow.NewMemoryIdempotencyStore())
	var calls atomic.Int64
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewSinkStep("charge", func(string) error { calls.Add(1); return nil }).WithIdempotencyGuard(guard)))
	want := readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted, Status: pipeflow.StatusCompleted}
	assertIdempotency(t, &p, "one", want)
	assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Outcome: pipeflow.IdempotencyDuplicateCompleted, Status: pipeflow.StatusSkipped})
	if calls.Load() != 1 {
		t.Fatalf("duplicate repeated side effect: calls=%d", calls.Load())
	}
	assertIdempotency(t, &p, "two", want)
	if calls.Load() != 2 {
		t.Fatalf("different key did not execute: calls=%d", calls.Load())
	}
}

func TestIdempotencyReadinessConcurrentClaim(t *testing.T) {
	guard := readinessGuard(t, pipeflow.NewMemoryIdempotencyStore())
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int64
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("charge", func(string) error { calls.Add(1); close(started); <-release; return nil }).WithIdempotencyGuard(guard)))
	execution, err := p.Start(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Outcome: pipeflow.IdempotencyDuplicateInProgress, Status: pipeflow.StatusFailed, Error: pipeflow.ErrIdempotencyInProgress})
	unblock()
	_, report, err := execution.Wait()
	if err != nil {
		t.Fatal(err)
	}
	got := readiness.ExpectIdempotency("p/s/charge", readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted, Status: pipeflow.StatusCompleted}).Check(readiness.Observation{Report: report})
	if got.Verdict != readiness.VerdictPass || calls.Load() != 1 {
		t.Fatalf("check=%+v calls=%d", got, calls.Load())
	}
}

func TestIdempotencyReadinessFailedClaimCanBeRetried(t *testing.T) {
	guard := readinessGuard(t, pipeflow.NewMemoryIdempotencyStore())
	failure := errors.New("charge unavailable")
	wrapped, err := readiness.Inject(func(string) error { return nil }, readiness.Fault{Calls: 1, Error: failure})
	if err != nil {
		t.Fatal(err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("charge", wrapped).WithIdempotencyGuard(guard)))
	assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyFailedReleasable, Status: pipeflow.StatusFailed, Error: failure})
	assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted, Status: pipeflow.StatusCompleted})
}

type readinessStoreFault struct {
	pipeflow.IdempotencyStore
	operation string
	failure   error
	block     bool
}

func (s readinessStoreFault) Complete(ctx context.Context, key string) error {
	if s.operation == "complete" {
		if s.block {
			<-ctx.Done()
			return ctx.Err()
		}
		return s.failure
	}
	return s.IdempotencyStore.Complete(ctx, key)
}
func (s readinessStoreFault) Release(ctx context.Context, key string) error {
	if s.operation == "release" {
		if s.block {
			<-ctx.Done()
			return ctx.Err()
		}
		return s.failure
	}
	return s.IdempotencyStore.Release(ctx, key)
}

func TestIdempotencyReadinessStoreFailures(t *testing.T) {
	for _, operation := range []string{"complete", "release"} {
		t.Run(operation, func(t *testing.T) {
			storeFailure := errors.New("store unavailable")
			guard := readinessGuard(t, readinessStoreFault{IdempotencyStore: pipeflow.NewMemoryIdempotencyStore(), operation: operation, failure: storeFailure})
			workFailure := errors.New("charge failed")
			p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("charge", func(string) error {
				if operation == "release" {
					return workFailure
				}
				return nil
			}).WithIdempotencyGuard(guard)))
			result := assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyStoreFailure, Status: pipeflow.StatusFailed, Error: storeFailure, StoreError: storeFailure})
			if operation == "release" && !errors.Is(result.Report.Error, workFailure) {
				t.Fatal("original failure lost")
			}
		})
	}
}

func TestIdempotencyReadinessBoundedFinalization(t *testing.T) {
	// A store may return its context deadline itself or lose the race to the
	// guard's finalization timer. Both must be visible as store failures.
	guard := readinessGuard(t, readinessStoreFault{IdempotencyStore: pipeflow.NewMemoryIdempotencyStore(), operation: "complete", block: true}).WithFinalizationTimeout(time.Millisecond)
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("charge", func(string) error { return nil }).WithIdempotencyGuard(guard)))
	result := assertIdempotency(t, &p, "one", readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyStoreFailure})
	if !errors.Is(result.Report.Error, context.DeadlineExceeded) && !errors.Is(result.Report.Error, pipeflow.ErrIdempotencyFinalizationTimeout) {
		t.Fatalf("err=%v", result.Report.Error)
	}
}

func TestIdempotencyReadinessRejectsMissingOrMismatchedEvidence(t *testing.T) {
	report := pipeflow.RunReport{Pipeline: "p", Stages: []pipeflow.StageReport{{Name: "s", Steps: []pipeflow.StepReport{{Name: "charge", Status: pipeflow.StatusCompleted, Idempotency: &pipeflow.IdempotencyReport{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted}}}}}}
	want := readiness.IdempotencyExpectation{Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted}
	for _, field := range []string{"guard", "claim", "outcome", "status", "error", "store_error"} {
		bad := want
		switch field {
		case "guard":
			bad.Guard = "wrong"
		case "claim":
			bad.Claim = "wrong"
		case "outcome":
			bad.Outcome = "wrong"
		case "status":
			bad.Status = pipeflow.StatusFailed
		case "error":
			bad.Error = errors.New("missing")
		case "store_error":
			bad.StoreError = errors.New("missing")
		}
		if got := readiness.ExpectIdempotency("p/s/charge", bad).Check(readiness.Observation{Report: report}); got.Verdict != readiness.VerdictFail {
			t.Fatalf("%s passed", field)
		}
	}
	if got := readiness.ExpectIdempotency("missing", want).Check(readiness.Observation{Report: report}); got.Verdict != readiness.VerdictFail {
		t.Fatal(got)
	}
	report.Stages[0].Steps[0].Idempotency = nil
	if got := readiness.ExpectIdempotency("p/s/charge", want).Check(readiness.Observation{Report: report}); got.Verdict != readiness.VerdictFail {
		t.Fatal(got)
	}
}
