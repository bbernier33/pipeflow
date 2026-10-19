package readiness_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestInjectionCallBudgetIsSafeAcrossConcurrentRuns(t *testing.T) {
	failure := errors.New("injected")
	wrapped, err := readiness.Inject(func() error { return nil }, readiness.Fault{Calls: 7, Error: failure})
	if err != nil {
		t.Fatal(err)
	}
	var failures atomic.Int64
	var workers sync.WaitGroup
	for range 30 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if errors.Is(wrapped.(func() error)(), failure) {
				failures.Add(1)
			}
		}()
	}
	workers.Wait()
	if failures.Load() != 7 {
		t.Fatalf("injected calls=%d", failures.Load())
	}
}

func TestInjectionPreservesContractAndNormalRetry(t *testing.T) {
	failure := errors.New("dependency unavailable")
	calls := 0
	fn := func(value int) (string, error) { calls++; return "done", nil }
	wrapped, err := readiness.Inject(fn, readiness.Fault{Calls: 2, Error: failure})
	if err != nil || reflect.TypeOf(wrapped) != reflect.TypeOf(fn) {
		t.Fatalf("wrapper=%T err=%v", wrapped, err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewSourceStep("fetch", wrapped, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 3}))))
	output, report, err := p.RunWithReport(context.Background(), 1)
	if err != nil || output != "done" || calls != 1 {
		t.Fatalf("output=%v calls=%d err=%v", output, calls, err)
	}
	attempts := report.Stages[0].Steps[0].Attempts
	if len(attempts) != 3 || !errors.Is(attempts[0].Error, failure) {
		t.Fatalf("attempts=%+v", attempts)
	}
}

func TestInjectionPanicRunsCleanupAndReportsLocation(t *testing.T) {
	wrapped, err := readiness.Inject(func(int) error { t.Fatal("original ran"); return nil }, readiness.Fault{Panic: "sink unavailable"})
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewSinkStep("sink", wrapped))).Finally("cleanup", func(context.Context, pipeflow.Finalization) error { cleaned = true; return nil })
	_, report, err := p.RunWithReport(context.Background(), 1)
	var panicError *pipeflow.PanicError
	if !cleaned || !errors.As(err, &panicError) || report.Stages[0].Steps[0].Status != pipeflow.StatusFailed {
		t.Fatalf("cleanup=%v report=%+v err=%v", cleaned, report, err)
	}
}

func TestInjectionTimeoutAndCancellation(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		fault := readiness.Fault{WaitForCancellation: true}
		if cancelParent {
			fault.Cancel = cancel
		}
		wrapped, err := readiness.Inject(func(context.Context, *pipeflow.Context, any) (any, error) { t.Fatal("original ran"); return nil, nil }, fault)
		if err != nil {
			t.Fatal(err)
		}
		p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("wait", wrapped, pipeflow.WithTimeout(time.Millisecond))))
		_, report, err := p.RunWithReport(ctx)
		cancel()
		want := context.DeadlineExceeded
		status := pipeflow.StatusTimeout
		if cancelParent {
			want = context.Canceled
			status = pipeflow.StatusCancelled
		}
		if !errors.Is(err, want) || report.Status != status {
			t.Fatalf("status=%s err=%v", report.Status, err)
		}
	}
}

func TestInjectedLatencyRejectsLateOrdinarySuccess(t *testing.T) {
	wrapped, err := readiness.Inject(func() (int, error) { return 42, nil }, readiness.Fault{Delay: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("slow", wrapped, pipeflow.WithTimeout(time.Millisecond))))
	output, _, err := p.RunWithReport(context.Background())
	if output != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestInjectionRejectsInvalidDefinitions(t *testing.T) {
	for _, fn := range []any{nil, 1, (func() error)(nil), func() {}, func(int, int) error { return nil }} {
		if _, err := readiness.Inject(fn, readiness.Fault{}); err == nil {
			t.Fatalf("accepted %T", fn)
		}
	}
	if _, err := readiness.Inject(func() error { return nil }, readiness.Fault{WaitForCancellation: true}); err == nil {
		t.Fatal("accepted contextless wait")
	}
}
