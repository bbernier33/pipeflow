package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestFinalizersRunLIFOOnSuccess(t *testing.T) {
	var order []string
	pipeline := NewPipeline("cleanup", NewStage("work", NewStep("step", func() error { return nil }))).
		Finally("a", func(_ context.Context, result Finalization) error {
			order = append(order, "a")
			if result.Status != StatusCompleted || result.Err != nil {
				t.Fatalf("finalization = %#v", result)
			}
			return nil
		}).
		Finally("b", func(context.Context, Finalization) error { order = append(order, "b"); return nil })

	_, report, err := pipeline.RunWithReport(context.Background())
	if err != nil || !reflect.DeepEqual(order, []string{"b", "a"}) {
		t.Fatalf("order=%v err=%v", order, err)
	}
	if len(report.Cleanups) != 2 || report.Cleanups[0].Status != StatusCompleted || report.Cleanups[1].Status != StatusCompleted {
		t.Fatalf("cleanup reports = %#v", report.Cleanups)
	}
}

func TestFinalizersRunAfterExecutionFailure(t *testing.T) {
	executionErr := errors.New("execution failed")
	called := false
	pipeline := NewPipeline("cleanup", NewStage("work", NewStep("step", func() error { return executionErr }))).
		Finally("release", func(_ context.Context, result Finalization) error {
			called = true
			if result.Status != StatusFailed || !errors.Is(result.Err, executionErr) {
				t.Fatalf("finalization = %#v", result)
			}
			return nil
		})

	_, err := pipeline.Run(context.Background())
	if !called || !errors.Is(err, executionErr) {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestFinalizerContextSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipeline := NewPipeline("cleanup").Finally("release", func(cleanupCtx context.Context, result Finalization) error {
		if cleanupCtx.Err() != nil {
			t.Fatalf("cleanup context = %v, want detached context", cleanupCtx.Err())
		}
		if result.Status != StatusCancelled || !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("finalization = %#v", result)
		}
		return nil
	})

	_, err := pipeline.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
}

func TestAllFinalizersRunAndErrorsAreJoined(t *testing.T) {
	executionErr := errors.New("execution")
	cleanupA := errors.New("cleanup A")
	cleanupB := errors.New("cleanup B")
	var order []string
	pipeline := NewPipeline("cleanup", NewStage("work", NewStep("step", func() error { return executionErr }))).
		Finally("a", func(context.Context, Finalization) error { order = append(order, "a"); return cleanupA }).
		Finally("b", func(context.Context, Finalization) error { order = append(order, "b"); return cleanupB })

	_, report, err := pipeline.RunWithReport(context.Background())
	if !reflect.DeepEqual(order, []string{"b", "a"}) {
		t.Fatalf("cleanup order = %v, want [b a]", order)
	}
	for _, target := range []error{executionErr, cleanupA, cleanupB} {
		if !errors.Is(err, target) {
			t.Fatalf("joined error %v does not contain %v", err, target)
		}
	}
	if len(report.Cleanups) != 2 || report.Cleanups[0].Name != "b" || report.Cleanups[1].Name != "a" {
		t.Fatalf("cleanup reports = %#v", report.Cleanups)
	}
	for _, cleanup := range report.Cleanups {
		if cleanup.Status != StatusFailed || cleanup.StartedAt.IsZero() || cleanup.EndedAt.IsZero() || cleanup.Error == nil {
			t.Fatalf("cleanup report = %#v", cleanup)
		}
	}
}

func TestCleanupFailureTurnsSuccessIntoFailure(t *testing.T) {
	cleanupErr := errors.New("close failed")
	pipeline := NewPipeline("cleanup").Finally("close", func(context.Context, Finalization) error { return cleanupErr })

	output, report, err := pipeline.RunWithReport(context.Background(), 42)
	if output != nil || !errors.Is(err, cleanupErr) || report.Status != StatusFailed {
		t.Fatalf("RunWithReport = (%v, %q, %v)", output, report.Status, err)
	}
	var typed *CleanupError
	if !errors.As(err, &typed) || typed.Name != "close" {
		t.Fatalf("error = %v, want named CleanupError", err)
	}
}

func TestStepPanicIsRecoveredAndCleanupRuns(t *testing.T) {
	called := false
	pipeline := NewPipeline("panic", NewStage("work", NewStep("explode", func() error { panic("boom") }))).
		Finally("release", func(context.Context, Finalization) error { called = true; return nil })

	_, report, err := pipeline.RunWithReport(context.Background())
	var panicErr *PanicError
	if !called || !errors.As(err, &panicErr) || panicErr.Value != "boom" || len(panicErr.Stack) == 0 {
		t.Fatalf("called=%v err=%#v", called, err)
	}
	if report.Status != StatusFailed || report.Stages[0].Steps[0].Status != StatusFailed {
		t.Fatalf("report = %#v", report)
	}
}

func TestPollingPredicatePanicIsRecoveredAndCleanupRuns(t *testing.T) {
	called := false
	pipeline := NewPipeline("panic", NewStage("work", NewStep("poll", func() (int, error) { return 1, nil },
		WithPolling(PollPolicy{Until: func(int) bool { panic("predicate boom") }})))).
		Finally("release", func(context.Context, Finalization) error { called = true; return nil })

	_, err := pipeline.Run(context.Background())
	var panicErr *PanicError
	if !called || !errors.As(err, &panicErr) || panicErr.Value != "predicate boom" {
		t.Fatalf("called=%v err=%#v", called, err)
	}
}

func TestFinalizerPanicDoesNotSkipRemainingFinalizers(t *testing.T) {
	remainingRan := false
	pipeline := NewPipeline("panic").
		Finally("remaining", func(context.Context, Finalization) error { remainingRan = true; return nil }).
		Finally("panic", func(context.Context, Finalization) error { panic("cleanup boom") })

	_, report, err := pipeline.RunWithReport(context.Background())
	var panicErr *PanicError
	if !remainingRan || !errors.As(err, &panicErr) || panicErr.Value != "cleanup boom" {
		t.Fatalf("remainingRan=%v err=%#v", remainingRan, err)
	}
	if len(report.Cleanups) != 2 || report.Cleanups[0].Status != StatusFailed || report.Cleanups[1].Status != StatusCompleted {
		t.Fatalf("cleanup reports = %#v", report.Cleanups)
	}
}

func TestFinalizerRunsAfterTimeout(t *testing.T) {
	called := false
	pipeline := NewPipeline("timeout", NewStage("work", NewStep("slow", func() error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, WithTimeout(time.Millisecond)))).Finally("release", func(_ context.Context, result Finalization) error {
		called = true
		if result.Status != StatusTimeout {
			t.Fatalf("finalization status = %q, want timeout", result.Status)
		}
		return nil
	})

	_, err := pipeline.Run(context.Background())
	if !called || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestAsyncExecutionRecoversPanicAndFinishesCleanup(t *testing.T) {
	called := false
	pipeline := NewPipeline("async-panic", NewStage("work", NewStep("explode", func() error { panic("boom") }))).
		Finally("release", func(context.Context, Finalization) error { called = true; return nil })

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := execution.Wait()
	var panicErr *PanicError
	if !called || !errors.As(err, &panicErr) || report.Status != StatusFailed {
		t.Fatalf("called=%v status=%q err=%#v", called, report.Status, err)
	}
}
