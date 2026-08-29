package pipeflow

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func requirePanicError(t *testing.T, err error, value any) *PanicError {
	t.Helper()
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error = %v, want PanicError", err)
	}
	if panicErr.Value != value || len(panicErr.Stack) == 0 || !strings.Contains(string(panicErr.Stack), "goroutine") {
		t.Fatalf("PanicError = %#v", panicErr)
	}
	return panicErr
}

func requireExecutionLocation(t *testing.T, err error, pipeline, stage, step string) *ExecutionError {
	t.Helper()
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) {
		t.Fatalf("error = %v, want ExecutionError", err)
	}
	if executionErr.Pipeline != pipeline || executionErr.Stage != stage || executionErr.Step != step {
		t.Fatalf("ExecutionError = %#v", executionErr)
	}
	return executionErr
}

func TestConditionPanicUsesStructuredFailurePath(t *testing.T) {
	finalized := false
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() error { t.Fatal("action ran"); return nil },
			WithCondition(func() bool { panic("condition") })),
	)).Finally("cleanup", func(context.Context, Finalization) error { finalized = true; return nil })

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "condition")
	requireExecutionLocation(t, err, "pipeline", "stage", "step")
	if !finalized || report.Status != StatusFailed || report.Stages[0].Steps[0].Status != StatusFailed || len(report.Stages[0].Steps[0].Attempts) != 0 {
		t.Fatalf("finalized=%v report=%#v", finalized, report)
	}
}

func TestPollingPanicFinishesPollReport(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step",
		func() (int, error) { return 1, nil },
		WithPolling(PollPolicy{Until: func(int) bool { panic("poll") }}),
	)))

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "poll")
	executionErr := requireExecutionLocation(t, err, "pipeline", "stage", "step")
	if executionErr.Poll != 1 {
		t.Fatalf("poll location = %d, want 1", executionErr.Poll)
	}
	poll := report.Stages[0].Steps[0].Polls[0]
	if poll.Status != StatusFailed || poll.EndedAt.IsZero() || poll.Error == nil {
		t.Fatalf("poll report = %#v", poll)
	}
}

func TestRetryPredicatePanicPreservesAttemptLocation(t *testing.T) {
	actionErr := errors.New("retry me")
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return actionErr },
		WithRetry(RetryPolicy{MaxAttempts: 3, RetryIf: func(error) bool { panic("retry predicate") }}))))

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "retry predicate")
	executionErr := requireExecutionLocation(t, err, "pipeline", "stage", "step")
	if executionErr.Attempt != 1 || len(report.Stages[0].Steps[0].Attempts) != 1 {
		t.Fatalf("location=%#v attempts=%#v", executionErr, report.Stages[0].Steps[0].Attempts)
	}
}

type panickingStageItem struct{}

func (panickingStageItem) Run(context.Context, *Context, any) (any, error) { panic("stage item") }

func TestCustomStageItemPanicFailsOwningStage(t *testing.T) {
	finalized := false
	pipeline := NewPipeline("pipeline", NewStage("stage", panickingStageItem{})).
		Finally("cleanup", func(context.Context, Finalization) error { finalized = true; return nil })

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "stage item")
	requireExecutionLocation(t, err, "pipeline", "stage", "")
	if !finalized || report.Stages[0].Status != StatusFailed || report.Stages[0].EndedAt.IsZero() {
		t.Fatalf("finalized=%v stage=%#v", finalized, report.Stages[0])
	}
}

func TestLifecycleHookPanicFailsRunAndStillFinalizes(t *testing.T) {
	finalizerRan := false
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil }))).
		WithLifecycleHook(func(event LifecycleEvent) {
			if event.Type == StepStarted {
				panic("hook")
			}
		}).Finally("cleanup", func(context.Context, Finalization) error { finalizerRan = true; return nil })

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "hook")
	requireExecutionLocation(t, err, "pipeline", "stage", "step")
	if !finalizerRan || report.Status != StatusFailed || report.Stages[0].Steps[0].Status != StatusFailed {
		t.Fatalf("finalizerRan=%v report=%#v", finalizerRan, report)
	}
}

func TestPipelineFinalizedHookPanicIsReported(t *testing.T) {
	pipeline := NewPipeline("pipeline").WithLifecycleHook(func(event LifecycleEvent) {
		if event.Type == PipelineFinalized {
			panic("finalized hook")
		}
	})

	output, report, err := pipeline.RunWithReport(context.Background(), 7)
	requirePanicError(t, err, "finalized hook")
	requireExecutionLocation(t, err, "pipeline", "", "")
	if output != nil || report.Status != StatusFailed || report.Error == nil {
		t.Fatalf("output=%v report=%#v", output, report)
	}
}

func TestAsyncLifecyclePanicCompletesExecution(t *testing.T) {
	pipeline := NewPipeline("pipeline").WithLifecycleHook(func(event LifecycleEvent) {
		if event.Type == PipelineStarted {
			panic("async hook")
		}
	})
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := execution.Wait()
	requirePanicError(t, err, "async hook")
	if report.Status != StatusFailed {
		t.Fatalf("report status = %q", report.Status)
	}
}
