package pipeflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInteractionRetryTimeoutAndRateLimit(t *testing.T) {
	var attempts atomic.Int32
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("request", func() error {
		attempts.Add(1)
		return errors.New("temporary")
	},
		WithRetry(RetryPolicy{MaxAttempts: 3}),
		WithRateLimit(RateLimitPolicy{MaxCalls: 1, Interval: time.Hour}),
		WithTimeout(20*time.Millisecond),
	)))

	_, report, err := pipeline.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || attempts.Load() != 1 {
		t.Fatalf("attempts=%d err=%v", attempts.Load(), err)
	}
	step := report.Stages[0].Steps[0]
	if report.Status != StatusTimeout || step.Status != StatusTimeout || len(step.Attempts) != 2 || step.Attempts[0].Status != StatusFailed || step.Attempts[1].Status != StatusTimeout {
		t.Fatalf("report=%#v", report)
	}
}

func TestInteractionParallelFailFastAndPanic(t *testing.T) {
	siblingStarted := make(chan struct{})
	siblingStopped := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{
		NewBranch("waiting", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
			close(siblingStarted)
			<-ctx.Done()
			close(siblingStopped)
			return nil, ctx.Err()
		})),
		NewBranch("panicking", NewStep("explode", func() error {
			<-siblingStarted
			panic("boom")
		})),
	}, WithParallelFailurePolicy(FailFast))))

	_, report, err := pipeline.RunWithReport(context.Background())
	requirePanicError(t, err, "boom")
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Parallel != "parallel" || executionErr.Branch != "panicking" || executionErr.Step != "explode" {
		t.Fatalf("location=%#v err=%v", executionErr, err)
	}
	select {
	case <-siblingStopped:
	default:
		t.Fatal("fail-fast returned before the sibling observed cancellation")
	}
	parallel := report.Stages[0].Parallels[0]
	if parallel.Status != StatusFailed || parallel.Branches[0].Status != StatusCancelled || parallel.Branches[1].Status != StatusFailed {
		t.Fatalf("parallel report=%#v", parallel)
	}
}

func TestInteractionBackgroundParentCancellationAndFinalizer(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	backgroundStarted := make(chan struct{})
	backgroundStopped := make(chan struct{})
	finalized := false
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("support", func(ctx context.Context) error {
		close(backgroundStarted)
		<-ctx.Done()
		close(backgroundStopped)
		return ctx.Err()
	}).Finally("release", func(ctx context.Context, result Finalization) error {
		finalized = true
		if ctx.Err() != nil || result.Status != StatusCancelled || !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("cleanup context/result: ctx=%v result=%#v", ctx.Err(), result)
		}
		select {
		case <-backgroundStopped:
		default:
			t.Fatal("finalizer ran before background stopped")
		}
		return nil
	})

	execution, err := pipeline.Start(parent)
	if err != nil {
		t.Fatal(err)
	}
	<-backgroundStarted
	cancel()
	_, report, err := execution.Wait()
	if !finalized || !errors.Is(err, context.Canceled) || report.Backgrounds[0].Status != StatusCancelled || report.Cleanups[0].Status != StatusCompleted {
		t.Fatalf("finalized=%v report=%#v err=%v", finalized, report, err)
	}
}

type interactionDynamicValue struct{ value any }

func (d interactionDynamicValue) Run(context.Context, *Context, any) (any, error) {
	return d.value, nil
}

func TestInteractionSubflowConditionAndRuntimeTypeBoundary(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("outer",
		interactionDynamicValue{value: 42},
		NewSubflow("subflow", NewStage("inner", NewStep("conditional", func(string) error { return nil },
			WithCondition(func(string) bool { return true })))),
	))
	if err := pipeline.Validate(); err != nil {
		t.Fatalf("dynamic boundary should defer type checking to execution: %v", err)
	}

	_, report, err := pipeline.RunWithReport(context.Background())
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Subflow != "subflow" || executionErr.Stage != "inner" || executionErr.Step != "conditional" || !strings.Contains(err.Error(), "cannot use int as string") {
		t.Fatalf("location=%#v err=%v", executionErr, err)
	}
	subflow := report.Stages[0].Subflows[0]
	if subflow.Status != StatusFailed || subflow.Stages[0].Steps[0].Status != StatusFailed || len(subflow.Stages[0].Steps[0].Attempts) != 0 {
		t.Fatalf("subflow report=%#v", subflow)
	}
}

type interactionRetryAfterError struct{ delay time.Duration }

func (e interactionRetryAfterError) Error() string             { return "retry later" }
func (e interactionRetryAfterError) RetryAfter() time.Duration { return e.delay }

func TestInteractionPollingRetryAfterAndTimeout(t *testing.T) {
	var calls atomic.Int32
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("poll", func() (int, error) {
		call := calls.Add(1)
		if call == 1 {
			return 1, nil
		}
		return 0, interactionRetryAfterError{delay: time.Second}
	},
		WithRetry(RetryPolicy{MaxAttempts: 2}),
		WithPolling(PollPolicy{MaxPolls: 3, Until: func(value int) bool { return value >= 2 }}),
		WithTimeout(25*time.Millisecond),
	)))

	_, report, err := pipeline.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusTimeout || len(step.Polls) != 2 || step.Polls[0].Status != StatusCompleted || step.Polls[0].Complete || step.Polls[1].Status != StatusTimeout {
		t.Fatalf("step report=%#v", step)
	}
}

func TestInteractionParallelScopedContextAndCancellation(t *testing.T) {
	root := NewContext()
	root.Set("shared", "root")
	ready := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{
		NewBranch("waiting", NewStep("wait", func(ctx context.Context, branch *Context, input any) (any, error) {
			branch.Set("branch", "waiting")
			close(ready)
			<-ctx.Done()
			if value, _ := branch.Get("branch"); value != "waiting" {
				return nil, errors.New("branch-local value changed")
			}
			return nil, ctx.Err()
		})),
		NewBranch("failure", NewStep("fail", func(_ context.Context, branch *Context, _ any) (any, error) {
			<-ready
			if _, exists := branch.Get("branch"); exists {
				return nil, errors.New("sibling context mutation leaked")
			}
			branch.Set("branch", "failure")
			return nil, errors.New("stop")
		})),
	}, WithParallelFailurePolicy(FailFast))))

	_, report, err := pipeline.RunWithReport(context.Background(), root, nil)
	if err == nil || !strings.Contains(err.Error(), "stop") {
		t.Fatalf("err=%v", err)
	}
	if _, exists := root.Get("branch"); exists {
		t.Fatal("branch-local value was merged into root Context")
	}
	parallel := report.Stages[0].Parallels[0]
	if parallel.Branches[0].Status != StatusCancelled || parallel.Branches[1].Status != StatusFailed {
		t.Fatalf("parallel report=%#v", parallel)
	}
}

func TestInteractionStartPanicAndWait(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("explode", func() (int, error) {
		panic("async boom")
	})))
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, report, err := execution.Wait()
	requirePanicError(t, err, "async boom")
	requireExecutionLocation(t, err, "pipeline", "stage", "explode")
	select {
	case <-execution.Done():
	default:
		t.Fatal("Done was not closed before Wait returned")
	}
	if execution.Status() != StatusFailed || report.Status != StatusFailed || execution.Report().Error == nil {
		t.Fatalf("status=%q report=%#v snapshot=%#v", execution.Status(), report, execution.Report())
	}
}
