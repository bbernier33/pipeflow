package pipeflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitForContext(goCtx context.Context, _ *Context, _ any) (any, error) {
	<-goCtx.Done()
	return nil, goCtx.Err()
}

func TestStepTimeoutPropagatesThroughReports(t *testing.T) {
	pipeCtx := NewContext()
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("wait", waitForContext, WithTimeout(10*time.Millisecond)),
	))

	_, report, err := p.RunWithReport(context.Background(), pipeCtx, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if report.Status != StatusTimeout || report.Stages[0].Status != StatusTimeout || report.Stages[0].Steps[0].Status != StatusTimeout || pipeCtx.Status() != StatusTimeout {
		t.Fatalf("unexpected timeout states: context=%s report=%+v", pipeCtx.Status(), report)
	}
	attempt := report.Stages[0].Steps[0].Attempts[0]
	if attempt.Status != StatusTimeout || !errors.Is(attempt.Error, context.DeadlineExceeded) {
		t.Fatalf("unexpected timeout attempt: %+v", attempt)
	}
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Pipeline != "pipeline" || executionErr.Stage != "stage" || executionErr.Step != "wait" || executionErr.Attempt != 1 {
		t.Fatalf("unexpected structured timeout: %+v", executionErr)
	}
}

func TestStepTimeoutRejectsLateSuccessFromUncooperativeFunction(t *testing.T) {
	consumerRan := false
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("slow", func() (int, error) {
			time.Sleep(20 * time.Millisecond)
			return 42, nil
		}, WithTimeout(5*time.Millisecond)),
		NewStep("consumer", func(int) error { consumerRan = true; return nil }),
	))

	output, report, err := p.RunWithReport(context.Background())
	if output != nil || !errors.Is(err, context.DeadlineExceeded) || consumerRan {
		t.Fatalf("expected late success rejected, output=%v consumer=%v err=%v", output, consumerRan, err)
	}
	if report.Stages[0].Steps[0].Status != StatusTimeout || report.Stages[0].Steps[1].Status != StatusSkipped {
		t.Fatalf("unexpected late-success report: %+v", report.Stages[0])
	}
}

func TestStepTimeoutCoversRetryDelay(t *testing.T) {
	attempts := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("retry", func() error {
			attempts++
			return errors.New("temporary")
		}, WithRetry(RetryPolicy{MaxAttempts: 3, Delay: time.Second}), WithTimeout(10*time.Millisecond)),
	))

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || attempts != 1 {
		t.Fatalf("expected timeout during retry delay after one attempt, attempts=%d err=%v", attempts, err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusTimeout || len(step.Attempts) != 1 || step.Attempts[0].Status != StatusFailed {
		t.Fatalf("unexpected retry-timeout history: %+v", step)
	}
}

func TestStageTimeoutSkipsRemainingStageAndPipelineWork(t *testing.T) {
	p := NewPipeline("pipeline",
		NewStage("timed",
			NewStep("wait", waitForContext),
			NewStep("skipped", func() error { t.Fatal("skipped step ran"); return nil }),
		).WithTimeout(10*time.Millisecond),
		NewStage("later", NewStep("never", func() error { t.Fatal("later stage ran"); return nil })),
	)

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout {
		t.Fatalf("expected pipeline timeout, report=%+v err=%v", report, err)
	}
	if report.Stages[0].Status != StatusTimeout || report.Stages[0].Steps[0].Status != StatusTimeout || report.Stages[0].Steps[1].Status != StatusSkipped || report.Stages[1].Status != StatusSkipped {
		t.Fatalf("unexpected stage-timeout report: %+v", report.Stages)
	}
}

func TestStageTimeoutRejectsLateSuccessFromCustomStageItem(t *testing.T) {
	item := delayedStageItem{delay: 20 * time.Millisecond}
	p := NewPipeline("pipeline", NewStage("stage", item).WithTimeout(5*time.Millisecond))

	output, report, err := p.RunWithReport(context.Background(), "input")
	if output != nil || !errors.Is(err, context.DeadlineExceeded) || report.Stages[0].Status != StatusTimeout {
		t.Fatalf("expected custom item late success rejected, output=%v report=%+v err=%v", output, report, err)
	}
}

func TestPipelineTimeoutSkipsFutureStages(t *testing.T) {
	p := NewPipeline("pipeline",
		NewStage("active", NewStep("wait", waitForContext)),
		NewStage("future", NewStep("never", func() error { t.Fatal("future stage ran"); return nil })),
	).WithTimeout(10 * time.Millisecond)

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout || report.Stages[0].Status != StatusTimeout || report.Stages[1].Status != StatusSkipped {
		t.Fatalf("unexpected pipeline-timeout report: %+v err=%v", report, err)
	}
}

func TestNestedTimeoutUsesEarliestDeadline(t *testing.T) {
	started := time.Now()
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("wait", waitForContext, WithTimeout(500*time.Millisecond)),
	).WithTimeout(20*time.Millisecond)).WithTimeout(time.Second)

	_, report, err := p.RunWithReport(context.Background())
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) || elapsed >= 300*time.Millisecond {
		t.Fatalf("expected earliest stage deadline, elapsed=%s err=%v", elapsed, err)
	}
	if report.Status != StatusTimeout || report.Stages[0].Status != StatusTimeout || report.Stages[0].Steps[0].Status != StatusTimeout {
		t.Fatalf("unexpected inherited timeout states: %+v", report)
	}
}

func TestParentCancellationRemainsCancelled(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("wait", func(goCtx context.Context, _ *Context, _ any) (any, error) {
			close(started)
			<-goCtx.Done()
			return nil, goCtx.Err()
		}, WithTimeout(time.Second)),
	)).WithTimeout(time.Second)

	execution, err := p.Start(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	_, report, err := execution.Wait()
	if !errors.Is(err, context.Canceled) || report.Status != StatusCancelled || report.Stages[0].Steps[0].Status != StatusCancelled {
		t.Fatalf("expected cancellation, report=%+v err=%v", report, err)
	}
}

func TestCurrentShowsRunningWorkBeforeTimeout(t *testing.T) {
	started := make(chan struct{})
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("wait", func(goCtx context.Context, _ *Context, _ any) (any, error) {
			close(started)
			<-goCtx.Done()
			return nil, goCtx.Err()
		}, WithTimeout(50*time.Millisecond)),
	))

	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	current := execution.Current()
	if current.Status != StatusRunning || len(current.Stages) != 1 || len(current.Stages[0].Steps) != 1 || current.Stages[0].Steps[0].Status != StatusRunning {
		t.Fatalf("unexpected pre-timeout state: %+v", current)
	}
	_, report, err := execution.Wait()
	if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout {
		t.Fatalf("expected eventual timeout, report=%+v err=%v", report, err)
	}
}

func TestNonPositiveTimeoutIsDisabled(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("step", func() (int, error) { return 1, nil }, WithTimeout(timeout)),
		).WithTimeout(timeout)).WithTimeout(timeout)
		output, report, err := p.RunWithReport(context.Background())
		if err != nil || output != 1 || report.Status != StatusCompleted {
			t.Fatalf("expected disabled timeout for %s, output=%v status=%s err=%v", timeout, output, report.Status, err)
		}
	}
}

func TestConcurrentStepTimeoutPropagatesToGroup(t *testing.T) {
	p := NewPipeline("pipeline", NewStage("stage", NewConcurrentSteps([]*Step{
		NewStep("timeout", waitForContext, WithTimeout(10*time.Millisecond)),
		NewStep("success", func() error { return nil }),
	})))

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout || report.Stages[0].Status != StatusTimeout {
		t.Fatalf("unexpected concurrent timeout: %+v err=%v", report, err)
	}
	if report.Stages[0].Steps[0].Status != StatusTimeout || report.Stages[0].Steps[1].Status != StatusCompleted {
		t.Fatalf("unexpected concurrent step states: %+v", report.Stages[0].Steps)
	}
}

type delayedStageItem struct {
	delay time.Duration
}

func (i delayedStageItem) Run(_ context.Context, _ *Context, input any) (any, error) {
	time.Sleep(i.delay)
	return input, nil
}
