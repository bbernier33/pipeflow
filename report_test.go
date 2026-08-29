package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRunWithReportSuccessfulHierarchy(t *testing.T) {
	p := NewPipeline("orders",
		NewStage("prepare",
			NewStep("load", func() (int, error) { return 4, nil }),
			NewStep("double", func(v int) (int, error) { return v * 2, nil }),
		),
		NewStage("output", NewStep("save", func(int) error { return nil })),
	)

	output, report, err := p.RunWithReport(context.Background())
	if err != nil || output != 8 {
		t.Fatalf("unexpected execution: output=%v err=%v", output, err)
	}
	assertFinishedReport(t, report.Status, report.StartedAt, report.EndedAt, report.Duration, StatusCompleted)
	if report.RunID == "" || len(report.RunID) != 32 || report.Pipeline != "orders" || report.Error != nil {
		t.Fatalf("unexpected run identity: %+v", report)
	}
	if len(report.Stages) != 2 || report.Stages[0].Name != "prepare" || report.Stages[1].Name != "output" {
		t.Fatalf("unexpected stages: %+v", report.Stages)
	}
	for _, stage := range report.Stages {
		assertFinishedReport(t, stage.Status, stage.StartedAt, stage.EndedAt, stage.Duration, StatusCompleted)
		for _, step := range stage.Steps {
			assertFinishedReport(t, step.Status, step.StartedAt, step.EndedAt, step.Duration, StatusCompleted)
			if len(step.Attempts) != 1 || step.Attempts[0].Status != StatusCompleted {
				t.Fatalf("expected one completed attempt for %s, got %+v", step.Name, step.Attempts)
			}
		}
	}
}

func TestRunWithReportRetryAttemptHistory(t *testing.T) {
	want := errors.New("temporary")
	attempts := 0
	p := NewPipeline("retry", NewStage("stage",
		NewStep("fetch", func() (string, error) {
			attempts++
			if attempts < 3 {
				return "discarded", want
			}
			return "ready", nil
		}, WithRetry(RetryPolicy{MaxAttempts: 3})),
	))

	output, report, err := p.RunWithReport(context.Background())
	if err != nil || output != "ready" || report.Status != StatusCompleted {
		t.Fatalf("unexpected retry execution: output=%v status=%s err=%v", output, report.Status, err)
	}
	got := report.Stages[0].Steps[0].Attempts
	if len(got) != 3 || got[0].Attempt != 1 || got[1].Attempt != 2 || got[2].Attempt != 3 {
		t.Fatalf("unexpected attempts: %+v", got)
	}
	if got[0].Status != StatusFailed || got[1].Status != StatusFailed || got[2].Status != StatusCompleted {
		t.Fatalf("unexpected attempt statuses: %+v", got)
	}
	for _, attempt := range got[:2] {
		var executionErr *ExecutionError
		if !errors.Is(attempt.Error, want) || !errors.As(attempt.Error, &executionErr) || executionErr.Pipeline != "retry" || executionErr.Attempt != attempt.Attempt {
			t.Fatalf("unexpected structured attempt error: %+v", attempt)
		}
	}
}

func TestRunWithReportFailureAndSkippedWork(t *testing.T) {
	want := errors.New("boom")
	p := NewPipeline("failure",
		NewStage("active",
			NewStep("fail", func() error { return want }),
			NewStep("skipped-step", func() error { t.Fatal("skipped step ran"); return nil }),
		),
		NewStage("skipped-stage", NewStep("never", func() error { t.Fatal("skipped stage ran"); return nil })),
	)

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, want) || report.Status != StatusFailed || !errors.Is(report.Error, want) {
		t.Fatalf("unexpected failure report: status=%s reportErr=%v err=%v", report.Status, report.Error, err)
	}
	active := report.Stages[0]
	if active.Status != StatusFailed || active.Steps[0].Status != StatusFailed || active.Steps[1].Status != StatusSkipped {
		t.Fatalf("unexpected active stage: %+v", active)
	}
	if report.Stages[1].Status != StatusSkipped || report.Stages[1].Steps[0].Status != StatusSkipped {
		t.Fatalf("unexpected skipped stage: %+v", report.Stages[1])
	}
	var executionErr *ExecutionError
	if !errors.As(active.Error, &executionErr) || executionErr.Pipeline != "failure" || executionErr.Stage != "active" || executionErr.Step != "fail" {
		t.Fatalf("expected structured stage error, got %v", active.Error)
	}
}

func TestRunWithReportCancellationAndTimeoutStates(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		goCtx, cancel := context.WithCancel(context.Background())
		cancel()
		p := NewPipeline("cancelled", NewStage("skipped", NewStep("never", func() error { return nil })))
		_, report, err := p.RunWithReport(goCtx)
		if !errors.Is(err, context.Canceled) || report.Status != StatusCancelled || report.Stages[0].Status != StatusSkipped {
			t.Fatalf("unexpected cancellation report: %+v err=%v", report, err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		goCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		p := NewPipeline("timeout", NewStage("skipped", NewStep("never", func() error { return nil })))
		_, report, err := p.RunWithReport(goCtx)
		if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout || report.Stages[0].Status != StatusSkipped {
			t.Fatalf("unexpected timeout report: %+v err=%v", report, err)
		}
	})
}

func TestRunWithReportConcurrentStepsStayInDeclarationOrder(t *testing.T) {
	p := NewPipeline("parallel", NewStage("fanout", NewConcurrentSteps([]*Step{
		NewStep("first", func() (string, error) { time.Sleep(10 * time.Millisecond); return "first", nil }),
		NewStep("second", func() (string, error) { return "second", nil }),
	})))

	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	steps := report.Stages[0].Steps
	if len(steps) != 2 || steps[0].Name != "first" || steps[1].Name != "second" {
		t.Fatalf("unexpected report order: %+v", steps)
	}
}

func TestRunWithReportFailFastMarksQueuedStepsSkipped(t *testing.T) {
	want := errors.New("stop")
	p := NewPipeline("parallel", NewStage("fanout", NewConcurrentSteps([]*Step{
		NewStep("fail", func() error { return want }),
		NewStep("queued", func() error { t.Fatal("queued step ran"); return nil }),
	}, WithMaxWorkers(1), WithFailurePolicy(FailFast))))

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
	steps := report.Stages[0].Steps
	if steps[0].Status != StatusFailed || steps[1].Status != StatusSkipped {
		t.Fatalf("unexpected fail-fast reports: %+v", steps)
	}
}

func TestRunWithReportIDsAreUnique(t *testing.T) {
	p := NewPipeline("empty")
	_, first, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID == second.RunID {
		t.Fatalf("expected unique run IDs, got %q", first.RunID)
	}
}

func TestRunReportDoesNotContainBusinessPayloadFields(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(RunReport{}), reflect.TypeOf(StageReport{}), reflect.TypeOf(StepReport{}), reflect.TypeOf(AttemptReport{})} {
		for i := 0; i < typ.NumField(); i++ {
			if typ.Field(i).Type == reflect.TypeOf((*any)(nil)).Elem() {
				t.Fatalf("report type %s retains an any payload field %s", typ, typ.Field(i).Name)
			}
		}
	}
}

func assertFinishedReport(t *testing.T, status Status, startedAt, endedAt time.Time, duration time.Duration, want Status) {
	t.Helper()
	if status != want || startedAt.IsZero() || endedAt.IsZero() || endedAt.Before(startedAt) || duration < 0 {
		t.Fatalf("invalid finished report: status=%s start=%s end=%s duration=%s", status, startedAt, endedAt, duration)
	}
}
