package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExecutionErrorContainsFullSequentialLocation(t *testing.T) {
	want := errors.New("save failed")
	p := NewPipeline("invoice pipeline",
		NewStage("output", NewStep("save", func() error { return want })),
	)

	_, err := p.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("expected underlying cause, got %v", err)
	}
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) {
		t.Fatalf("expected ExecutionError, got %T", err)
	}
	if executionErr.Pipeline != "invoice pipeline" || executionErr.Stage != "output" || executionErr.Step != "save" || executionErr.Attempt != 1 {
		t.Fatalf("unexpected location: %+v", executionErr)
	}
	if errors.Unwrap(executionErr) != want {
		t.Fatalf("expected direct unwrap to original cause, got %v", errors.Unwrap(executionErr))
	}
	wantMessage := `pipeflow: pipeline "invoice pipeline", stage "output", step "save", attempt 1: save failed`
	if executionErr.Error() != wantMessage {
		t.Fatalf("expected %q, got %q", wantMessage, executionErr.Error())
	}
}

func TestExecutionErrorRecordsExhaustedRetryAttempt(t *testing.T) {
	want := errors.New("unavailable")
	step := NewStep("fetch", func() error { return want }, WithRetry(RetryPolicy{MaxAttempts: 3}))

	_, err := step.Run(context.Background(), NewContext(), nil)
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Step != "fetch" || executionErr.Attempt != 3 {
		t.Fatalf("expected fetch attempt 3, got %+v", executionErr)
	}
}

func TestExecutionErrorRecordsCancellationDuringRetryDelay(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	step := NewStep("fetch", func() error {
		cancel()
		return errors.New("temporary")
	}, WithRetry(RetryPolicy{MaxAttempts: 3, Delay: time.Second}))

	_, err := step.Run(goCtx, NewContext(), nil)
	var executionErr *ExecutionError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &executionErr) || executionErr.Attempt != 1 {
		t.Fatalf("expected cancellation during attempt 1, got %+v / %v", executionErr, err)
	}
}

func TestExecutionErrorEnrichesEveryConcurrentFailure(t *testing.T) {
	firstCause := errors.New("first failed")
	secondCause := errors.New("second failed")
	p := NewPipeline("parallel",
		NewStage("fanout", NewConcurrentSteps([]*Step{
			NewStep("first", func() error { return firstCause }),
			NewStep("second", func() error { return secondCause }),
		})),
	)

	_, err := p.Run(context.Background())
	if !errors.Is(err, firstCause) || !errors.Is(err, secondCause) {
		t.Fatalf("expected both causes, got %v", err)
	}
	executionErrors := collectExecutionErrors(err)
	if len(executionErrors) != 2 {
		t.Fatalf("expected two structured branch errors, got %d: %v", len(executionErrors), err)
	}
	gotSteps := []string{executionErrors[0].Step, executionErrors[1].Step}
	if !reflect.DeepEqual(gotSteps, []string{"first", "second"}) {
		t.Fatalf("expected declaration-order branch errors, got %v", gotSteps)
	}
	for _, executionErr := range executionErrors {
		if executionErr.Pipeline != "parallel" || executionErr.Stage != "fanout" || executionErr.Attempt != 1 {
			t.Fatalf("incomplete branch location: %+v", executionErr)
		}
	}
}

func TestExecutionErrorForPipelineCancellation(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	cancel()
	p := NewPipeline("cancelled")

	_, err := p.Run(goCtx)
	var executionErr *ExecutionError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &executionErr) {
		t.Fatalf("expected structured cancellation, got %v", err)
	}
	if executionErr.Pipeline != "cancelled" || executionErr.Stage != "" || executionErr.Step != "" || executionErr.Attempt != 0 {
		t.Fatalf("unexpected cancellation location: %+v", executionErr)
	}
}

func TestPipelineFailedHookReceivesExecutionError(t *testing.T) {
	want := errors.New("boom")
	p := NewPipeline("hooks", NewStage("stage", NewStep("step", func() error { return want })))
	var hookErr error
	p.OnFailed(func(event PipelineEvent) { hookErr = event.Err })

	_, returnedErr := p.Run(context.Background())
	var executionErr *ExecutionError
	if hookErr != returnedErr || !errors.As(hookErr, &executionErr) {
		t.Fatalf("expected hook to receive returned structured error, hook=%v returned=%v", hookErr, returnedErr)
	}
}

func TestValidationErrorIsNotExecutionError(t *testing.T) {
	p := NewPipeline("invalid", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewStep("consume", func(string) error { return nil }),
	))

	_, err := p.Run(context.Background())
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) {
		t.Fatalf("expected pre-run validation error, got execution error %+v", executionErr)
	}
	if err == nil || !strings.Contains(err.Error(), "expects string") {
		t.Fatalf("expected compatibility error, got %v", err)
	}
}

func collectExecutionErrors(err error) []*ExecutionError {
	if err == nil {
		return nil
	}
	if executionErr, ok := err.(*ExecutionError); ok {
		return []*ExecutionError{executionErr}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result []*ExecutionError
		for _, child := range joined.Unwrap() {
			result = append(result, collectExecutionErrors(child)...)
		}
		return result
	}
	return nil
}
