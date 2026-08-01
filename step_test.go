package pipeflow

import (
	"errors"
	"testing"
)

func TestStepRunExecutesActionAndReturnsOutput(t *testing.T) {
	executed := false

	step := NewStep("Test", func(ctx *Context, input any) (any, error) {
		executed = true
		return input, nil
	})
	ctx := NewContext()
	output, err := step.Run(ctx, 5)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if output.(int) != 5 {
		t.Errorf("expected output 5, got %v", output)
	}

	if !executed {
		t.Error("expected step action to be executed")
	}
}

func TestStepRunReturnsActionError(t *testing.T) {
	expected := errors.New("boom")

	step := NewStep("Fail", func(ctx *Context, input any) (any, error) {
		return nil, expected
	})

	ctx := NewContext()
	_, err := step.Run(ctx, 5)

	if err != expected {
		t.Errorf("expected error %v, got %v", expected, err)
	}
}

func TestStepRunRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	expected := errors.New("temporary failure")

	step := NewStep("Retry", func(ctx *Context, input any) (any, error) {
		attempts++

		if attempts < 3 {
			return nil, expected
		}

		return "done", nil
	}, WithRetry(RetryPolicy{
		MaxAttempts: 3,
	}),
	)
	ctx := NewContext()
	output, err := step.Run(ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if output != "done" {
		t.Errorf("expected output 'done', got %v", output)
	}
}

func TestStepRunReturnErrorAfterRetryExhaustion(t *testing.T) {
	attempts := 0
	expected := errors.New("persistent failure")

	step := NewStep("RetryFail", func(ctx *Context, input any) (any, error) {
		attempts++
		return nil, expected
	}, WithRetry(RetryPolicy{
		MaxAttempts: 3,
	}))

	ctx := NewContext()
	_, err := step.Run(ctx, nil)

	if err != expected {
		t.Errorf("expected error %v, got %v", expected, err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestStepRunDoesNotRetryAfterSuccess(t *testing.T) {
	attempts := 0

	step := NewStep("NoRetry", func(ctx *Context, input any) (any, error) {
		attempts++
		return "success", nil
	}, WithRetry(RetryPolicy{
		MaxAttempts: 3,
	}))

	ctx := NewContext()
	_, err := step.Run(ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}
