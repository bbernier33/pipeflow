package pipeflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConcurrentStepsReturnsResultsInDeclaredOrder(t *testing.T) {
	ctx := NewContext()
	goCtx := context.Background()

	first := NewStep(
		"first",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return "first-result", nil
		},
	)

	second := NewStep(
		"second",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return "second-result", nil
		},
	)

	group := NewConcurrentSteps([]*Step{first, second})

	output, err := group.Run(goCtx, ctx, "input")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	results, ok := output.([]any)
	if !ok {
		t.Fatalf("expected output type []any, got %T", output)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 reults, got %d", len(results))
	}
	if results[0] != "first-result" {
		t.Errorf("expected first-result at index 0, got %v", results[0])
	}
	if results[1] != "second-result" {
		t.Errorf("expected second-result at index 1, got %v", results[1])
	}
}

func TestConcurrentStepsPassesSameInputToEveryStep(t *testing.T) {
	ctx := NewContext()

	goCtx := context.Background()

	expectedInput := "shared-input"

	first := NewStep(
		"first",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			if input != expectedInput {
				t.Errorf("first step received %v, expected %v", input, expectedInput)
			}
			return nil, nil
		},
	)

	second := NewStep(
		"second",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			if input != expectedInput {
				t.Errorf("second step received %v, expected %v", input, expectedInput)
			}
			return nil, nil
		},
	)

	group := NewConcurrentSteps([]*Step{first, second})

	_, err := group.Run(goCtx, ctx, expectedInput)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

}

func TestConcurrentStepStartsStepsConcurrently(t *testing.T) {
	ctx := NewContext()

	goCtx := context.Background()

	started := make(chan string, 2)
	release := make(chan struct{})

	first := NewStep(
		"first",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			started <- "first"
			<-release
			return "first-result", nil
		},
	)

	second := NewStep(
		"second",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			started <- "second"
			<-release
			return "second-result", nil
		},
	)

	group := NewConcurrentSteps([]*Step{first, second})

	done := make(chan error, 1)
	go func() {
		_, err := group.Run(goCtx, ctx, nil)
		done <- err
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
			// A step started.
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for concurrent steps to start")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestConcurrentStepsReturnsSetpError(t *testing.T) {
	ctx := NewContext()

	goCtx := context.Background()

	expectedErr := errors.New("Step failed")
	successfulStep := NewStep(
		"successful",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return "success", nil
		},
	)
	failingStep := NewStep(
		"failing",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return nil, expectedErr
		},
	)

	group := NewConcurrentSteps([]*Step{successfulStep, failingStep})
	output, err := group.Run(goCtx, ctx, nil)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error to contain %v, got %v", expectedErr, err)
	}
	if output != nil {
		t.Errorf("expected nil output when a step fails, got %v", output)
	}
}

func TestConcurrentStepsJoinsMultipleErrors(t *testing.T) {
	ctx := NewContext()

	goCtx := context.Background()

	firstErr := errors.New("first failure")
	secondErr := errors.New("second failure")

	first := NewStep(
		"first",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return nil, firstErr
		},
	)
	second := NewStep(
		"second",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return nil, secondErr
		},
	)

	group := NewConcurrentSteps([]*Step{first, second})

	_, err := group.Run(goCtx, ctx, nil)

	if err == nil {
		t.Fatal("expected an error, got nit")
	}
	if !errors.Is(err, firstErr) {
		t.Errorf("expected joined error to contain fristErr")
	}
	if !errors.Is(err, secondErr) {
		t.Errorf("expected joined error to contain secondErr")
	}
}

func TestConcurrentStepsWithNoStepsReturnsEmptyResults(t *testing.T) {
	ctx := NewContext()

	goCtx := context.Background()

	group := NewConcurrentSteps([]*Step{})

	output, err := group.Run(goCtx, ctx, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	results, ok := output.([]any)
	if !ok {
		t.Fatalf("expected output type []any, got %T", output)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}

}
