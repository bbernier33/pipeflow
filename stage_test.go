package pipeflow

import (
	"context"
	"errors"
	"testing"
)

func TestStageRunExecutesStepsInOrder(t *testing.T) {
	executionOrder := []string{}

	goCtx := context.Background()

	stepOne := NewStep("Step One", func(goCtx context.Context, ctx *Context, input any) (any, error) {
		executionOrder = append(executionOrder, "Step One")
		return input.(int) + 1, nil
	})

	stepTwo := NewStep("Step Two", func(goCtx context.Context, ctx *Context, input any) (any, error) {
		executionOrder = append(executionOrder, "Step Two")
		return input.(int) * 2, nil
	})

	stage := NewStage("Test Stage", stepOne, stepTwo)

	ctx := NewContext()

	output, err := stage.Run(goCtx, ctx, 5)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if output.(int) != 12 {
		t.Errorf("expected output to be 12, got %v", output)
	}

	if len(executionOrder) != 2 {
		t.Fatalf("expected 2 executed steps, got %d", len(executionOrder))
	}

	if executionOrder[0] != "Step One" {
		t.Errorf("expected first step to be 'Step One', got %s", executionOrder[0])
	}

	if executionOrder[1] != "Step Two" {
		t.Errorf("expected second step to be 'Step Two', got %s", executionOrder[1])
	}
}

func TestStageRunStopsAfterStepError(t *testing.T) {
	executionOrder := []string{}
	expectedError := errors.New("step failed")

	goCtx := context.Background()

	stepOne := NewStep("Step One", func(goCtx context.Context, ctx *Context, input any) (any, error) {
		executionOrder = append(executionOrder, "Step One")
		return input, nil
	})

	stepTwo := NewStep("Step Two", func(goCtx context.Context, ctx *Context, input any) (any, error) {
		executionOrder = append(executionOrder, "Step Two")
		return nil, expectedError
	})

	stepThree := NewStep("Step Three", func(goCtx context.Context, ctx *Context, input any) (any, error) {
		executionOrder = append(executionOrder, "Step Three")
		return input, nil
	})

	stage := NewStage("Test Stage", stepOne, stepTwo, stepThree)
	ctx := NewContext()

	_, err := stage.Run(goCtx, ctx, 5)

	if !errors.Is(err, expectedError) {
		t.Errorf("expected error %v, got %v", expectedError, err)
	}

	if len(executionOrder) != 2 {
		t.Fatalf("expected 2 executed steps, got %d", len(executionOrder))
	}

	if executionOrder[0] != "Step One" {
		t.Errorf("expected first step to be 'Step One', got %s", executionOrder[0])
	}

	if executionOrder[1] != "Step Two" {
		t.Errorf("expected second step to be 'Step Two', got %s", executionOrder[1])
	}
}
