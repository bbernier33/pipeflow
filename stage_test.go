package pipeflow

import (
	"errors"
	"testing"
)

func TestStageRunExecutesStepsInOrder(t *testing.T) {
	executionOrder := []string{}

	stepOne := NewStep("Step One", func() error {
		executionOrder = append(executionOrder, "Step One")
		return nil
	})

	stepTwo := NewStep("Step Two", func() error {
		executionOrder = append(executionOrder, "Step Two")
		return nil
	})

	stage := NewStage("Test Stage", stepOne, stepTwo)

	err := stage.Run()

	if err != nil {
		t.Errorf("expected no error, got %v", err)
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

	stepOne := NewStep("Step One", func() error {
		executionOrder = append(executionOrder, "Step One")
		return nil
	})

	stepTwo := NewStep("Step Two", func() error {
		executionOrder = append(executionOrder, "Step Two")
		return expectedError
	})

	stepThree := NewStep("Step Three", func() error {
		executionOrder = append(executionOrder, "Step Three")
		return nil
	})

	stage := NewStage("Test Stage", stepOne, stepTwo, stepThree)

	err := stage.Run()

	if err != expectedError {
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
