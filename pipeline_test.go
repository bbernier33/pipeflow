package pipeflow

import (
	"errors"
	"testing"
)

func TestPipelineRunExecutesStagesInOrder(t *testing.T) {
	executionOrder := []string{}

	stepOne := NewStep("Step One", func() error {
		executionOrder = append(executionOrder, "Step One")
		return nil
	})

	stepTwo := NewStep("Step Two", func() error {
		executionOrder = append(executionOrder, "Step Two")
		return nil
	})

	stageOne := NewStage("Stage One", stepOne)
	stageTwo := NewStage("Stage Two", stepTwo)

	pipeline := NewPipeline("Pipeline", stageOne, stageTwo)

	err := pipeline.Run()

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

func TestPipelineRunStopsAfterStageError(t *testing.T) {
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

	stageOne := NewStage("Stage One", stepOne)
	stageTwo := NewStage("Stage Two", stepTwo)
	stageThree := NewStage("Stage Three", stepThree)

	pipeline := NewPipeline("Pipeline", stageOne, stageTwo, stageThree)

	err := pipeline.Run()

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
