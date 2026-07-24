package pipeflow

import (
	"errors"
	"testing"
)

func TestStepRunExecutesActionAndReturnsOutput(t *testing.T) {
	executed := false

	step := NewStep("Test", func(input any) (any, error) {
		executed = true
		return input, nil
	})

	output, err := step.Run(5)

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

	step := NewStep("Fail", func(input any) (any, error) {
		return nil, expected
	})

	_, err := step.Run(5)

	if err != expected {
		t.Errorf("expected error %v, got %v", expected, err)
	}
}
