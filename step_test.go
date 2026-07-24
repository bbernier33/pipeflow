package pipeflow

import (
	"errors"
	"testing"
)

func TestStepRunExecutesAction(t *testing.T) {
	executed := false

	step := NewStep("Test", func() error {
		executed = true
		return nil
	})

	err := step.Run()

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !executed {
		t.Error("expected step action to be executed")
	}
}

func TestStepRunReturnsActionError(t *testing.T) {
	expected := errors.New("boom")

	step := NewStep("Fail", func() error {
		return expected
	})

	err := step.Run()

	if err != expected {
		t.Errorf("expected error %v, got %v", expected, err)
	}
}
