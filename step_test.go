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
