package pipeflow

import (
	"errors"
	"testing"
)

type TestLogger struct {
	Messages []string
}

func (l *TestLogger) Info(message string) {
	l.Messages = append(l.Messages, "INFO: "+message)
}

func (l *TestLogger) Error(message string) {
	l.Messages = append(l.Messages, "ERROR: "+message)
}

func TestStepLogsExecution(t *testing.T) {
	logger := &TestLogger{}
	ctx := NewContextWithLogger(logger)

	step := NewStep("Test Step", func(ctx *Context, input any) (any, error) {
		return input, nil
	})

	_, err := step.Run(ctx, nil)

	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"INFO: Running step: Test Step",
		"INFO: Completed step: Test Step",
	}

	if len(logger.Messages) != len(expected) {
		t.Fatalf(
			"expected %d log messages, got %d",
			len(expected),
			len(logger.Messages),
		)
	}

	for i, message := range expected {
		if logger.Messages[i] != message {
			t.Errorf("expected %q, got %q", message, logger.Messages[i])
		}
	}
}

func TestStepLogsError(t *testing.T) {
	logger := &TestLogger{}
	ctx := NewContextWithLogger(logger)

	expected := errors.New("boom")

	step := NewStep("Test Step", func(ctx *Context, input any) (any, error) {
		return nil, expected
	})

	_, err := step.Run(ctx, nil)

	if err != expected {
		t.Fatal("expected error")
	}

	expectedMessages := []string{
		"INFO: Running step: Test Step",
		"ERROR: Step Test Step failed after 1 attempt(s)",
	}

	if len(logger.Messages) != len(expectedMessages) {
		t.Fatalf(
			"expected %d log messages, got %d",
			len(expectedMessages),
			len(logger.Messages),
		)
	}

	for i, expected := range expectedMessages {
		if logger.Messages[i] != expected {
			t.Errorf("expected %q, got %q", expected, logger.Messages[i])
		}
	}
}
