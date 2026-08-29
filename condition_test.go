package pipeflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestConditionTrueRunsTransformer(t *testing.T) {
	pipeline := NewPipeline("orders", NewStage("process",
		NewStep("produce", func() (int, error) { return 20, nil }),
		NewStep("tax", func(value int) (int, error) { return value + 2, nil }, WithCondition(func(value int) bool { return value == 20 })),
		NewStep("consume", func(value int) error {
			if value != 22 {
				t.Fatalf("consumer received %d", value)
			}
			return nil
		}),
	))

	output, err := pipeline.Run(context.Background())
	if err != nil || output != 22 {
		t.Fatalf("Run = (%v, %v)", output, err)
	}
}

func TestConditionFalsePassesInputThrough(t *testing.T) {
	var transformed atomic.Bool
	pipeline := NewPipeline("orders", NewStage("process",
		NewStep("produce", func() (int, error) { return 20, nil }),
		NewStep("optional tax", func(value int) (int, error) {
			transformed.Store(true)
			return value + 2, nil
		}, WithCondition(func(value int) bool { return false })),
		NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))

	output, err := pipeline.Run(context.Background())
	if err != nil || output != 40 || transformed.Load() {
		t.Fatalf("Run = (%v, %v), transformed = %v", output, err, transformed.Load())
	}
}

func TestParameterlessConditionPreservesFlowingValue(t *testing.T) {
	called := false
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (string, error) { return "value", nil }),
		NewStep("optional side effect", func() error { called = true; return nil }, WithCondition(func() bool { return false })),
	))

	output, err := pipeline.Run(context.Background())
	if err != nil || output != "value" || called {
		t.Fatalf("Run = (%v, %v), called = %v", output, err, called)
	}
}

func TestSkippedConditionReportAndLifecycle(t *testing.T) {
	var events []LifecycleEventType
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("skip", func() error { t.Fatal("action ran"); return nil }, WithCondition(func() bool { return false })),
	)).WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event.Type) })

	_, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusSkipped || !step.StartedAt.IsZero() || !step.EndedAt.IsZero() || step.Duration != 0 || step.Error != nil || len(step.Attempts) != 0 || len(step.Polls) != 0 {
		t.Fatalf("step report = %#v", step)
	}
	foundSkipped := false
	for _, event := range events {
		if event == StepStarted || event == StepCompleted || event == StepFailed {
			t.Fatalf("unexpected lifecycle event %q", event)
		}
		foundSkipped = foundSkipped || event == StepSkipped
	}
	if !foundSkipped {
		t.Fatalf("events = %v", events)
	}
}

func TestConditionEvaluatedOnceBeforeRetryAndPolling(t *testing.T) {
	var conditions, actions atomic.Int32
	step := NewStep("work", func() (int, error) {
		actions.Add(1)
		return int(actions.Load()), nil
	},
		WithCondition(func() bool { conditions.Add(1); return true }),
		WithRetry(RetryPolicy{MaxAttempts: 3}),
		WithPolling(PollPolicy{MaxPolls: 3, Until: func(value int) bool { return value == 2 }}),
	)

	output, err := step.Run(context.Background(), NewContext(), nil)
	if err != nil || output != 2 || conditions.Load() != 1 || actions.Load() != 2 {
		t.Fatalf("Run = (%v, %v), conditions = %d, actions = %d", output, err, conditions.Load(), actions.Load())
	}
}

func TestConditionValidationAndRuntimeTypeChecks(t *testing.T) {
	staticMismatch := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewStep("consume", func(int) error { return nil }, WithCondition(func(string) bool { return true })),
	))
	if err := staticMismatch.Validate(); err == nil || !strings.Contains(err.Error(), "condition expects string") {
		t.Fatalf("Validate error = %v", err)
	}

	// A conditional transformer can produce either its input or output type, so
	// validation deliberately leaves the following type unknown.
	runtimeMismatch := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewStep("optional format", func(int) (string, error) { return "one", nil }, WithCondition(func(int) bool { return false })),
		NewStep("consume", func(string) error { return nil }),
	))
	if err := runtimeMismatch.Validate(); err != nil {
		t.Fatalf("Validate = %v", err)
	}
	if _, err := runtimeMismatch.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot use int as string") {
		t.Fatalf("Run error = %v", err)
	}
}

func TestConditionSupportsNilAndZeroValues(t *testing.T) {
	type item struct{}
	var nilSeen, zeroSeen bool
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("nil", func() (*item, error) { return nil, nil }),
		NewStep("nil condition", func(*item) error { t.Fatal("nil action ran"); return nil }, WithCondition(func(value *item) bool { nilSeen = value == nil; return false })),
		NewStep("zero", func(*item) (int, error) { return 0, nil }),
		NewStep("zero condition", func(int) error { t.Fatal("zero action ran"); return nil }, WithCondition(func(value int) bool { zeroSeen = value == 0; return false })),
	))

	output, err := pipeline.Run(context.Background())
	if err != nil || output != 0 || !nilSeen || !zeroSeen {
		t.Fatalf("Run = (%#v, %v), nilSeen = %v, zeroSeen = %v", output, err, nilSeen, zeroSeen)
	}
}

func TestConditionCancellationAndPanic(t *testing.T) {
	t.Run("pre-cancelled does not evaluate condition", func(t *testing.T) {
		goCtx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		step := NewStep("work", func() error { return nil }, WithCondition(func() bool { called = true; return false }))
		if _, err := step.Run(goCtx, NewContext(), nil); !errors.Is(err, context.Canceled) || called {
			t.Fatalf("Run error = %v, called = %v", err, called)
		}
	})

	t.Run("panic becomes PanicError", func(t *testing.T) {
		step := NewStep("work", func() error { return nil }, WithCondition(func() bool { panic("condition panic") }))
		_, err := step.Run(context.Background(), NewContext(), nil)
		var panicErr *PanicError
		if !errors.As(err, &panicErr) {
			t.Fatalf("Run error = %T %v", err, err)
		}
	})
}

func TestInvalidConditions(t *testing.T) {
	tests := []any{nil, true, func() {}, func() error { return nil }, func(int, int) bool { return true }}
	for _, condition := range tests {
		step := NewStep("work", func() error { return nil }, WithCondition(condition))
		pipeline := NewPipeline("pipeline", NewStage("stage", step))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "condition must be func() bool or func(T) bool") {
			t.Fatalf("condition %T: Validate error = %v", condition, err)
		}
	}
}

func TestConditionInParallelBranch(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{
		NewBranch("branch", NewStep("optional", func(int) (int, error) { return 99, nil }, WithCondition(func(int) bool { return false }))),
	})))

	output, report, err := pipeline.RunWithReport(context.Background(), 7)
	results := output.(ParallelResults)
	step := report.Stages[0].Parallels[0].Branches[0].Steps[0]
	if err != nil || results[0].Value != 7 || step.Status != StatusSkipped {
		t.Fatalf("Run = (%#v, %#v, %v)", output, step, err)
	}
}
