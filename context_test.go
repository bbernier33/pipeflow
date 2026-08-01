package pipeflow

import (
	"testing"
	"time"
)

func TestContextSetAndGet(t *testing.T) {
	ctx := NewContext()

	ctx.Set("foo", "bar")
	value, exists := ctx.Get("foo")

	if !exists {
		t.Fatal("expected value to exist")
	}
	if value != "bar" {
		t.Errorf("expected bar, got %v", value)
	}
}

func TestContextGetMissingKey(t *testing.T) {
	ctx := NewContext()

	_, exists := ctx.Get("missing")
	if exists {
		t.Fatal("expected value to not exist")
	}

}

func TestPipelineSharesContextAcressSteps(t *testing.T) {
	ctx := NewContext()

	stepOne := NewStep("Set Value", func(ctx *Context, input any) (any, error) {
		ctx.Set("count", 10)
		return input, nil
	})

	stepTwo := NewStep("Get Value", func(ctx *Context, input any) (any, error) {
		value, exists := ctx.Get("count")

		if !exists {
			t.Fatal("expecgted count to exist")
		}

		return value, nil
	})

	stageOne := NewStage("Stage One", stepOne)
	stageTwo := NewStage("Stage Two", stepTwo)

	pipeline := NewPipeline("Pipeline", stageOne, stageTwo)

	output, err := pipeline.Run(ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if output.(int) != 10 {
		t.Errorf("expected output 10, got %v", output)
	}

}

func TestNewContextPendingStatus(t *testing.T) {
	ctx := NewContext()

	if ctx.Status() != StatusPending {
		t.Fatalf(
			"expected status %q, for %q",
			StatusPending,
			ctx.Status(),
		)
	}
}

func TestNewContextHasNoExecutionTiming(t *testing.T) {
	ctx := NewContext()

	if !ctx.StartedAt().IsZero() {
		t.Fatalf("expected start time to be zero")
	}
	if !ctx.EndedAt().IsZero() {
		t.Fatal("expected end time to be zero")
	}
	if ctx.Duration() != 0 {
		t.Fatalf("expected duration 0, got %s", ctx.Duration())
	}
}

func TestContextTracksCompletedExecution(t *testing.T) {
	ctx := NewContext()

	ctx.markStarted()
	time.Sleep(time.Millisecond)
	ctx.markCompleted()

	if ctx.Status() != StatusCompleted {
		t.Fatalf(
			"expected status %q, got %q",
			StatusCompleted,
			ctx.Status(),
		)
	}
	if ctx.StartedAt().IsZero() {
		t.Fatalf("expected start time to be recorded")
	}
	if ctx.EndedAt().IsZero() {
		t.Fatalf("expected end time to be recorded")
	}
	if ctx.Duration() <= 0 {
		t.Fatalf("extected positiv duration, got %s", ctx.Duration())
	}
}

func TestContextTracksFailedExecution(t *testing.T) {
	ctx := NewContext()

	ctx.markStarted()
	ctx.markFailed()

	if ctx.Status() != StatusFailed {
		t.Fatalf(
			"expected status %q, got %q",
			StatusFailed,
			ctx.Status(),
		)
	}
	if ctx.EndedAt().IsZero() {
		t.Fatalf("expected end time to be recorded")
	}
}
