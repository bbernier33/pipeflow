package pipeflow

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValueFlowProducerTransformerConsumer(t *testing.T) {
	var consumed string
	p := NewPipeline("flow", NewStage("stage",
		NewStep("produce", func() (int, error) { return 21, nil }),
		NewStep("transform", func(v int) (string, error) { return "value=42", nil }),
		NewStep("consume", func(v string) error { consumed = v; return nil }),
	))

	output, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != "value=42" || consumed != "value=42" {
		t.Fatalf("expected flowing and consumed value=42, got output=%v consumed=%q", output, consumed)
	}
}

func TestValueFlowSideEffectPassesThrough(t *testing.T) {
	seen := 0
	p := NewPipeline("flow", NewStage("stage",
		NewStep("produce", func() (int, error) { return 4, nil }),
		NewStep("observe", func(v int) error { seen = v; return nil }),
		NewStep("transform", func(v int) (int, error) { return v * 3, nil }),
	))

	output, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != 12 || seen != 4 {
		t.Fatalf("expected output 12 and side effect 4, got %v and %d", output, seen)
	}
}

func TestValueFlowNoArgumentActionPreservesCurrentValue(t *testing.T) {
	called := false
	p := NewPipeline("flow", NewStage("stage",
		NewStep("action", func() error { called = true; return nil }),
		NewStep("transform", func(v int) (int, error) { return v + 1, nil }),
	))

	output, err := p.Run(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if !called || output != 10 {
		t.Fatalf("expected action and output 10, got called=%v output=%v", called, output)
	}
}

func TestValueFlowAcrossStages(t *testing.T) {
	p := NewPipeline("flow",
		NewStage("produce", NewStep("produce", func() (int, error) { return 7, nil })),
		NewStage("transform", NewStep("transform", func(v int) (string, error) { return strings.Repeat("x", v), nil })),
	)

	output, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != "xxxxxxx" {
		t.Fatalf("expected stage output to flow to next stage, got %v", output)
	}
}

func TestValueFlowRejectsIncompatibleAdjacentStepsBeforeExecution(t *testing.T) {
	executed := false
	p := NewPipeline("flow", NewStage("stage",
		NewStep("produce", func() (int, error) { executed = true; return 1, nil }),
		NewStep("consume", func(string) error { return nil }),
	))

	_, err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), `step "consume" expects string but previous output is int`) {
		t.Fatalf("expected descriptive compatibility error, got %v", err)
	}
	if executed {
		t.Fatal("expected validation before execution")
	}
}

func TestValueFlowRejectsIncompatibleInitialInput(t *testing.T) {
	p := NewPipeline("flow", NewStage("stage", NewStep("consume", func(int) error { return nil })))
	_, err := p.Run(context.Background(), "wrong")
	if err == nil || !strings.Contains(err.Error(), "expects int but previous output is string") {
		t.Fatalf("expected pre-execution input error, got %v", err)
	}
}

func TestValueFlowNilAndZeroValues(t *testing.T) {
	t.Run("typed nil", func(t *testing.T) {
		p := NewPipeline("nil", NewStage("stage",
			NewStep("produce", func() (*int, error) { return nil, nil }),
			NewStep("consume", func(v *int) error {
				if v != nil {
					t.Fatalf("expected nil pointer, got %v", v)
				}
				return nil
			}),
		))
		output, err := p.Run(context.Background())
		if err != nil || output != (*int)(nil) {
			t.Fatalf("expected typed nil flow, got output=%v err=%v", output, err)
		}
	})

	t.Run("zero", func(t *testing.T) {
		p := NewPipeline("zero", NewStage("stage",
			NewStep("produce", func() (int, error) { return 0, nil }),
			NewStep("transform", func(v int) (int, error) { return v, nil }),
		))
		output, err := p.Run(context.Background())
		if err != nil || output != 0 {
			t.Fatalf("expected zero value flow, got output=%v err=%v", output, err)
		}
	})
}

func TestValueFlowPreservesFunctionError(t *testing.T) {
	want := errors.New("business failure")
	p := NewPipeline("errors", NewStage("stage", NewStep("fail", func() (int, error) { return 0, want })))
	_, got := p.Run(context.Background())
	if !errors.Is(got, want) {
		t.Fatalf("expected original error cause, got %v", got)
	}
}

func TestLegacyExecutionAPIStillWorks(t *testing.T) {
	ctx := NewContext()
	p := NewPipeline("legacy", NewStage("stage", NewStep("legacy",
		func(_ context.Context, got *Context, input any) (any, error) {
			if got != ctx {
				t.Fatal("expected supplied Pipeflow context")
			}
			return input.(int) + 1, nil
		})))
	output, err := p.Run(context.Background(), ctx, 4)
	if err != nil || output != 5 {
		t.Fatalf("expected legacy output 5, got output=%v err=%v", output, err)
	}
}

func TestUnsupportedStepSignatureIsValidationError(t *testing.T) {
	p := NewPipeline("invalid", NewStage("stage", NewStep("bad", func() int { return 1 })))
	_, err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported function signature") {
		t.Fatalf("expected unsupported signature error, got %v", err)
	}
}
