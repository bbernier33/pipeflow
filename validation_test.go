package pipeflow

import (
	"context"
	"strings"
	"testing"
)

func TestValidateInputRejectsInitialValueBeforeExecution(t *testing.T) {
	run, hook := false, false
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("consume", func(int) error { run = true; return nil }))).WithLifecycleHook(func(LifecycleEvent) { hook = true })
	if err := pipeline.Validate(); err != nil {
		t.Fatalf("Validate = %v", err)
	}
	if err := pipeline.ValidateInput("wrong"); err == nil || !strings.Contains(err.Error(), "expects int but previous output is string") {
		t.Fatalf("ValidateInput = %v", err)
	}
	_, report, err := pipeline.RunWithReport(context.Background(), "wrong")
	if err == nil || run || hook || report.RunID != "" {
		t.Fatalf("RunWithReport report=%#v err=%v run=%v hook=%v", report, err, run, hook)
	}
}

func TestValidateInputHandlesNilByGoAssignability(t *testing.T) {
	value := NewPipeline("value", NewStage("stage", NewStep("consume", func(int) error { return nil })))
	if err := value.ValidateInput(nil); err == nil || !strings.Contains(err.Error(), "previous output is nil") {
		t.Fatalf("value ValidateInput = %v", err)
	}
	pointer := NewPipeline("pointer", NewStage("stage", NewStep("consume", func(*int) error { return nil })))
	if err := pointer.ValidateInput(nil); err != nil {
		t.Fatalf("pointer ValidateInput = %v", err)
	}
}

func TestValidateConcurrentStepFlowAndOutput(t *testing.T) {
	invalidInput := NewPipeline("pipeline", NewStage("stage", NewStep("produce", func() (int, error) { return 1, nil }), NewConcurrentSteps([]*Step{NewStep("invalid", func(string) error { return nil })})))
	if err := invalidInput.Validate(); err == nil || !strings.Contains(err.Error(), "step \"invalid\" expects string") {
		t.Fatalf("Validate = %v", err)
	}
	invalidOutput := NewPipeline("pipeline", NewStage("stage", NewConcurrentSteps([]*Step{NewStep("one", func() error { return nil })}), NewStep("consume", func(string) error { return nil })))
	if err := invalidOutput.Validate(); err == nil || !strings.Contains(err.Error(), "previous output is []interface {}") {
		t.Fatalf("Validate = %v", err)
	}
}

func TestValidationRejectsInvalidWorkerAndFailurePolicies(t *testing.T) {
	tests := []struct {
		item StageItem
		want string
	}{
		{NewConcurrentSteps(nil, WithMaxWorkers(-1)), "max workers cannot be negative"},
		{NewConcurrentSteps(nil, WithFailurePolicy(FailurePolicy(99))), "invalid failure policy"},
		{NewParallel("parallel", nil, WithParallelFailurePolicy(FailurePolicy(99))), "invalid failure policy"},
	}
	for _, test := range tests {
		pipeline := NewPipeline("pipeline", NewStage("stage", test.item))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Validate = %v", err)
		}
	}
}

func TestValidationRejectsEmptyAndDuplicateScopedNames(t *testing.T) {
	finalizer := func(context.Context, Finalization) error { return nil }
	tests := []struct {
		pipeline Pipeline
		want     string
	}{
		{NewPipeline(""), "pipeline name cannot be empty"},
		{NewPipeline("pipeline", NewStage("")), "stage with an empty name"},
		{NewPipeline("pipeline", NewStage("same"), NewStage("same")), "duplicate stage name"},
		{NewPipeline("pipeline", NewStage("stage", NewStep("", func() error { return nil }))), "step with an empty name"},
		{NewPipeline("pipeline", NewStage("stage", NewStep("same", func() error { return nil }), NewConcurrentSteps([]*Step{NewStep("same", func() error { return nil })}))), "duplicate step name"},
		{NewPipeline("pipeline", NewStage("stage", NewParallel("same", nil), NewParallel("same", nil))), "duplicate parallel name"},
		{NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{NewBranch("same"), NewBranch("same")}))), "duplicate branch name"},
		{NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{NewBranch("branch", NewStep("same", func() error { return nil }), NewStep("same", func() error { return nil }))}))), "duplicate step name"},
		{NewPipeline("pipeline", NewStage("stage", NewSubflow("same"), NewSubflow("same"))), "duplicate subflow name"},
		{NewPipeline("pipeline", NewStage("stage", NewSubflow("subflow", NewStage("same"), NewStage("same")))), "duplicate stage name"},
		{NewPipeline("pipeline").WithBackground("", func(context.Context) error { return nil }), "background name cannot be empty"},
		{NewPipeline("pipeline").Finally("", finalizer), "finalizer name cannot be empty"},
		{NewPipeline("pipeline").Finally("same", finalizer).Finally("same", finalizer), "duplicate finalizer name"},
	}
	for _, test := range tests {
		if err := test.pipeline.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("want %q, Validate = %v", test.want, err)
		}
	}
}

func TestValidationRejectsNilExecutionUnits(t *testing.T) {
	var step *Step
	var concurrent *ConcurrentSteps
	var parallel *Parallel
	var subflow *Subflow
	for _, item := range []StageItem{step, concurrent, parallel, subflow} {
		pipeline := NewPipeline("pipeline", NewStage("stage", item))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "nil item") {
			t.Fatalf("item %T: Validate = %v", item, err)
		}
	}
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{NewBranch("branch", nil)})))
	if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "nil step") {
		t.Fatalf("Validate = %v", err)
	}
}

func TestValidationPreservesDocumentedDefaultsAndEmptyComposition(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewConcurrentSteps(nil, WithMaxWorkers(0)), NewParallel("parallel", nil), NewSubflow("subflow"))).WithTimeout(0)
	if err := pipeline.Validate(); err != nil {
		t.Fatalf("Validate = %v", err)
	}
}
