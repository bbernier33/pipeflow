package pipeflow

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestPipelineDescribeSimpleTree(t *testing.T) {
	pipeline := NewPipeline("Invoice Processing",
		NewStage("Ingest", NewStep("Read", func() error { return nil }), NewStep("Parse", func() error { return nil })),
		NewStage("Normalize", NewStep("Clean", func() error { return nil }), NewStep("Deduplicate", func() error { return nil })),
		NewStage("Output", NewStep("Store", func() error { return nil })),
	)
	description := pipeline.Describe()
	if description.Kind != DescriptionPipeline || description.Name != "Invoice Processing" || len(description.Children) != 3 {
		t.Fatalf("Describe = %#v", description)
	}
	want := "Invoice Processing\n" +
		"├── Ingest\n" +
		"│   ├── Read\n" +
		"│   └── Parse\n" +
		"├── Normalize\n" +
		"│   ├── Clean\n" +
		"│   └── Deduplicate\n" +
		"└── Output\n" +
		"    └── Store"
	if got := description.String(); got != want {
		t.Fatalf("String():\n%s\nwant:\n%s", got, want)
	}
	if got := fmt.Sprint(description); got != want {
		t.Fatalf("fmt.Sprint = %q", got)
	}
}

func TestPipelineDescribeAllCompositionKinds(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("host",
		NewConcurrentSteps([]*Step{NewStep("one", func() error { return nil }), NewStep("two", func() error { return nil })}),
		NewParallel("providers", []Branch{NewBranch("gmail", NewStep("fetch", func() error { return nil }))}),
		NewSubflow("prepare", NewStage("nested", NewStep("normalize", func() error { return nil }))),
		describedCustomItem{},
	))
	description := pipeline.Describe()
	children := description.Children[0].Children
	wantKinds := []DescriptionKind{DescriptionConcurrent, DescriptionParallel, DescriptionSubflow, DescriptionCustomItem}
	for i, want := range wantKinds {
		if children[i].Kind != want {
			t.Fatalf("child %d kind = %q, want %q", i, children[i].Kind, want)
		}
	}
	if children[0].Children[1].Name != "two" || children[1].Children[0].Kind != DescriptionBranch || children[1].Children[0].Children[0].Name != "fetch" {
		t.Fatalf("description = %#v", description)
	}
	if children[2].Children[0].Children[0].Name != "normalize" || children[3].Type != "pipeflow.describedCustomItem" {
		t.Fatalf("description = %#v", description)
	}
}

func TestPipelineDescribeDoesNotExecuteOrExposePolicies(t *testing.T) {
	called := false
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { called = true; return nil }, WithTimeout(1), WithRetry(RetryPolicy{MaxAttempts: 3}))))
	description := pipeline.Describe()
	if called {
		t.Fatal("Describe executed user code")
	}
	step := description.Children[0].Children[0]
	if step.Kind != DescriptionStep || step.Name != "step" || step.Type != "" || len(step.Children) != 0 {
		t.Fatalf("step description = %#v", step)
	}
}

func TestPipelineDescribeReturnsIndependentTrees(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil })))
	first := pipeline.Describe()
	first.Name = "changed"
	first.Children[0].Name = "changed"
	first.Children[0].Children[0].Name = "changed"
	second := pipeline.Describe()
	if second.Name != "pipeline" || second.Children[0].Name != "stage" || second.Children[0].Children[0].Name != "step" {
		t.Fatalf("mutation leaked: %#v", second)
	}
}

func TestPipelineDescribeInvalidDefinitionsWithoutPanicking(t *testing.T) {
	var step *Step
	pipeline := NewPipeline("", NewStage("", step))
	description := pipeline.Describe()
	if description.String() != "<unnamed pipeline>\n└── <unnamed stage>\n    └── *pipeflow.Step" {
		t.Fatalf("String = %q", description.String())
	}
	if description.Children[0].Children[0].Kind != DescriptionInvalidItem {
		t.Fatalf("description = %#v", description)
	}
}

type describedCustomItem struct{}

func (describedCustomItem) Run(context.Context, *Context, any) (any, error) { return nil, nil }

func TestDescriptionContainsOnlyDefinitionData(t *testing.T) {
	description := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil }))).Describe()
	want := Description{Kind: DescriptionPipeline, Name: "pipeline", Children: []Description{{Kind: DescriptionStage, Name: "stage", Children: []Description{{Kind: DescriptionStep, Name: "step"}}}}}
	if !reflect.DeepEqual(description, want) {
		t.Fatalf("Describe = %#v, want %#v", description, want)
	}
}
