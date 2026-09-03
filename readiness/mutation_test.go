package readiness_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

type mutationOrder struct {
	Provider string
	Count    int
	Tags     []string
}

func TestGenerateMutationsIsTypeAwareAndDoesNotModifySample(t *testing.T) {
	sample := mutationOrder{Provider: "Clover", Count: 3, Tags: []string{"new", "paid"}}
	want := mutationOrder{Provider: "Clover", Count: 3, Tags: []string{"new", "paid"}}
	mutations, err := readiness.GenerateMutations(sample)
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) == 0 || !reflect.DeepEqual(sample, want) {
		t.Fatalf("mutations=%d sample=%#v", len(mutations), sample)
	}
	kinds := make(map[readiness.MutationKind]bool)
	paths := make(map[string]bool)
	for _, mutation := range mutations {
		kinds[mutation.Kind] = true
		paths[mutation.Path] = true
	}
	for _, kind := range []readiness.MutationKind{readiness.MutationZero, readiness.MutationEmpty, readiness.MutationTruncated, readiness.MutationUppercase, readiness.MutationReordered, readiness.MutationDuplicate} {
		if !kinds[kind] {
			t.Errorf("missing mutation kind %q", kind)
		}
	}
	if !paths["$.Provider"] || !paths["$.Count"] || !paths["$.Tags"] {
		t.Fatalf("paths = %#v", paths)
	}
}

func TestGenerateMutationsSupportsMapStructureAndInterfaceTypeChanges(t *testing.T) {
	sample := map[string]any{"provider": "clover", "count": 2}
	mutations, err := readiness.GenerateMutations(sample)
	if err != nil {
		t.Fatal(err)
	}
	var missing, extra, changed bool
	for _, mutation := range mutations {
		switch mutation.Kind {
		case readiness.MutationMissingKey:
			missing = true
		case readiness.MutationExtraKey:
			extra = true
		case readiness.MutationTypeChange:
			changed = true
			if mutation.Path == `$["provider"]` && mutation.OriginalType != "string" {
				t.Fatalf("type change metadata = %#v", mutation)
			}
		}
	}
	if !missing || !extra || !changed {
		t.Fatalf("missing=%v extra=%v changed=%v mutations=%#v", missing, extra, changed, mutations)
	}
	if !reflect.DeepEqual(sample, map[string]any{"provider": "clover", "count": 2}) {
		t.Fatalf("sample mutated: %#v", sample)
	}
}

func TestGenerateMutationsIsDeterministicAndBounded(t *testing.T) {
	sample := map[string]any{"z": "Zulu", "a": "Alpha"}
	first, err := readiness.GenerateMutations(sample, readiness.WithMutationMaxCases(3))
	if err != nil {
		t.Fatal(err)
	}
	second, err := readiness.GenerateMutations(sample, readiness.WithMutationMaxCases(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || !reflect.DeepEqual(first, second) {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
}

func TestMutationPlanRunsScenarioForEachCase(t *testing.T) {
	pipeline := pipeflow.NewPipeline("payloads", pipeflow.NewStage("validate",
		pipeflow.NewStep("accept", func(map[string]any) error { return nil }),
	))
	scenario := readiness.NewScenario("mutated payload", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewMutationPlan(scenario, map[string]any{"provider": "Clover"}, readiness.WithMutationMaxCases(8)).Run(context.Background())
	if result.Verdict != readiness.VerdictPass || result.Error != nil || len(result.Cases) != 8 {
		t.Fatalf("result = %#v", result)
	}
	for _, mutationCase := range result.Cases {
		if mutationCase.Scenario.Report.RunID == "" || mutationCase.Mutation.Path == "" {
			t.Fatalf("case = %#v", mutationCase)
		}
	}
}

func TestMutationPlanPropagatesScenarioVerdicts(t *testing.T) {
	pipeline := pipeflow.NewPipeline("payloads", pipeflow.NewStage("validate",
		pipeflow.NewStep("reject empty", func(value string) error { return nil }),
	))
	scenario := readiness.NewScenario("mutated string", &pipeline).Expect(readiness.Expect("non-empty", func(observation readiness.Observation) readiness.Assessment {
		if observation.Output == "" {
			return readiness.Failed("empty value reached output")
		}
		return readiness.Passed("value remained non-empty")
	}))
	result := readiness.NewMutationPlan(scenario, "Clover").Run(context.Background())
	if result.Verdict != readiness.VerdictFail || len(result.Cases) == 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestMutationConfigurationValidationAndNoApplicableMutation(t *testing.T) {
	if _, err := readiness.GenerateMutations(1, readiness.WithMutationMaxDepth(0)); err == nil {
		t.Fatal("expected max depth error")
	}
	pipeline := pipeflow.NewPipeline("empty")
	scenario := readiness.NewScenario("nil", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewMutationPlan(scenario, nil).Run(context.Background())
	if result.Verdict != readiness.VerdictReview || result.Error == nil {
		t.Fatalf("result = %#v", result)
	}
}
