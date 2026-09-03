package readiness_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func semanticPipeline() pipeflow.Pipeline {
	return pipeflow.NewPipeline("identifiers", pipeflow.NewStage("validate",
		pipeflow.NewStep("uuid", func(value string) error {
			if !strings.HasPrefix(value, "v4-") {
				return errors.New("unsupported uuid version")
			}
			return nil
		}),
	))
}

func TestSemanticPlanComparesExpectedAndObservedBehavior(t *testing.T) {
	pipeline := semanticPipeline()
	scenario := readiness.NewScenario("uuid behavior", &pipeline).
		Expect(readiness.Expect("execution remains controlled", func(readiness.Observation) readiness.Assessment {
			return readiness.Passed("execution returned normally")
		}))
	plan := readiness.NewSemanticPlan(scenario,
		readiness.NewSemanticCase("uuid v4", "$.id", "v4-123").ExpectBehavior(readiness.SemanticAccept),
		readiness.NewSemanticCase("uuid v1", "$.id", "v1-123").ExpectBehavior(readiness.SemanticReject).
			Verify(func(result readiness.Result) readiness.Assessment {
				if result.Report.Error != nil && strings.Contains(result.Report.Error.Error(), "unsupported uuid version") {
					return readiness.Passed("unsupported version rejected")
				}
				return readiness.Failed("wrong rejection reason")
			}),
	)
	result := plan.Run(context.Background())
	if result.Verdict != readiness.VerdictPass || len(result.Cases) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.Cases[0].Observed != readiness.SemanticAccept || result.Cases[1].Observed != readiness.SemanticReject {
		t.Fatalf("cases = %#v", result.Cases)
	}
}

func TestSemanticPlanUnspecifiedRequiresReview(t *testing.T) {
	pipeline := semanticPipeline()
	scenario := readiness.NewScenario("explore", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewSemanticPlan(scenario,
		readiness.NewSemanticCase("uppercase prefix", "$.id", "V4-123"),
	).Run(context.Background())
	if result.Verdict != readiness.VerdictReview || result.Cases[0].Observed != readiness.SemanticReject {
		t.Fatalf("result = %#v", result)
	}
}

func TestSemanticPlanDetectsBehaviorRegression(t *testing.T) {
	pipeline := semanticPipeline()
	scenario := readiness.NewScenario("regression", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewSemanticPlan(scenario,
		readiness.NewSemanticCase("v1 must be accepted", "$.id", "v1-123").ExpectBehavior(readiness.SemanticAccept),
	).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Cases[0].Observed != readiness.SemanticReject {
		t.Fatalf("result = %#v", result)
	}
}

func TestSemanticEvidenceCheckCanRejectWrongFailure(t *testing.T) {
	pipeline := semanticPipeline()
	scenario := readiness.NewScenario("reason", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewSemanticPlan(scenario,
		readiness.NewSemanticCase("wrong reason", "$.id", "v1-123").
			ExpectBehavior(readiness.SemanticReject).
			Verify(func(readiness.Result) readiness.Assessment {
				return readiness.Failed("expected validation code missing")
			}),
	).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Cases[0].Check == nil {
		t.Fatalf("result = %#v", result)
	}
}

func TestSemanticPlanValidatesSpecification(t *testing.T) {
	pipeline := semanticPipeline()
	scenario := readiness.NewScenario("invalid", &pipeline).Expect(readiness.RunSucceeds())
	tests := [][]readiness.SemanticCase{
		nil,
		{readiness.NewSemanticCase("", "$.id", "value")},
		{readiness.NewSemanticCase("case", "", "value")},
		{readiness.NewSemanticCase("same", "$", "one"), readiness.NewSemanticCase("same", "$", "two")},
		{readiness.NewSemanticCase("case", "$", "value").ExpectBehavior("sometimes")},
	}
	for _, cases := range tests {
		if result := readiness.NewSemanticPlan(scenario, cases...).Run(context.Background()); result.Error == nil || result.Verdict != readiness.VerdictFail {
			t.Fatalf("cases %#v accepted: %#v", cases, result)
		}
	}
}

func TestSemanticCaseResultDoesNotExposeInput(t *testing.T) {
	resultType := reflect.TypeOf(readiness.SemanticCaseResult{})
	if _, exists := resultType.FieldByName("Input"); exists {
		t.Fatal("SemanticCaseResult must not retain input")
	}
}
