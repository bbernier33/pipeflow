package readiness_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestScenarioRunsPipelineAndEvaluatesOutput(t *testing.T) {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("produce", func() (int, error) { return 20, nil }),
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))
	scenario := readiness.NewScenario("sample order", &pipeline).
		Arrange("a safe sample enters the real processing flow").
		Expect(
			readiness.RunSucceeds(),
			readiness.Expect("output is transformed", func(observation readiness.Observation) readiness.Assessment {
				if observation.Output != 40 {
					return readiness.Failed("unexpected output")
				}
				return readiness.Passed("output is 40")
			}),
		)

	result := scenario.Run(context.Background())
	if result.Verdict != readiness.VerdictPass || len(result.Checks) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.Report.Status != pipeflow.StatusCompleted || result.Report.RunID == "" {
		t.Fatalf("report = %#v", result.Report)
	}
}

func TestExpectedPipelineFailureCanPass(t *testing.T) {
	want := errors.New("dependency unavailable")
	pipeline := pipeflow.NewPipeline("failure", pipeflow.NewStage("call",
		pipeflow.NewStep("dependency", func() error { return want }),
	))
	result := readiness.NewScenario("dependency outage", &pipeline).
		Expect(readiness.Expect("failure is observable", func(observation readiness.Observation) readiness.Assessment {
			if errors.Is(observation.Err, want) && observation.Report.Status == pipeflow.StatusFailed {
				return readiness.Passed("expected dependency failure was reported")
			}
			return readiness.Failed("expected failure was not reported")
		})).Run(context.Background())

	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("result = %#v", result)
	}
}

func TestFailTakesPrecedenceOverReview(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	result := readiness.NewScenario("assessment", &pipeline).Expect(
		readiness.Expect("unknown behavior", func(readiness.Observation) readiness.Assessment {
			return readiness.NeedsReview("expectation not classified")
		}),
		readiness.Expect("required behavior", func(readiness.Observation) readiness.Assessment {
			return readiness.Failed("requirement not met")
		}),
	).Run(context.Background())

	if result.Verdict != readiness.VerdictFail {
		t.Fatalf("verdict = %q", result.Verdict)
	}
}

func TestNilInputIsDistinctFromNoInput(t *testing.T) {
	pipeline := pipeflow.NewPipeline("nil", pipeflow.NewStage("consume",
		pipeflow.NewStep("consume nil", func(value *int) error {
			if value != nil {
				t.Fatal("expected nil input")
			}
			return nil
		}),
	))
	result := readiness.NewScenario("nil input", &pipeline).
		WithInput((*int)(nil)).Expect(readiness.RunSucceeds()).Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("result = %#v", result)
	}
}

func TestScenarioRejectsInvalidDefinition(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	result := readiness.NewScenario("missing expectations", &pipeline).Run(context.Background())
	if result.Error == nil || !strings.Contains(result.Error.Error(), "at least one expectation") {
		t.Fatalf("error = %v", result.Error)
	}
}

func TestExpectationPanicBecomesFailure(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	result := readiness.NewScenario("panic", &pipeline).
		Expect(readiness.Expect("unsafe check", func(readiness.Observation) readiness.Assessment {
			panic("broken assertion")
		})).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Checks[0].Error == nil {
		t.Fatalf("result = %#v", result)
	}
}

func TestInvalidVerdictBecomesFailure(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	result := readiness.NewScenario("invalid verdict", &pipeline).
		Expect(readiness.Expect("invalid", func(readiness.Observation) readiness.Assessment {
			return readiness.Assessment{Verdict: "maybe"}
		})).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Checks[0].Error == nil {
		t.Fatalf("result = %#v", result)
	}
}
