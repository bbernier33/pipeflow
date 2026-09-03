package readiness_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestEndToEndVerifiesRealPipelineWithSafeBoundaries(t *testing.T) {
	stored := 0
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("normalize", func(value int) (int, error) { return value * 2, nil }),
		pipeflow.NewSinkStep("capture", func(value int) error { stored = value; return nil }),
	))
	scenario := readiness.NewScenario("safe order", &pipeline).
		Arrange("representative sample to an in-memory capture sink").
		WithInput(21).
		Expect(readiness.RunSucceeds(), readiness.Expect("captured once", func(readiness.Observation) readiness.Assessment {
			if stored == 42 {
				return readiness.Passed("capture contains 42")
			}
			return readiness.Failed("capture does not contain 42")
		}))

	result := readiness.NewEndToEnd(scenario,
		readiness.SampleIngress("representative order"),
		readiness.SafeSink("in-memory capture"),
	).Run(context.Background())
	if result.Verdict != readiness.VerdictPass || result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Coverage) != 4 {
		t.Fatalf("coverage = %#v", result.Coverage)
	}
	if result.Coverage[3].Path != "orders/process/capture" || result.Coverage[3].Attempts != 1 {
		t.Fatalf("sink coverage = %#v", result.Coverage[3])
	}
}

func TestEndToEndRequiresControlledBoundariesBeforeExecution(t *testing.T) {
	executed := false
	pipeline := pipeflow.NewPipeline("pipeline", pipeflow.NewStage("stage", pipeflow.NewStep("run", func() error { executed = true; return nil })))
	scenario := readiness.NewScenario("unsafe", &pipeline).Expect(readiness.RunSucceeds())

	result := readiness.NewEndToEnd(scenario, readiness.SampleIngress("sample")).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Error == nil || executed {
		t.Fatalf("result = %#v executed=%v", result, executed)
	}
}

func TestEndToEndContractFailureStopsBeforeExecution(t *testing.T) {
	executed := false
	pipeline := pipeflow.NewPipeline("pipeline", pipeflow.NewStage("stage",
		pipeflow.NewStep("produce", func() (int, error) { executed = true; return 1, nil }),
		pipeflow.NewStep("consume", func(string) error { return nil }),
	))
	scenario := readiness.NewScenario("invalid", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewEndToEnd(scenario, readiness.SampleIngress("sample"), readiness.SafeSink("capture")).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Error == nil || executed || result.Scenario.Report.RunID != "" {
		t.Fatalf("result = %#v executed=%v", result, executed)
	}
}

func TestEndToEndExpectedFailureCanPassAndRetainsCoverage(t *testing.T) {
	want := errors.New("planned outage")
	pipeline := pipeflow.NewPipeline("pipeline", pipeflow.NewStage("stage", pipeflow.NewStep("dependency", func() error { return want })))
	scenario := readiness.NewScenario("outage", &pipeline).Expect(readiness.Expect("outage reported", func(observation readiness.Observation) readiness.Assessment {
		if errors.Is(observation.Err, want) {
			return readiness.Passed("planned outage observed")
		}
		return readiness.Failed("planned outage missing")
	}))
	result := readiness.NewEndToEnd(scenario, readiness.SampleIngress("trigger"), readiness.SafeSink("disabled output")).Run(context.Background())
	if result.Verdict != readiness.VerdictPass || len(result.Coverage) != 3 || result.Coverage[2].Status != pipeflow.StatusFailed {
		t.Fatalf("result = %#v", result)
	}
}

func TestEndToEndRejectsUnsupportedOrDuplicateBoundaries(t *testing.T) {
	pipeline := pipeflow.NewPipeline("pipeline")
	scenario := readiness.NewScenario("boundaries", &pipeline).Expect(readiness.RunSucceeds())
	tests := [][]readiness.Boundary{
		{readiness.SampleIngress("same"), readiness.SafeSink("same")},
		{{Name: "production", Kind: "production_sink"}, readiness.SampleIngress("sample")},
		{readiness.SampleIngress("one"), readiness.SampleIngress("two"), readiness.SafeSink("sink")},
	}
	for _, boundaries := range tests {
		if result := readiness.NewEndToEnd(scenario, boundaries...).Run(context.Background()); result.Error == nil {
			t.Fatalf("boundaries %#v accepted", boundaries)
		}
	}
}
