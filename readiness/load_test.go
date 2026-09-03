package readiness_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestLoadPlanRunsBoundedConcurrentScenarios(t *testing.T) {
	var active atomic.Int64
	var maximum atomic.Int64
	pipeline := pipeflow.NewPipeline("load", pipeflow.NewStage("work",
		pipeflow.NewStep("process", func(value int) error {
			current := active.Add(1)
			for previous := maximum.Load(); current > previous; previous = maximum.Load() {
				if maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			active.Add(-1)
			return nil
		}),
	))
	scenario := readiness.NewScenario("finite load", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 20, Concurrency: 4}).
		Inputs(func(index int) (any, error) { return index, nil }).
		Expect(readiness.PipelineFailureRateAtMost(0), readiness.P95Within(time.Second)).
		Run(context.Background())

	if result.Verdict != readiness.VerdictPass || result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	if result.Summary.Completed != 20 || result.Summary.MaxConcurrent > 4 || maximum.Load() > 4 {
		t.Fatalf("summary=%#v observed max=%d", result.Summary, maximum.Load())
	}
	if result.Summary.P50Latency <= 0 || result.Summary.P95Latency <= 0 || result.Summary.Throughput <= 0 {
		t.Fatalf("summary = %#v", result.Summary)
	}
	for index, run := range result.Runs {
		if run.Index != index || run.Report.RunID == "" || run.Report.Status != pipeflow.StatusCompleted {
			t.Fatalf("run %d = %#v", index, run)
		}
	}
}

func TestLoadPlanSeparatesPipelineAndScenarioFailures(t *testing.T) {
	pipeline := pipeflow.NewPipeline("load", pipeflow.NewStage("work",
		pipeflow.NewStep("process", func(value int) error {
			if value%2 == 0 {
				return errors.New("rejected")
			}
			return nil
		}),
	))
	scenario := readiness.NewScenario("mixed", &pipeline).Expect(readiness.Expect("classified", func(observation readiness.Observation) readiness.Assessment {
		if observation.Err != nil {
			return readiness.Passed("expected rejection")
		}
		return readiness.Passed("expected acceptance")
	}))
	result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 10, Concurrency: 2}).
		Inputs(func(index int) (any, error) { return index, nil }).
		Expect(readiness.PipelineFailureRateAtMost(0.5)).
		Run(context.Background())
	if result.Verdict != readiness.VerdictPass || result.Summary.PipelineFailures != 5 || result.Summary.ScenarioFailures != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestLoadExpectationsCanFailAggregateResult(t *testing.T) {
	pipeline := pipeflow.NewPipeline("slow", pipeflow.NewStage("work", pipeflow.NewStep("wait", func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})))
	scenario := readiness.NewScenario("slow", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 2, Concurrency: 1}).
		Expect(readiness.P95Within(time.Nanosecond)).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Checks[0].Verdict != readiness.VerdictFail {
		t.Fatalf("result = %#v", result)
	}
}

func TestLoadPlanInputFailureAndPanicBecomeRunFailures(t *testing.T) {
	pipeline := pipeflow.NewPipeline("input", pipeflow.NewStage("work", pipeflow.NewStep("accept", func(int) error { return nil })))
	scenario := readiness.NewScenario("input", &pipeline).Expect(readiness.RunSucceeds())
	for _, input := range []readiness.LoadInput{
		func(int) (any, error) { return nil, errors.New("input unavailable") },
		func(int) (any, error) { panic("bad generator") },
	} {
		result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 1, Concurrency: 1}).Inputs(input).Run(context.Background())
		if result.Verdict != readiness.VerdictFail || result.Runs[0].Error == nil {
			t.Fatalf("result = %#v", result)
		}
	}
}

func TestLoadPlanHonorsCancellation(t *testing.T) {
	pipeline := pipeflow.NewPipeline("cancel", pipeflow.NewStage("work", pipeflow.NewStep("wait", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})))
	scenario := readiness.NewScenario("cancel", &pipeline).Expect(readiness.RunSucceeds())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 10, Concurrency: 2}).Run(ctx)
	if result.Verdict != readiness.VerdictFail || !errors.Is(result.Error, context.Canceled) || result.Summary.Started != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestLoadPlanValidatesConfiguration(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	scenario := readiness.NewScenario("load", &pipeline).Expect(readiness.RunSucceeds())
	configs := []readiness.LoadConfig{
		{Runs: 0, Concurrency: 1},
		{Runs: 1, Concurrency: 0},
		{Runs: 1, Concurrency: 1, StartsPerSecond: -1},
	}
	for _, config := range configs {
		if result := readiness.NewLoadPlan(scenario, config).Run(context.Background()); result.Error == nil {
			t.Fatalf("config %#v accepted", config)
		}
	}
}
