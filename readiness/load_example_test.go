package readiness_test

import (
	"context"
	"fmt"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleLoadPlan() {
	pipeline := pipeflow.NewPipeline("events", pipeflow.NewStage("process",
		pipeflow.NewStep("handle", func(int) error { return nil }),
	))
	scenario := readiness.NewScenario("event load", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 10, Concurrency: 2}).
		Inputs(func(index int) (any, error) { return index, nil }).
		Expect(
			readiness.PipelineFailureRateAtMost(0),
			readiness.P95Within(time.Second),
		).
		Run(context.Background())

	fmt.Println(result.Verdict, result.Summary.Completed)
	// Output: pass 10
}
