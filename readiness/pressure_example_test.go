package readiness_test

import (
	"context"
	"fmt"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExamplePressurePlan() {
	pipeline := pipeflow.NewPipeline("events", pipeflow.NewStage("consume",
		pipeflow.NewStep("store", func(int) error {
			time.Sleep(time.Millisecond)
			return nil
		}),
	))
	scenario := readiness.NewScenario("bounded queue", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{
		Items:                 5,
		Worker:                pipeflow.WorkerOptions{Workers: 1, Buffer: 1, MaxInFlight: 2},
		BackpressureThreshold: 100 * time.Microsecond,
	}).Inputs(func(index int) (any, error) { return index, nil }).Expect(
		readiness.BackpressureObserved(),
		readiness.AllAcceptedWorkDrained(),
		readiness.QueueDepthAtMost(1),
	).Run(context.Background())

	fmt.Println(result.Verdict, result.Summary.Drained)
	// Output: pass true
}
