package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleEndToEnd() {
	var captured int
	pipeline := pipeflow.NewPipeline("prices", pipeflow.NewStage("process",
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
		pipeflow.NewSinkStep("capture", func(value int) error { captured = value; return nil }),
	))
	scenario := readiness.NewScenario("safe sample", &pipeline).
		WithInput(21).
		Expect(readiness.RunSucceeds(), readiness.Expect("sink received output", func(readiness.Observation) readiness.Assessment {
			if captured == 42 {
				return readiness.Passed("captured 42")
			}
			return readiness.Failed("wrong captured value")
		}))

	result := readiness.NewEndToEnd(scenario,
		readiness.SampleIngress("fixture"),
		readiness.SafeSink("capture variable"),
	).Run(context.Background())

	fmt.Println(result.Verdict, len(result.Coverage))
	// Output: pass 4
}
