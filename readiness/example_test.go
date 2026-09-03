package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleScenario() {
	pipeline := pipeflow.NewPipeline("prices", pipeflow.NewStage("normalize",
		pipeflow.NewStep("load", func() (int, error) { return 21, nil }),
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))

	result := readiness.NewScenario("safe sample", &pipeline).
		Arrange("use a representative sample and no production writes").
		Expect(
			readiness.RunSucceeds(),
			readiness.Expect("normalized price", func(observation readiness.Observation) readiness.Assessment {
				if observation.Output == 42 {
					return readiness.Passed("price is 42")
				}
				return readiness.Failed("price was not 42")
			}),
		).Run(context.Background())

	fmt.Println(result.Verdict)
	// Output: pass
}
