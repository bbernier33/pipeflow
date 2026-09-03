package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleMutationPlan() {
	pipeline := pipeflow.NewPipeline("providers", pipeflow.NewStage("normalize",
		pipeflow.NewStep("accept", func(map[string]any) error { return nil }),
	))
	scenario := readiness.NewScenario("provider mutations", &pipeline).
		Expect(readiness.RunSucceeds())

	result := readiness.NewMutationPlan(
		scenario,
		map[string]any{"provider": "Clover"},
		readiness.WithMutationMaxCases(5),
	).Run(context.Background())

	fmt.Println(result.Verdict, len(result.Cases))
	// Output: pass 5
}
