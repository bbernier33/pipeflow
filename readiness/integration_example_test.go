package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleIntegration() {
	// This example substitutes a capture for the application's staging adapter.
	// Applications must wire the declared destination into their actual functions.
	captured := 0
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("output", pipeflow.NewSinkStep("store", func(value int) error { captured = value; return nil })))
	scenario := readiness.NewScenario("staging output", &pipeline).WithInput(42).Expect(readiness.RunSucceeds(), readiness.Expect("stored sample", func(readiness.Observation) readiness.Assessment {
		if captured == 42 {
			return readiness.Passed("sample captured")
		}
		return readiness.Failed("sample missing")
	}))
	target := readiness.StagingSink("dev orders")
	verification := readiness.NewIntegration(scenario, readiness.IntegrationStagingOutput, readiness.SampleIngress("fixture"), target)
	denied := verification.Run(context.Background())
	allowed := verification.WithPolicy(readiness.IntegrationPolicy{Allowed: []readiness.Boundary{target}}).Run(context.Background())
	fmt.Println(denied.Verdict, allowed.Verdict, captured)
	// Output: fail pass 42
}
