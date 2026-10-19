package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleBuildReadinessReport() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process", pipeflow.NewStep("sample", func() error { return nil })))
	scenario := readiness.NewScenario("safe sample", &pipeline).Expect(readiness.RunSucceeds())
	e2e := readiness.NewEndToEnd(scenario, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	coverage := readiness.DetectUntestedPaths(&pipeline, []pipeflow.RunReport{e2e.Scenario.Report})
	report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{},
		readiness.ReadinessEvidence{Name: "contracts", Result: readiness.InspectContracts(&pipeline)},
		readiness.ReadinessEvidence{Name: "sample E2E", Result: e2e},
		readiness.ReadinessEvidence{Name: "required paths", Result: coverage},
	)
	fmt.Println(report.Status, report.Passed)
	// Output: READY 3
}
