package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleDetectUntestedPaths() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process", pipeflow.NewStep("discount", func(int) error { return nil }, pipeflow.WithCondition(func(value int) bool { return value > 0 }))))
	_, skipped, _ := pipeline.RunWithReport(context.Background(), 0)
	gap := readiness.DetectUntestedPaths(&pipeline, []pipeflow.RunReport{skipped})
	_, exercised, _ := pipeline.RunWithReport(context.Background(), 1)
	covered := readiness.DetectUntestedPaths(&pipeline, []pipeflow.RunReport{skipped, exercised}, readiness.CoverageRequirement{
		Path: "orders/process/discount", Behavior: readiness.CoverageSkipped,
	})
	fmt.Println(gap.Verdict, len(gap.Gaps), covered.Verdict)
	// Output: review 1 pass
}
