package readiness_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleSemanticPlan() {
	pipeline := pipeflow.NewPipeline("identifiers", pipeflow.NewStage("validate",
		pipeflow.NewStep("uuid", func(value string) error {
			if !strings.HasPrefix(value, "v4-") {
				return errors.New("unsupported UUID version")
			}
			return nil
		}),
	))
	scenario := readiness.NewScenario("UUID semantics", &pipeline).
		Expect(readiness.Expect("controlled execution", func(readiness.Observation) readiness.Assessment {
			return readiness.Passed("execution remained controlled")
		}))
	result := readiness.NewSemanticPlan(scenario,
		readiness.NewSemanticCase("UUID v4", "$.id", "v4-123").ExpectBehavior(readiness.SemanticAccept),
		readiness.NewSemanticCase("UUID v1", "$.id", "v1-123").ExpectBehavior(readiness.SemanticReject),
		readiness.NewSemanticCase("uppercase UUID", "$.id", "V4-123"),
	).Run(context.Background())

	fmt.Println(result.Verdict, result.Cases[2].Verdict)
	// Output: review review
}
