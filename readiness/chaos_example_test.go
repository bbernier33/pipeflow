package readiness_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbernier33/pipeflow/v3"
	"github.com/bbernier33/pipeflow/v3/readiness"
)

func ExampleChaosPlan() {
	plan := readiness.NewChaosPlan(2026, 5, func(ctx context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		failures := 1 + c.Random().Intn(3)
		call, err := readiness.Inject(func() error { return nil }, readiness.Fault{Calls: failures, Error: errors.New("dependency outage")})
		if err != nil {
			return readiness.ChaosEvidence{}, err
		}
		pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("load", pipeflow.NewStep("fetch", call, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 4}))))
		result := readiness.NewScenario("outage and retries", &pipeline).
			Arrange(fmt.Sprintf("first %d calls fail", failures)).
			Expect(readiness.RunSucceeds()).Run(ctx)
		return readiness.ChaosEvidence{Scenario: &result}, nil
	})
	result := plan.Run(context.Background())
	replay := plan.RunCase(context.Background(), result.Cases[2].Case.Index)
	fmt.Println(result.Verdict, replay.Verdict, replay.Case.Seed == result.Cases[2].Case.Seed)
	// Output: pass pass true
}
