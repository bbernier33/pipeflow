package readiness_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleExpectRecovery() {
	unauthorized := errors.New("expired credentials")
	fetch, err := readiness.Inject(func() (int, error) { return 42, nil }, readiness.Fault{Calls: 2, Error: unauthorized})
	if err != nil {
		panic(err)
	}
	recovery := pipeflow.NewRecoveryStage("refresh credentials",
		pipeflow.NewStep("refresh", func(pipeflow.Failure) error { return nil }),
		pipeflow.NewStep("retry", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil }),
	)
	step := pipeflow.NewStep("fetch", fetch, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 2})).
		WithRecovery(recovery, pipeflow.RecoveryPolicy{MaxAttempts: 1})
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("load", step))
	result := readiness.NewScenario("expired credentials", &pipeline).Expect(
		readiness.RunSucceeds(),
		readiness.ExpectRecovery("orders/load/fetch", readiness.RecoveryExpectation{
			Attempts: 3,
			Status:   pipeflow.StatusCompleted,
			Runs: []readiness.RecoveryRunExpectation{{
				Name: "refresh credentials", Status: pipeflow.StatusCompleted,
				Decision: pipeflow.RecoveryRetryStep, OriginalError: unauthorized,
			}},
		}),
	).Run(context.Background())
	fmt.Println(result.Verdict)
	// Output: pass
}
