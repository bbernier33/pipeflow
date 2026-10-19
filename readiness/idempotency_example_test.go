package readiness_test

import (
	"context"
	"fmt"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleExpectIdempotency() {
	guard, err := pipeflow.NewIdempotencyGuard("payments", pipeflow.NewMemoryIdempotencyStore(), func(id string) string { return id })
	if err != nil {
		panic(err)
	}
	charges := 0
	p := pipeflow.NewPipeline("orders", pipeflow.NewStage("output", pipeflow.NewSinkStep("charge", func(string) error { charges++; return nil }).WithIdempotencyGuard(guard)))
	scenario := readiness.NewScenario("redelivery", &p).WithInput("order-1")
	first := scenario.Expect(readiness.RunSucceeds(), readiness.ExpectIdempotency("orders/output/charge", readiness.IdempotencyExpectation{
		Guard: "payments", Claim: pipeflow.IdempotencyClaimed, Outcome: pipeflow.IdempotencyExecuted, Status: pipeflow.StatusCompleted,
	})).Run(context.Background())
	duplicate := scenario.Expect(readiness.RunSucceeds(), readiness.ExpectIdempotency("orders/output/charge", readiness.IdempotencyExpectation{
		Guard: "payments", Outcome: pipeflow.IdempotencyDuplicateCompleted, Status: pipeflow.StatusSkipped,
	})).Run(context.Background())
	fmt.Println(first.Verdict, duplicate.Verdict, charges)
	// Output: pass pass 1
}
