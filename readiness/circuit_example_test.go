package readiness_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func ExampleExpectCircuit() {
	unavailable := errors.New("dependency unavailable")
	breaker, err := pipeflow.NewCircuitBreaker("inventory", pipeflow.CircuitBreakerPolicy{
		FailureThreshold: 1, ObservationWindow: time.Minute, OpenDuration: time.Minute, HalfOpenMaxProbes: 1,
		IsFailure: func(err error) bool { return errors.Is(err, unavailable) },
	})
	if err != nil {
		panic(err)
	}
	call, err := readiness.Inject(func() error { return nil }, readiness.Fault{Error: unavailable})
	if err != nil {
		panic(err)
	}
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("load", pipeflow.NewStep("fetch", call).WithCircuitBreaker(breaker)))
	result := readiness.NewScenario("inventory outage", &pipeline).Expect(
		readiness.ExpectCircuit("orders/load/fetch", readiness.CircuitExpectation{
			Circuit: pipeflow.CircuitReport{Dependency: "inventory", StateBefore: pipeflow.CircuitClosed, StateAfter: pipeflow.CircuitOpen},
			Status:  pipeflow.StatusFailed, Error: unavailable,
		}),
	).Run(context.Background())
	fmt.Println(result.Verdict)
	// Output: pass
}
