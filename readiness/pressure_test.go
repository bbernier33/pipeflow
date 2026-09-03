package readiness_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestPressurePlanObservesBackpressureAndGracefulDrain(t *testing.T) {
	pipeline := pipeflow.NewPipeline("pressure", pipeflow.NewStage("work",
		pipeflow.NewStep("slow consumer", func(int) error {
			time.Sleep(4 * time.Millisecond)
			return nil
		}),
	))
	scenario := readiness.NewScenario("bounded worker", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{
		Items:                 8,
		Worker:                pipeflow.WorkerOptions{Workers: 1, Buffer: 1, MaxInFlight: 2},
		BackpressureThreshold: time.Millisecond,
	}).Inputs(func(index int) (any, error) { return index, nil }).Expect(
		readiness.BackpressureObserved(),
		readiness.AllAcceptedWorkDrained(),
		readiness.QueueDepthAtMost(1),
		readiness.CompletionOrderPreserved(),
	).Run(context.Background())

	if result.Verdict != readiness.VerdictPass || result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	if result.Summary.Accepted != 8 || result.Summary.Completed != 8 || result.Summary.BlockedSubmissions == 0 {
		t.Fatalf("summary = %#v", result.Summary)
	}
	if result.Summary.MaxInFlight > 2 || result.Summary.MaxActive > 1 || result.Summary.MaxQueueDepth > 1 {
		t.Fatalf("bounds exceeded: %#v", result.Summary)
	}
	if !reflect.DeepEqual(result.Summary.CompletionOrder, []int{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("completion order = %v", result.Summary.CompletionOrder)
	}
}

func TestPressurePlanRetainsFullReportsWithoutOutputs(t *testing.T) {
	pipeline := pipeflow.NewPipeline("reports", pipeflow.NewStage("work", pipeflow.NewStep("step", func(int) error { return nil })))
	scenario := readiness.NewScenario("reports", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{Items: 1, Worker: pipeflow.WorkerOptions{Workers: 1}}).
		Inputs(func(int) (any, error) { return 1, nil }).Run(context.Background())
	if result.Items[0].Scenario.Report.RunID == "" || len(result.Items[0].Scenario.Report.Stages) != 1 {
		t.Fatalf("item = %#v", result.Items[0])
	}
	if _, exists := reflect.TypeOf(readiness.PressureItemResult{}).FieldByName("Output"); exists {
		t.Fatal("PressureItemResult must not retain output")
	}
}

func TestPressurePlanInputFailureIsRejected(t *testing.T) {
	pipeline := pipeflow.NewPipeline("input")
	scenario := readiness.NewScenario("input", &pipeline).Expect(readiness.RunSucceeds())
	result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{Items: 2, Worker: pipeflow.WorkerOptions{Workers: 1}}).
		Inputs(func(index int) (any, error) {
			if index == 0 {
				return nil, errors.New("source unavailable")
			}
			return index, nil
		}).Run(context.Background())
	if result.Verdict != readiness.VerdictFail || result.Summary.Rejected != 1 || result.Summary.Accepted != 1 {
		t.Fatalf("result = %#v", result)
	}
}

func TestPressurePlanSubmitTimeoutDoesNotLoseAcceptedWork(t *testing.T) {
	release := make(chan struct{})
	pipeline := pipeflow.NewPipeline("timeout", pipeflow.NewStage("work", pipeflow.NewStep("blocked", func(int) error {
		<-release
		return nil
	})))
	scenario := readiness.NewScenario("timeout", &pipeline).Expect(readiness.RunSucceeds())
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{
		Items: 3, SubmitTimeout: 2 * time.Millisecond,
		Worker: pipeflow.WorkerOptions{Workers: 1, MaxInFlight: 1},
	}).Inputs(func(index int) (any, error) { return index, nil }).Run(context.Background())
	if result.Summary.Rejected == 0 || !result.Summary.Drained || result.Summary.Accepted != result.Summary.Completed {
		t.Fatalf("result = %#v", result)
	}
}

func TestPressurePlanValidatesConfiguration(t *testing.T) {
	pipeline := pipeflow.NewPipeline("empty")
	scenario := readiness.NewScenario("invalid", &pipeline).Expect(readiness.RunSucceeds())
	configs := []readiness.PressureConfig{
		{},
		{Items: 1, SubmitTimeout: -1},
		{Items: 1, SampleInterval: -1},
	}
	for _, config := range configs {
		if result := readiness.NewPressurePlan(scenario, config).Run(context.Background()); result.Error == nil {
			t.Fatalf("config %#v accepted", config)
		}
	}
}
