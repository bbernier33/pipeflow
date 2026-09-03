package readiness_test

import (
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestInspectContractsPassesCompatibleTopology(t *testing.T) {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() ([]int, error) { return nil, nil }),
		pipeflow.NewStep("store", func([]int) error { return nil }),
	))
	result := readiness.InspectContracts(&pipeline)
	if result.Verdict != readiness.VerdictPass || result.Error != nil || len(result.Boundaries) != 1 {
		t.Fatalf("result = %#v", result)
	}
}

func TestInspectContractsFailsIncompatibleTopology(t *testing.T) {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() ([]int, error) { return nil, nil }),
		pipeflow.NewStep("store", func([]string) error { return nil }),
	))
	result := readiness.InspectContracts(&pipeline)
	if result.Verdict != readiness.VerdictFail || result.Error == nil {
		t.Fatalf("result = %#v", result)
	}
	boundary := result.Boundaries[0]
	if boundary.Producer.Name != "load" || boundary.Consumer.Name != "store" || boundary.Status != pipeflow.FlowContractIncompatible {
		t.Fatalf("boundary = %#v", boundary)
	}
}

func TestInspectContractsReviewsUnknownInitialInput(t *testing.T) {
	pipeline := pipeflow.NewPipeline("consumer", pipeflow.NewStage("process",
		pipeflow.NewStep("store", func(int) error { return nil }),
	))
	if result := readiness.InspectContracts(&pipeline); result.Verdict != readiness.VerdictReview {
		t.Fatalf("result = %#v", result)
	}
	if result := readiness.InspectContractsInput(&pipeline, 1); result.Verdict != readiness.VerdictPass {
		t.Fatalf("result with input = %#v", result)
	}
}

func TestInspectContractsRejectsNilPipeline(t *testing.T) {
	result := readiness.InspectContracts(nil)
	if result.Verdict != readiness.VerdictFail || result.Error == nil {
		t.Fatalf("result = %#v", result)
	}
}
