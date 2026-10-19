package pipeflow

import "testing"

func TestInspectFlowReportsSequentialContracts(t *testing.T) {
	pipeline := NewPipeline("orders", NewStage("process",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewStep("audit", func(int) error { return nil }),
		NewStep("format", func(int) (string, error) { return "", nil }),
		NewStep("store", func(string) error { return nil }),
	))
	report := pipeline.InspectFlow()
	if report.Error != nil || len(report.Boundaries) != 3 {
		t.Fatalf("report = %#v", report)
	}
	wantProducers := []string{"produce", "produce", "format"}
	for index, boundary := range report.Boundaries {
		if boundary.Status != FlowContractCompatible || boundary.Producer.Name != wantProducers[index] {
			t.Fatalf("boundary %d = %#v", index, boundary)
		}
	}
}

func TestInspectFlowInputReportsUnknownAndKnownInitialContract(t *testing.T) {
	pipeline := NewPipeline("consumer", NewStage("stage", NewStep("consume", func(int) error { return nil })))
	unknown := pipeline.InspectFlow()
	if unknown.Error != nil || unknown.Boundaries[0].Status != FlowContractUnknown {
		t.Fatalf("unknown = %#v", unknown)
	}
	compatible := pipeline.InspectFlowInput(4)
	if compatible.Error != nil || compatible.Boundaries[0].Status != FlowContractCompatible {
		t.Fatalf("compatible = %#v", compatible)
	}
	incompatible := pipeline.InspectFlowInput("wrong")
	if incompatible.Error == nil || incompatible.Boundaries[0].Status != FlowContractIncompatible {
		t.Fatalf("incompatible = %#v", incompatible)
	}
}

func TestInspectFlowTraversesParallelAndSubflow(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("outer",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewParallel("fanout", []Branch{NewBranch("left", NewStep("left step", func(int) error { return nil }))}),
		NewStep("join", func(ParallelResults) error { return nil }),
		NewSubflow("nested", NewStage("inner", NewStep("nested step", func(ParallelResults) error { return nil }))),
	))
	report := pipeline.InspectFlow()
	if report.Error != nil || len(report.Boundaries) != 3 {
		t.Fatalf("report = %#v", report)
	}
	for _, boundary := range report.Boundaries {
		if boundary.Status != FlowContractCompatible {
			t.Fatalf("boundary = %#v", boundary)
		}
	}
}
