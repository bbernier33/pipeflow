package readiness_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func TestIntegrationLevelsExecuteOnlyDeclaredCombinations(t *testing.T) {
	tests := []struct {
		level      readiness.IntegrationLevel
		boundaries []readiness.Boundary
	}{
		{readiness.IntegrationSafeSample, []readiness.Boundary{readiness.SampleIngress("fixture"), readiness.SafeSink("capture")}},
		{readiness.IntegrationRealIngress, []readiness.Boundary{readiness.RealIngress("source"), readiness.SafeSink("capture")}},
		{readiness.IntegrationStagingOutput, []readiness.Boundary{readiness.SampleIngress("fixture"), readiness.StagingSink("staging")}},
		{readiness.IntegrationSafeEnvironment, []readiness.Boundary{readiness.RealIngress("source"), readiness.StagingSink("staging")}},
		{readiness.IntegrationProduction, []readiness.Boundary{readiness.ProductionIngress("source"), readiness.ProductionSink("output")}},
	}
	for _, test := range tests {
		calls := 0
		p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("run", func() error { calls++; return nil })))
		scenario := readiness.NewScenario("integration", &p).Expect(readiness.RunSucceeds())
		result := readiness.NewIntegration(scenario, test.level, test.boundaries...).WithPolicy(readiness.IntegrationPolicy{Allowed: test.boundaries}).Run(context.Background())
		if result.Verdict != readiness.VerdictPass || result.Error != nil || calls != 1 || result.Scenario.Report.RunID == "" {
			t.Fatalf("level=%d result=%+v calls=%d", test.level, result, calls)
		}
		if result.EndedAt.IsZero() || result.EndedAt.Before(result.StartedAt) {
			t.Fatal("missing integration timing")
		}
	}
}

func TestIntegrationDeniesUnapprovedBoundaryBeforeExecution(t *testing.T) {
	calls := 0
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("run", func() error { calls++; return nil })))
	scenario := readiness.NewScenario("denied", &p).Expect(readiness.RunSucceeds())
	for _, allowed := range [][]readiness.Boundary{nil, {readiness.StagingSink("wrong")}, {readiness.SafeSink("output")}} {
		result := readiness.NewIntegration(scenario, readiness.IntegrationProduction, readiness.SampleIngress("fixture"), readiness.ProductionSink("output")).WithPolicy(readiness.IntegrationPolicy{Allowed: allowed}).Run(context.Background())
		if !errors.Is(result.Error, readiness.ErrIntegrationBoundaryDenied) || result.Verdict != readiness.VerdictFail || calls != 0 || result.Scenario.Report.RunID != "" {
			t.Fatalf("result=%+v calls=%d", result, calls)
		}
	}
}

func TestIntegrationRejectsInvalidTopologyAndLevel(t *testing.T) {
	p := pipeflow.NewPipeline("p")
	scenario := readiness.NewScenario("invalid", &p).Expect(readiness.RunSucceeds())
	for _, test := range []struct {
		level      readiness.IntegrationLevel
		boundaries []readiness.Boundary
	}{
		{0, nil},
		{readiness.IntegrationSafeSample, []readiness.Boundary{readiness.SampleIngress("fixture")}},
		{readiness.IntegrationSafeSample, []readiness.Boundary{readiness.SampleIngress("same"), readiness.SafeSink("same")}},
		{readiness.IntegrationSafeSample, []readiness.Boundary{readiness.SampleIngress("fixture"), readiness.ProductionSink("output")}},
		{readiness.IntegrationProduction, []readiness.Boundary{readiness.SampleIngress("fixture"), readiness.SafeSink("capture")}},
	} {
		result := readiness.NewIntegration(scenario, test.level, test.boundaries...).WithPolicy(readiness.IntegrationPolicy{Allowed: test.boundaries}).Run(context.Background())
		if result.Error == nil || result.Verdict != readiness.VerdictFail {
			t.Fatalf("accepted %+v", test)
		}
	}
}

func TestIntegrationPolicyIsDetached(t *testing.T) {
	p := pipeflow.NewPipeline("p")
	scenario := readiness.NewScenario("detached", &p).Expect(readiness.RunSucceeds())
	grants := []readiness.Boundary{readiness.StagingSink("staging")}
	verification := readiness.NewIntegration(scenario, readiness.IntegrationStagingOutput, readiness.SampleIngress("fixture"), readiness.StagingSink("staging")).WithPolicy(readiness.IntegrationPolicy{Allowed: grants})
	grants[0] = readiness.ProductionSink("wrong")
	if result := verification.Run(context.Background()); result.Verdict != readiness.VerdictPass {
		t.Fatal(result.Error)
	}
}

func TestIntegrationContractFailurePreventsSideEffects(t *testing.T) {
	called := false
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("source", func() (int, error) { called = true; return 1, nil }), pipeflow.NewSinkStep("sink", func(string) error { return nil })))
	scenario := readiness.NewScenario("contract", &p).Expect(readiness.RunSucceeds())
	result := readiness.NewIntegration(scenario, readiness.IntegrationSafeSample, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	if result.Error == nil || called || result.Scenario.Report.RunID != "" {
		t.Fatalf("result=%+v called=%v", result, called)
	}
}

func TestIntegrationRetainsExpectedFailureAndReviewSemantics(t *testing.T) {
	failure := errors.New("planned source outage")
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("fail", func() error { return failure })))
	scenario := readiness.NewScenario("expected outage", &p).Expect(readiness.Expect("outage reported", func(o readiness.Observation) readiness.Assessment {
		if errors.Is(o.Err, failure) {
			return readiness.Passed("expected source failure")
		}
		return readiness.Failed("failure missing")
	}))
	result := readiness.NewIntegration(scenario, readiness.IntegrationSafeSample, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	if result.Verdict != readiness.VerdictPass || !errors.Is(result.Scenario.Report.Error, failure) {
		t.Fatalf("result=%+v", result)
	}
	consumer := pipeflow.NewPipeline("consumer", pipeflow.NewStage("s", pipeflow.NewStep("accept", func(any) error { return nil })))
	unresolved := readiness.NewScenario("unknown ingress contract", &consumer).Expect(readiness.RunSucceeds())
	review := readiness.NewIntegration(unresolved, readiness.IntegrationSafeSample, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	if review.Verdict != readiness.VerdictReview {
		t.Fatalf("result=%+v", review)
	}
}

func TestExistingEndToEndStillRejectsRealBoundaryDeclarations(t *testing.T) {
	p := pipeflow.NewPipeline("p")
	scenario := readiness.NewScenario("safe only", &p).Expect(readiness.RunSucceeds())
	result := readiness.NewEndToEnd(scenario, readiness.RealIngress("source"), readiness.SafeSink("capture")).Run(context.Background())
	if result.Error == nil || result.Scenario.Report.RunID != "" {
		t.Fatalf("result=%+v", result)
	}
}
