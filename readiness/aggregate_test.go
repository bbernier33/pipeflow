package readiness_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bbernier33/pipeflow/v3"
	"github.com/bbernier33/pipeflow/v3/readiness"
)

func aggregateScenario(t *testing.T, verdict readiness.Verdict) readiness.Result {
	t.Helper()
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("run", func() error { return nil })))
	return readiness.NewScenario("sample", &p).Expect(readiness.Expect("required behavior", func(readiness.Observation) readiness.Assessment {
		return readiness.Assessment{Verdict: verdict, Observed: "observed behavior"}
	})).Run(context.Background())
}

func TestReadinessDefaultBaselineRequiresContractsEndToEndCoverage(t *testing.T) {
	if report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{}); report.Status != readiness.ReadinessNotReady || len(report.Missing) != 3 {
		t.Fatalf("report=%+v", report)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("run", func() error { return nil })))
	scenario := readiness.NewScenario("sample", &p).Expect(readiness.RunSucceeds())
	e2e := readiness.NewEndToEnd(scenario, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	coverage := readiness.DetectUntestedPaths(&p, []pipeflow.RunReport{e2e.Scenario.Report})
	report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{},
		readiness.ReadinessEvidence{Name: "contracts", Result: readiness.InspectContracts(&p)},
		readiness.ReadinessEvidence{Name: "sample", Result: e2e},
		readiness.ReadinessEvidence{Name: "paths", Result: coverage},
	)
	if report.Status != readiness.ReadinessReady || report.Passed != 3 || len(report.Missing) != 0 {
		t.Fatalf("report=%+v", report)
	}
	if len(report.Items[1].RunIDs) != 1 || !strings.Contains(report.String(), "RESULT: READY") {
		t.Fatalf("report=%s", report)
	}
}

func TestReadinessOutcomePrecedence(t *testing.T) {
	pass, review, fail := aggregateScenario(t, readiness.VerdictPass), aggregateScenario(t, readiness.VerdictReview), aggregateScenario(t, readiness.VerdictFail)
	policy := readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryScenario}}
	for _, test := range []struct {
		result   readiness.Result
		warnings []string
		status   readiness.ReadinessStatus
	}{
		{pass, nil, readiness.ReadinessReady},
		{pass, []string{"staging sink only"}, readiness.ReadinessReadyWithWarnings},
		{review, []string{"staging sink only"}, readiness.ReadinessReviewRequired},
		{fail, []string{"staging sink only"}, readiness.ReadinessNotReady},
	} {
		report := readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "sample", Result: test.result, Warnings: test.warnings})
		if report.Status != test.status {
			t.Fatalf("want=%s report=%+v", test.status, report)
		}
	}
	policy.WarningsAsFailures = true
	if report := readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "sample", Result: pass, Warnings: []string{"warning"}}); report.Status != readiness.ReadinessNotReady {
		t.Fatal(report)
	}
}

func TestReadinessGapsCannotHideCriticalRequirements(t *testing.T) {
	policy := readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryCoverage}}
	gap := readiness.CoverageGap{CoverageRequirement: readiness.CoverageRequirement{Path: "p/s/step", Behavior: readiness.CoverageExecuted}}
	coverage := readiness.CoverageResult{Verdict: readiness.VerdictReview, Gaps: []readiness.CoverageGap{gap}}
	build := func() readiness.ReadinessReport {
		return readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "paths", Result: coverage})
	}
	if got := build(); got.Status != readiness.ReadinessReviewRequired || got.UntestedPaths != 1 {
		t.Fatal(got)
	}
	policy.AllowNonCriticalGaps = true
	if got := build(); got.Status != readiness.ReadinessReadyWithWarnings || got.Warnings != 1 {
		t.Fatal(got)
	}
	coverage.Gaps[0].Critical = true
	if got := build(); got.Status != readiness.ReadinessNotReady || got.CriticalGaps != 1 {
		t.Fatal(got)
	}
}

func TestReadinessExpectedPipelineFailureDoesNotBecomeToolFailure(t *testing.T) {
	failure := errors.New("expected outage")
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("fail", func() error { return failure })))
	result := readiness.NewScenario("outage", &p).Expect(readiness.Expect("outage observed", func(o readiness.Observation) readiness.Assessment {
		if errors.Is(o.Err, failure) {
			return readiness.Passed("expected outage")
		}
		return readiness.Failed("outage missing")
	})).Run(context.Background())
	report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryFailureInjection}}, readiness.ReadinessEvidence{Name: "outage", Category: readiness.CategoryFailureInjection, Result: result})
	if report.Status != readiness.ReadinessReady {
		t.Fatal(report)
	}
}

func TestReadinessRejectsInvalidEvidenceAndPolicy(t *testing.T) {
	policy := readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryScenario}}
	for _, value := range []any{nil, 123, readiness.Result{}, (*readiness.Result)(nil)} {
		if report := readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "bad", Result: value}); report.Status != readiness.ReadinessNotReady || report.Items[0].Error == nil {
			t.Fatal(report)
		}
	}
	pass := aggregateScenario(t, readiness.VerdictPass)
	for _, required := range [][]readiness.ReadinessCategory{{"unknown"}, {readiness.CategoryScenario, readiness.CategoryScenario}} {
		if report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{Required: required}); report.Error == nil || report.Status != readiness.ReadinessNotReady {
			t.Fatal(report)
		}
	}
	for _, entries := range [][]readiness.ReadinessEvidence{
		{{Result: pass}},
		{{Name: "same", Result: pass}, {Name: "same", Result: pass}},
	} {
		if report := readiness.BuildReadinessReport(policy, entries...); report.Error == nil || report.Status != readiness.ReadinessNotReady {
			t.Fatal(report)
		}
	}
	if report := readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "wrong category", Category: readiness.CategoryLoad, Result: pass}); report.Status != readiness.ReadinessNotReady {
		t.Fatal(report)
	}
}

func TestReadinessReportDetachesMutableInputSlices(t *testing.T) {
	result := aggregateScenario(t, readiness.VerdictReview)
	warnings := []string{"known limitation"}
	required := []readiness.ReadinessCategory{readiness.CategoryScenario}
	report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{Required: required}, readiness.ReadinessEvidence{Name: "sample", Result: result, Warnings: warnings})
	result.Checks[0].Name = "mutated"
	warnings[0] = "mutated"
	required[0] = readiness.CategoryChaos
	if report.Items[0].Checks[0].Name == "mutated" || report.Items[0].Warnings[0] == "mutated" || report.Required[0] != readiness.CategoryScenario {
		t.Fatal("report aliases caller slices")
	}
}

func TestReadinessCollectsExistingPlanEvidence(t *testing.T) {
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("run", func(int) error { return nil })))
	scenario := readiness.NewScenario("sample", &p).WithInput(1).Expect(readiness.RunSucceeds())
	load := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 2, Concurrency: 1}).Run(context.Background())
	pressure := readiness.NewPressurePlan(scenario, readiness.PressureConfig{Items: 2, Worker: pipeflow.WorkerOptions{Workers: 1, Buffer: 1}}).Run(context.Background())
	mutations := readiness.NewMutationPlan(scenario, 1).Run(context.Background())
	semantic := readiness.NewSemanticPlan(scenario, readiness.NewSemanticCase("valid", "$", 1).ExpectBehavior(readiness.SemanticAccept)).Run(context.Background())
	chaos := readiness.NewChaosPlan(1, 1, func(ctx context.Context, _ readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		result := scenario.Run(ctx)
		return readiness.ChaosEvidence{Scenario: &result}, nil
	}).Run(context.Background())
	integration := readiness.NewIntegration(scenario, readiness.IntegrationSafeSample, readiness.SampleIngress("fixture"), readiness.SafeSink("capture")).Run(context.Background())
	for _, test := range []struct {
		category readiness.ReadinessCategory
		value    any
	}{
		{readiness.CategoryLoad, load}, {readiness.CategoryPressure, pressure}, {readiness.CategoryPayloadMutation, mutations}, {readiness.CategorySemanticMutation, semantic}, {readiness.CategoryChaos, chaos}, {readiness.CategoryIntegration, integration},
	} {
		report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{test.category}}, readiness.ReadinessEvidence{Name: "plan", Result: test.value})
		if report.Status != readiness.ReadinessReady || len(report.Items[0].RunIDs) == 0 {
			t.Fatalf("category=%s report=%+v", test.category, report)
		}
	}
}

func TestReadinessFailedChecksCannotBeHiddenByParentPass(t *testing.T) {
	result := aggregateScenario(t, readiness.VerdictFail)
	result.Verdict = readiness.VerdictPass
	policy := readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryScenario}}
	report := readiness.BuildReadinessReport(policy, readiness.ReadinessEvidence{Name: "inconsistent", Result: result})
	if report.Status != readiness.ReadinessNotReady || report.Items[0].Checks[0].Verdict != readiness.VerdictFail {
		t.Fatal(report)
	}
}

func TestReadinessSemanticExpectedRejectionKeepsItsClassification(t *testing.T) {
	failure := errors.New("invalid identifier")
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("validate", func(string) error { return failure })))
	scenario := readiness.NewScenario("identifier", &p).Expect(readiness.RunSucceeds())
	semantic := readiness.NewSemanticPlan(scenario, readiness.NewSemanticCase("bad identifier", "$", "bad").ExpectBehavior(readiness.SemanticReject)).Run(context.Background())
	report := readiness.BuildReadinessReport(readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategorySemanticMutation}}, readiness.ReadinessEvidence{Name: "rejection", Result: semantic})
	if report.Status != readiness.ReadinessReady {
		t.Fatal(report)
	}
}

func TestReadinessMixedEvidenceFailsBeforeReviewAndWarnings(t *testing.T) {
	policy := readiness.ReadinessPolicy{Required: []readiness.ReadinessCategory{readiness.CategoryScenario, readiness.CategoryLoad}}
	report := readiness.BuildReadinessReport(policy,
		readiness.ReadinessEvidence{Name: "pass", Result: aggregateScenario(t, readiness.VerdictPass)},
		readiness.ReadinessEvidence{Name: "review", Result: aggregateScenario(t, readiness.VerdictReview), Warnings: []string{"known issue"}},
		readiness.ReadinessEvidence{Name: "fail", Result: aggregateScenario(t, readiness.VerdictFail)},
	)
	if report.Status != readiness.ReadinessNotReady || report.Passed != 1 || report.Failed != 1 || report.Reviews != 1 || report.Warnings != 1 || len(report.Missing) != 1 || report.Missing[0] != readiness.CategoryLoad {
		t.Fatal(report)
	}
	if !strings.Contains(report.String(), "MISSING: load") || !strings.Contains(report.String(), "check required behavior") {
		t.Fatal(report.String())
	}
}
