package readiness_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/readiness"
)

func chaosScenario(t *testing.T, c readiness.ChaosCase) readiness.Scenario {
	t.Helper()
	random := c.Random()
	failures := 1 + random.Intn(3)
	delay := time.Duration(random.Intn(2)) * time.Microsecond
	dependency := errors.New("dependency unavailable")
	action, err := readiness.Inject(func(input int) (int, error) { return input * 2, nil }, readiness.Fault{Calls: failures, Delay: delay, Error: dependency})
	if err != nil {
		t.Fatal(err)
	}
	p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("call", action, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: failures + 1}))))
	return readiness.NewScenario("seeded outage", &p).Arrange(fmt.Sprintf("first %d calls fail; delay=%s", failures, delay)).WithInput(21).Expect(readiness.RunSucceeds(), readiness.Expect("real result", func(o readiness.Observation) readiness.Assessment {
		if o.Output != 42 || len(o.Report.Stages[0].Steps[0].Attempts) != failures+1 {
			return readiness.Failed("output or attempt count mismatch")
		}
		return readiness.Passed("original result delivered after bounded retry")
	}))
}

func TestChaosReplayRebuildsIdenticalConditions(t *testing.T) {
	plan := readiness.NewChaosPlan(42, 8, func(ctx context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		result := chaosScenario(t, c).Run(ctx)
		return readiness.ChaosEvidence{Scenario: &result}, nil
	})
	all := plan.Run(context.Background())
	if all.Verdict != readiness.VerdictPass || len(all.Cases) != 8 {
		t.Fatalf("result=%+v", all)
	}
	for _, original := range all.Cases {
		replayed := plan.RunCase(context.Background(), original.Case.Index)
		if replayed.Verdict != original.Verdict || replayed.Case != original.Case || replayed.Evidence.Scenario.Arrangement != original.Evidence.Scenario.Arrangement {
			t.Fatalf("original=%+v replay=%+v", original, replayed)
		}
		if replayed.Evidence.Scenario.Report.RunID == original.Evidence.Scenario.Report.RunID {
			t.Fatal("replay must execute a fresh run")
		}
	}
}

func TestChaosCombinesPressureAndDependencyFaults(t *testing.T) {
	plan := readiness.NewChaosPlan(123, 3, func(ctx context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		count := 3 + c.Random().Intn(3)
		action, err := readiness.Inject(func(int) error { return nil }, readiness.Fault{Calls: 1, Error: errors.New("temporary outage"), Delay: time.Microsecond})
		if err != nil {
			return readiness.ChaosEvidence{}, err
		}
		p := pipeflow.NewPipeline("p", pipeflow.NewStage("s", pipeflow.NewStep("sink", action, pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 2}))))
		scenario := readiness.NewScenario("burst and outage", &p).Arrange(fmt.Sprintf("%d items, one dependency error, delayed sink", count)).Expect(readiness.RunSucceeds())
		result := readiness.NewPressurePlan(scenario, readiness.PressureConfig{Items: count, Worker: pipeflow.WorkerOptions{Workers: 1, Buffer: 1, MaxInFlight: 2}}).Inputs(func(index int) (any, error) { return index, nil }).Expect(readiness.AllAcceptedWorkDrained(), readiness.QueueDepthAtMost(1)).Run(ctx)
		return readiness.ChaosEvidence{Pressure: &result}, nil
	})
	result := plan.Run(context.Background())
	if result.Verdict != readiness.VerdictPass {
		t.Fatalf("result=%+v", result)
	}
	for _, trial := range result.Cases {
		if !trial.Evidence.Pressure.Summary.Drained {
			t.Fatal("accepted work did not drain")
		}
	}
}

func TestChaosLoadEvidenceAndReview(t *testing.T) {
	plan := readiness.NewChaosPlan(0, 1, func(ctx context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		p := pipeflow.NewPipeline("p")
		scenario := readiness.NewScenario("explore", &p).Expect(readiness.Expect("unclassified", func(readiness.Observation) readiness.Assessment { return readiness.NeedsReview("classify behavior") }))
		result := readiness.NewLoadPlan(scenario, readiness.LoadConfig{Runs: 2, Concurrency: 2}).Run(ctx)
		return readiness.ChaosEvidence{Load: &result}, nil
	})
	if got := plan.Run(context.Background()); got.Verdict != readiness.VerdictReview {
		t.Fatalf("result=%+v", got)
	}
}

func TestChaosFailuresRemainReplayableAndDoNotStopCases(t *testing.T) {
	failure := errors.New("setup failed")
	plan := readiness.NewChaosPlan(7, 3, func(_ context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		if c.Index == 0 {
			return readiness.ChaosEvidence{}, failure
		}
		if c.Index == 1 {
			panic("broken experiment")
		}
		return readiness.ChaosEvidence{}, nil
	})
	result := plan.Run(context.Background())
	if result.Verdict != readiness.VerdictFail || len(result.Cases) != 3 || !errors.Is(result.Cases[0].Error, failure) || result.Cases[1].Error == nil || result.Cases[2].Error == nil {
		t.Fatalf("result=%+v", result)
	}
	if got := plan.RunCase(context.Background(), 0); got.Case != result.Cases[0].Case || !errors.Is(got.Error, failure) {
		t.Fatal(got)
	}
}

func TestChaosValidationAndCancellation(t *testing.T) {
	experiment := func(context.Context, readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		t.Fatal("experiment ran")
		return readiness.ChaosEvidence{}, nil
	}
	for _, plan := range []readiness.ChaosPlan{readiness.NewChaosPlan(1, 0, experiment), readiness.NewChaosPlan(1, 1, nil)} {
		if got := plan.Run(context.Background()); got.Error == nil {
			t.Fatal(got)
		}
	}
	plan := readiness.NewChaosPlan(1, 2, experiment)
	for _, index := range []int{-1, 2} {
		if got := plan.RunCase(context.Background(), index); got.Error == nil {
			t.Fatal(got)
		}
	}
	if got := plan.Run(nil); got.Error == nil {
		t.Fatal(got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := plan.Run(ctx); !errors.Is(got.Error, context.Canceled) || len(got.Cases) != 0 {
		t.Fatal(got)
	}
}

func TestChaosRandomIsLocalAndSeedsAreIndependent(t *testing.T) {
	var seeds []int64
	plan := readiness.NewChaosPlan(99, 3, func(_ context.Context, c readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		seeds = append(seeds, c.Seed)
		a, b := c.Random(), c.Random()
		if a.Int63() != b.Int63() {
			t.Fatal("generator is not repeatable")
		}
		return readiness.ChaosEvidence{}, nil
	})
	plan.Run(context.Background())
	first := append([]int64(nil), seeds...)
	seeds = nil
	plan.Run(context.Background())
	if !reflect.DeepEqual(first, seeds) || seeds[0] == seeds[1] || seeds[1] == seeds[2] {
		t.Fatalf("seeds=%v replay=%v", first, seeds)
	}
}

func TestChaosSeedMappingIsStable(t *testing.T) {
	plan := readiness.NewChaosPlan(0, 1, func(context.Context, readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		return readiness.ChaosEvidence{}, nil
	})
	trial := plan.RunCase(context.Background(), 0)
	if uint64(trial.Case.Seed) != 0xe220a8397b1dcdaf {
		t.Fatalf("seed mapping changed: %x", uint64(trial.Case.Seed))
	}
}

func TestChaosStopsFollowingCasesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	plan := readiness.NewChaosPlan(42, 3, func(context.Context, readiness.ChaosCase) (readiness.ChaosEvidence, error) {
		calls++
		cancel()
		return readiness.ChaosEvidence{}, nil
	})
	result := plan.Run(ctx)
	if calls != 1 || len(result.Cases) != 1 || !errors.Is(result.Error, context.Canceled) || result.Verdict != readiness.VerdictFail {
		t.Fatalf("calls=%d result=%+v", calls, result)
	}
}

func TestChaosRejectsAmbiguousAndInvalidEvidence(t *testing.T) {
	for _, evidence := range []readiness.ChaosEvidence{
		{Scenario: &readiness.Result{Verdict: readiness.VerdictPass}, Load: &readiness.LoadResult{Verdict: readiness.VerdictPass}},
		{Scenario: &readiness.Result{}},
		{Pressure: &readiness.PressureResult{Verdict: readiness.VerdictPass, Error: errors.New("pressure setup failed")}},
	} {
		plan := readiness.NewChaosPlan(1, 1, func(context.Context, readiness.ChaosCase) (readiness.ChaosEvidence, error) { return evidence, nil })
		trial := plan.RunCase(context.Background(), 0)
		if trial.Verdict != readiness.VerdictFail || trial.Error == nil {
			t.Fatalf("accepted invalid evidence: %+v", trial)
		}
	}
}
