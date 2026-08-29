package pipeflow

import (
	"context"
	"testing"
	"time"
)

func TestExecutionStateIncludesCompletedRunningAndPendingWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("pipeline",
		NewStage("ingest", NewStep("load", func() (int, error) { return 10, nil })),
		NewStage("process", NewStep("transform", func(value int) (int, error) {
			close(started)
			<-release
			return value + 1, nil
		})),
		NewStage("output", NewStep("save", func(int) error { return nil })),
	)

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for active Step")
	}
	first := execution.State()
	time.Sleep(5 * time.Millisecond)
	second := execution.State()
	if first.RunID == "" || first.Pipeline != "pipeline" || first.Status != StatusRunning || len(first.Stages) != 3 {
		t.Fatalf("State = %#v", first)
	}
	if first.Stages[0].Status != StatusCompleted || first.Stages[1].Status != StatusRunning || first.Stages[2].Status != StatusPending {
		t.Fatalf("stage states = %#v", first.Stages)
	}
	active := first.Stages[1].Steps[0]
	if active.Status != StatusRunning || active.Attempt != 1 {
		t.Fatalf("active Step = %#v", active)
	}
	if second.Duration <= first.Duration || second.Stages[1].Duration <= first.Stages[1].Duration || second.Stages[1].Steps[0].Duration <= active.Duration || second.Stages[1].Steps[0].AttemptDuration <= active.AttemptDuration {
		t.Fatalf("durations did not advance: first=%#v second=%#v", first, second)
	}
	if len(execution.Current().Stages) != 1 {
		t.Fatalf("Current no longer represents active-only work: %#v", execution.Current())
	}

	close(release)
	_, finalReport, err := execution.Wait()
	if err != nil {
		t.Fatal(err)
	}
	final := execution.State()
	if final.Status != StatusCompleted || len(final.Stages) != 3 || len(execution.Current().Stages) != 0 || final.Duration != finalReport.Duration {
		t.Fatalf("final State = %#v", final)
	}
	for _, stage := range final.Stages {
		if stage.Status != StatusCompleted {
			t.Fatalf("final stage = %#v", stage)
		}
	}
}

func TestExecutionStateShowsRetryAndPollOnlyWhileActive(t *testing.T) {
	secondAttempt := make(chan struct{})
	release := make(chan struct{})
	attempts := 0
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("poll", func() (int, error) {
		attempts++
		if attempts == 1 {
			return 0, errTestRetry
		}
		close(secondAttempt)
		<-release
		return 2, nil
	}, WithRetry(RetryPolicy{MaxAttempts: 2}), WithPolling(PollPolicy{MaxPolls: 1, Until: func(value int) bool { return true }}))))

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-secondAttempt
	time.Sleep(2 * time.Millisecond)
	step := execution.State().Stages[0].Steps[0]
	if step.Attempt != 2 || step.Poll != 1 || step.AttemptDuration <= 0 {
		t.Fatalf("running Step state = %#v", step)
	}
	close(release)
	_, _, _ = execution.Wait()
	step = execution.State().Stages[0].Steps[0]
	if step.Status != StatusCompleted || step.Attempt != 0 || step.Poll != 0 {
		t.Fatalf("completed Step state = %#v", step)
	}
}

func TestExecutionStateIncludesParallelSubflowAndBackgroundHierarchy(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	wait := func() error { started <- struct{}{}; <-release; return nil }
	pipeline := NewPipeline("pipeline", NewStage("host",
		NewParallel("parallel", []Branch{
			NewBranch("one", NewStep("wait", wait)),
			NewBranch("two", NewStep("wait", wait)),
		}),
		NewSubflow("unused", NewStage("nested", NewStep("unused", func() error { return nil }))),
	)).WithBackground("support", func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		<-started
	}
	state := execution.State()
	if len(state.Backgrounds) != 1 || state.Backgrounds[0].Status != StatusRunning {
		t.Fatalf("background state = %#v", state.Backgrounds)
	}
	parallel := state.Stages[0].Parallels[0]
	if parallel.Status != StatusRunning || len(parallel.Branches) != 2 || parallel.Branches[0].Steps[0].Status != StatusRunning || state.Stages[0].Subflows[0].Status != StatusPending {
		t.Fatalf("hierarchy = %#v", state)
	}
	close(release)
	_, _, _ = execution.Wait()
}

func TestExecutionStateSnapshotMutationIsIsolated(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("wait", func() error {
		close(started)
		<-release
		return nil
	})))
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	state := execution.State()
	state.Stages[0].Name = "changed"
	state.Stages[0].Steps[0].Name = "changed"
	again := execution.State()
	if again.Stages[0].Name != "stage" || again.Stages[0].Steps[0].Name != "wait" {
		t.Fatalf("State mutation leaked: %#v", again)
	}
	close(release)
	_, _, _ = execution.Wait()
}

var errTestRetry = testRetryError{}

type testRetryError struct{}

func (testRetryError) Error() string { return "retry" }
