package pipeflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestExecutionLiveStateAndRunningDurations(t *testing.T) {
	secondAttemptStarted := make(chan struct{})
	release := make(chan struct{})
	attempts := 0
	p := NewPipeline("pipeline",
		NewStage("prepare", NewStep("ready", func() (int, error) { return 10, nil })),
		NewStage("process", NewStep("extract", func(v int) (int, error) {
			attempts++
			if attempts == 1 {
				return 0, errors.New("retry")
			}
			close(secondAttemptStarted)
			<-release
			return v + 1, nil
		}, WithRetry(RetryPolicy{MaxAttempts: 2}))),
		NewStage("output", NewStep("save", func(int) error { return nil })),
	)

	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondAttemptStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second attempt")
	}

	first := execution.Report()
	time.Sleep(5 * time.Millisecond)
	second := execution.Report()
	if first.Status != StatusRunning || second.Duration <= first.Duration {
		t.Fatalf("expected advancing run duration, first=%s second=%s status=%s", first.Duration, second.Duration, second.Status)
	}
	if first.Stages[0].Status != StatusCompleted || first.Stages[1].Status != StatusRunning || first.Stages[2].Status != StatusPending {
		t.Fatalf("unexpected stage states: %+v", first.Stages)
	}
	step := first.Stages[1].Steps[0]
	if step.Status != StatusRunning || len(step.Attempts) != 2 || step.Attempts[0].Status != StatusFailed || step.Attempts[1].Status != StatusRunning {
		t.Fatalf("unexpected live step: %+v", step)
	}

	current := execution.Current()
	if current.Pipeline != "pipeline" || current.Status != StatusRunning || len(current.Stages) != 1 || len(current.Stages[0].Steps) != 1 {
		t.Fatalf("unexpected current state: %+v", current)
	}
	currentStep := current.Stages[0].Steps[0]
	if current.Stages[0].Name != "process" || currentStep.Name != "extract" || currentStep.Attempt != 2 || currentStep.Duration <= 0 || currentStep.AttemptDuration <= 0 {
		t.Fatalf("unexpected active step: %+v", currentStep)
	}

	close(release)
	output, final, err := execution.Wait()
	if err != nil || output != 11 || final.Status != StatusCompleted || execution.Status() != StatusCompleted {
		t.Fatalf("unexpected final result: output=%v report=%+v err=%v", output, final, err)
	}
	if len(execution.Current().Stages) != 0 {
		t.Fatalf("expected no active stages after completion: %+v", execution.Current())
	}
	select {
	case <-execution.Done():
	default:
		t.Fatal("expected Done to be closed")
	}
}

func TestExecutionCurrentIncludesConcurrentActiveSteps(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	makeStep := func(name string) *Step {
		return NewStep(name, func() error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	p := NewPipeline("parallel", NewStage("fanout", NewConcurrentSteps([]*Step{makeStep("a"), makeStep("b")})))

	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent steps")
		}
	}
	current := execution.Current()
	if len(current.Stages) != 1 || len(current.Stages[0].Steps) != 2 || current.Stages[0].Steps[0].Name != "a" || current.Stages[0].Steps[1].Name != "b" {
		t.Fatalf("unexpected concurrent current state: %+v", current)
	}
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionReportSnapshotsAreIndependent(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewPipeline("snapshot", NewStage("stage", NewStep("step", func() error {
		close(started)
		<-release
		return nil
	})))
	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started

	snapshot := execution.Report()
	snapshot.Stages[0].Name = "modified"
	snapshot.Stages[0].Steps[0].Name = "modified"
	again := execution.Report()
	if again.Stages[0].Name != "stage" || again.Stages[0].Steps[0].Name != "step" {
		t.Fatalf("snapshot mutation leaked into live state: %+v", again)
	}
	close(release)
	_, _, _ = execution.Wait()
}

func TestExecutionWaitReturnsFailureAndCancellation(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		want := errors.New("boom")
		p := NewPipeline("failure", NewStage("stage", NewStep("fail", func() error { return want })))
		execution, err := p.Start(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		output, report, err := execution.Wait()
		if output != nil || !errors.Is(err, want) || report.Status != StatusFailed {
			t.Fatalf("unexpected failed execution: output=%v status=%s err=%v", output, report.Status, err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		goCtx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		p := NewPipeline("cancel", NewStage("stage", NewStep("wait", func(goCtx context.Context, _ *Context, _ any) (any, error) {
			close(started)
			<-goCtx.Done()
			return nil, goCtx.Err()
		})))
		execution, err := p.Start(goCtx)
		if err != nil {
			t.Fatal(err)
		}
		<-started
		cancel()
		_, report, err := execution.Wait()
		if !errors.Is(err, context.Canceled) || report.Status != StatusCancelled {
			t.Fatalf("unexpected cancelled execution: status=%s err=%v", report.Status, err)
		}
	})
}

func TestExecutionConcurrentSnapshotsAreSafe(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewPipeline("reads", NewStage("stage", NewStep("wait", func() error {
		close(started)
		<-release
		return nil
	})))
	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = execution.Status()
				_ = execution.Current()
				_ = execution.Report()
			}
		}()
	}
	wg.Wait()
	close(release)
	_, _, _ = execution.Wait()
}

func TestPipelineStartRejectsInvalidConfigurationSynchronously(t *testing.T) {
	p := NewPipeline("invalid", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewStep("consume", func(string) error { return nil }),
	))
	execution, err := p.Start(context.Background())
	if err == nil || execution != nil {
		t.Fatalf("expected synchronous validation failure, execution=%v err=%v", execution, err)
	}
}
