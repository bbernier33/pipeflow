package pipeflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSubflowPropagatesValuesAcrossNestedStages(t *testing.T) {
	prepare := NewSubflow("prepare",
		NewStage("normalize", NewStep("trim", func(value string) (string, error) { return strings.TrimSpace(value), nil })),
		NewStage("measure", NewStep("length", func(value string) (int, error) { return len(value), nil })),
	)
	pipeline := NewPipeline("pipeline", NewStage("process",
		prepare,
		NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))

	output, err := pipeline.Run(context.Background(), "  abc  ")
	if err != nil || output != 6 {
		t.Fatalf("Run = (%v, %v)", output, err)
	}
}

func TestSubflowIsReusableWithoutIndependentRunState(t *testing.T) {
	increment := NewSubflow("increment", NewStage("work", NewStep("add", func(value int) (int, error) { return value + 1, nil })))
	pipeline := NewPipeline("pipeline",
		NewStage("first", increment),
		NewStage("second", increment),
	)

	output, report, err := pipeline.RunWithReport(context.Background(), 1)
	if err != nil || output != 3 || len(report.Stages[0].Subflows) != 1 || len(report.Stages[1].Subflows) != 1 {
		t.Fatalf("RunWithReport = (%v, %#v, %v)", output, report, err)
	}
	for _, stage := range report.Stages {
		for _, subflow := range stage.Subflows {
			if subflow.Name != "increment" || subflow.Status != StatusCompleted || len(subflow.Stages) != 1 || subflow.Stages[0].Steps[0].Status != StatusCompleted {
				t.Fatalf("subflow report = %#v", subflow)
			}
		}
	}
}

func TestSubflowReportAndLifecycleHierarchy(t *testing.T) {
	var events []LifecycleEvent
	pipeline := NewPipeline("pipeline", NewStage("outer",
		NewSubflow("prepare", NewStage("inner", NewStep("work", func() error { return nil }))),
	)).WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event) })

	_, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	subflow := report.Stages[0].Subflows[0]
	if subflow.Status != StatusCompleted || subflow.StartedAt.IsZero() || subflow.EndedAt.IsZero() || len(subflow.Stages) != 1 || subflow.Stages[0].Name != "inner" {
		t.Fatalf("subflow report = %#v", subflow)
	}
	want := []LifecycleEventType{PipelineStarted, StageStarted, SubflowStarted, StageStarted, StepStarted, StepCompleted, StageCompleted, SubflowCompleted, StageCompleted, PipelineCompleted, PipelineFinalized}
	if len(events) != len(want) {
		t.Fatalf("events = %#v", events)
	}
	for i := range want {
		if events[i].Type != want[i] {
			t.Fatalf("event %d = %q, want %q", i, events[i].Type, want[i])
		}
		if i >= 3 && i <= 6 && events[i].Subflow != "prepare" {
			t.Fatalf("nested event %d has subflow %q", i, events[i].Subflow)
		}
	}
}

func TestSubflowFailureHasLocationAndSkipsRemainingWork(t *testing.T) {
	wantErr := errors.New("invalid")
	pipeline := NewPipeline("pipeline", NewStage("outer",
		NewSubflow("prepare",
			NewStage("validate", NewStep("check", func() error { return wantErr })),
			NewStage("unused", NewStep("unused", func() error { t.Fatal("unused ran"); return nil })),
		),
		NewStep("after", func() error { t.Fatal("after ran"); return nil }),
	))

	_, report, err := pipeline.RunWithReport(context.Background())
	var executionErr *ExecutionError
	if !errors.Is(err, wantErr) || !errors.As(err, &executionErr) || executionErr.Subflow != "prepare" || executionErr.Stage != "validate" || executionErr.Step != "check" {
		t.Fatalf("error = %#v", err)
	}
	subflow := report.Stages[0].Subflows[0]
	if subflow.Status != StatusFailed || subflow.Stages[0].Status != StatusFailed || subflow.Stages[1].Status != StatusSkipped || report.Stages[0].Steps[0].Status != StatusSkipped {
		t.Fatalf("report = %#v", report)
	}
}

func TestSubflowValidatesFlowAcrossBoundaries(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("outer",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewSubflow("invalid", NewStage("inner", NewStep("consume", func(string) error { return nil }))),
	))
	if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), `subflow "invalid"`) || !strings.Contains(err.Error(), "expects string") {
		t.Fatalf("Validate error = %v", err)
	}
}

func TestNestedSubflowsRemainSequential(t *testing.T) {
	inner := NewSubflow("inner", NewStage("transform", NewStep("add", func(value int) (int, error) { return value + 1, nil })))
	outer := NewSubflow("outer", NewStage("nested", inner))
	pipeline := NewPipeline("pipeline", NewStage("host", outer))

	output, report, err := pipeline.RunWithReport(context.Background(), 1)
	innerReport := report.Stages[0].Subflows[0].Stages[0].Subflows[0]
	if err != nil || output != 2 || innerReport.Status != StatusCompleted || innerReport.Stages[0].Steps[0].Status != StatusCompleted {
		t.Fatalf("RunWithReport = (%v, %#v, %v)", output, report, err)
	}
}

func TestExecutionCurrentShowsNestedSubflow(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("outer", NewSubflow("prepare",
		NewStage("inner", NewStep("wait", func() error { close(started); <-release; return nil })),
	)))

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested Step")
	}
	current := execution.Current()
	if len(current.Stages) != 1 || len(current.Stages[0].Subflows) != 1 || len(current.Stages[0].Subflows[0].Stages) != 1 || current.Stages[0].Subflows[0].Stages[0].Steps[0].Name != "wait" {
		t.Fatalf("Current = %#v", current)
	}
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSubflowReportSnapshotsAreIndependent(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("outer", NewSubflow("subflow",
		NewStage("inner", NewStep("wait", func() error { close(started); <-release; return nil })),
	)))
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	snapshot := execution.Report()
	snapshot.Stages[0].Subflows[0].Stages[0].Name = "changed"
	if got := execution.Report().Stages[0].Subflows[0].Stages[0].Name; got != "inner" {
		t.Fatalf("snapshot mutation leaked: %q", got)
	}
	close(release)
	_, _, _ = execution.Wait()
}

func TestSubflowCancellation(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("outer", NewSubflow("work",
		NewStage("inner", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})),
	)))
	execution, err := pipeline.Start(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	_, report, err := execution.Wait()
	if !errors.Is(err, context.Canceled) || report.Stages[0].Subflows[0].Status != StatusCancelled {
		t.Fatalf("Wait error = %v, report = %#v", err, report)
	}
}
