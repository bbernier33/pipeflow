package pipeflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLifecycleSuccessOrderingAndIdentity(t *testing.T) {
	var events []LifecycleEvent
	pipeline := NewPipeline("orders",
		NewStage("prepare",
			NewStep("produce", func() (int, error) { return 2, nil }),
			NewStep("double", func(value int) (int, error) { return value * 2, nil }),
		),
	).WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event) })

	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil || output != 4 {
		t.Fatalf("RunWithReport() = (%v, _, %v), want (4, _, nil)", output, err)
	}
	want := []LifecycleEventType{
		PipelineStarted, StageStarted, StepStarted, StepCompleted,
		StepStarted, StepCompleted, StageCompleted, PipelineCompleted, PipelineFinalized,
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d: %#v", len(events), len(want), events)
	}
	for i, eventType := range want {
		if events[i].Type != eventType {
			t.Fatalf("event %d = %q, want %q", i, events[i].Type, eventType)
		}
		if events[i].RunID == "" || events[i].RunID != report.RunID || events[i].Pipeline != "orders" || events[i].OccurredAt.IsZero() {
			t.Fatalf("event %d has incomplete identity: %#v", i, events[i])
		}
	}
	if events[len(events)-1].Status != StatusCompleted || events[len(events)-1].Err != nil {
		t.Fatalf("final event = %#v", events[len(events)-1])
	}
}

func TestLifecycleFailureAndFinalization(t *testing.T) {
	wantErr := errors.New("boom")
	var events []LifecycleEvent
	pipeline := NewPipeline("orders",
		NewStage("process", NewStep("charge", func() error { return wantErr })),
	).WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event) })

	_, report, err := pipeline.RunWithReport(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunWithReport error = %v, want %v", err, wantErr)
	}
	want := []LifecycleEventType{
		PipelineStarted, StageStarted, StepStarted, StepFailed,
		StageFailed, PipelineFailed, PipelineFinalized,
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d", len(events), len(want))
	}
	for i, eventType := range want {
		if events[i].Type != eventType {
			t.Fatalf("event %d = %q, want %q", i, events[i].Type, eventType)
		}
	}
	for _, index := range []int{3, 4, 5, 6} {
		if events[index].Status != StatusFailed || !errors.Is(events[index].Err, wantErr) {
			t.Fatalf("failure event %d = %#v", index, events[index])
		}
	}
	if report.Status != StatusFailed {
		t.Fatalf("report status = %q, want failed", report.Status)
	}
}

func TestLifecycleCancellationStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var events []LifecycleEvent
	pipeline := NewPipeline("cancelled", NewStage("unused", NewStep("unused", func() error { return nil }))).
		WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event) })

	_, _, err := pipeline.RunWithReport(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithReport error = %v, want context.Canceled", err)
	}
	if len(events) != 3 || events[0].Type != PipelineStarted || events[1].Type != PipelineFailed || events[2].Type != PipelineFinalized {
		t.Fatalf("unexpected events: %#v", events)
	}
	if events[1].Status != StatusCancelled || events[2].Status != StatusCancelled {
		t.Fatalf("cancellation statuses = %q, %q", events[1].Status, events[2].Status)
	}
}

func TestLifecycleTimeoutStatus(t *testing.T) {
	var events []LifecycleEvent
	pipeline := NewPipeline("timeout",
		NewStage("work", NewStep("slow", func() error {
			time.Sleep(25 * time.Millisecond)
			return nil
		}, WithTimeout(time.Millisecond))),
	).WithLifecycleHook(func(event LifecycleEvent) { events = append(events, event) })

	_, err := pipeline.Run(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want deadline exceeded", err)
	}
	for _, eventType := range []LifecycleEventType{StepFailed, StageFailed, PipelineFailed, PipelineFinalized} {
		found := false
		for _, event := range events {
			if event.Type == eventType {
				found = true
				if event.Status != StatusTimeout {
					t.Fatalf("%s status = %q, want timeout", eventType, event.Status)
				}
			}
		}
		if !found {
			t.Fatalf("missing %s in %#v", eventType, events)
		}
	}
}

func TestLifecycleDoesNotStartForInvalidPipeline(t *testing.T) {
	called := false
	pipeline := NewPipeline("invalid", NewStage("stage", NewStep("bad", func(string) string { return "" }))).
		WithLifecycleHook(func(LifecycleEvent) { called = true })

	if _, err := pipeline.Run(context.Background()); err == nil {
		t.Fatal("Run error = nil, want validation error")
	}
	if called {
		t.Fatal("hook ran for an execution that failed validation")
	}
}

func TestLifecycleHooksRunSynchronouslyInRegistrationOrder(t *testing.T) {
	var order []int
	startedHooks := 0
	pipeline := NewPipeline("ordered", NewStage("stage", NewStep("step", func() error {
		if startedHooks != 2 {
			t.Fatalf("action began after %d started hooks, want 2", startedHooks)
		}
		return nil
	}))).WithLifecycleHook(func(event LifecycleEvent) {
		if event.Type == PipelineStarted {
			order = append(order, 1)
			startedHooks++
		}
	}).WithLifecycleHook(func(event LifecycleEvent) {
		if event.Type == PipelineStarted {
			order = append(order, 2)
			startedHooks++
		}
	})

	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("hook order = %v, want [1 2]", order)
	}
}

func TestLifecycleConcurrentStepsPreservePerStepCausality(t *testing.T) {
	var mu sync.Mutex
	var events []LifecycleEvent
	pipeline := NewPipeline("parallel",
		NewStage("work", NewConcurrentSteps([]*Step{
			NewStep("one", func() error { time.Sleep(time.Millisecond); return nil }),
			NewStep("two", func() error { return nil }),
		})),
	).WithLifecycleHook(func(event LifecycleEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})

	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"one", "two"} {
		started, completed := -1, -1
		for i, event := range events {
			if event.Step == name && event.Type == StepStarted {
				started = i
			}
			if event.Step == name && event.Type == StepCompleted {
				completed = i
			}
		}
		if started < 0 || completed <= started {
			t.Fatalf("step %q causality missing in %#v", name, events)
		}
	}
}
