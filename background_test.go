package pipeflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundStartsBeforeStageAndStopsBeforeFinalizer(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var finalizerSawStopped bool
	var lifecycle []LifecycleEventType
	pipeline := NewPipeline("managed", NewStage("work", NewStep("step", func() error {
		return nil
	}))).WithBackground("heartbeat", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}).Finally("verify", func(context.Context, Finalization) error {
		select {
		case <-stopped:
			finalizerSawStopped = true
		default:
		}
		return nil
	}).WithLifecycleHook(func(event LifecycleEvent) { lifecycle = append(lifecycle, event.Type) })

	_, report, err := pipeline.RunWithReport(context.Background())
	if err != nil || !finalizerSawStopped {
		t.Fatalf("err=%v finalizerSawStopped=%v", err, finalizerSawStopped)
	}
	select {
	case <-started:
	default:
		t.Fatal("background function was never invoked")
	}
	backgroundStart, stageStart := -1, -1
	for i, eventType := range lifecycle {
		if eventType == BackgroundStarted {
			backgroundStart = i
		}
		if eventType == StageStarted {
			stageStart = i
		}
	}
	if backgroundStart < 0 || stageStart <= backgroundStart {
		t.Fatalf("lifecycle order = %v", lifecycle)
	}
	if len(report.Backgrounds) != 1 || report.Backgrounds[0].Status != StatusCompleted || report.Backgrounds[0].Error != nil {
		t.Fatalf("background report = %#v", report.Backgrounds)
	}
}

func TestFatalBackgroundFailureCancelsMainExecution(t *testing.T) {
	wantErr := errors.New("lease lost")
	workerStarted := make(chan struct{})
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		close(workerStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("lease", func(context.Context) error {
		<-workerStarted
		return wantErr
	})

	output, report, err := pipeline.RunWithReport(context.Background())
	var backgroundErr *BackgroundError
	if output != nil || !errors.Is(err, wantErr) || !errors.As(err, &backgroundErr) || backgroundErr.Name != "lease" {
		t.Fatalf("RunWithReport = (%v, _, %#v)", output, err)
	}
	if report.Status != StatusFailed || report.Backgrounds[0].Status != StatusFailed {
		t.Fatalf("report = %#v", report)
	}
}

func TestNonFatalBackgroundFailureIsReportOnly(t *testing.T) {
	wantErr := errors.New("telemetry unavailable")
	pipeline := NewPipeline("managed", NewStage("work", NewStep("produce", func() (int, error) { return 42, nil }))).
		WithBackground("telemetry", func(context.Context) error { return wantErr }, WithBackgroundFailurePolicy(BackgroundNonFatal))

	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil || output != 42 || report.Status != StatusCompleted {
		t.Fatalf("RunWithReport = (%v, %q, %v)", output, report.Status, err)
	}
	if report.Backgrounds[0].Status != StatusFailed || !errors.Is(report.Backgrounds[0].Error, wantErr) {
		t.Fatalf("background report = %#v", report.Backgrounds[0])
	}
}

func TestBackgroundParentCancellationRemainsVisible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	backgroundStarted := make(chan struct{})
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-backgroundStarted
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("watcher", func(ctx context.Context) error {
		close(backgroundStarted)
		<-ctx.Done()
		return ctx.Err()
	})

	_, report, err := pipeline.RunWithReport(ctx)
	if !errors.Is(err, context.Canceled) || report.Backgrounds[0].Status != StatusCancelled || !errors.Is(report.Backgrounds[0].Error, context.Canceled) {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func TestBackgroundPanicIsRecovered(t *testing.T) {
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("panic", func(context.Context) error { panic("boom") })

	_, _, err := pipeline.RunWithReport(context.Background())
	var panicErr *PanicError
	var backgroundErr *BackgroundError
	if !errors.As(err, &panicErr) || panicErr.Value != "boom" || !errors.As(err, &backgroundErr) {
		t.Fatalf("error = %#v", err)
	}
}

func TestBackgroundConfigurationValidation(t *testing.T) {
	tests := map[string]Pipeline{
		"nil function": NewPipeline("invalid").WithBackground("worker", nil),
		"invalid policy": NewPipeline("invalid").WithBackground("worker", func(context.Context) error { return nil },
			WithBackgroundFailurePolicy(BackgroundFailurePolicy(99))),
		"duplicate name": NewPipeline("invalid").
			WithBackground("worker", func(context.Context) error { return nil }).
			WithBackground("worker", func(context.Context) error { return nil }),
	}
	for name, pipeline := range tests {
		t.Run(name, func(t *testing.T) {
			if err := pipeline.Validate(); err == nil {
				t.Fatal("Validate error = nil")
			}
		})
	}
}

func TestBackgroundLiveState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("heartbeat", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	execution, err := pipeline.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	current := execution.Current()
	if len(current.Backgrounds) != 1 || current.Backgrounds[0].Name != "heartbeat" || current.Backgrounds[0].Status != StatusRunning {
		t.Fatalf("current = %#v", current)
	}
	cancel()
	if _, _, err := execution.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v", err)
	}
}

func TestBackgroundFatalErrorsJoinInDeclarationOrder(t *testing.T) {
	release := make(chan struct{})
	var ready atomic.Int32
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	worker := func(err error) BackgroundFunc {
		return func(context.Context) error {
			if ready.Add(1) == 2 {
				close(release)
			}
			<-release
			return err
		}
	}
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("first", worker(firstErr)).WithBackground("second", worker(secondErr))

	_, _, err := pipeline.RunWithReport(context.Background())
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) || strings.Index(err.Error(), "first") > strings.Index(err.Error(), "second") {
		t.Fatalf("error = %v", err)
	}
}

func TestPreCancelledRunSkipsBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ran atomic.Bool
	pipeline := NewPipeline("managed").WithBackground("worker", func(context.Context) error { ran.Store(true); return nil })
	_, report, err := pipeline.RunWithReport(ctx)
	if !errors.Is(err, context.Canceled) || ran.Load() || report.Backgrounds[0].Status != StatusSkipped {
		t.Fatalf("ran=%v status=%q err=%v", ran.Load(), report.Backgrounds[0].Status, err)
	}
}

func TestBackgroundLifecycleEvents(t *testing.T) {
	var events []LifecycleEvent
	pipeline := NewPipeline("managed").WithBackground("worker", func(context.Context) error { return nil }).
		WithLifecycleHook(func(event LifecycleEvent) {
			if event.Background != "" {
				events = append(events, event)
			}
		})
	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != BackgroundStarted || events[1].Type != BackgroundCompleted || events[0].Background != "worker" {
		t.Fatalf("events = %#v", events)
	}
}

func TestBackgroundTimeoutIsRecorded(t *testing.T) {
	pipeline := NewPipeline("managed", NewStage("work", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))).WithBackground("worker", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}).WithTimeout(5 * time.Millisecond)
	_, report, err := pipeline.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || report.Backgrounds[0].Status != StatusTimeout {
		t.Fatalf("status=%q err=%v", report.Backgrounds[0].Status, err)
	}
}
