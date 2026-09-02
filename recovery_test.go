package pipeflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func retryRecovery(name string, repair func(Failure) error) *RecoveryStage {
	return NewRecoveryStage(name,
		NewStep("repair", repair),
		NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryRetryStep, nil }),
	)
}

func TestRecoveryRepairsAndRetriesOriginalStep(t *testing.T) {
	sentinel := errors.New("expired credential")
	var calls int
	step := NewStep("fetch", func() (string, error) {
		calls++
		if calls == 1 {
			return "", sentinel
		}
		return "ok", nil
	}).WithRecovery(retryRecovery("refresh", func(f Failure) error {
		if !errors.Is(f.Err, sentinel) || f.Pipeline != "p" || f.Stage != "s" || f.Step != "fetch" || f.Attempt != 1 || f.RunID == "" {
			t.Fatalf("unexpected failure context: %+v", f)
		}
		return nil
	}), RecoveryPolicy{MaxAttempts: 1, Timeout: time.Second})

	p := NewPipeline("p", NewStage("s", step))
	output, report, err := p.RunWithReport(context.Background())
	if err != nil || output != "ok" {
		t.Fatalf("output=%v err=%v", output, err)
	}
	recoveries := report.Stages[0].Steps[0].Recoveries
	if len(recoveries) != 1 || recoveries[0].Decision != RecoveryRetryStep || recoveries[0].Status != StatusCompleted || !errors.Is(recoveries[0].OriginalError, sentinel) {
		t.Fatalf("unexpected recovery report: %+v", recoveries)
	}
	if len(recoveries[0].Stages) != 1 || len(recoveries[0].Stages[0].Steps) != 2 {
		t.Fatalf("missing nested reports: %+v", recoveries[0])
	}
}

func TestRecoveryIsBoundedAndPreservesOriginalFailure(t *testing.T) {
	sentinel := errors.New("still broken")
	var calls atomic.Int32
	step := NewStep("fetch", func() error { calls.Add(1); return sentinel }).WithRecovery(
		retryRecovery("repair", func(Failure) error { return nil }), RecoveryPolicy{MaxAttempts: 2},
	)
	p := NewPipeline("p", NewStage("s", step))
	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("original failure was not preserved: %v", err)
	}
	var recoveryErr *RecoveryError
	if !errors.As(err, &recoveryErr) || recoveryErr.Decision != RecoveryFailStep {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want initial + 2 recovered retries", calls.Load())
	}
	if got := len(report.Stages[0].Steps[0].Recoveries); got != 2 {
		t.Fatalf("recoveries=%d", got)
	}
}

func TestRecoveryExplicitFailureDecision(t *testing.T) {
	sentinel := errors.New("provider down")
	recovery := NewRecoveryStage("diagnose", NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryFailPipeline, nil }))
	p := NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return sentinel }).WithRecovery(recovery, RecoveryPolicy{})))
	_, _, err := p.RunWithReport(context.Background())
	var recoveryErr *RecoveryError
	if !errors.As(err, &recoveryErr) || recoveryErr.Decision != RecoveryFailPipeline || !errors.Is(err, sentinel) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRecoveryValidationRejectsBusinessOutputAndRecursion(t *testing.T) {
	businessOutput := NewRecoveryStage("bad", NewStep("value", func(Failure) (string, error) { return "replacement", nil }))
	p := NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return nil }).WithRecovery(businessOutput, RecoveryPolicy{})))
	if err := p.Validate(); err == nil {
		t.Fatal("expected decision output validation error")
	}

	inner := NewStep("inner", func(Failure) (RecoveryDecision, error) { return RecoveryFailStep, nil }).WithRecovery(
		NewRecoveryStage("recursive", NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryFailStep, nil })), RecoveryPolicy{},
	)
	p = NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return nil }).WithRecovery(NewRecoveryStage("outer", inner), RecoveryPolicy{})))
	if err := p.Validate(); err == nil {
		t.Fatal("expected recursive recovery validation error")
	}
}

func TestRecoveryTimeout(t *testing.T) {
	recovery := NewRecoveryStage("slow", NewStep("wait", func(goCtx context.Context, _ *Context, input any) (any, error) {
		<-goCtx.Done()
		return input, goCtx.Err()
	}), NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryRetryStep, nil }))
	p := NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return errors.New("fail") }).WithRecovery(recovery, RecoveryPolicy{Timeout: time.Millisecond})))
	_, _, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected recovery deadline: %v", err)
	}
}

func TestStepTimeoutCanEnterRecovery(t *testing.T) {
	var calls atomic.Int32
	step := NewStep("fetch", func(goCtx context.Context, _ *Context, input any) (any, error) {
		if calls.Add(1) == 1 {
			<-goCtx.Done()
			return nil, goCtx.Err()
		}
		return "recovered", nil
	}, WithTimeout(time.Millisecond)).WithRecovery(retryRecovery("repair", func(f Failure) error {
		if !errors.Is(f.Err, context.DeadlineExceeded) {
			t.Fatalf("failure=%v", f.Err)
		}
		return nil
	}), RecoveryPolicy{Timeout: time.Second})
	p := NewPipeline("p", NewStage("s", step))
	output, _, err := p.RunWithReport(context.Background())
	if err != nil || output != "recovered" {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestConcurrentRecoveriesAreRunScoped(t *testing.T) {
	var repaired atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return errors.New("fail") }).WithRecovery(
		NewRecoveryStage("repair", NewStep("count", func(Failure) error { repaired.Add(1); return nil }), NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryFailStep, nil })), RecoveryPolicy{},
	)))
	done := make(chan struct{}, 2)
	for range 2 {
		go func() { _, _, _ = p.RunWithReport(context.Background()); done <- struct{}{} }()
	}
	<-done
	<-done
	if repaired.Load() != 2 {
		t.Fatalf("recoveries=%d; recovery is per run", repaired.Load())
	}
}

func TestRecoveryLifecycleAndObservation(t *testing.T) {
	var lifecycle []LifecycleEvent
	var traces []TraceEvent
	p := NewPipeline("p", NewStage("s", NewStep("fetch", func() error { return errors.New("fail") }).WithRecovery(
		NewRecoveryStage("repair", NewStep("decide", func(Failure) (RecoveryDecision, error) { return RecoveryFailStep, nil })), RecoveryPolicy{},
	))).WithLifecycleHook(func(event LifecycleEvent) { lifecycle = append(lifecycle, event) }).WithObserver(ObserverFuncs{Trace: func(event TraceEvent) { traces = append(traces, event) }})
	_, _, _ = p.RunWithReport(context.Background())
	foundLifecycle, foundTrace := false, false
	for _, event := range lifecycle {
		if event.Type == RecoveryCompleted && event.Recovery == "repair" && event.RecoveryAttempt == 1 && event.RecoveryDecision == RecoveryFailStep {
			foundLifecycle = true
		}
	}
	for _, event := range traces {
		if event.Scope == ObservationRecovery && event.Phase == ObservationCompleted && event.Location.Recovery == "repair" && event.Location.RecoveryAttempt == 1 {
			foundTrace = true
		}
	}
	if !foundLifecycle || !foundTrace {
		t.Fatalf("lifecycle=%+v traces=%+v", lifecycle, traces)
	}
}
