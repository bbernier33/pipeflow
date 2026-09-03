package pipeflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testBreaker(t *testing.T, threshold, probes int, cooldown time.Duration, target error) *CircuitBreaker {
	t.Helper()
	b, err := NewCircuitBreaker("provider", CircuitBreakerPolicy{FailureThreshold: threshold, ObservationWindow: time.Minute, OpenDuration: cooldown, HalfOpenMaxProbes: probes, IsFailure: func(err error) bool { return errors.Is(err, target) }})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCircuitOpensAcrossRunsAndShortCircuits(t *testing.T) {
	dependencyErr := errors.New("dependency unavailable")
	breaker := testBreaker(t, 2, 1, time.Minute, dependencyErr)
	var calls atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error { calls.Add(1); return dependencyErr }).WithCircuitBreaker(breaker)))
	for range 2 {
		_, _, _ = p.RunWithReport(context.Background())
	}
	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, ErrCircuitOpen) || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
	circuit := report.Stages[0].Steps[0].Circuit
	if circuit == nil || !circuit.ShortCircuited || circuit.StateAfter != CircuitOpen {
		t.Fatalf("report=%+v", circuit)
	}
	if snapshot := breaker.Snapshot(); snapshot.State != CircuitOpen || snapshot.QualifyingFails != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestCircuitIgnoresNonQualifyingBusinessErrors(t *testing.T) {
	dependencyErr := errors.New("dependency")
	businessErr := errors.New("validation")
	breaker := testBreaker(t, 1, 1, time.Minute, dependencyErr)
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error { return businessErr }).WithCircuitBreaker(breaker)))
	for range 3 {
		_, _, err := p.RunWithReport(context.Background())
		if !errors.Is(err, businessErr) {
			t.Fatal(err)
		}
	}
	if state := breaker.Snapshot().State; state != CircuitClosed {
		t.Fatalf("state=%s", state)
	}
}

func TestCircuitHalfOpenProbeClosesOnSuccess(t *testing.T) {
	dependencyErr := errors.New("dependency")
	breaker := testBreaker(t, 1, 1, time.Millisecond, dependencyErr)
	var fail atomic.Bool
	fail.Store(true)
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error {
		if fail.Load() {
			return dependencyErr
		}
		return nil
	}).WithCircuitBreaker(breaker)))
	_, _, _ = p.RunWithReport(context.Background())
	time.Sleep(2 * time.Millisecond)
	fail.Store(false)
	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state := breaker.Snapshot().State; state != CircuitClosed {
		t.Fatalf("state=%s", state)
	}
	if circuit := report.Stages[0].Steps[0].Circuit; circuit == nil || !circuit.Probe || circuit.StateBefore != CircuitHalfOpen || circuit.StateAfter != CircuitClosed {
		t.Fatalf("report=%+v", circuit)
	}
}

func TestCircuitLimitsConcurrentHalfOpenProbes(t *testing.T) {
	dependencyErr := errors.New("dependency")
	breaker := testBreaker(t, 1, 1, time.Millisecond, dependencyErr)
	failing := NewPipeline("fail", NewStage("s", NewStep("call", func() error { return dependencyErr }).WithCircuitBreaker(breaker)))
	_, _, _ = failing.RunWithReport(context.Background())
	time.Sleep(2 * time.Millisecond)
	started, release := make(chan struct{}), make(chan struct{})
	probe := NewPipeline("probe", NewStage("s", NewStep("call", func() error { close(started); <-release; return nil }).WithCircuitBreaker(breaker)))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _, _ = probe.RunWithReport(context.Background()) }()
	<-started
	_, _, err := probe.RunWithReport(context.Background())
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected rejected extra probe: %v", err)
	}
	close(release)
	wg.Wait()
}

func TestCircuitObservesTerminalResultAfterRetryAndRecovery(t *testing.T) {
	dependencyErr := errors.New("dependency")
	breaker := testBreaker(t, 1, 1, time.Minute, dependencyErr)
	var calls atomic.Int32
	step := NewStep("call", func() error {
		if calls.Add(1) < 3 {
			return dependencyErr
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 2})).WithRecovery(
		retryRecovery("repair", func(Failure) error { return nil }), RecoveryPolicy{MaxAttempts: 1},
	).WithCircuitBreaker(breaker)
	p := NewPipeline("p", NewStage("s", step))
	_, _, err := p.RunWithReport(context.Background())
	if err != nil || breaker.Snapshot().State != CircuitClosed {
		t.Fatalf("err=%v snapshot=%+v", err, breaker.Snapshot())
	}
}

func TestCircuitPolicyValidation(t *testing.T) {
	_, err := NewCircuitBreaker("provider", CircuitBreakerPolicy{})
	if err == nil {
		t.Fatal("expected invalid policy")
	}
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error { return nil }).WithCircuitBreaker(nil)))
	if err := p.Validate(); err == nil {
		t.Fatal("expected nil breaker validation error")
	}
}

func TestCircuitObservationWindowExpiresFailures(t *testing.T) {
	target := errors.New("dependency")
	breaker, err := NewCircuitBreaker("provider", CircuitBreakerPolicy{FailureThreshold: 2, ObservationWindow: time.Millisecond, OpenDuration: time.Minute, HalfOpenMaxProbes: 1, IsFailure: func(err error) bool { return errors.Is(err, target) }})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error { return target }).WithCircuitBreaker(breaker)))
	_, _, _ = p.RunWithReport(context.Background())
	time.Sleep(2 * time.Millisecond)
	_, _, _ = p.RunWithReport(context.Background())
	if snapshot := breaker.Snapshot(); snapshot.State != CircuitClosed || snapshot.QualifyingFails != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestCircuitFailurePredicatePanicUsesPanicPolicy(t *testing.T) {
	breaker, err := NewCircuitBreaker("provider", CircuitBreakerPolicy{FailureThreshold: 1, ObservationWindow: time.Minute, OpenDuration: time.Minute, HalfOpenMaxProbes: 1, IsFailure: func(error) bool { panic("predicate") }})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline("p", NewStage("s", NewStep("call", func() error { return errors.New("failure") }).WithCircuitBreaker(breaker)))
	_, _, err = p.RunWithReport(context.Background())
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("expected PanicError: %v", err)
	}
}
