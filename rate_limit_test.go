package pipeflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitAppliesToEveryRetryAttempt(t *testing.T) {
	var calls []time.Time
	wantErr := errors.New("temporary")
	step := NewStep("call", func() error {
		calls = append(calls, time.Now())
		if len(calls) < 3 {
			return wantErr
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 3}), WithRateLimit(RateLimitPolicy{MaxCalls: 1, Interval: 20 * time.Millisecond}))

	if _, err := step.Run(context.Background(), NewContext(), nil); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls = %d", len(calls))
	}
	for i := 1; i < len(calls); i++ {
		if gap := calls[i].Sub(calls[i-1]); gap < 15*time.Millisecond {
			t.Fatalf("call gap %d = %s", i, gap)
		}
	}
}

func TestRateLimitAllowsConfiguredBurst(t *testing.T) {
	var calls []time.Time
	step := NewStep("poll", func() (int, error) {
		calls = append(calls, time.Now())
		return len(calls), nil
	}, WithPolling(PollPolicy{MaxPolls: 3, Until: func(value int) bool { return value == 3 }}),
		WithRateLimit(RateLimitPolicy{MaxCalls: 2, Interval: 30 * time.Millisecond}))

	if _, err := step.Run(context.Background(), NewContext(), nil); err != nil {
		t.Fatal(err)
	}
	if calls[1].Sub(calls[0]) > 10*time.Millisecond {
		t.Fatalf("configured burst was delayed: %s", calls[1].Sub(calls[0]))
	}
	if gap := calls[2].Sub(calls[1]); gap < 10*time.Millisecond {
		t.Fatalf("third call was not throttled: %s", gap)
	}
}

func TestSharedRateLimitKeyBoundsConcurrencyAcrossSteps(t *testing.T) {
	var active, maximum atomic.Int32
	work := func() error {
		current := active.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		return nil
	}
	policy := RateLimitPolicy{Key: "provider", MaxConcurrent: 1}
	pipeline := NewPipeline("pipeline", NewStage("stage", NewConcurrentSteps([]*Step{
		NewStep("one", work, WithRateLimit(policy)),
		NewStep("two", work, WithRateLimit(policy)),
		NewStep("three", work, WithRateLimit(policy)),
	})))

	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
}

func TestRateLimitWaitIsCancellationAware(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	first := make(chan struct{})
	var calls atomic.Int32
	step := NewStep("poll", func() (int, error) {
		call := calls.Add(1)
		if call == 1 {
			close(first)
		}
		return int(call), nil
	}, WithPolling(PollPolicy{MaxPolls: 2, Until: func(value int) bool { return value == 2 }}),
		WithRateLimit(RateLimitPolicy{MaxCalls: 1, Interval: time.Hour}))
	pipeline := NewPipeline("pipeline", NewStage("stage", step))
	execution, err := pipeline.Start(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	<-first
	cancel()
	_, report, err := execution.Wait()
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 || report.Stages[0].Steps[0].Status != StatusCancelled {
		t.Fatalf("Wait error = %v, calls = %d, report = %#v", err, calls.Load(), report)
	}
}

type retryAfterTestError struct {
	delay time.Duration
}

func (e retryAfterTestError) Error() string             { return "slow down" }
func (e retryAfterTestError) RetryAfter() time.Duration { return e.delay }

func TestRetryAfterEstablishesMinimumRetryDelay(t *testing.T) {
	var mu sync.Mutex
	var calls []time.Time
	delay := 20 * time.Millisecond
	step := NewStep("call", func() error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, time.Now())
		if len(calls) == 1 {
			return retryAfterTestError{delay: delay}
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 2, Delay: time.Millisecond}), WithRateLimit(RateLimitPolicy{MaxConcurrent: 1}))
	pipeline := NewPipeline("pipeline", NewStage("stage", step))

	_, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gap := calls[1].Sub(calls[0]); gap < 15*time.Millisecond {
		t.Fatalf("retry gap = %s", gap)
	}
	if got := report.Stages[0].Steps[0].Attempts[0].RetryDelay; got != delay {
		t.Fatalf("recorded retry delay = %s", got)
	}
}

func TestRateLimitStateResetsBetweenRuns(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("call", func() error { return nil }, WithRateLimit(RateLimitPolicy{MaxCalls: 1, Interval: time.Hour})),
	))
	shared := NewContext()
	for run := 0; run < 2; run++ {
		started := time.Now()
		if _, err := pipeline.Run(context.Background(), shared, nil); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
			t.Fatalf("run %d inherited limiter state: %s", run, elapsed)
		}
	}
}

func TestRateLimitConfigurationErrorsAreClear(t *testing.T) {
	tests := []RateLimitPolicy{
		{},
		{MaxCalls: -1},
		{MaxCalls: 1},
		{Interval: time.Second},
		{MaxConcurrent: -1},
	}
	for _, policy := range tests {
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("call", func() error { return nil }, WithRateLimit(policy))))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "rate limit") {
			t.Fatalf("policy %#v: Validate error = %v", policy, err)
		}
	}
}

func TestSharedRateLimitKeyRejectsConflictingPolicies(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewConcurrentSteps([]*Step{
		NewStep("one", func() error { return nil }, WithRateLimit(RateLimitPolicy{Key: "provider", MaxConcurrent: 1})),
		NewStep("two", func() error { return nil }, WithRateLimit(RateLimitPolicy{Key: "provider", MaxConcurrent: 2})),
	})))

	err := pipeline.Validate()
	if err == nil || !strings.Contains(err.Error(), `rate limit key "provider" has conflicting policies`) {
		t.Fatalf("Validate error = %v", err)
	}
}

func TestSkippedStepDoesNotConsumeRateLimit(t *testing.T) {
	step := NewStep("skip", func() error { t.Fatal("action ran"); return nil },
		WithCondition(func() bool { return false }),
		WithRateLimit(RateLimitPolicy{MaxCalls: 1, Interval: time.Hour}),
	)
	if _, err := step.Run(context.Background(), NewContext(), nil); err != nil {
		t.Fatal(err)
	}
}
