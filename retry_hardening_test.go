package pipeflow

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestRetryDelayStrategies(t *testing.T) {
	t.Run("fixed remains default", func(t *testing.T) {
		policy := RetryPolicy{Delay: 10 * time.Millisecond}
		for attempt := 1; attempt <= 5; attempt++ {
			if got := policy.delayAfter(attempt); got != 10*time.Millisecond {
				t.Fatalf("attempt %d: expected fixed 10ms, got %s", attempt, got)
			}
		}
	})

	t.Run("exponential", func(t *testing.T) {
		policy := RetryPolicy{Delay: 10 * time.Millisecond, Backoff: ExponentialBackoff}
		want := []time.Duration{10, 20, 40, 80}
		for i, milliseconds := range want {
			if got := policy.delayAfter(i + 1); got != milliseconds*time.Millisecond {
				t.Fatalf("attempt %d: expected %s, got %s", i+1, milliseconds*time.Millisecond, got)
			}
		}
	})

	t.Run("maximum delay", func(t *testing.T) {
		policy := RetryPolicy{Delay: 10 * time.Millisecond, Backoff: ExponentialBackoff, MaxDelay: 25 * time.Millisecond}
		if got := policy.delayAfter(4); got != 25*time.Millisecond {
			t.Fatalf("expected capped delay 25ms, got %s", got)
		}
	})

	t.Run("overflow saturates", func(t *testing.T) {
		policy := RetryPolicy{Delay: time.Duration(math.MaxInt64 / 2), Backoff: ExponentialBackoff, MaxDelay: time.Second}
		if got := policy.delayAfter(10); got != time.Second {
			t.Fatalf("expected overflow-safe maximum, got %s", got)
		}
	})
}

func TestRetryJitterBoundsAndFinalCap(t *testing.T) {
	policy := RetryPolicy{Delay: 100 * time.Millisecond, Jitter: 0.25}
	for i := 0; i < 1000; i++ {
		got := policy.delayAfter(1)
		if got < 75*time.Millisecond || got > 125*time.Millisecond {
			t.Fatalf("jittered delay outside bounds: %s", got)
		}
	}

	policy.MaxDelay = 90 * time.Millisecond
	for i := 0; i < 100; i++ {
		if got := policy.delayAfter(1); got > 90*time.Millisecond {
			t.Fatalf("jitter exceeded final cap: %s", got)
		}
	}
}

func TestRetryPredicateStopsNonRetryableError(t *testing.T) {
	want := errors.New("permanent")
	attempts := 0
	predicateCalls := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() error { attempts++; return want }, WithRetry(RetryPolicy{
			MaxAttempts: 5,
			RetryIf: func(err error) bool {
				predicateCalls++
				return !errors.Is(err, want)
			},
		})),
	))

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, want) || attempts != 1 || predicateCalls != 1 {
		t.Fatalf("unexpected non-retryable result: attempts=%d predicate=%d err=%v", attempts, predicateCalls, err)
	}
	step := report.Stages[0].Steps[0]
	if len(step.Attempts) != 1 || step.Attempts[0].RetryDelay != 0 {
		t.Fatalf("unexpected non-retryable history: %+v", step.Attempts)
	}
}

func TestRetryPredicateReceivesEachRetryableBusinessError(t *testing.T) {
	temporary := errors.New("temporary")
	permanent := errors.New("permanent")
	attempts := 0
	var seen []error
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() error {
			attempts++
			if attempts == 1 {
				return temporary
			}
			return permanent
		}, WithRetry(RetryPolicy{
			MaxAttempts: 5,
			RetryIf: func(err error) bool {
				seen = append(seen, err)
				return errors.Is(err, temporary)
			},
		})),
	))

	_, _, err := p.RunWithReport(context.Background())
	if !errors.Is(err, permanent) || attempts != 2 || len(seen) != 2 || seen[0] != temporary || seen[1] != permanent {
		t.Fatalf("unexpected selective retry: attempts=%d seen=%v err=%v", attempts, seen, err)
	}
}

func TestRetryContextErrorsBypassPredicate(t *testing.T) {
	predicateCalls := 0
	step := NewStep("step", func() error { return context.Canceled }, WithRetry(RetryPolicy{
		MaxAttempts: 3,
		RetryIf:     func(error) bool { predicateCalls++; return true },
	}))
	_, err := step.Run(context.Background(), NewContext(), nil)
	if !errors.Is(err, context.Canceled) || predicateCalls != 0 {
		t.Fatalf("expected cancellation without predicate, calls=%d err=%v", predicateCalls, err)
	}
}

func TestRetryReportRecordsScheduledDelay(t *testing.T) {
	attempts := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary")
			}
			return nil
		}, WithRetry(RetryPolicy{MaxAttempts: 3, Delay: time.Millisecond, Backoff: ExponentialBackoff})),
	))

	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	attemptReports := report.Stages[0].Steps[0].Attempts
	if len(attemptReports) != 3 || attemptReports[0].RetryDelay != time.Millisecond || attemptReports[1].RetryDelay != 2*time.Millisecond || attemptReports[2].RetryDelay != 0 {
		t.Fatalf("unexpected recorded delays: %+v", attemptReports)
	}
}

func TestRetryTimeoutDuringBackoff(t *testing.T) {
	attempts := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() error { attempts++; return errors.New("temporary") },
			WithRetry(RetryPolicy{MaxAttempts: 3, Delay: time.Second}),
			WithTimeout(10*time.Millisecond),
		),
	))

	_, report, err := p.RunWithReport(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || attempts != 1 || report.Status != StatusTimeout {
		t.Fatalf("unexpected backoff timeout: attempts=%d report=%+v err=%v", attempts, report, err)
	}
	attempt := report.Stages[0].Steps[0].Attempts[0]
	if attempt.Status != StatusFailed || attempt.RetryDelay != time.Second {
		t.Fatalf("unexpected timed backoff attempt: %+v", attempt)
	}
}

func TestRetryBackoffResetsForEachPoll(t *testing.T) {
	calls := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("step", func() (int, error) {
			calls++
			if calls == 1 || calls == 3 {
				return 0, errors.New("temporary")
			}
			return calls, nil
		},
			WithRetry(RetryPolicy{MaxAttempts: 2, Delay: time.Millisecond, Backoff: ExponentialBackoff}),
			WithPolling(PollPolicy{MaxPolls: 2, Until: func(value int) bool { return value == 4 }}),
		),
	))

	_, report, err := p.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	polls := report.Stages[0].Steps[0].Polls
	if len(polls) != 2 || polls[0].Attempts[0].RetryDelay != time.Millisecond || polls[1].Attempts[0].RetryDelay != time.Millisecond {
		t.Fatalf("expected backoff reset per poll: %+v", polls)
	}
}

func TestRetryConfigurationValidation(t *testing.T) {
	for name, policy := range map[string]RetryPolicy{
		"strategy": {Backoff: BackoffStrategy(99)},
		"jitter":   {Jitter: math.NaN()},
	} {
		t.Run(name, func(t *testing.T) {
			executed := false
			p := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { executed = true; return nil }, WithRetry(policy))))
			_, err := p.Run(context.Background())
			if err == nil || executed {
				t.Fatalf("expected pre-run retry validation, executed=%v err=%v", executed, err)
			}
		})
	}
}

func TestRetryPolicyNormalization(t *testing.T) {
	step := NewStep("step", func() error { return errors.New("boom") }, WithRetry(RetryPolicy{
		MaxAttempts: 0,
		Delay:       -time.Second,
		MaxDelay:    -time.Second,
		Jitter:      2,
	}))
	if step.retryPolicy.MaxAttempts != 1 || step.retryPolicy.Delay != 0 || step.retryPolicy.MaxDelay != 0 || step.retryPolicy.Jitter != 1 {
		t.Fatalf("unexpected normalized policy: %+v", *step.retryPolicy)
	}
}
