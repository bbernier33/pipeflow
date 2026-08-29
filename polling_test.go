package pipeflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type pollState struct {
	Value int
	Ready bool
}

func TestPollingFlowsSatisfyingValueToNextStep(t *testing.T) {
	polls := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (pollState, error) {
			polls++
			return pollState{Value: polls, Ready: polls == 3}, nil
		}, WithPolling(PollPolicy{
			MaxPolls: 5,
			Until:    func(state pollState) bool { return state.Ready },
		})),
		NewStep("transform", func(state pollState) (int, error) { return state.Value * 2, nil }),
	))

	output, report, err := p.RunWithReport(context.Background())
	if err != nil || output != 6 || polls != 3 {
		t.Fatalf("unexpected polling flow: output=%v polls=%d err=%v", output, polls, err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusCompleted || len(step.Polls) != 3 || len(step.Attempts) != 3 {
		t.Fatalf("unexpected poll report: %+v", step)
	}
	if step.Polls[0].Complete || step.Polls[1].Complete || !step.Polls[2].Complete {
		t.Fatalf("unexpected completion flags: %+v", step.Polls)
	}
	for i, poll := range step.Polls {
		if poll.Poll != i+1 || poll.Status != StatusCompleted || len(poll.Attempts) != 1 || poll.Duration < 0 {
			t.Fatalf("unexpected poll %d: %+v", i+1, poll)
		}
	}
}

func TestPollingSupportsPassThroughAndNoInputPredicates(t *testing.T) {
	t.Run("pass through", func(t *testing.T) {
		calls := 0
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func(value int) error { calls++; return nil }, WithPolling(PollPolicy{
				MaxPolls: 3,
				Until:    func(value int) bool { return calls == 2 && value == 7 },
			})),
		))
		output, err := p.Run(context.Background(), 7)
		if err != nil || output != 7 || calls != 2 {
			t.Fatalf("unexpected pass-through polling: output=%v calls=%d err=%v", output, calls, err)
		}
	})

	t.Run("no input predicate", func(t *testing.T) {
		calls := 0
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func() error { calls++; return nil }, WithPolling(PollPolicy{
				MaxPolls: 3,
				Until:    func() bool { return calls == 2 },
			})),
		))
		output, err := p.Run(context.Background(), "flow")
		if err != nil || output != "flow" || calls != 2 {
			t.Fatalf("unexpected no-input polling: output=%v calls=%d err=%v", output, calls, err)
		}
	})
}

func TestPollingLimitExceededStopsFlow(t *testing.T) {
	consumerRan := false
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (int, error) { return 1, nil }, WithPolling(PollPolicy{
			MaxPolls: 3,
			Until:    func(int) bool { return false },
		})),
		NewStep("consumer", func(int) error { consumerRan = true; return nil }),
	))

	output, report, err := p.RunWithReport(context.Background())
	if output != nil || !errors.Is(err, ErrPollLimitExceeded) || consumerRan || report.Status != StatusFailed {
		t.Fatalf("unexpected poll exhaustion: output=%v consumer=%v status=%s err=%v", output, consumerRan, report.Status, err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusFailed || len(step.Polls) != 3 || step.Polls[2].Complete || report.Stages[0].Steps[1].Status != StatusSkipped {
		t.Fatalf("unexpected exhaustion report: %+v", report.Stages[0])
	}
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Poll != 3 || executionErr.Attempt != 0 || !strings.Contains(err.Error(), "poll 3") {
		t.Fatalf("unexpected exhaustion location: %+v / %v", executionErr, err)
	}
}

func TestPollingTimeoutAndCancellation(t *testing.T) {
	t.Run("poll timeout", func(t *testing.T) {
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func() (bool, error) { return false, nil }, WithPolling(PollPolicy{
				Every:   time.Second,
				Timeout: 10 * time.Millisecond,
				Until:   func(ready bool) bool { return ready },
			})),
		))
		_, report, err := p.RunWithReport(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) || report.Status != StatusTimeout || report.Stages[0].Steps[0].Status != StatusTimeout {
			t.Fatalf("unexpected polling timeout: %+v err=%v", report, err)
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		goCtx, cancel := context.WithCancel(context.Background())
		firstPoll := make(chan struct{})
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func() (bool, error) { close(firstPoll); return false, nil }, WithPolling(PollPolicy{
				Every: time.Second,
				Until: func(bool) bool { return false },
			})),
		))
		execution, err := p.Start(goCtx)
		if err != nil {
			t.Fatal(err)
		}
		<-firstPoll
		cancel()
		_, report, err := execution.Wait()
		if !errors.Is(err, context.Canceled) || report.Status != StatusCancelled {
			t.Fatalf("unexpected polling cancellation: %+v err=%v", report, err)
		}
	})
}

func TestPollingRetryInteraction(t *testing.T) {
	operationCalls := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (int, error) {
			operationCalls++
			if operationCalls == 1 {
				return 0, errors.New("temporary")
			}
			return operationCalls, nil
		}, WithRetry(RetryPolicy{MaxAttempts: 2}), WithPolling(PollPolicy{
			MaxPolls: 2,
			Until:    func(value int) bool { return value == 3 },
		})),
	))

	output, report, err := p.RunWithReport(context.Background())
	if err != nil || output != 3 || operationCalls != 3 {
		t.Fatalf("unexpected polling retry: output=%v calls=%d err=%v", output, operationCalls, err)
	}
	step := report.Stages[0].Steps[0]
	if len(step.Polls) != 2 || len(step.Polls[0].Attempts) != 2 || len(step.Polls[1].Attempts) != 1 || len(step.Attempts) != 3 {
		t.Fatalf("unexpected nested attempt history: %+v", step)
	}
	if step.Polls[0].Attempts[0].Status != StatusFailed || step.Polls[0].Attempts[1].Status != StatusCompleted || !step.Polls[1].Complete {
		t.Fatalf("unexpected retry statuses: %+v", step.Polls)
	}
}

func TestPollingPredicateValidation(t *testing.T) {
	t.Run("static mismatch", func(t *testing.T) {
		executed := false
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func() (int, error) { executed = true; return 1, nil }, WithPolling(PollPolicy{
				Until: func(string) bool { return true },
			})),
		))
		_, err := p.Run(context.Background())
		if err == nil || executed || !strings.Contains(err.Error(), "predicate expects string but step output is int") {
			t.Fatalf("expected pre-run mismatch, executed=%v err=%v", executed, err)
		}
	})

	t.Run("unsupported predicate", func(t *testing.T) {
		p := NewPipeline("pipeline", NewStage("stage",
			NewStep("poll", func() (int, error) { return 1, nil }, WithPolling(PollPolicy{Until: func(int) error { return nil }})),
		))
		_, err := p.Run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "polling predicate must be") {
			t.Fatalf("expected predicate signature error, got %v", err)
		}
	})
}

func TestPollingNilValue(t *testing.T) {
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (*int, error) { return nil, nil }, WithPolling(PollPolicy{
			MaxPolls: 1,
			Until:    func(value *int) bool { return value == nil },
		})),
	))
	output, err := p.Run(context.Background())
	if err != nil || output != (*int)(nil) {
		t.Fatalf("expected typed nil polling output, output=%v err=%v", output, err)
	}
}

func TestCurrentShowsActivePoll(t *testing.T) {
	secondPollStarted := make(chan struct{})
	release := make(chan struct{})
	polls := 0
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (int, error) {
			polls++
			if polls == 2 {
				close(secondPollStarted)
				<-release
			}
			return polls, nil
		}, WithPolling(PollPolicy{MaxPolls: 2, Until: func(value int) bool { return value == 2 }})),
	))

	execution, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-secondPollStarted
	current := execution.Current()
	if len(current.Stages) != 1 || len(current.Stages[0].Steps) != 1 || current.Stages[0].Steps[0].Poll != 2 || current.Stages[0].Steps[0].Attempt != 1 {
		t.Fatalf("unexpected active poll: %+v", current)
	}
	close(release)
	_, _, err = execution.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCurrentKeepsLastPollVisibleDuringInterval(t *testing.T) {
	firstPredicateCalled := make(chan struct{})
	p := NewPipeline("pipeline", NewStage("stage",
		NewStep("poll", func() (bool, error) {
			return false, nil
		}, WithPolling(PollPolicy{Every: time.Second, MaxPolls: 2, Until: func(bool) bool {
			select {
			case <-firstPredicateCalled:
			default:
				close(firstPredicateCalled)
			}
			return false
		}})),
	))
	goCtx, cancel := context.WithCancel(context.Background())
	execution, err := p.Start(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	<-firstPredicateCalled
	deadline := time.Now().Add(time.Second)
	for execution.Report().Stages[0].Steps[0].Polls[0].Status == StatusRunning && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	current := execution.Current()
	if len(current.Stages) != 1 || len(current.Stages[0].Steps) != 1 || current.Stages[0].Steps[0].Poll != 1 || current.Stages[0].Steps[0].Attempt != 0 {
		t.Fatalf("unexpected interval state: %+v", current)
	}
	cancel()
	_, _, _ = execution.Wait()
}

func TestNonPollingStepHasNoPollReports(t *testing.T) {
	p := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil })))
	_, report, err := p.RunWithReport(context.Background())
	if err != nil || len(report.Stages[0].Steps[0].Polls) != 0 {
		t.Fatalf("unexpected polls for ordinary step: %+v err=%v", report.Stages[0].Steps[0], err)
	}
}
