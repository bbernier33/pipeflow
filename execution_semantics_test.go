package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type recordingStageItem struct {
	name  string
	order *[]string
}

func (i recordingStageItem) Run(_ context.Context, _ *Context, input any) (any, error) {
	*i.order = append(*i.order, i.name)
	return input, nil
}

func TestExecutionSemanticsSequentialOrdering(t *testing.T) {
	var order []string
	p := NewPipeline("pipeline",
		NewStage("first",
			recordingStageItem{name: "item-a", order: &order},
			NewStep("step-b", func(v int) (int, error) { order = append(order, "step-b"); return v + 1, nil }),
		),
		NewStage("second",
			NewStep("step-c", func(v int) error { order = append(order, "step-c"); return nil }),
		),
	)

	output, err := p.Run(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if output != 2 || !reflect.DeepEqual(order, []string{"item-a", "step-b", "step-c"}) {
		t.Fatalf("unexpected output/order: %v %v", output, order)
	}
}

func TestExecutionSemanticsWaitAllWaitsAndJoinsInDeclarationOrder(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	secondFinished := make(chan struct{})
	group := NewConcurrentSteps([]*Step{
		NewStep("first", func() error { return firstErr }),
		NewStep("second", func() error {
			time.Sleep(10 * time.Millisecond)
			close(secondFinished)
			return secondErr
		}),
	})

	output, err := group.Run(context.Background(), NewContext(), "flow")
	if output != nil || !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("expected nil output and both errors, got output=%v err=%v", output, err)
	}
	select {
	case <-secondFinished:
	default:
		t.Fatal("WaitAll returned before all work completed")
	}
	if strings.Index(err.Error(), "first") > strings.Index(err.Error(), "second") {
		t.Fatalf("expected errors in declaration order, got %q", err)
	}
}

func TestExecutionSemanticsFailFastReturnsTriggeringBusinessError(t *testing.T) {
	triggerErr := errors.New("trigger")
	laterErr := errors.New("later")
	group := NewConcurrentSteps([]*Step{
		NewStep("declared-first", func(goCtx context.Context, _ *Context, _ any) (any, error) {
			<-goCtx.Done()
			time.Sleep(10 * time.Millisecond)
			return nil, laterErr
		}),
		NewStep("fails-first", func() error { return triggerErr }),
	}, WithFailurePolicy(FailFast))

	_, err := group.Run(context.Background(), NewContext(), nil)
	if !errors.Is(err, triggerErr) {
		t.Fatalf("expected triggering error identity, got %v", err)
	}
}

func TestExecutionSemanticsFailFastReturnsCancellationOnlyFailure(t *testing.T) {
	group := NewConcurrentSteps([]*Step{
		NewStep("cancel", func() error { return context.Canceled }),
	}, WithFailurePolicy(FailFast))

	_, err := group.Run(context.Background(), NewContext(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExecutionSemanticsCancellationSkipsSequentialWork(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	var secondRan, laterStageRan bool
	p := NewPipeline("pipeline",
		NewStage("first",
			NewStep("cancel", func(v int) error { cancel(); return nil }),
			NewStep("skipped", func(v int) error { secondRan = true; return nil }),
		),
		NewStage("later", NewStep("also-skipped", func(v int) error { laterStageRan = true; return nil })),
	)

	output, err := p.Run(goCtx, 1)
	if output != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation with nil output, got output=%v err=%v", output, err)
	}
	if secondRan || laterStageRan {
		t.Fatalf("expected later work skipped, got second=%v stage=%v", secondRan, laterStageRan)
	}
}

func TestExecutionSemanticsPreCancelledEmptyPipelineFails(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	cancel()
	pipeCtx := NewContext()
	p := NewPipeline("empty")
	_, err := p.Run(goCtx, pipeCtx, nil)
	if !errors.Is(err, context.Canceled) || pipeCtx.Status() != StatusCancelled {
		t.Fatalf("expected failed cancellation, got status=%s err=%v", pipeCtx.Status(), err)
	}
}

func TestExecutionSemanticsBoundedFailFastSkipsQueuedWork(t *testing.T) {
	triggerErr := errors.New("stop")
	var queuedRuns atomic.Int32
	group := NewConcurrentSteps([]*Step{
		NewStep("fail", func() error { return triggerErr }),
		NewStep("queued-a", func() error { queuedRuns.Add(1); return nil }),
		NewStep("queued-b", func() error { queuedRuns.Add(1); return nil }),
	}, WithMaxWorkers(1), WithFailurePolicy(FailFast))

	_, err := group.Run(context.Background(), NewContext(), nil)
	if !errors.Is(err, triggerErr) || queuedRuns.Load() != 0 {
		t.Fatalf("expected trigger and no queued work, got err=%v runs=%d", err, queuedRuns.Load())
	}
}

func TestExecutionSemanticsRetryValueAndFailurePropagation(t *testing.T) {
	t.Run("eventual success flows once", func(t *testing.T) {
		var attempts, consumers int
		p := NewPipeline("retry", NewStage("stage",
			NewStep("retry", func(v int) (int, error) {
				attempts++
				if attempts < 3 {
					return 0, errors.New("temporary")
				}
				return v + 1, nil
			}, WithRetry(RetryPolicy{MaxAttempts: 3})),
			NewStep("consume", func(v int) error { consumers++; return nil }),
		))
		output, err := p.Run(context.Background(), 4)
		if err != nil || output != 5 || attempts != 3 || consumers != 1 {
			t.Fatalf("unexpected retry result: output=%v err=%v attempts=%d consumers=%d", output, err, attempts, consumers)
		}
	})

	t.Run("exhaustion stops value flow", func(t *testing.T) {
		want := errors.New("failed")
		var consumerRan bool
		p := NewPipeline("retry", NewStage("stage",
			NewStep("retry", func(int) (int, error) { return 0, want }, WithRetry(RetryPolicy{MaxAttempts: 2})),
			NewStep("consume", func(int) error { consumerRan = true; return nil }),
		))
		output, err := p.Run(context.Background(), 4)
		if output != nil || !errors.Is(err, want) || consumerRan {
			t.Fatalf("expected failure to stop flow, got output=%v err=%v consumer=%v", output, err, consumerRan)
		}
	})
}

func TestExecutionSemanticsConcurrentWorkerLimit(t *testing.T) {
	var active, maximum atomic.Int32
	makeStep := func() *Step {
		return NewStep("work", func() error {
			now := active.Add(1)
			for old := maximum.Load(); now > old && !maximum.CompareAndSwap(old, now); old = maximum.Load() {
			}
			time.Sleep(5 * time.Millisecond)
			active.Add(-1)
			return nil
		})
	}
	group := NewConcurrentSteps([]*Step{makeStep(), makeStep(), makeStep(), makeStep()}, WithMaxWorkers(2))
	_, err := group.Run(context.Background(), NewContext(), nil)
	if err != nil || maximum.Load() != 2 {
		t.Fatalf("expected worker maximum 2, got max=%d err=%v", maximum.Load(), err)
	}
}
