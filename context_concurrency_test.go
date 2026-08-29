package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestContextConcurrentSetAndGet(t *testing.T) {
	ctx := NewContext()
	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", index)
			for value := 0; value < 100; value++ {
				ctx.Set(key, value)
				ctx.Get(key)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if _, ok := ctx.Get(fmt.Sprintf("key-%d", i)); !ok {
			t.Fatalf("missing key %d", i)
		}
	}
}

func TestConcurrentStepsSafelyShareContextValues(t *testing.T) {
	ctx := NewContext()
	steps := make([]*Step, 32)
	for i := range steps {
		index := i
		steps[i] = NewStep(fmt.Sprintf("step-%d", i), func() error {
			ctx.Set(fmt.Sprintf("result-%d", index), index)
			return nil
		})
	}
	pipeline := NewPipeline("shared", NewStage("parallel", NewConcurrentSteps(steps)))
	if _, err := pipeline.Run(context.Background(), ctx, nil); err != nil {
		t.Fatal(err)
	}
	for i := range steps {
		if value, ok := ctx.Get(fmt.Sprintf("result-%d", i)); !ok || value != i {
			t.Fatalf("result %d = (%v, %v)", i, value, ok)
		}
	}
}

func TestContextRejectsOverlappingRunsAndAllowsReuse(t *testing.T) {
	ctx := NewContext()
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("exclusive", NewStage("stage", NewStep("wait", func() error {
		close(started)
		<-release
		return nil
	})))

	execution, err := pipeline.Start(context.Background(), ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	_, report, err := pipeline.RunWithReport(context.Background(), ctx, nil)
	if !errors.Is(err, ErrContextInUse) || report.RunID != "" {
		t.Fatalf("overlapping run = (report=%#v, err=%v)", report, err)
	}
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}

	reused := NewPipeline("reuse")
	if _, err := reused.Run(context.Background(), ctx, nil); err != nil {
		t.Fatalf("sequential reuse failed: %v", err)
	}
}

func TestContextMetadataSupportsConcurrentReaders(t *testing.T) {
	ctx := NewContext()
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("readers", NewStage("stage", NewStep("wait", func() error {
		close(started)
		<-release
		return nil
	})))
	execution, err := pipeline.Start(context.Background(), ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = ctx.Status()
				_ = ctx.StartedAt()
				_ = ctx.EndedAt()
				_ = ctx.Duration()
				_ = ctx.CurrentStage()
				_ = ctx.CurrentStep()
			}
		}()
	}
	wg.Wait()
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
}

type concurrencyDetectingLogger struct {
	active    int
	maxActive int
}

func (l *concurrencyDetectingLogger) Info(string)  { l.record() }
func (l *concurrencyDetectingLogger) Error(string) { l.record() }

func (l *concurrencyDetectingLogger) record() {
	l.active++
	if l.active > l.maxActive {
		l.maxActive = l.active
	}
	time.Sleep(50 * time.Microsecond)
	l.active--
}

func TestContextSerializesLoggerCalls(t *testing.T) {
	logger := &concurrencyDetectingLogger{}
	ctx := NewContextWithLogger(logger)
	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ctx.Logger().Info("message")
		}()
	}
	wg.Wait()
	if logger.maxActive != 1 {
		t.Fatalf("maximum concurrent logger calls = %d, want 1", logger.maxActive)
	}
}
