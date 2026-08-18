package pipeflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestConcurrentStepsMaxWorkersOne(t *testing.T) {
	ctx := NewContext()
	goCtx := context.Background()

	var mu sync.Mutex

	active := 0
	maxActive := 0

	makeStep := func(name string) *Step {
		return NewStep(
			name,
			func(goCtx context.Context, ctx *Context, input any) (any, error) {
				mu.Lock()
				active++

				if active > maxActive {
					maxActive = active
				}

				mu.Unlock()
				time.Sleep(50 * time.Millisecond)

				mu.Lock()
				active--
				mu.Unlock()

				return name, nil
			},
		)
	}

	steps := []*Step{
		makeStep("A"),
		makeStep("B"),
		makeStep("C"),
		makeStep("D"),
	}

	group := NewConcurrentSteps(
		steps,
		WithMaxWorkers(1),
	)

	_, err := group.Run(goCtx, ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if maxActive != 1 {
		t.Fatalf("expected max active workers to be 1, got %v", maxActive)
	}
}

func TestConcurrentStepsMaxWorkersLimit(t *testing.T) {
	ctx := NewContext()
	goCtx := context.Background()

	var mu sync.Mutex

	active := 0
	maxActive := 3

	makeStep := func(name string) *Step {
		return NewStep(
			name,
			func(goCtx context.Context, ctx *Context, input any) (any, error) {
				mu.Lock()
				active++

				if active > maxActive {
					maxActive = active
				}

				mu.Unlock()
				time.Sleep(50 * time.Millisecond)

				mu.Lock()
				active--
				mu.Unlock()

				return name, nil
			},
		)
	}

	steps := []*Step{
		makeStep("A"),
		makeStep("B"),
		makeStep("C"),
		makeStep("D"),
		makeStep("E"),
		makeStep("F"),
	}

	group := NewConcurrentSteps(
		steps,
		WithMaxWorkers(3),
	)

	_, err := group.Run(goCtx, ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if maxActive != 3 {
		t.Fatalf("expected max active workers to be 3, got %v", maxActive)
	}

}

func TestConcurrentStepsUnlimitedWorkers(t *testing.T) {
	ctx := NewContext()
	goCtx := context.Background()

	var mu sync.Mutex

	active := 0
	maxActive := 0

	makeStep := func(name string) *Step {
		return NewStep(
			name,
			func(goCtx context.Context, ctx *Context, input any) (any, error) {
				mu.Lock()
				active++

				if active > maxActive {
					maxActive = active
				}

				mu.Unlock()
				time.Sleep(50 * time.Millisecond)

				mu.Lock()
				active--
				mu.Unlock()

				return name, nil
			},
		)
	}

	steps := []*Step{
		makeStep("A"),
		makeStep("B"),
		makeStep("C"),
		makeStep("D"),
		makeStep("E"),
		makeStep("F"),
	}

	group := NewConcurrentSteps(steps)

	_, err := group.Run(goCtx, ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if maxActive != len(steps) {
		t.Fatalf("expected all %d steps to run concurrently, got max %d", len(steps), maxActive)
	}
}

func TestConcurrentStepsMaxWorkersRespectsCancellation(t *testing.T) {
	ctx := NewContext()
	goCtx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	release := make(chan struct{})

	first := NewStep(
		"first",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			close(started)
			<-release
			return "first", nil
		},
	)

	second := NewStep(
		"second",
		func(goCtx context.Context, ctx *Context, input any) (any, error) {
			return "second", nil
		},
	)

	group := NewConcurrentSteps(
		[]*Step{first, second},
		WithMaxWorkers(1),
	)

	done := make(chan error, 1)

	go func() {
		_, err := group.Run(goCtx, ctx, nil)
		done <- err
	}()

	<-started

	cancel()
	close(release)

	err := <-done

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestConcurrentStepsMaxWorkersFailFastDoesNotStartQueuedSteps(t *testing.T) {
	ctx := NewContext()
	goCtx := context.Background()

	var mu sync.Mutex
	started := map[string]bool{}

	makeStep := func(name string, fail bool) *Step {
		return NewStep(
			name,
			func(goCtx context.Context, ctx *Context, input any) (any, error) {
				mu.Lock()
				started[name] = true
				mu.Unlock()

				if fail {
					return nil, errors.New("boom")
				}

				select {
				case <-goCtx.Done():
					return nil, goCtx.Err()
				case <-time.After(100 * time.Millisecond):
					return name, nil
				}
			},
		)
	}

	steps := []*Step{
		makeStep("A", false),
		makeStep("B", true),
		makeStep("C", false),
		makeStep("D", false),
	}

	group := NewConcurrentSteps(
		steps,
		WithMaxWorkers(2),
		WithFailurePolicy(FailFast),
	)

	_, err := group.Run(goCtx, ctx, nil)

	if err == nil {
		t.Fatal("expected an error")
	}

	mu.Lock()
	defer mu.Unlock()

	if started["C"] {
		t.Fatal("expected C to remain queued")
	}

	if started["D"] {
		t.Fatal("expected D to remain queued")
	}
}
