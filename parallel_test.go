package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelBranchesReceiveSameInputAndJoinInDeclarationOrder(t *testing.T) {
	pipeline := NewPipeline("providers", NewStage("lookup",
		NewParallel("accounts", []Branch{
			NewBranch("gmail",
				NewStep("normalize", func(value int) (int, error) { return value + 1, nil }),
				NewStep("fetch", func(value int) (string, error) {
					time.Sleep(5 * time.Millisecond)
					return "gmail-" + string(rune('0'+value)), nil
				}),
			),
			NewBranch("microsoft", NewStep("fetch", func(value int) (string, error) {
				return "microsoft-" + string(rune('0'+value)), nil
			})),
		}),
		NewStep("join", func(results ParallelResults) (string, error) {
			return results[0].Name + ":" + results[0].Value.(string) + "," + results[1].Name + ":" + results[1].Value.(string), nil
		}),
	))

	output, err := pipeline.Run(context.Background(), 1)
	if err != nil || output != "gmail:gmail-2,microsoft:microsoft-1" {
		t.Fatalf("Run = (%v, %v)", output, err)
	}
}

func TestParallelWaitAllJoinsErrorsInDeclarationOrderAndDiscardsPartialValues(t *testing.T) {
	firstErr := errors.New("first branch")
	thirdErr := errors.New("third branch")
	parallel := NewParallel("fanout", []Branch{
		NewBranch("first", NewStep("fail", func() error { return firstErr })),
		NewBranch("successful", NewStep("produce", func() (int, error) { return 42, nil })),
		NewBranch("third", NewStep("fail", func() error { return thirdErr })),
	})

	output, err := parallel.Run(context.Background(), NewContext(), nil)
	if output != nil || !errors.Is(err, firstErr) || !errors.Is(err, thirdErr) {
		t.Fatalf("Run = (%#v, %v)", output, err)
	}
	if strings.Index(err.Error(), "first branch") > strings.Index(err.Error(), "third branch") {
		t.Fatalf("errors not declaration ordered: %v", err)
	}
}

func TestParallelFailFastCancelsSiblingAndWaits(t *testing.T) {
	wantErr := errors.New("provider failed")
	siblingStarted := make(chan struct{})
	siblingFinished := make(chan struct{})
	parallel := NewParallel("fanout", []Branch{
		NewBranch("waiting", NewStep("wait", func(goCtx context.Context, _ *Context, _ any) (any, error) {
			close(siblingStarted)
			<-goCtx.Done()
			close(siblingFinished)
			return nil, goCtx.Err()
		})),
		NewBranch("failure", NewStep("fail", func() error { <-siblingStarted; return wantErr })),
	}, WithParallelFailurePolicy(FailFast))

	output, err := parallel.Run(context.Background(), NewContext(), nil)
	if output != nil || !errors.Is(err, wantErr) {
		t.Fatalf("Run = (%v, %v)", output, err)
	}
	select {
	case <-siblingFinished:
	default:
		t.Fatal("FailFast returned before its sibling stopped")
	}
}

func TestParallelReportHierarchyAndStructuredBranchError(t *testing.T) {
	wantErr := errors.New("failed")
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewParallel("providers", []Branch{
			NewBranch("gmail", NewStep("fetch", func() error { return wantErr })),
			NewBranch("microsoft", NewStep("fetch", func() error { return nil })),
		}),
	))

	_, report, err := pipeline.RunWithReport(context.Background())
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Parallel != "providers" || executionErr.Branch != "gmail" || executionErr.Step != "fetch" {
		t.Fatalf("structured error = %#v", executionErr)
	}
	parallel := report.Stages[0].Parallels[0]
	if parallel.Status != StatusFailed || len(parallel.Branches) != 2 {
		t.Fatalf("parallel report = %#v", parallel)
	}
	if parallel.Branches[0].Status != StatusFailed || parallel.Branches[1].Status != StatusCompleted {
		t.Fatalf("branch reports = %#v", parallel.Branches)
	}
	if parallel.Branches[0].Steps[0].Status != StatusFailed || parallel.Branches[1].Steps[0].Status != StatusCompleted {
		t.Fatalf("step reports = %#v", parallel.Branches)
	}
}

func TestParallelValidatesEachBranchFlow(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }),
		NewParallel("branches", []Branch{
			NewBranch("valid", NewStep("consume", func(int) error { return nil })),
			NewBranch("invalid", NewStep("consume", func(string) error { return nil })),
		}),
	))

	err := pipeline.Validate()
	if err == nil || !strings.Contains(err.Error(), `parallel "branches" branch "invalid"`) {
		t.Fatalf("Validate error = %v", err)
	}
}

func TestParallelEmptyBranchPassesThroughInput(t *testing.T) {
	parallel := NewParallel("branches", []Branch{NewBranch("empty"), NewBranch("transform", NewStep("add", func(value int) (int, error) { return value + 1, nil }))})
	output, err := parallel.Run(context.Background(), NewContext(), 4)
	want := ParallelResults{{Name: "empty", Value: 4}, {Name: "transform", Value: 5}}
	if err != nil || !reflect.DeepEqual(output, want) {
		t.Fatalf("Run = (%#v, %v), want %#v", output, err, want)
	}
}

func TestParallelLiveStateShowsBranches(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	waitStep := func() *Step {
		return NewStep("wait", func() error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("parallel", []Branch{
		NewBranch("one", waitStep()), NewBranch("two", waitStep()),
	})))
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	<-started
	current := execution.Current()
	if len(current.Stages) != 1 || len(current.Stages[0].Parallels) != 1 || len(current.Stages[0].Parallels[0].Branches) != 2 {
		t.Fatalf("current = %#v", current)
	}
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestParallelBranchesInheritAndIsolateContextValues(t *testing.T) {
	ctx := NewContext()
	ctx.Set("inherited", "root")
	branchOneSet := make(chan struct{})
	var firstContext, secondContext *Context
	parallel := NewParallel("isolated", []Branch{
		NewBranch("one",
			NewStep("set", func(_ context.Context, branchCtx *Context, input any) (any, error) {
				firstContext = branchCtx
				if inherited, _ := branchCtx.Get("inherited"); inherited != "root" {
					return nil, errors.New("missing inherited value")
				}
				branchCtx.Set("local", "one")
				close(branchOneSet)
				return input, nil
			}),
			NewStep("read", func(_ context.Context, branchCtx *Context, input any) (any, error) {
				value, _ := branchCtx.Get("local")
				return value, nil
			}),
		),
		NewBranch("two", NewStep("read", func(_ context.Context, branchCtx *Context, input any) (any, error) {
			secondContext = branchCtx
			<-branchOneSet
			if _, exists := branchCtx.Get("local"); exists {
				return nil, errors.New("sibling mutation was visible")
			}
			branchCtx.Set("local", "two")
			return "two", nil
		})),
	})
	output, err := parallel.Run(context.Background(), ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := ParallelResults{{Name: "one", Value: "one"}, {Name: "two", Value: "two"}}
	if !reflect.DeepEqual(output, want) {
		t.Fatalf("output = %#v, want %#v", output, want)
	}
	if firstContext == nil || secondContext == nil || firstContext == secondContext || firstContext == ctx || secondContext == ctx {
		t.Fatalf("branch contexts were not isolated: root=%p first=%p second=%p", ctx, firstContext, secondContext)
	}
	if _, exists := ctx.Get("local"); exists {
		t.Fatal("branch-local mutation was merged into the shared Context")
	}
}

func TestParallelBranchContextShallowCopiesReferencedValues(t *testing.T) {
	ctx := NewContext()
	var shared atomic.Int32
	ctx.Set("shared", &shared)
	makeBranch := func(name string) Branch {
		return NewBranch(name, NewStep("increment", func(_ context.Context, branchCtx *Context, input any) (any, error) {
			value, _ := branchCtx.Get("shared")
			value.(*atomic.Int32).Add(1)
			return input, nil
		}))
	}
	parallel := NewParallel("references", []Branch{makeBranch("one"), makeBranch("two")})
	if _, err := parallel.Run(context.Background(), ctx, nil); err != nil {
		t.Fatal(err)
	}
	if shared.Load() != 2 {
		t.Fatalf("shared referenced value = %d, want 2", shared.Load())
	}
}

func TestParallelBranchContextSharesExecutionMetadata(t *testing.T) {
	ctx := NewContext()
	pipeline := NewPipeline("metadata", NewStage("stage", NewParallel("parallel", []Branch{
		NewBranch("branch", NewStep("observe", func(_ context.Context, branchCtx *Context, input any) (any, error) {
			if branchCtx.Status() != StatusRunning || branchCtx.StartedAt().IsZero() {
				return nil, errors.New("branch did not share execution metadata")
			}
			return input, nil
		})),
	})))
	if _, err := pipeline.Run(context.Background(), ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestParallelLifecycleEventsCarryBranchIdentity(t *testing.T) {
	var mu sync.Mutex
	var events []LifecycleEvent
	pipeline := NewPipeline("pipeline", NewStage("stage", NewParallel("providers", []Branch{
		NewBranch("gmail", NewStep("fetch", func() error { return nil })),
	}))).WithLifecycleHook(func(event LifecycleEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})

	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []LifecycleEventType{ParallelStarted, BranchStarted, StepStarted, StepCompleted, BranchCompleted, ParallelCompleted}
	var got []LifecycleEvent
	for _, event := range events {
		if event.Parallel == "providers" {
			got = append(got, event)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("parallel events = %#v", got)
	}
	for i, eventType := range want {
		if got[i].Type != eventType {
			t.Fatalf("event %d = %q, want %q", i, got[i].Type, eventType)
		}
		if got[i].Type == StepStarted || got[i].Type == StepCompleted {
			if got[i].Branch != "gmail" || got[i].Step != "fetch" {
				t.Fatalf("step event identity = %#v", got[i])
			}
		}
	}
}
