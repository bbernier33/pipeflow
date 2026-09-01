package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Branch is a sequential value-flow path within a Parallel group.
type Branch struct {
	name  string
	steps []*Step
}

// NewBranch creates a sequential branch for a Parallel group.
func NewBranch(name string, steps ...*Step) Branch {
	return Branch{name: name, steps: append([]*Step(nil), steps...)}
}

// BranchResult is one successful branch's final value.
type BranchResult struct {
	Name  string
	Value any
}

// ParallelResults contains successful branch outputs in declaration order.
type ParallelResults []BranchResult

// Parallel executes isolated Branches concurrently and joins their outputs in
// declaration order.
type Parallel struct {
	name             string
	branches         []Branch
	failurePolicy    FailurePolicy
	failurePolicySet bool
	configErr        error
}

// ParallelOption configures a Parallel group.
type ParallelOption func(*Parallel)

// WithParallelFailurePolicy selects WaitAll or FailFast behavior.
func WithParallelFailurePolicy(policy FailurePolicy) ParallelOption {
	return func(parallel *Parallel) { parallel.failurePolicy = policy; parallel.failurePolicySet = true }
}

// NewParallel creates a named group of concurrent Branches.
func NewParallel(name string, branches []Branch, options ...ParallelOption) *Parallel {
	parallel := &Parallel{name: name, branches: append([]Branch(nil), branches...), failurePolicy: WaitAll}
	for _, option := range options {
		option(parallel)
	}
	return parallel
}

// Run executes the Parallel group directly.
func (p *Parallel) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	return p.run(goCtx, ctx, input, nil, -1, -1, nil, "")
}

func (p *Parallel) run(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport, parallelReport int, lifecycle *lifecycleDispatcher, stageName string) (output any, err error) {
	if err := p.validateInput(reflect.TypeOf(input), input != nil); err != nil {
		return nil, err
	}
	if recorder != nil {
		recorder.startParallel(stageReport, parallelReport)
		defer func() {
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emitLocated(ParallelFailed, stageName, p.name, "", "", statusForError(err), err)
			} else {
				hookErr = lifecycle.emitLocated(ParallelCompleted, stageName, p.name, "", "", StatusCompleted, nil)
			}
			if hookErr != nil {
				err = errors.Join(err, annotateExecutionBranch(hookErr, p.name, ""))
			}
			recorder.finishParallel(stageReport, parallelReport, err)
		}()
		if hookErr := lifecycle.emitLocated(ParallelStarted, stageName, p.name, "", "", StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionBranch(hookErr, p.name, "")
		}
	}
	defer recoverPanic(&err)
	if p.failurePolicy == FailFast {
		results, err := p.runFailFast(goCtx, ctx, input, recorder, stageReport, parallelReport, lifecycle, stageName)
		if err != nil {
			return nil, err
		}
		return results, nil
	}
	results, err := p.runWaitAll(goCtx, ctx, input, recorder, stageReport, parallelReport, lifecycle, stageName)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (p *Parallel) runWaitAll(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport, parallelReport int, lifecycle *lifecycleDispatcher, stageName string) (ParallelResults, error) {
	results := make(ParallelResults, len(p.branches))
	errs := make([]error, len(p.branches))
	branchContexts := ctx.branchContexts(len(p.branches))
	var wg sync.WaitGroup
	wg.Add(len(p.branches))
	for i := range p.branches {
		go func(index int) {
			defer wg.Done()
			value, err := p.runBranch(goCtx, branchContexts[index], input, recorder, stageReport, parallelReport, index, lifecycle, stageName)
			results[index] = BranchResult{Name: p.branches[index].name, Value: value}
			errs[index] = err
		}(i)
	}
	wg.Wait()
	var joined []error
	for _, err := range errs {
		if err != nil {
			joined = append(joined, err)
		}
	}
	if len(joined) > 0 {
		return nil, errors.Join(joined...)
	}
	return results, nil
}

func (p *Parallel) runFailFast(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport, parallelReport int, lifecycle *lifecycleDispatcher, stageName string) (ParallelResults, error) {
	groupCtx, cancel := context.WithCancel(goCtx)
	defer cancel()
	results := make(ParallelResults, len(p.branches))
	branchContexts := ctx.branchContexts(len(p.branches))
	var firstErr, firstBusinessErr error
	var firstOnce, businessOnce sync.Once
	var wg sync.WaitGroup
	wg.Add(len(p.branches))
	for i := range p.branches {
		go func(index int) {
			defer wg.Done()
			value, err := p.runBranch(groupCtx, branchContexts[index], input, recorder, stageReport, parallelReport, index, lifecycle, stageName)
			results[index] = BranchResult{Name: p.branches[index].name, Value: value}
			if err != nil {
				firstOnce.Do(func() { firstErr = err })
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					businessOnce.Do(func() { firstBusinessErr = err })
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
	if firstBusinessErr != nil {
		return nil, firstBusinessErr
	}
	if goCtx.Err() != nil {
		return nil, goCtx.Err()
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func (p *Parallel) runBranch(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport, parallelReport, branchReport int, lifecycle *lifecycleDispatcher, stageName string) (output any, err error) {
	branch := &p.branches[branchReport]
	if recorder != nil {
		recorder.startBranch(stageReport, parallelReport, branchReport)
		defer func() {
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emitLocated(BranchFailed, stageName, p.name, branch.name, "", statusForError(err), err)
			} else {
				hookErr = lifecycle.emitLocated(BranchCompleted, stageName, p.name, branch.name, "", StatusCompleted, nil)
			}
			if hookErr != nil {
				err = errors.Join(err, annotateExecutionBranch(hookErr, p.name, branch.name))
			}
			recorder.finishBranch(stageReport, parallelReport, branchReport, err)
		}()
		if hookErr := lifecycle.emitLocated(BranchStarted, stageName, p.name, branch.name, "", StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionBranch(hookErr, p.name, branch.name)
		}
	}
	defer recoverPanic(&err)
	current := input
	for i, step := range branch.steps {
		if err := goCtx.Err(); err != nil {
			return nil, annotateExecutionBranch(err, p.name, branch.name)
		}
		current, err = step.run(goCtx, ctx, current, recorder, branchStepPath(stageReport, parallelReport, branchReport, i), lifecycle, stageName, p.name, branch.name)
		if err != nil {
			return nil, annotateExecutionBranch(err, p.name, branch.name)
		}
	}
	return current, nil
}

func (p *Parallel) validateInput(current reflect.Type, known bool) error {
	_, _, err := p.validateFlow(current, known)
	return err
}

func (p *Parallel) validateFlow(current reflect.Type, known bool) (reflect.Type, bool, error) {
	for _, branch := range p.branches {
		branchType, branchKnown := current, known
		for _, step := range branch.steps {
			var err error
			branchType, branchKnown, err = step.validateFlow(branchType, branchKnown)
			if err != nil {
				return nil, false, fmt.Errorf("pipeflow: parallel %q branch %q: %w", p.name, branch.name, err)
			}
		}
	}
	return reflect.TypeOf(ParallelResults{}), true, nil
}
