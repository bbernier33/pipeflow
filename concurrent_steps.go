package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type ConcurrentSteps struct {
	steps         []*Step
	failurePolicy FailurePolicy
	maxWorkers    int
	configErr     error
}

func WithMaxWorkers(max int) ConcurrentStepsOption {
	return func(c *ConcurrentSteps) {
		if max < 0 {
			c.configErr = fmt.Errorf("concurrent steps max workers cannot be negative")
			return
		}
		c.maxWorkers = max
	}
}

func (c *ConcurrentSteps) validateFlow(current reflect.Type, known bool) (reflect.Type, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("nil concurrent steps")
	}
	if c.configErr != nil {
		return nil, false, c.configErr
	}
	for _, step := range c.steps {
		if step == nil {
			return nil, false, fmt.Errorf("concurrent steps contains a nil step")
		}
		if _, _, err := step.validateFlow(current, known); err != nil {
			return nil, false, err
		}
	}
	return reflect.TypeOf([]any{}), true, nil
}

type ConcurrentStepsOption func(*ConcurrentSteps)

func WithFailurePolicy(policy FailurePolicy) ConcurrentStepsOption {
	return func(c *ConcurrentSteps) {
		c.failurePolicy = policy
	}
}

func NewConcurrentSteps(steps []*Step, options ...ConcurrentStepsOption) *ConcurrentSteps {
	concurrent := &ConcurrentSteps{
		steps:         steps,
		failurePolicy: WaitAll,
	}

	for _, option := range options {
		option(concurrent)
	}

	return concurrent
}

func (c *ConcurrentSteps) Run(
	goCtx context.Context,
	ctx *Context,
	input any,
) (any, error) {
	return c.run(goCtx, ctx, input, nil, -1, -1, nil, "")
}

func (c *ConcurrentSteps) run(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport, firstStepReport int, lifecycle *lifecycleDispatcher, stageName string) (any, error) {
	if c.failurePolicy == FailFast {
		return c.runFailFast(goCtx, ctx, input, recorder, stageReport, firstStepReport, lifecycle, stageName)
	}
	return c.runWaitAll(goCtx, ctx, input, recorder, stageReport, firstStepReport, lifecycle, stageName)
}

func (c *ConcurrentSteps) runWaitAll(
	goCtx context.Context,
	ctx *Context,
	input any,
	recorder *runRecorder,
	stageReport, firstStepReport int, lifecycle *lifecycleDispatcher, stageName string,
) (any, error) {
	results := make([]any, len(c.steps))
	errs := make([]error, len(c.steps))

	var wg sync.WaitGroup

	if c.maxWorkers <= 0 {
		wg.Add(len(c.steps))

		for i, step := range c.steps {
			go func(index int, currentStep *Step) {
				defer wg.Done()

				result, err := currentStep.run(goCtx, ctx, input, recorder, topLevelStepPath(stageReport, firstStepReport+index), lifecycle, stageName, "", "")
				results[index] = result
				errs[index] = err
			}(i, step)
		}
	} else {
		semaphore := make(chan struct{}, c.maxWorkers)

		for i, step := range c.steps {
			select {
			case semaphore <- struct{}{}:
			case <-goCtx.Done():
				errs[i] = goCtx.Err()
				continue
			}
			if err := goCtx.Err(); err != nil {
				<-semaphore
				errs[i] = err
				continue
			}

			wg.Add(1)

			go func(index int, currentStep *Step) {
				defer wg.Done()
				defer func() {
					<-semaphore
				}()

				result, err := currentStep.run(goCtx, ctx, input, recorder, topLevelStepPath(stageReport, firstStepReport+index), lifecycle, stageName, "", "")
				results[index] = result
				errs[index] = err
			}(i, step)
		}
	}

	wg.Wait()

	var joinedErrors []error

	for _, err := range errs {
		if err != nil {
			joinedErrors = append(joinedErrors, err)
		}
	}

	if len(joinedErrors) > 0 {
		return nil, errors.Join(joinedErrors...)
	}

	return results, nil
}

func (c *ConcurrentSteps) runFailFast(
	goCtx context.Context,
	ctx *Context,
	input any,
	recorder *runRecorder,
	stageReport, firstStepReport int, lifecycle *lifecycleDispatcher, stageName string,
) (any, error) {
	groupCtx, cancel := context.WithCancel(goCtx)
	defer cancel()

	results := make([]any, len(c.steps))
	var firstErr error
	var firstErrOnce sync.Once
	var firstNonCancellationErr error
	var firstNonCancellationErrOnce sync.Once

	var wg sync.WaitGroup
	recordFailure := func(err error) {
		if err == nil {
			return
		}
		firstErrOnce.Do(func() { firstErr = err })
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			firstNonCancellationErrOnce.Do(func() { firstNonCancellationErr = err })
		}
		cancel()
	}

	if c.maxWorkers <= 0 {
		wg.Add(len(c.steps))

		for i, step := range c.steps {
			go func(index int, currentStep *Step) {
				defer wg.Done()

				result, err := currentStep.run(groupCtx, ctx, input, recorder, topLevelStepPath(stageReport, firstStepReport+index), lifecycle, stageName, "", "")
				results[index] = result
				recordFailure(err)
			}(i, step)
		}
	} else {
		semaphore := make(chan struct{}, c.maxWorkers)

	launchLoop:
		for i, step := range c.steps {
			select {
			case semaphore <- struct{}{}:
			case <-groupCtx.Done():
				break launchLoop
			}
			if groupCtx.Err() != nil {
				<-semaphore
				break launchLoop
			}

			wg.Add(1)

			go func(index int, currentStep *Step) {
				defer wg.Done()
				defer func() {
					<-semaphore
				}()

				result, err := currentStep.run(groupCtx, ctx, input, recorder, topLevelStepPath(stageReport, firstStepReport+index), lifecycle, stageName, "", "")
				results[index] = result
				recordFailure(err)
			}(i, step)
		}
	}

	wg.Wait()

	if firstNonCancellationErr != nil {
		return nil, firstNonCancellationErr
	}

	if goCtx.Err() != nil {
		return nil, goCtx.Err()
	}
	if firstErr != nil {
		return nil, firstErr
	}

	return results, nil
}
