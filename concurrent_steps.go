package pipeflow

import (
	"context"
	"errors"
	"sync"
)

type ConcurrentSteps struct {
	steps         []*Step
	failurePolicy FailurePolicy
	maxWorkers    int
}

func WithMaxWorkers(max int) ConcurrentStepsOption {
	return func(c *ConcurrentSteps) {
		c.maxWorkers = max
	}
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
	if c.failurePolicy == FailFast {
		return c.runFailFast(goCtx, ctx, input)
	}
	return c.runWaitAll(goCtx, ctx, input)
}

func (c *ConcurrentSteps) runWaitAll(
	goCtx context.Context,
	ctx *Context,
	input any,
) (any, error) {
	results := make([]any, len(c.steps))
	errs := make([]error, len(c.steps))

	var wg sync.WaitGroup

	if c.maxWorkers <= 0 {
		wg.Add(len(c.steps))

		for i, step := range c.steps {
			go func(index int, currentStep *Step) {
				defer wg.Done()

				result, err := currentStep.Run(goCtx, ctx, input)
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

			wg.Add(1)

			go func(index int, currentStep *Step) {
				defer wg.Done()
				defer func() {
					<-semaphore
				}()

				result, err := currentStep.Run(goCtx, ctx, input)
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
) (any, error) {
	groupCtx, cancel := context.WithCancel(goCtx)
	defer cancel()

	results := make([]any, len(c.steps))
	errs := make([]error, len(c.steps))

	var wg sync.WaitGroup

	if c.maxWorkers <= 0 {
		wg.Add(len(c.steps))

		for i, step := range c.steps {
			go func(index int, currentStep *Step) {
				defer wg.Done()

				result, err := currentStep.Run(groupCtx, ctx, input)
				results[index] = result
				errs[index] = err

				if err != nil {
					cancel()
				}
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

			wg.Add(1)

			go func(index int, currentStep *Step) {
				defer wg.Done()
				defer func() {
					<-semaphore
				}()

				result, err := currentStep.Run(groupCtx, ctx, input)
				results[index] = result
				errs[index] = err

				if err != nil {
					cancel()
				}
			}(i, step)
		}
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
	}

	if goCtx.Err() != nil {
		return nil, goCtx.Err()
	}

	return results, nil
}
