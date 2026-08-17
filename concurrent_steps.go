package pipeflow

import (
	"context"
	"errors"
	"sync"
)

type ConcurrentSteps struct {
	steps         []*Step
	failurePolicy FailurePolicy
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

func (c *ConcurrentSteps) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	results := make([]any, len(c.steps))
	errs := make([]error, len(c.steps))

	var wg sync.WaitGroup
	wg.Add(len(c.steps))

	for i, step := range c.steps {
		go func(index int, currentStep *Step) {
			defer wg.Done()

			result, err := currentStep.Run(goCtx, ctx, input)
			results[index] = result
			errs[index] = err
		}(i, step)
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
