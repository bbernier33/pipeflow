package pipeflow

import (
	"errors"
	"sync"
)

type ConcurrentSteps struct {
	steps []*Step
}

func NewConcurrentSteps(steps ...*Step) *ConcurrentSteps {
	return &ConcurrentSteps{
		steps: steps,
	}
}

func (c *ConcurrentSteps) Run(ctx *Context, input any) (any, error) {
	results := make([]any, len(c.steps))
	errs := make([]error, len(c.steps))

	var wg sync.WaitGroup
	wg.Add(len(c.steps))

	for i, step := range c.steps {
		go func(index int, currentStep *Step) {
			defer wg.Done()

			result, err := currentStep.Run(ctx, input)
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
