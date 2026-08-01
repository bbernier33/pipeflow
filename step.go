package pipeflow

import (
	"fmt"
	"time"
)

type Step struct {
	name        string
	action      func(*Context, any) (any, error)
	retryPolicy *RetryPolicy
}

func NewStep(
	name string,
	action func(*Context, any) (any, error),
	options ...StepOption,
) *Step {
	step := &Step{
		name:   name,
		action: action,
	}

	for _, option := range options {
		option(step)
	}

	return step
}

func (s *Step) Run(ctx *Context, input any) (any, error) {
	ctx.Logger().Info("Running step: " + s.name)

	maxAttempts := 1
	delay := time.Duration(0)

	if s.retryPolicy != nil {
		maxAttempts = s.retryPolicy.MaxAttempts
		delay = s.retryPolicy.Delay
	}

	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		output, err := s.action(ctx, input)

		if err == nil {
			ctx.Logger().Info("Completed step: " + s.name)
			return output, nil
		}

		lastErr = err

		if attempt < maxAttempts {
			ctx.Logger().Info(
				fmt.Sprintf(
					"Step %s failed on attempt %d/%d, retrying in %s",
					s.name,
					attempt,
					maxAttempts,
					delay,
				),
			)

			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}

	ctx.Logger().Error(
		fmt.Sprintf(
			"Step %s failed after %d attempt(s)",
			s.name,
			maxAttempts,
		),
	)

	return nil, lastErr
}
