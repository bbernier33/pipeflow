package pipeflow

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

// ErrPollLimitExceeded is returned when polling does not satisfy its predicate
// within MaxPolls.
var ErrPollLimitExceeded = errors.New("pipeflow: poll limit exceeded")

// PollPolicy controls successful-operation polling for a Step.
type PollPolicy struct {
	Every    time.Duration
	MaxPolls int
	Timeout  time.Duration
	Until    any
}

type compiledPollPolicy struct {
	every         time.Duration
	maxPolls      int
	timeout       time.Duration
	predicate     func(any) (bool, error)
	predicateType reflect.Type
}

func compilePollPolicy(step *Step, policy PollPolicy) (*compiledPollPolicy, error) {
	if policy.Every < 0 {
		policy.Every = 0
	}
	if policy.MaxPolls < 0 {
		policy.MaxPolls = 0
	}
	if policy.Timeout < 0 {
		policy.Timeout = 0
	}

	t := reflect.TypeOf(policy.Until)
	if t == nil || t.Kind() != reflect.Func || t.NumIn() > 1 || t.NumOut() != 1 || t.Out(0).Kind() != reflect.Bool {
		return nil, fmt.Errorf("pipeflow: step %q polling predicate must be func() bool or func(T) bool", step.name)
	}

	var predicateType reflect.Type
	if t.NumIn() == 1 {
		predicateType = t.In(0)
		if step.outputType != nil && !step.outputType.AssignableTo(predicateType) {
			return nil, fmt.Errorf("pipeflow: step %q polling predicate expects %s but step output is %s", step.name, predicateType, step.outputType)
		}
	}

	v := reflect.ValueOf(policy.Until)
	predicate := func(value any) (result bool, err error) {
		defer recoverPanic(&err)
		args := make([]reflect.Value, 0, 1)
		if predicateType != nil {
			if inputErr := acceptsValue(predicateType, value); inputErr != nil {
				return false, fmt.Errorf("polling predicate input: %w", inputErr)
			}
			args = append(args, reflectValue(value, predicateType))
		}
		result = v.Call(args)[0].Bool()
		return result, err
	}

	return &compiledPollPolicy{
		every:         policy.Every,
		maxPolls:      policy.MaxPolls,
		timeout:       policy.Timeout,
		predicate:     predicate,
		predicateType: predicateType,
	}, nil
}
