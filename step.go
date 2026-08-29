package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

type stepFlow int

const (
	flowUnknown stepFlow = iota
	flowPreserve
	flowReplace
)

// Step adapts one supported ordinary Go function into a value-flow execution
// unit.
type Step struct {
	name        string
	action      func(context.Context, *Context, any) (any, error)
	retryPolicy *RetryPolicy
	inputType   reflect.Type
	outputType  reflect.Type
	flow        stepFlow
	configErr   error
	timeout     time.Duration
	pollPolicy  *compiledPollPolicy
	condition   *compiledCondition
	rateLimit   *RateLimitPolicy
	metadata    *compiledMetadataExtractor
}

// NewStep creates a Step from a supported ordinary Go function and options.
func NewStep(
	name string,
	action any,
	options ...StepOption,
) *Step {
	step := &Step{
		name: name,
	}
	step.action, step.inputType, step.outputType, step.flow, step.configErr = adaptStepAction(name, action)

	for _, option := range options {
		option(step)
	}

	return step
}

// Run executes the Step directly with an explicit Pipeflow Context and input.
func (s *Step) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	return s.run(goCtx, ctx, input, nil, stepReportPath{}, nil, "", "", "")
}

func (s *Step) run(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, reportPath stepReportPath, lifecycle *lifecycleDispatcher, stageName, parallelName, branchName string) (output any, err error) {
	skipped := false
	if s.timeout > 0 {
		var cancel context.CancelFunc
		goCtx, cancel = context.WithTimeout(goCtx, s.timeout)
		defer cancel()
	}
	if recorder != nil {
		recorder.startStep(reportPath)
		defer func() {
			if skipped {
				return
			}
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emitLocated(StepFailed, stageName, parallelName, branchName, s.name, statusForError(err), err)
			} else {
				hookErr = lifecycle.emitLocated(StepCompleted, stageName, parallelName, branchName, s.name, StatusCompleted, nil)
			}
			if hookErr != nil {
				hookErr = annotateExecutionError(hookErr, "", "", s.name, 0)
				err = errors.Join(err, hookErr)
			}
			recorder.finishStep(reportPath, err)
		}()
	}
	defer recoverPanic(&err)
	if s.configErr != nil {
		return nil, s.configErr
	}
	if err := goCtx.Err(); err != nil {
		return nil, annotateExecutionError(err, "", "", s.name, 0)
	}
	if s.condition != nil {
		shouldRun, conditionErr := s.condition.predicate(input)
		if conditionErr != nil {
			return nil, annotateExecutionError(conditionErr, "", "", s.name, 0)
		}
		if err := goCtx.Err(); err != nil {
			return nil, annotateExecutionError(err, "", "", s.name, 0)
		}
		if !shouldRun {
			if recorder != nil {
				if hookErr := lifecycle.emitLocated(StepSkipped, stageName, parallelName, branchName, s.name, StatusSkipped, nil); hookErr != nil {
					return nil, annotateExecutionError(hookErr, "", "", s.name, 0)
				}
				recorder.skipStep(reportPath)
			}
			skipped = true
			ctx.Logger().Info("Skipped step: " + s.name)
			return input, nil
		}
	}
	if recorder != nil {
		if hookErr := lifecycle.emitLocated(StepStarted, stageName, parallelName, branchName, s.name, StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionError(hookErr, "", "", s.name, 0)
		}
	}
	if err := acceptsValue(s.inputType, input); err != nil {
		return nil, annotateExecutionError(fmt.Errorf("input: %w", err), "", "", s.name, 0)
	}
	ctx.Logger().Info("Running step: " + s.name)
	if s.pollPolicy == nil {
		output, err = s.runAttempts(goCtx, ctx, input, recorder, reportPath, 0)
		if err == nil {
			err = s.recordMetadata(output, recorder, reportPath)
		}
		if err == nil {
			ctx.Logger().Info("Completed step: " + s.name)
		}
		return output, err
	}

	pollCtx := goCtx
	if s.pollPolicy.timeout > 0 {
		var cancel context.CancelFunc
		pollCtx, cancel = context.WithTimeout(goCtx, s.pollPolicy.timeout)
		defer cancel()
	}

	for poll := 1; ; poll++ {
		if recorder != nil {
			recorder.startPoll(reportPath, poll)
		}
		output, err = s.runAttempts(pollCtx, ctx, input, recorder, reportPath, poll)
		if err != nil {
			err = annotateExecutionPoll(err, poll)
			if recorder != nil {
				recorder.finishPoll(reportPath, poll, false, err)
			}
			return nil, err
		}

		complete, predicateErr := s.pollPolicy.predicate(output)
		if contextErr := pollCtx.Err(); contextErr != nil {
			predicateErr = contextErr
			complete = false
		}
		if predicateErr != nil {
			err = annotateExecutionPoll(annotateExecutionError(predicateErr, "", "", s.name, 0), poll)
			if recorder != nil {
				recorder.finishPoll(reportPath, poll, false, err)
			}
			return nil, err
		}
		if recorder != nil {
			recorder.finishPoll(reportPath, poll, complete, nil)
		}
		if complete {
			if err = s.recordMetadata(output, recorder, reportPath); err != nil {
				return nil, err
			}
			ctx.Logger().Info("Completed step: " + s.name)
			return output, nil
		}
		if s.pollPolicy.maxPolls > 0 && poll >= s.pollPolicy.maxPolls {
			err = annotateExecutionPoll(
				annotateExecutionError(fmt.Errorf("%w after %d poll(s)", ErrPollLimitExceeded, poll), "", "", s.name, 0),
				poll,
			)
			ctx.Logger().Error(fmt.Sprintf("Step %s exceeded its poll limit", s.name))
			return nil, err
		}
		if s.pollPolicy.every > 0 {
			select {
			case <-time.After(s.pollPolicy.every):
			case <-pollCtx.Done():
				err = annotateExecutionPoll(annotateExecutionError(pollCtx.Err(), "", "", s.name, 0), poll)
				return nil, err
			}
		} else {
			select {
			case <-pollCtx.Done():
				err = annotateExecutionPoll(annotateExecutionError(pollCtx.Err(), "", "", s.name, 0), poll)
				return nil, err
			default:
			}
		}
	}
}

func (s *Step) validateFlow(current reflect.Type, known bool) (reflect.Type, bool, error) {
	if s.configErr != nil {
		return nil, false, s.configErr
	}
	if s.condition != nil && s.condition.predicateType != nil && known && (current == nil || !current.AssignableTo(s.condition.predicateType)) && !(current == nil && isNilable(s.condition.predicateType)) {
		return nil, false, fmt.Errorf("step %q condition expects %s but previous output is %s", s.name, s.condition.predicateType, typeName(current))
	}
	if s.inputType != nil && known && (current == nil || !current.AssignableTo(s.inputType)) {
		if current == nil && isNilable(s.inputType) {
			// A nil flowing value is valid for nilable inputs.
		} else {
			return nil, false, fmt.Errorf("step %q expects %s but previous output is %s", s.name, s.inputType, typeName(current))
		}
	}
	if s.metadata != nil && s.metadata.inputType != nil && known {
		resultType, resultKnown := current, known
		if s.flow == flowReplace {
			resultType, resultKnown = s.outputType, true
		} else if s.flow == flowUnknown {
			resultType, resultKnown = nil, false
		}
		if resultKnown && (resultType == nil || !resultType.AssignableTo(s.metadata.inputType)) && !(resultType == nil && isNilable(s.metadata.inputType)) {
			return nil, false, fmt.Errorf("step %q result metadata extractor expects %s but result is %s", s.name, s.metadata.inputType, typeName(resultType))
		}
	}
	switch s.flow {
	case flowReplace:
		if s.condition != nil && (!known || current != s.outputType) {
			return nil, false, nil
		}
		return s.outputType, true, nil
	case flowUnknown:
		return nil, false, nil
	default:
		return current, known, nil
	}
}

func typeName(t reflect.Type) string {
	if t == nil {
		return "nil"
	}
	return t.String()
}

func isNilable(t reflect.Type) bool {
	if t == nil {
		return true
	}
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func (s *Step) recordMetadata(output any, recorder *runRecorder, path stepReportPath) error {
	if s.metadata == nil || recorder == nil {
		return nil
	}
	metadata, err := s.metadata.extract(output)
	if err != nil {
		return annotateExecutionError(err, "", "", s.name, 0)
	}
	recorder.setStepMetadata(path, metadata)
	return nil
}

func (s *Step) runAttempts(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, reportPath stepReportPath, poll int) (any, error) {

	policy := RetryPolicy{MaxAttempts: 1}
	if s.retryPolicy != nil {
		policy = *s.retryPolicy
	}
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}

	var lastErr error

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if recorder != nil {
			recorder.startAttempt(reportPath, poll, attempt)
		}
		select {
		case <-goCtx.Done():
			err := annotateExecutionError(goCtx.Err(), "", "", s.name, attempt)
			if recorder != nil {
				recorder.finishAttempt(reportPath, poll, attempt, err)
			}
			return nil, err
		default:
		}

		var output any
		var err error
		var limiter *runLimiter
		var release func()
		if s.rateLimit != nil {
			limiter, err = ctx.rateLimiter(s, *s.rateLimit)
			if err == nil {
				release, err = limiter.acquire(goCtx)
			}
		}
		if err == nil {
			output, err = invokeStepAction(s.action, goCtx, ctx, input)
			if release != nil {
				release()
			}
			if limiter != nil {
				if observeErr := limiter.observe(err); observeErr != nil {
					err = errors.Join(err, observeErr)
				}
			}
		}
		if contextErr := goCtx.Err(); contextErr != nil {
			output = nil
			err = contextErr
		}
		if recorder != nil {
			recorder.finishAttempt(reportPath, poll, attempt, err)
		}

		if err == nil {
			return output, nil
		}

		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, annotateExecutionError(err, "", "", s.name, attempt)
		}

		if attempt < policy.MaxAttempts {
			if policy.RetryIf != nil {
				retry, predicateErr := invokeRetryPredicate(policy.RetryIf, err)
				if predicateErr != nil {
					return nil, annotateExecutionError(predicateErr, "", "", s.name, attempt)
				}
				if !retry {
					ctx.Logger().Error(fmt.Sprintf("Step %s failed with a non-retryable error", s.name))
					return nil, annotateExecutionError(err, "", "", s.name, attempt)
				}
			}
			delay := policy.delayAfter(attempt)
			minimum, retryAfterErr := retryAfter(err)
			if retryAfterErr != nil {
				return nil, annotateExecutionError(retryAfterErr, "", "", s.name, attempt)
			}
			if minimum > delay {
				delay = minimum
			}
			if recorder != nil {
				recorder.setAttemptRetryDelay(reportPath, poll, delay)
			}
			ctx.Logger().Info(
				fmt.Sprintf(
					"Step %s failed on attempt %d/%d, retrying in %s",
					s.name,
					attempt,
					policy.MaxAttempts,
					delay,
				),
			)

			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-goCtx.Done():
					return nil, annotateExecutionError(goCtx.Err(), "", "", s.name, attempt)
				}
			}
		}
	}

	ctx.Logger().Error(
		fmt.Sprintf(
			"Step %s failed after %d attempt(s)",
			s.name,
			policy.MaxAttempts,
		),
	)

	return nil, annotateExecutionError(lastErr, "", "", s.name, policy.MaxAttempts)
}

func invokeRetryPredicate(predicate func(error) bool, failure error) (retry bool, err error) {
	defer recoverPanic(&err)
	return predicate(failure), nil
}

func invokeStepAction(action func(context.Context, *Context, any) (any, error), goCtx context.Context, ctx *Context, input any) (output any, err error) {
	defer recoverPanic(&err)
	return action(goCtx, ctx, input)
}

func adaptStepAction(name string, action any) (func(context.Context, *Context, any) (any, error), reflect.Type, reflect.Type, stepFlow, error) {
	if legacy, ok := action.(func(context.Context, *Context, any) (any, error)); ok {
		return legacy, nil, nil, flowUnknown, nil
	}

	t := reflect.TypeOf(action)
	if t == nil || t.Kind() != reflect.Func {
		return nil, nil, nil, flowUnknown, fmt.Errorf("pipeflow: step %q action must be a function", name)
	}
	if t.NumIn() > 1 || (t.NumOut() != 1 && t.NumOut() != 2) || t.Out(t.NumOut()-1) != errorType {
		return nil, nil, nil, flowUnknown, fmt.Errorf("pipeflow: step %q has unsupported function signature %s", name, t)
	}

	var inputType, outputType reflect.Type
	flow := flowPreserve
	if t.NumIn() == 1 {
		inputType = t.In(0)
	}
	if t.NumOut() == 2 {
		outputType = t.Out(0)
		flow = flowReplace
	} else if inputType != nil {
		outputType = inputType
	}

	v := reflect.ValueOf(action)
	wrapped := func(_ context.Context, _ *Context, input any) (any, error) {
		args := make([]reflect.Value, 0, 1)
		if inputType != nil {
			args = append(args, reflectValue(input, inputType))
		}
		results := v.Call(args)
		if errValue := results[len(results)-1]; !errValue.IsNil() {
			return nil, errValue.Interface().(error)
		}
		if len(results) == 2 {
			return results[0].Interface(), nil
		}
		return input, nil
	}

	return wrapped, inputType, outputType, flow, nil
}

func acceptsValue(expected reflect.Type, value any) error {
	if expected == nil {
		return nil
	}
	if value == nil {
		switch expected.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			return nil
		default:
			return fmt.Errorf("cannot use nil as %s", expected)
		}
	}
	actual := reflect.TypeOf(value)
	if !actual.AssignableTo(expected) {
		return fmt.Errorf("cannot use %s as %s", actual, expected)
	}
	return nil
}

func reflectValue(value any, expected reflect.Type) reflect.Value {
	if value == nil {
		return reflect.Zero(expected)
	}
	return reflect.ValueOf(value)
}
