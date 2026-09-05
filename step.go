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
	name           string
	role           StepRole
	roleSet        bool
	action         func(context.Context, *Context, any) (any, error)
	retryPolicy    *RetryPolicy
	inputType      reflect.Type
	outputType     reflect.Type
	flow           stepFlow
	configErr      error
	timeout        time.Duration
	timeoutSet     bool
	retrySet       bool
	pollingSet     bool
	rateLimitSet   bool
	pollPolicy     *compiledPollPolicy
	condition      *compiledCondition
	rateLimit      *RateLimitPolicy
	metadata       *compiledMetadataExtractor
	recovery       *RecoveryStage
	recoveryPolicy RecoveryPolicy
	circuit        *CircuitBreaker
	idempotency    *IdempotencyGuard
}

// NewStep creates a Step from a supported ordinary Go function and options.
func NewStep(
	name string,
	action any,
	options ...StepOption,
) *Step {
	return newStep(name, action, StepRoleNormal, false, options...)
}

func newStep(name string, action any, role StepRole, roleSet bool, options ...StepOption) *Step {
	step := &Step{
		name: name, role: role, roleSet: roleSet,
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
	if recorder != nil {
		recorder.startStep(reportPath)
		defer func() {
			if skipped {
				return
			}
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emitStep(StepFailed, stageName, parallelName, branchName, s.name, s.Role(), statusForError(err), err)
			} else {
				hookErr = lifecycle.emitStep(StepCompleted, stageName, parallelName, branchName, s.name, s.Role(), StatusCompleted, nil)
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
				if hookErr := lifecycle.emitStep(StepSkipped, stageName, parallelName, branchName, s.name, s.Role(), StatusSkipped, nil); hookErr != nil {
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
		if hookErr := lifecycle.emitStep(StepStarted, stageName, parallelName, branchName, s.name, s.Role(), StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionError(hookErr, "", "", s.name, 0)
		}
	}
	if err := acceptsValue(s.inputType, input); err != nil {
		return nil, annotateExecutionError(fmt.Errorf("input: %w", err), "", "", s.name, 0)
	}
	ctx.Logger().Info("Running step: " + s.name)
	if s.idempotency != nil {
		key, keyErr := s.idempotency.key.extract(input)
		if keyErr != nil {
			return nil, annotateExecutionError(keyErr, "", "", s.name, 0)
		}
		claim, claimErr := s.idempotency.store.Claim(goCtx, s.idempotency.storageKey(key))
		if claimErr != nil {
			lifecycle.emitOperational(ObservationIdempotency, stageName, parallelName, branchName, s.name, s.Role(), ObservationFailed, StatusFailed, claimErr, func(location *ObservationLocation) {
				location.Guard, location.IdempotencyOutcome = s.idempotency.name, IdempotencyStoreFailure
			})
			return nil, annotateExecutionError(claimErr, "", "", s.name, 0)
		}
		switch claim {
		case IdempotencyCompleted:
			lifecycle.emitOperational(ObservationIdempotency, stageName, parallelName, branchName, s.name, s.Role(), ObservationSkipped, StatusSkipped, nil, func(location *ObservationLocation) {
				location.Guard, location.IdempotencyOutcome = s.idempotency.name, IdempotencyDuplicateCompleted
			})
			if recorder != nil {
				recorder.setIdempotency(reportPath, IdempotencyReport{Guard: s.idempotency.name, Outcome: IdempotencyDuplicateCompleted})
				recorder.skipStep(reportPath)
			}
			if recorder != nil {
				if hookErr := lifecycle.emitStep(StepSkipped, stageName, parallelName, branchName, s.name, s.Role(), StatusSkipped, nil); hookErr != nil {
					return nil, annotateExecutionError(hookErr, "", "", s.name, 0)
				}
			}
			skipped = true
			return input, nil
		case IdempotencyInProgress:
			duplicateErr := &IdempotencyInProgressError{Guard: s.idempotency.name}
			lifecycle.emitOperational(ObservationIdempotency, stageName, parallelName, branchName, s.name, s.Role(), ObservationFailed, StatusFailed, duplicateErr, func(location *ObservationLocation) {
				location.Guard, location.IdempotencyOutcome = s.idempotency.name, IdempotencyDuplicateInProgress
			})
			if recorder != nil {
				recorder.setIdempotency(reportPath, IdempotencyReport{Guard: s.idempotency.name, Outcome: IdempotencyDuplicateInProgress})
			}
			return nil, annotateExecutionError(duplicateErr, "", "", s.name, 0)
		case IdempotencyClaimed:
			if recorder != nil {
				recorder.setIdempotency(reportPath, IdempotencyReport{Guard: s.idempotency.name, Claim: IdempotencyClaimed})
			}
		default:
			return nil, annotateExecutionError(fmt.Errorf("pipeflow: idempotency guard %q store returned invalid claim state %q", s.idempotency.name, claim), "", "", s.name, 0)
		}
		defer func() {
			outcome := IdempotencyExecuted
			var storeErr error
			if err == nil {
				storeErr = s.idempotency.finalizeStore(goCtx, "complete", func(finalizeCtx context.Context) error {
					return s.idempotency.store.Complete(finalizeCtx, s.idempotency.storageKey(key))
				})
			} else {
				outcome = IdempotencyFailedReleasable
				storeErr = s.idempotency.finalizeStore(goCtx, "release", func(finalizeCtx context.Context) error {
					return s.idempotency.store.Release(finalizeCtx, s.idempotency.storageKey(key))
				})
			}
			if storeErr != nil {
				outcome = IdempotencyStoreFailure
			}
			if recorder != nil {
				recorder.setIdempotency(reportPath, IdempotencyReport{Guard: s.idempotency.name, Claim: IdempotencyClaimed, Outcome: outcome, Error: storeErr})
			}
			phase, status := ObservationCompleted, StatusCompleted
			if outcome == IdempotencyFailedReleasable || outcome == IdempotencyStoreFailure {
				phase, status = ObservationFailed, StatusFailed
			}
			lifecycle.emitOperational(ObservationIdempotency, stageName, parallelName, branchName, s.name, s.Role(), phase, status, storeErr, func(location *ObservationLocation) {
				location.Guard, location.IdempotencyOutcome = s.idempotency.name, outcome
			})
			if storeErr != nil {
				output = nil
				err = errors.Join(err, annotateExecutionError(storeErr, "", "", s.name, 0))
			}
		}()
	}
	var permit circuitPermit
	if s.circuit != nil {
		var circuitErr error
		permit, circuitErr = s.circuit.acquire(time.Now())
		if circuitErr != nil {
			state := s.circuit.Snapshot().State
			lifecycle.emitOperational(ObservationCircuit, stageName, parallelName, branchName, s.name, s.Role(), ObservationSkipped, StatusFailed, circuitErr, func(location *ObservationLocation) {
				location.Dependency, location.CircuitState, location.ShortCircuited = s.circuit.name, state, true
			})
			if recorder != nil {
				recorder.setCircuit(reportPath, CircuitReport{Dependency: s.circuit.name, StateBefore: s.circuit.Snapshot().State, StateAfter: s.circuit.Snapshot().State, ShortCircuited: true})
			}
			return nil, annotateExecutionError(circuitErr, "", "", s.name, 0)
		}
		defer func() {
			after, predicateErr := permit.finish(err)
			phase, status := ObservationCompleted, StatusCompleted
			if err != nil || predicateErr != nil {
				phase, status = ObservationFailed, StatusFailed
			}
			lifecycle.emitOperational(ObservationCircuit, stageName, parallelName, branchName, s.name, s.Role(), phase, status, errors.Join(err, predicateErr), func(location *ObservationLocation) {
				location.Dependency, location.CircuitState, location.Probe = s.circuit.name, after.State, permit.probe
			})
			if recorder != nil {
				recorder.setCircuit(reportPath, CircuitReport{Dependency: s.circuit.name, StateBefore: permit.before.State, StateAfter: after.State, Probe: permit.probe})
			}
			if predicateErr != nil {
				output = nil
				err = errors.Join(err, annotateExecutionError(predicateErr, "", "", s.name, 0))
			}
		}()
	}
	for recoveryAttempt := 0; ; recoveryAttempt++ {
		normalCtx := goCtx
		cancel := func() {}
		if s.timeout > 0 {
			normalCtx, cancel = context.WithTimeout(goCtx, s.timeout)
		}
		output, err = s.runNormal(normalCtx, ctx, input, recorder, reportPath, lifecycle, stageName, parallelName, branchName)
		if completionErr := contextCompletionErr(normalCtx, time.Now()); completionErr != nil {
			output = nil
			err = annotateExecutionError(completionErr, "", "", s.name, recoveryAttemptNumber(err))
		}
		cancel()
		if err == nil || s.recovery == nil {
			return output, err
		}
		if parentErr := goCtx.Err(); parentErr != nil {
			return nil, annotateExecutionError(parentErr, "", "", s.name, recoveryAttemptNumber(err))
		}
		if recoveryAttempt >= s.recoveryPolicy.MaxAttempts {
			return nil, &RecoveryError{Stage: stageName, Step: s.name, Recovery: s.recovery.name, Attempt: recoveryAttempt, Decision: RecoveryFailStep, Original: err}
		}
		runID, pipeline := "", ""
		if recorder != nil {
			runID, pipeline = recorder.runIdentity()
		}
		failure := Failure{Err: err, RunID: runID, Pipeline: pipeline, Stage: stageName, Step: s.name, Attempt: recoveryAttemptNumber(err)}
		decision, recoveryErr := s.runRecovery(goCtx, ctx, failure, recorder, reportPath, lifecycle, recoveryAttempt+1)
		if recoveryErr != nil || decision != RecoveryRetryStep {
			return nil, &RecoveryError{Pipeline: pipeline, Stage: stageName, Step: s.name, Recovery: s.recovery.name, Attempt: recoveryAttempt + 1, Decision: decision, Original: err, RecoveryErr: recoveryErr}
		}
	}
}

func recoveryAttemptNumber(err error) int { return recoveryAttempt(err) }

func (s *Step) runNormal(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, reportPath stepReportPath, lifecycle *lifecycleDispatcher, stageName, parallelName, branchName string) (output any, err error) {
	if s.pollPolicy == nil {
		output, err = s.runAttempts(goCtx, ctx, input, recorder, reportPath, 0, lifecycle, stageName, parallelName, branchName)
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
		output, err = s.runAttempts(pollCtx, ctx, input, recorder, reportPath, poll, lifecycle, stageName, parallelName, branchName)
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
	if s.recovery != nil {
		if err := s.recovery.validate(); err != nil {
			return nil, false, err
		}
	}
	if s.idempotency != nil {
		if s.flow != flowPreserve {
			return nil, false, fmt.Errorf("step %q idempotency guard requires a pass-through function", s.name)
		}
		if s.idempotency.key.inputType != nil && known && (current == nil || !current.AssignableTo(s.idempotency.key.inputType)) && !(current == nil && isNilable(s.idempotency.key.inputType)) {
			return nil, false, fmt.Errorf("step %q idempotency key expects %s but previous output is %s", s.name, s.idempotency.key.inputType, typeName(current))
		}
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

func (s *Step) runAttempts(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, reportPath stepReportPath, poll int, lifecycle *lifecycleDispatcher, stageName, parallelName, branchName string) (any, error) {

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
			lifecycle.emitAttempt(stageName, parallelName, branchName, s.name, s.Role(), poll, attempt, ObservationStarted, StatusRunning, nil)
		}
		select {
		case <-goCtx.Done():
			err := annotateExecutionError(goCtx.Err(), "", "", s.name, attempt)
			if recorder != nil {
				recorder.finishAttempt(reportPath, poll, attempt, err)
				lifecycle.emitAttempt(stageName, parallelName, branchName, s.name, s.Role(), poll, attempt, ObservationFailed, statusForError(err), err)
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
		if contextErr := contextCompletionErr(goCtx, time.Now()); contextErr != nil {
			output = nil
			err = contextErr
		}
		if recorder != nil {
			recorder.finishAttempt(reportPath, poll, attempt, err)
			phase := ObservationCompleted
			if err != nil {
				phase = ObservationFailed
			}
			lifecycle.emitAttempt(stageName, parallelName, branchName, s.name, s.Role(), poll, attempt, phase, statusForError(err), err)
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

func contextCompletionErr(ctx context.Context, completedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !completedAt.Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
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
