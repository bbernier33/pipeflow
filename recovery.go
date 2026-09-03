package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// RecoveryDecision is the explicit control outcome of an Operational Recovery.
type RecoveryDecision string

const (
	RecoveryRetryStep    RecoveryDecision = "retry_step"
	RecoveryFailStep     RecoveryDecision = "fail_step"
	RecoveryFailStage    RecoveryDecision = "fail_stage"
	RecoveryFailPipeline RecoveryDecision = "fail_pipeline"
)

func (d RecoveryDecision) valid() bool {
	switch d {
	case RecoveryRetryStep, RecoveryFailStep, RecoveryFailStage, RecoveryFailPipeline:
		return true
	default:
		return false
	}
}

// Failure is metadata-only context supplied to a Recovery Stage. It never
// contains the Step input or output.
type Failure struct {
	Err      error
	RunID    string
	Pipeline string
	Stage    string
	Step     string
	Attempt  int
}

// RecoveryPolicy bounds Operational Recovery independently of normal retries.
type RecoveryPolicy struct {
	MaxAttempts int
	Timeout     time.Duration
}

// RecoveryStage is a dormant, Step-owned Stage that produces control decisions.
type RecoveryStage struct {
	name  string
	stage Stage
}

// NewRecoveryStage builds a Recovery Stage from normal Pipeflow StageItems.
// Its flowing input is Failure and its final output must be RecoveryDecision.
func NewRecoveryStage(name string, items ...StageItem) *RecoveryStage {
	return &RecoveryStage{name: name, stage: NewStage(name, items...)}
}

// RecoveryError preserves the original Step failure and records the final
// recovery decision. RecoveryErr is non-nil when the Recovery Stage itself failed.
type RecoveryError struct {
	Pipeline    string
	Stage       string
	Step        string
	Recovery    string
	Attempt     int
	Decision    RecoveryDecision
	Original    error
	RecoveryErr error
}

func (e *RecoveryError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("pipeflow: recovery %q for stage %q step %q decided %s", e.Recovery, e.Stage, e.Step, e.Decision)
	if e.RecoveryErr != nil {
		return message + ": " + e.RecoveryErr.Error()
	}
	if e.Original != nil {
		return message + ": " + e.Original.Error()
	}
	return message
}

func (e *RecoveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return errors.Join(e.Original, e.RecoveryErr)
}

// WithRecovery attaches bounded Operational Recovery to the Step.
func (s *Step) WithRecovery(recovery *RecoveryStage, policy RecoveryPolicy) *Step {
	if s == nil {
		return nil
	}
	if recovery == nil {
		if s.configErr == nil {
			s.configErr = fmt.Errorf("pipeflow: step %q has nil recovery stage", s.name)
		}
		return s
	}
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Timeout < 0 {
		if s.configErr == nil {
			s.configErr = fmt.Errorf("pipeflow: step %q recovery timeout cannot be negative", s.name)
		}
		return s
	}
	s.recovery = recovery
	s.recoveryPolicy = policy
	return s
}

func (r *RecoveryStage) validate() error {
	if r == nil {
		return fmt.Errorf("pipeflow: nil recovery stage")
	}
	if r.name == "" {
		return fmt.Errorf("pipeflow: recovery stage name cannot be empty")
	}
	if err := r.stage.Validate(); err != nil {
		return fmt.Errorf("pipeflow: recovery %q: %w", r.name, err)
	}
	out, known, err := r.stage.validateFlow(reflect.TypeOf(Failure{}), true)
	if err != nil {
		return fmt.Errorf("pipeflow: recovery %q: %w", r.name, err)
	}
	decisionType := reflect.TypeOf(RecoveryDecision(""))
	if !known || out != decisionType {
		return fmt.Errorf("pipeflow: recovery %q must end with %s, got %s", r.name, decisionType, typeName(out))
	}
	if step := nestedRecoveryStep(r.stage.items); step != nil {
		return fmt.Errorf("pipeflow: recovery %q step %q cannot have recovery", r.name, step.name)
	}
	return nil
}

func nestedRecoveryStep(items []StageItem) *Step {
	for _, item := range items {
		switch typed := item.(type) {
		case *Step:
			if typed != nil && typed.recovery != nil {
				return typed
			}
		case *ConcurrentSteps:
			for _, step := range typed.steps {
				if step != nil && step.recovery != nil {
					return step
				}
			}
		case *Parallel:
			for _, branch := range typed.branches {
				for _, step := range branch.steps {
					if step != nil && step.recovery != nil {
						return step
					}
				}
			}
		case *Subflow:
			if typed != nil {
				for _, stage := range typed.stages {
					if step := nestedRecoveryStep(stage.items); step != nil {
						return step
					}
				}
			}
		}
	}
	return nil
}

func recoveryAttempt(err error) int {
	var executionErr *ExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.Attempt
	}
	return 0
}

func (s *Step) runRecovery(goCtx context.Context, ctx *Context, failure Failure, recorder *runRecorder, path stepReportPath, lifecycle *lifecycleDispatcher, recoveryAttempt int) (decision RecoveryDecision, err error) {
	recoveryCtx := goCtx
	if s.recoveryPolicy.Timeout > 0 {
		var cancel context.CancelFunc
		recoveryCtx, cancel = context.WithTimeout(goCtx, s.recoveryPolicy.Timeout)
		defer cancel()
	}
	var nested *runRecorder
	if recorder != nil {
		runID, pipeline := recorder.runIdentity()
		nested = newNestedRecorder(runID, pipeline, []Stage{s.recovery.stage})
		nested.startRun()
	}
	started := time.Now()
	var output any
	var runErr error
	if lifecycle != nil {
		runErr = lifecycle.emitRecovery(RecoveryStarted, failure.Stage, failure.Step, s.recovery.name, recoveryAttempt, "", StatusRunning, nil)
	}
	if runErr == nil {
		output, runErr = s.recovery.stage.run(recoveryCtx, ctx, failure, nested, 0, lifecycle.scopedRecovery(s.recovery.name, recoveryAttempt))
	}
	if contextErr := recoveryCtx.Err(); contextErr != nil {
		runErr = contextErr
	}
	if runErr == nil {
		var ok bool
		decision, ok = output.(RecoveryDecision)
		if !ok || !decision.valid() {
			runErr = fmt.Errorf("pipeflow: recovery %q returned invalid decision %v", s.recovery.name, output)
		}
	}
	if lifecycle != nil {
		event := RecoveryCompleted
		if runErr != nil {
			event = RecoveryFailed
		}
		if hookErr := lifecycle.emitRecovery(event, failure.Stage, failure.Step, s.recovery.name, recoveryAttempt, decision, statusForError(runErr), runErr); hookErr != nil {
			runErr = errors.Join(runErr, hookErr)
		}
	}
	if nested != nil {
		nested.finishRun(runErr)
		recorder.appendRecovery(path, RecoveryReport{Name: s.recovery.name, Attempt: recoveryAttempt, Decision: decision, OriginalError: failure.Err, StartedAt: started, EndedAt: time.Now(), Status: statusForError(runErr), Error: runErr, Stages: nested.snapshot().Stages})
	}
	return decision, runErr
}
