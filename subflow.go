package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// Subflow is a reusable sequential group of Stages embedded in a parent run.
// It shares the parent's Context, cancellation, value flow, and Run ID.
type Subflow struct {
	name   string
	stages []Stage
}

// NewSubflow creates a reusable sequential Stage group.
func NewSubflow(name string, stages ...Stage) *Subflow {
	return &Subflow{name: name, stages: append([]Stage(nil), stages...)}
}

// Run executes the Subflow directly without creating an independent Pipeline run.
func (s *Subflow) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	if err := s.ValidateInput(input); err != nil {
		return nil, err
	}
	return s.run(goCtx, ctx, input, nil, -1, -1, nil)
}

// Validate checks Subflow structure and statically knowable value flow.
func (s *Subflow) Validate() error {
	if s == nil {
		return fmt.Errorf("pipeflow: nil subflow")
	}
	if s.name == "" {
		return fmt.Errorf("pipeflow: subflow name cannot be empty")
	}
	if err := validateStageDefinitions("subflow "+fmt.Sprintf("%q", s.name), s.stages); err != nil {
		return err
	}
	if err := validateSharedRateLimits(s.stages); err != nil {
		return err
	}
	_, _, err := s.validateFlow(nil, false)
	return err
}

// ValidateInput validates Subflow value flow using an actual initial input.
func (s *Subflow) ValidateInput(input any) error {
	if err := s.Validate(); err != nil {
		return err
	}
	_, _, err := s.validateFlow(reflect.TypeOf(input), true)
	return err
}

func (s *Subflow) run(goCtx context.Context, ctx *Context, input any, parent *runRecorder, parentStage, reportIndex int, lifecycle *lifecycleDispatcher) (output any, err error) {
	if _, _, err := s.validateFlow(nil, false); err != nil {
		return nil, err
	}
	var recorder *runRecorder
	if parent != nil {
		runID, pipeline := parent.runIdentity()
		recorder = newNestedRecorder(runID, pipeline, s.stages)
		parent.startSubflow(parentStage, reportIndex, recorder)
		recorder.startRun()
		scopedLifecycle := lifecycle.scopedSubflow(s.name)
		lifecycle = scopedLifecycle
		defer func() {
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emitSubflow(SubflowFailed, s.name, statusForError(err), err)
			} else {
				hookErr = lifecycle.emitSubflow(SubflowCompleted, s.name, StatusCompleted, nil)
			}
			if hookErr != nil {
				err = errors.Join(err, annotateExecutionSubflow(hookErr, s.name))
			}
			recorder.finishRun(err)
			parent.finishSubflow(parentStage, reportIndex, err)
		}()
		if hookErr := lifecycle.emitSubflow(SubflowStarted, s.name, StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionSubflow(hookErr, s.name)
		}
	}
	defer recoverPanic(&err)

	current := input
	for i := range s.stages {
		if err := goCtx.Err(); err != nil {
			return nil, annotateExecutionSubflow(err, s.name)
		}
		current, err = s.stages[i].run(goCtx, ctx, current, recorder, i, lifecycle)
		if err != nil {
			return nil, annotateExecutionSubflow(err, s.name)
		}
	}
	return current, nil
}

func (s *Subflow) validateFlow(current reflect.Type, known bool) (reflect.Type, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("pipeflow: nil subflow")
	}
	for i := range s.stages {
		var err error
		current, known, err = s.stages[i].validateFlow(current, known)
		if err != nil {
			return nil, false, fmt.Errorf("pipeflow: subflow %q: %w", s.name, err)
		}
	}
	return current, known, nil
}
