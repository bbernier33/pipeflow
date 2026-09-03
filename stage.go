package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Stage is a sequential group of StageItems whose final value becomes the
// input to the next Stage.
type Stage struct {
	name       string
	items      []StageItem
	timeout    time.Duration
	timeoutSet bool
	metadata   *compiledMetadataExtractor
	configErr  error
}

// WithResultMetadata returns a Stage that extracts small scalar reporting
// facts from its successful final value.
func (s Stage) WithResultMetadata(extractor any) Stage {
	compiled, err := compileMetadataExtractor("stage", s.name, extractor, nil)
	if err != nil {
		s.configErr = err
	} else {
		s.metadata = compiled
	}
	return s
}

// WithTimeout returns a Stage limited by a total execution timeout.
func (s Stage) WithTimeout(timeout time.Duration) Stage {
	s.timeoutSet = true
	if timeout > 0 {
		s.timeout = timeout
	} else {
		s.timeout = 0
	}
	return s
}

// NewStage creates a sequential Stage from items.
func NewStage(name string, items ...StageItem) Stage {
	return Stage{
		name:  name,
		items: items,
	}
}

// Run executes the Stage directly with an explicit Pipeflow Context and input.
func (s *Stage) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	if err := s.ValidateInput(input); err != nil {
		return nil, err
	}
	return s.run(goCtx, ctx, input, nil, -1, nil)
}

func (s *Stage) run(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, stageReport int, lifecycle *lifecycleDispatcher) (output any, err error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if recorder != nil {
		recorder.startStage(stageReport)
		defer func() {
			var hookErr error
			if err != nil {
				hookErr = lifecycle.emit(StageFailed, s.name, "", statusForError(err), err)
			} else {
				hookErr = lifecycle.emit(StageCompleted, s.name, "", StatusCompleted, nil)
			}
			if hookErr != nil {
				err = errors.Join(err, annotateExecutionError(hookErr, "", s.name, "", 0))
			}
			recorder.finishStage(stageReport, err)
		}()
		if hookErr := lifecycle.emit(StageStarted, s.name, "", StatusRunning, nil); hookErr != nil {
			return nil, annotateExecutionError(hookErr, "", s.name, "", 0)
		}
	}
	defer recoverPanic(&err)
	if s.timeout > 0 {
		var cancel context.CancelFunc
		goCtx, cancel = context.WithTimeout(goCtx, s.timeout)
		defer cancel()
	}
	if err := goCtx.Err(); err != nil {
		return nil, annotateExecutionError(err, "", s.name, "", 0)
	}
	ctx.Logger().Info("starting stage: " + s.name)
	current := input
	stepReport := 0
	parallelReport := 0
	subflowReport := 0

	for _, item := range s.items {
		if err := goCtx.Err(); err != nil {
			ctx.Logger().Error("stage cancelled: " + s.name)
			return nil, annotateExecutionError(err, "", s.name, "", 0)
		}
		var output any
		var err error
		switch typed := item.(type) {
		case *Step:
			output, err = typed.run(goCtx, ctx, current, recorder, topLevelStepPath(stageReport, stepReport), lifecycle, s.name, "", "")
			stepReport++
		case *ConcurrentSteps:
			output, err = typed.run(goCtx, ctx, current, recorder, stageReport, stepReport, lifecycle, s.name)
			stepReport += len(typed.steps)
		case *Parallel:
			output, err = typed.run(goCtx, ctx, current, recorder, stageReport, parallelReport, lifecycle, s.name)
			parallelReport++
		case *Subflow:
			output, err = typed.run(goCtx, ctx, current, recorder, stageReport, subflowReport, lifecycle)
			subflowReport++
		default:
			output, err = item.Run(goCtx, ctx, current)
		}
		if contextErr := goCtx.Err(); contextErr != nil {
			output = nil
			err = contextErr
		}
		if err != nil {
			ctx.Logger().Error("stage failed: " + s.name)
			return nil, annotateExecutionError(err, "", s.name, "", 0)
		}
		current = output
	}

	if s.metadata != nil && recorder != nil {
		metadata, metadataErr := s.metadata.extract(current)
		if metadataErr != nil {
			return nil, annotateExecutionError(metadataErr, "", s.name, "", 0)
		}
		recorder.setStageMetadata(stageReport, metadata)
	}
	ctx.Logger().Info("completed stage " + s.name)
	return current, nil
}

// Validate checks statically knowable value-flow compatibility between steps.
func (s *Stage) Validate() error {
	if err := validateStageDefinitions("standalone stage", []Stage{*s}); err != nil {
		return err
	}
	if err := validateSharedRateLimits([]Stage{*s}); err != nil {
		return err
	}
	_, _, err := s.validateFlow(nil, false)
	return err
}

// ValidateInput validates Stage value flow using an actual initial input.
func (s *Stage) ValidateInput(input any) error {
	if err := validateStageDefinitions("standalone stage", []Stage{*s}); err != nil {
		return err
	}
	if err := validateSharedRateLimits([]Stage{*s}); err != nil {
		return err
	}
	_, _, err := s.validateFlow(reflect.TypeOf(input), true)
	return err
}

func (s *Stage) validateFlow(current reflect.Type, known bool) (reflect.Type, bool, error) {
	if s.configErr != nil {
		return nil, false, s.configErr
	}
	for _, item := range s.items {
		switch typed := item.(type) {
		case *Step:
			step := typed
			var err error
			current, known, err = step.validateFlow(current, known)
			if err != nil {
				return nil, false, fmt.Errorf("pipeflow: stage %q: %w", s.name, err)
			}
		case *ConcurrentSteps:
			var err error
			current, known, err = typed.validateFlow(current, known)
			if err != nil {
				return nil, false, fmt.Errorf("pipeflow: stage %q: %w", s.name, err)
			}
		case *Parallel:
			var err error
			current, known, err = typed.validateFlow(current, known)
			if err != nil {
				return nil, false, fmt.Errorf("pipeflow: stage %q: %w", s.name, err)
			}
		case *Subflow:
			var err error
			current, known, err = typed.validateFlow(current, known)
			if err != nil {
				return nil, false, fmt.Errorf("pipeflow: stage %q: %w", s.name, err)
			}
		default:
			current, known = nil, false
		}
	}
	if s.metadata != nil && s.metadata.inputType != nil && known && (current == nil || !current.AssignableTo(s.metadata.inputType)) && !(current == nil && isNilable(s.metadata.inputType)) {
		return nil, false, fmt.Errorf("pipeflow: stage %q result metadata extractor expects %s but result is %s", s.name, s.metadata.inputType, typeName(current))
	}
	return current, known, nil
}
