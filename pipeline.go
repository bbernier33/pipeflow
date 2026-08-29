package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Pipeline struct {
	name    string
	stages  []Stage
	timeout time.Duration

	onStarted   []PipelineHook
	onCompleted []PipelineHook
	onFailed    []PipelineHook
	lifecycle   []LifecycleHook
	finalizers  []namedFinalizer
	backgrounds []backgroundTask
}

// WithTimeout returns a Pipeline limited by a total execution timeout.
func (p Pipeline) WithTimeout(timeout time.Duration) Pipeline {
	if timeout > 0 {
		p.timeout = timeout
	} else {
		p.timeout = 0
	}
	return p
}

// WithLifecycleHook returns a Pipeline observed by hook. Hooks run
// synchronously in registration order. A hook panic fails its owning boundary.
func (p Pipeline) WithLifecycleHook(hook LifecycleHook) Pipeline {
	if hook != nil {
		p.lifecycle = append(append([]LifecycleHook(nil), p.lifecycle...), hook)
	}
	return p
}

// Finally returns a Pipeline with a named cleanup callback. Finalizers run in
// reverse registration order after execution work stops.
func (p Pipeline) Finally(name string, finalizer Finalizer) Pipeline {
	if finalizer != nil {
		p.finalizers = append(append([]namedFinalizer(nil), p.finalizers...), namedFinalizer{name: name, fn: finalizer})
	}
	return p
}

// WithBackground returns a Pipeline with managed support work. Background work
// is cancelled and joined before finalizers run.
func (p Pipeline) WithBackground(name string, work BackgroundFunc, options ...BackgroundOption) Pipeline {
	task := backgroundTask{name: name, fn: work, failurePolicy: BackgroundFatal}
	for _, option := range options {
		option(&task)
	}
	p.backgrounds = append(append([]backgroundTask(nil), p.backgrounds...), task)
	return p
}

// NewPipeline creates a sequential Pipeline from stages.
func NewPipeline(name string, stages ...Stage) Pipeline {
	return Pipeline{
		name:   name,
		stages: stages,
	}
}

// Run executes the Pipeline. It accepts an optional input, or the compatibility
// form (*Context, input), and returns the final flowing value.
func (p *Pipeline) Run(goCtx context.Context, args ...any) (any, error) {
	output, _, err := p.RunWithReport(goCtx, args...)
	return output, err
}

// RunWithReport executes the pipeline and returns its structured execution report.
func (p *Pipeline) RunWithReport(goCtx context.Context, args ...any) (any, RunReport, error) {
	ctx, input, recorder, err := p.prepareExecution(args)
	if err != nil {
		return nil, RunReport{}, err
	}
	if err := ctx.markStarted(); err != nil {
		return nil, RunReport{}, err
	}
	recorder.startRun()
	executionCtx, cancel := p.timeoutContext(goCtx)
	defer cancel()
	return p.execute(executionCtx, ctx, input, recorder)
}

// Start begins pipeline execution asynchronously and returns run-scoped state.
func (p *Pipeline) Start(goCtx context.Context, args ...any) (*Execution, error) {
	ctx, input, recorder, err := p.prepareExecution(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.markStarted(); err != nil {
		return nil, err
	}
	recorder.startRun()
	executionCtx, cancel := p.timeoutContext(goCtx)
	execution := &Execution{recorder: recorder, done: make(chan struct{})}
	go func() {
		defer cancel()
		output, report, runErr := p.execute(executionCtx, ctx, input, recorder)
		execution.finish(output, report, runErr)
	}()
	return execution, nil
}

func (p *Pipeline) timeoutContext(goCtx context.Context) (context.Context, context.CancelFunc) {
	if p.timeout > 0 {
		return context.WithTimeout(goCtx, p.timeout)
	}
	return context.WithCancel(goCtx)
}

func (p *Pipeline) prepareExecution(args []any) (*Context, any, *runRecorder, error) {
	ctx, input, err := executionArgs(args)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := p.ValidateInput(input); err != nil {
		return nil, nil, nil, err
	}
	recorder, err := newRunRecorder(p.name, p.stages, p.backgrounds)
	if err != nil {
		return nil, nil, nil, err
	}
	return ctx, input, recorder, nil
}

func (p *Pipeline) execute(goCtx context.Context, ctx *Context, input any, recorder *runRecorder) (any, RunReport, error) {
	dispatcher := newLifecycleDispatcher(recorder.snapshot().RunID, p.name, p.lifecycle)
	runErr := dispatcher.emit(PipelineStarted, "", "", StatusRunning, nil)
	if hookErr := p.runStartedHooks(PipelineEvent{
		Name:    p.name,
		Context: ctx,
	}); hookErr != nil {
		runErr = errors.Join(runErr, hookErr)
	}
	if runErr != nil {
		runErr = annotateExecutionError(runErr, p.name, "", "", 0)
	} else {
		if logErr := invokeLog(func() { ctx.Logger().Info("starting pipeline: " + p.name) }); logErr != nil {
			runErr = annotateExecutionError(logErr, p.name, "", "", 0)
		}
	}
	executionCtx, cancelExecution := context.WithCancel(goCtx)
	defer cancelExecution()
	var backgrounds *backgroundManager
	if runErr == nil && goCtx.Err() == nil && len(p.backgrounds) > 0 {
		backgrounds = startBackgrounds(executionCtx, cancelExecution, p.name, p.backgrounds, recorder, dispatcher)
	}
	var output any
	if runErr == nil {
		output, runErr = p.executeStages(executionCtx, ctx, input, recorder, dispatcher)
	}
	if backgrounds != nil {
		runErr = backgrounds.stopAndWait(runErr, goCtx.Err())
	} else {
		cancelExecution()
	}
	preCleanupErr := runErr
	if preCleanupErr != nil {
		preCleanupErr = annotateExecutionError(preCleanupErr, p.name, "", "", 0)
	}
	result := Finalization{
		RunID: recorder.snapshot().RunID, Pipeline: p.name,
		Status: statusForError(preCleanupErr), Err: preCleanupErr,
	}
	cleanupCtx := context.WithoutCancel(goCtx)
	var cleanupErrs []error
	for i := len(p.finalizers) - 1; i >= 0; i-- {
		finalizer := p.finalizers[i]
		reportIndex := recorder.startCleanup(finalizer.name)
		err := runFinalizer(cleanupCtx, finalizer.fn, result)
		if err != nil {
			err = &CleanupError{Pipeline: p.name, Name: finalizer.name, Err: err}
			cleanupErrs = append(cleanupErrs, err)
		}
		recorder.finishCleanup(reportIndex, err)
	}
	if len(cleanupErrs) > 0 {
		runErr = errors.Join(append([]error{preCleanupErr}, cleanupErrs...)...)
		output = nil
	} else {
		runErr = preCleanupErr
	}

	if runErr != nil {
		if logErr := invokeLog(func() { ctx.Logger().Error("pipeline failed: " + p.name) }); logErr != nil {
			runErr = errors.Join(runErr, annotateExecutionError(logErr, p.name, "", "", 0))
		}
		status := statusForError(runErr)
		hookErr := errors.Join(
			dispatcher.emit(PipelineFailed, "", "", status, runErr),
			p.runFailedHooks(PipelineEvent{Name: p.name, Context: ctx, Err: runErr}),
		)
		if hookErr != nil {
			runErr = errors.Join(runErr, annotateExecutionError(hookErr, p.name, "", "", 0))
		}
	} else {
		if logErr := invokeLog(func() { ctx.Logger().Info("completed pipeline: " + p.name) }); logErr != nil {
			runErr = annotateExecutionError(logErr, p.name, "", "", 0)
			output = nil
		}
		var hookErr error
		if runErr != nil {
			hookErr = errors.Join(
				dispatcher.emit(PipelineFailed, "", "", StatusFailed, runErr),
				p.runFailedHooks(PipelineEvent{Name: p.name, Context: ctx, Err: runErr}),
			)
		} else {
			hookErr = errors.Join(
				dispatcher.emit(PipelineCompleted, "", "", StatusCompleted, nil),
				p.runCompletedHooks(PipelineEvent{Name: p.name, Context: ctx}),
			)
		}
		if hookErr != nil {
			runErr = annotateExecutionError(hookErr, p.name, "", "", 0)
			output = nil
		}
	}
	status := statusForError(runErr)
	if hookErr := dispatcher.emit(PipelineFinalized, "", "", status, runErr); hookErr != nil {
		runErr = errors.Join(runErr, annotateExecutionError(hookErr, p.name, "", "", 0))
		output = nil
		status = statusForError(runErr)
	}
	ctx.markFinished(status)
	recorder.finishRun(runErr)
	return output, recorder.snapshot(), runErr
}

func (p *Pipeline) executeStages(goCtx context.Context, ctx *Context, input any, recorder *runRecorder, dispatcher *lifecycleDispatcher) (output any, err error) {
	defer recoverPanic(&err)
	if err := goCtx.Err(); err != nil {
		ctx.Logger().Error("pipeline cancelled: " + p.name)
		return nil, err
	}

	current := input

	for i, stage := range p.stages {
		if err := goCtx.Err(); err != nil {
			ctx.Logger().Error("pipeline cancelled: " + p.name)
			return nil, err
		}
		ctx.setCurrentStage(stage.name)
		output, err := stage.run(goCtx, ctx, current, recorder, i, dispatcher)
		if err != nil {
			return nil, annotateExecutionError(err, p.name, stage.name, "", 0)
		}
		current = output
	}

	return current, nil
}

// Validate checks statically knowable value-flow compatibility across the pipeline.
func (p *Pipeline) Validate() error {
	return p.validateFlowFrom(nil, false)
}

// ValidateInput validates the Pipeline structure and value flow using an
// actual initial input. Run and Start call it before execution begins.
func (p *Pipeline) ValidateInput(input any) error {
	return p.validateFlowFrom(reflect.TypeOf(input), true)
}

func (p *Pipeline) validateFlowFrom(current reflect.Type, known bool) error {
	if err := p.validateStructure(); err != nil {
		return err
	}
	if err := validateSharedRateLimits(p.stages); err != nil {
		return err
	}
	for i := range p.stages {
		var err error
		current, known, err = p.stages[i].validateFlow(current, known)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateSharedRateLimits(stages []Stage) error {
	policies := make(map[string]RateLimitPolicy)
	var visitStages func([]Stage) error
	visitStep := func(step *Step) error {
		if step == nil || step.rateLimit == nil || step.rateLimit.Key == "" {
			return nil
		}
		policy := *step.rateLimit
		if existing, ok := policies[policy.Key]; ok && existing != policy {
			return fmt.Errorf("pipeflow: rate limit key %q has conflicting policies", policy.Key)
		}
		policies[policy.Key] = policy
		return nil
	}
	visitStages = func(current []Stage) error {
		for _, stage := range current {
			for _, item := range stage.items {
				switch typed := item.(type) {
				case *Step:
					if err := visitStep(typed); err != nil {
						return err
					}
				case *ConcurrentSteps:
					for _, step := range typed.steps {
						if err := visitStep(step); err != nil {
							return err
						}
					}
				case *Parallel:
					for _, branch := range typed.branches {
						for _, step := range branch.steps {
							if err := visitStep(step); err != nil {
								return err
							}
						}
					}
				case *Subflow:
					if typed != nil {
						if err := visitStages(typed.stages); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}
	return visitStages(stages)
}

func executionArgs(args []any) (*Context, any, error) {
	switch len(args) {
	case 0:
		return NewContext(), nil, nil
	case 1:
		return NewContext(), args[0], nil
	case 2:
		ctx, ok := args[0].(*Context)
		if !ok || ctx == nil {
			return nil, nil, fmt.Errorf("pipeflow: legacy Run form requires *pipeflow.Context as its first argument")
		}
		return ctx, args[1], nil
	default:
		return nil, nil, fmt.Errorf("pipeflow: Run accepts zero or one input, or a Pipeflow context and input")
	}
}

// OnStarted registers the original Pipeline-start callback.
// Deprecated: use WithLifecycleHook and observe PipelineStarted.
func (p *Pipeline) OnStarted(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onStarted = append(p.onStarted, hook)
}

// OnCompleted registers the original Pipeline-completed callback.
// Deprecated: use WithLifecycleHook and observe PipelineCompleted.
func (p *Pipeline) OnCompleted(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onCompleted = append(p.onCompleted, hook)
}

// OnFailed registers the original Pipeline-failed callback.
// Deprecated: use WithLifecycleHook and observe PipelineFailed.
func (p *Pipeline) OnFailed(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onFailed = append(p.onFailed, hook)
}

func (p *Pipeline) runStartedHooks(event PipelineEvent) error {
	var failures []error
	for _, hook := range p.onStarted {
		if err := invokePipelineHook(hook, event); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (p *Pipeline) runCompletedHooks(event PipelineEvent) error {
	var failures []error
	for _, hook := range p.onCompleted {
		if err := invokePipelineHook(hook, event); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (p *Pipeline) runFailedHooks(event PipelineEvent) error {
	var failures []error
	for _, hook := range p.onFailed {
		if err := invokePipelineHook(hook, event); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func invokePipelineHook(hook PipelineHook, event PipelineEvent) (err error) {
	defer recoverPanic(&err)
	hook(event)
	return nil
}

func invokeLog(log func()) (err error) {
	defer recoverPanic(&err)
	log()
	return nil
}
