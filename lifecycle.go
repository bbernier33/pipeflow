package pipeflow

import (
	"errors"
	"time"
)

// LifecycleEventType identifies a stable execution lifecycle boundary.
type LifecycleEventType string

const (
	PipelineStarted     LifecycleEventType = "pipeline_started"
	PipelineCompleted   LifecycleEventType = "pipeline_completed"
	PipelineFailed      LifecycleEventType = "pipeline_failed"
	PipelineFinalized   LifecycleEventType = "pipeline_finalized"
	StageStarted        LifecycleEventType = "stage_started"
	StageCompleted      LifecycleEventType = "stage_completed"
	StageFailed         LifecycleEventType = "stage_failed"
	StepStarted         LifecycleEventType = "step_started"
	StepCompleted       LifecycleEventType = "step_completed"
	StepFailed          LifecycleEventType = "step_failed"
	StepSkipped         LifecycleEventType = "step_skipped"
	SubflowStarted      LifecycleEventType = "subflow_started"
	SubflowCompleted    LifecycleEventType = "subflow_completed"
	SubflowFailed       LifecycleEventType = "subflow_failed"
	ParallelStarted     LifecycleEventType = "parallel_started"
	ParallelCompleted   LifecycleEventType = "parallel_completed"
	ParallelFailed      LifecycleEventType = "parallel_failed"
	BranchStarted       LifecycleEventType = "branch_started"
	BranchCompleted     LifecycleEventType = "branch_completed"
	BranchFailed        LifecycleEventType = "branch_failed"
	BackgroundStarted   LifecycleEventType = "background_started"
	BackgroundCompleted LifecycleEventType = "background_completed"
	BackgroundFailed    LifecycleEventType = "background_failed"
	RecoveryStarted     LifecycleEventType = "recovery_started"
	RecoveryCompleted   LifecycleEventType = "recovery_completed"
	RecoveryFailed      LifecycleEventType = "recovery_failed"
)

// LifecycleEvent is an immutable execution fact. It never contains flowing
// business values and does not provide execution control.
type LifecycleEvent struct {
	Type             LifecycleEventType
	RunID            string
	Pipeline         string
	Stage            string
	Step             string
	Parallel         string
	Branch           string
	Background       string
	Subflow          string
	Recovery         string
	RecoveryAttempt  int
	RecoveryDecision RecoveryDecision
	Role             StepRole
	Status           Status
	OccurredAt       time.Time
	Err              error
}

func (d *lifecycleDispatcher) emitBackground(eventType LifecycleEventType, background string, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{
		Type: eventType, RunID: d.runID, Pipeline: d.pipeline, Background: background,
		Status: status, OccurredAt: time.Now(), Err: err,
	}
	d.observeLifecycle(event)
	return d.runHooks(event)
}

// LifecycleHook observes execution synchronously. Hooks cannot mutate flowing
// values; a hook panic fails its owning boundary. Callers should keep hooks brief.
type LifecycleHook func(LifecycleEvent)

type lifecycleDispatcher struct {
	runID           string
	pipeline        string
	hooks           []LifecycleHook
	subflow         string
	recovery        string
	recoveryAttempt int
	observation     *observationDispatcher
}

func (d *lifecycleDispatcher) scopedRecovery(name string, attempt int) *lifecycleDispatcher {
	if d == nil {
		return nil
	}
	scoped := *d
	scoped.recovery, scoped.recoveryAttempt = name, attempt
	return &scoped
}

func (d *lifecycleDispatcher) emitRecovery(eventType LifecycleEventType, stage, step, recovery string, attempt int, decision RecoveryDecision, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{Type: eventType, RunID: d.runID, Pipeline: d.pipeline, Stage: stage, Step: step, Recovery: recovery, RecoveryAttempt: attempt, RecoveryDecision: decision, Status: status, OccurredAt: time.Now(), Err: err}
	d.observeLifecycle(event)
	return d.runHooks(event)
}

func (d *lifecycleDispatcher) scopedSubflow(name string) *lifecycleDispatcher {
	if d == nil {
		return nil
	}
	scoped := *d
	scoped.subflow = name
	return &scoped
}

func (d *lifecycleDispatcher) emitSubflow(eventType LifecycleEventType, name string, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{Type: eventType, RunID: d.runID, Pipeline: d.pipeline, Subflow: name, Status: status, OccurredAt: time.Now(), Err: err}
	d.observeLifecycle(event)
	return d.runHooks(event)
}

func (d *lifecycleDispatcher) emitAttempt(stage, parallel, branch, step string, role StepRole, poll, attempt int, phase ObservationPhase, status Status, err error) {
	if d == nil || d.observation == nil {
		return
	}
	d.observation.emit(ObservationAttempt, phase, ObservationLocation{RunID: d.runID, Pipeline: d.pipeline, Stage: stage, Step: step, Role: role, Parallel: parallel, Branch: branch, Subflow: d.subflow, Recovery: d.recovery, RecoveryAttempt: d.recoveryAttempt, Poll: poll, Attempt: attempt}, status, err)
}

func (d *lifecycleDispatcher) emitStep(eventType LifecycleEventType, stage, parallel, branch, step string, role StepRole, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{Type: eventType, RunID: d.runID, Pipeline: d.pipeline, Stage: stage, Parallel: parallel, Branch: branch, Step: step, Subflow: d.subflow, Recovery: d.recovery, RecoveryAttempt: d.recoveryAttempt, Role: role, Status: status, OccurredAt: time.Now(), Err: err}
	d.observeLifecycle(event)
	return d.runHooks(event)
}

func newLifecycleDispatcher(runID, pipeline string, hooks []LifecycleHook, observer Observer) *lifecycleDispatcher {
	if len(hooks) == 0 && observer == nil {
		return nil
	}
	return &lifecycleDispatcher{runID: runID, pipeline: pipeline, hooks: append([]LifecycleHook(nil), hooks...), observation: newObservationDispatcher(observer)}
}

func (d *lifecycleDispatcher) emit(eventType LifecycleEventType, stage, step string, status Status, err error) error {
	return d.emitLocated(eventType, stage, "", "", step, status, err)
}

func (d *lifecycleDispatcher) emitLocated(eventType LifecycleEventType, stage, parallel, branch, step string, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{
		Type: eventType, RunID: d.runID, Pipeline: d.pipeline,
		Stage: stage, Parallel: parallel, Branch: branch, Step: step,
		Subflow: d.subflow,
		Status:  status, OccurredAt: time.Now(), Err: err,
	}
	d.observeLifecycle(event)
	return d.runHooks(event)
}

func (d *lifecycleDispatcher) observeLifecycle(event LifecycleEvent) {
	if d == nil || d.observation == nil {
		return
	}
	scope, phase := observationForLifecycle(event.Type)
	if scope == "" {
		return
	}
	d.observation.emit(scope, phase, ObservationLocation{RunID: event.RunID, Pipeline: event.Pipeline, Stage: event.Stage, Step: event.Step, Role: event.Role, Parallel: event.Parallel, Branch: event.Branch, Subflow: event.Subflow, Background: event.Background, Recovery: event.Recovery, RecoveryAttempt: event.RecoveryAttempt}, event.Status, event.Err)
}

func observationForLifecycle(t LifecycleEventType) (ObservationScope, ObservationPhase) {
	switch t {
	case PipelineStarted:
		return ObservationPipeline, ObservationStarted
	case PipelineCompleted:
		return ObservationPipeline, ObservationCompleted
	case PipelineFailed:
		return ObservationPipeline, ObservationFailed
	case PipelineFinalized:
		return ObservationPipeline, ObservationFinalized
	case StageStarted:
		return ObservationStage, ObservationStarted
	case StageCompleted:
		return ObservationStage, ObservationCompleted
	case StageFailed:
		return ObservationStage, ObservationFailed
	case StepStarted:
		return ObservationStep, ObservationStarted
	case StepCompleted:
		return ObservationStep, ObservationCompleted
	case StepFailed:
		return ObservationStep, ObservationFailed
	case StepSkipped:
		return ObservationStep, ObservationSkipped
	case ParallelStarted:
		return ObservationParallel, ObservationStarted
	case ParallelCompleted:
		return ObservationParallel, ObservationCompleted
	case ParallelFailed:
		return ObservationParallel, ObservationFailed
	case BranchStarted:
		return ObservationBranch, ObservationStarted
	case BranchCompleted:
		return ObservationBranch, ObservationCompleted
	case BranchFailed:
		return ObservationBranch, ObservationFailed
	case SubflowStarted:
		return ObservationSubflow, ObservationStarted
	case SubflowCompleted:
		return ObservationSubflow, ObservationCompleted
	case SubflowFailed:
		return ObservationSubflow, ObservationFailed
	case BackgroundStarted:
		return ObservationBackground, ObservationStarted
	case BackgroundCompleted:
		return ObservationBackground, ObservationCompleted
	case BackgroundFailed:
		return ObservationBackground, ObservationFailed
	case RecoveryStarted:
		return ObservationRecovery, ObservationStarted
	case RecoveryCompleted:
		return ObservationRecovery, ObservationCompleted
	case RecoveryFailed:
		return ObservationRecovery, ObservationFailed
	default:
		return "", ""
	}
}

func (d *lifecycleDispatcher) runHooks(event LifecycleEvent) error {
	var failures []error
	for _, hook := range d.hooks {
		if err := invokeLifecycleHook(hook, event); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func invokeLifecycleHook(hook LifecycleHook, event LifecycleEvent) (err error) {
	defer recoverPanic(&err)
	hook(event)
	return nil
}
