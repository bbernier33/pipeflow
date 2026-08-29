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
)

// LifecycleEvent is an immutable execution fact. It never contains flowing
// business values and does not provide execution control.
type LifecycleEvent struct {
	Type       LifecycleEventType
	RunID      string
	Pipeline   string
	Stage      string
	Step       string
	Parallel   string
	Branch     string
	Background string
	Subflow    string
	Status     Status
	OccurredAt time.Time
	Err        error
}

func (d *lifecycleDispatcher) emitBackground(eventType LifecycleEventType, background string, status Status, err error) error {
	if d == nil {
		return nil
	}
	event := LifecycleEvent{
		Type: eventType, RunID: d.runID, Pipeline: d.pipeline, Background: background,
		Status: status, OccurredAt: time.Now(), Err: err,
	}
	return d.runHooks(event)
}

// LifecycleHook observes execution synchronously. Hooks cannot mutate flowing
// values; a hook panic fails its owning boundary. Callers should keep hooks brief.
type LifecycleHook func(LifecycleEvent)

type lifecycleDispatcher struct {
	runID    string
	pipeline string
	hooks    []LifecycleHook
	subflow  string
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
	return d.runHooks(event)
}

func newLifecycleDispatcher(runID, pipeline string, hooks []LifecycleHook) *lifecycleDispatcher {
	if len(hooks) == 0 {
		return nil
	}
	return &lifecycleDispatcher{runID: runID, pipeline: pipeline, hooks: append([]LifecycleHook(nil), hooks...)}
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
	return d.runHooks(event)
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
