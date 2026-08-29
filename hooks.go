package pipeflow

// PipelineEvent is the compatibility event used by the original Pipeline-only
// hooks.
// Deprecated: use LifecycleEvent with Pipeline lifecycle event types.
type PipelineEvent struct {
	Name    string
	Context *Context
	Err     error
}

// PipelineHook is the compatibility callback used by OnStarted, OnCompleted,
// and OnFailed.
// Deprecated: use LifecycleHook.
type PipelineHook func(PipelineEvent)
