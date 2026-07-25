package pipeflow

type PipelineEvent struct {
	Name    string
	Context *Context
	Err     error
}

type PipelineHook func(PipelineEvent)
