package pipeflow

type Pipeline struct {
	name   string
	stages []Stage

	onStarted   []PipelineHook
	onCompleted []PipelineHook
	onFailed    []PipelineHook
}

func NewPipeline(name string, stages ...Stage) Pipeline {
	return Pipeline{
		name:   name,
		stages: stages,
	}
}

func (p *Pipeline) Run(ctx *Context, input any) (any, error) {
	ctx.markStarted()
	ctx.Logger().Info("starting pipeline: " + p.name)

	p.runStartedHooks(PipelineEvent{
		Name:    p.name,
		Context: ctx,
	})

	current := input

	for _, stage := range p.stages {
		ctx.setCurrentStage(stage.name)
		output, err := stage.Run(ctx, current)
		if err != nil {
			ctx.Logger().Error("pipeline failed: " + p.name)
			ctx.markFailed()

			p.runFailedHooks(PipelineEvent{
				Name:    p.name,
				Context: ctx,
				Err:     err,
			})

			return nil, err
		}
		current = output
	}

	ctx.Logger().Info("completed pipeline: " + p.name)
	ctx.markCompleted()

	p.runCompletedHooks(PipelineEvent{
		Name:    p.name,
		Context: ctx,
	})

	return current, nil
}

func (p *Pipeline) OnStarted(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onStarted = append(p.onStarted, hook)
}

func (p *Pipeline) OnCompleted(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onCompleted = append(p.onCompleted, hook)
}

func (p *Pipeline) OnFailed(hook PipelineHook) {
	if hook == nil {
		return
	}
	p.onFailed = append(p.onFailed, hook)
}

func (p *Pipeline) runStartedHooks(event PipelineEvent) {
	for _, hook := range p.onStarted {
		hook(event)
	}
}

func (p *Pipeline) runCompletedHooks(event PipelineEvent) {
	for _, hook := range p.onCompleted {
		hook(event)
	}
}

func (p *Pipeline) runFailedHooks(event PipelineEvent) {
	for _, hook := range p.onFailed {
		hook(event)
	}
}
