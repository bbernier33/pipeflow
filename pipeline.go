package pipeflow

type Pipeline struct {
	name   string
	stages []Stage
}

func NewPipeline(name string, stages ...Stage) Pipeline {
	return Pipeline{
		name:   name,
		stages: stages,
	}
}

func (p *Pipeline) Run(ctx *Context, input any) (any, error) {
	ctx.Logger().Info("starting pipeline: " + p.name)

	current := input

	for _, stage := range p.stages {
		output, err := stage.Run(ctx, current)
		if err != nil {
			ctx.Logger().Error("pipeline failed: " + p.name)
			return nil, err
		}
		current = output
	}

	ctx.Logger().Info("completed pipeline: " + p.name)

	return current, nil
}
