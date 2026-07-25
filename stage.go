package pipeflow

type Stage struct {
	name  string
	steps []Step
}

func NewStage(name string, steps ...Step) Stage {
	return Stage{
		name:  name,
		steps: steps,
	}
}

func (s *Stage) Run(ctx *Context, input any) (any, error) {
	ctx.Logger().Info("starting stage: " + s.name)

	current := input
	for _, step := range s.steps {
		output, err := step.Run(ctx, current)
		if err != nil {
			ctx.Logger().Error("stage failed: " + s.name)
			return nil, err
		}
		current = output
	}

	ctx.Logger().Info("completed stage " + s.name)

	return current, nil
}
