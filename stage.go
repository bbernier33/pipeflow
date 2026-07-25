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
	current := input
	for _, step := range s.steps {
		output, err := step.Run(ctx, current)
		if err != nil {
			return nil, err
		}
		current = output
	}
	return current, nil
}
