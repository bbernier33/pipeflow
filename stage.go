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

func (s *Stage) Run(input any) (any, error) {
	current := input
	for _, step := range s.steps {
		output, err := step.Run(current)
		if err != nil {
			return nil, err
		}
		current = output
	}
	return current, nil
}
