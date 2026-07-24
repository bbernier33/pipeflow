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

func (s *Stage) Run() error {
	for _, step := range s.steps {
		err := step.Run()
		if err != nil {
			return err
		}
	}
	return nil
}
