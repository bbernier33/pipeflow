package pipeflow

type Step struct {
	name   string
	action func() error
	count  int
}

func NewStep(name string, action func() error) Step {
	return Step{
		name:   name,
		action: action,
	}
}

func (s *Step) Run() error {
	return s.action()
}
