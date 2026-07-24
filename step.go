package pipeflow

type Step struct {
	name   string
	action func(any) (any, error)
	count  int
}

func NewStep(name string, action func(any) (any, error)) Step {
	return Step{
		name:   name,
		action: action,
	}
}

func (s *Step) Run(input any) (any, error) {
	return s.action(input)
}
