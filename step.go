package pipeflow

type Step struct {
	name   string
	action func(*Context, any) (any, error)
	count  int
}

func NewStep(name string, action func(*Context, any) (any, error)) Step {
	return Step{
		name:   name,
		action: action,
	}
}

func (s *Step) Run(ctx *Context, input any) (any, error) {
	return s.action(ctx, input)
}
