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
	ctx.Logger().Info("Running step: " + s.name)

	output, err := s.action(ctx, input)

	if err != nil {
		ctx.Logger().Error("step failed: " + s.name)
		return nil, err
	}

	ctx.Logger().Info("completed step: " + s.name)

	return output, nil

}
