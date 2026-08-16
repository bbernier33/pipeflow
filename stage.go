package pipeflow

import "context"

type Stage struct {
	name  string
	items []StageItem
}

func NewStage(name string, items ...StageItem) Stage {
	return Stage{
		name:  name,
		items: items,
	}
}

func (s *Stage) Run(goCtx context.Context, ctx *Context, input any) (any, error) {
	ctx.Logger().Info("starting stage: " + s.name)
	current := input

	for _, item := range s.items {
		output, err := item.Run(goCtx, ctx, current)
		if err != nil {
			ctx.Logger().Error("stage failed: " + s.name)
			return nil, err
		}
		current = output
	}

	ctx.Logger().Info("completed stage " + s.name)
	ctx.setCurrentStep("")
	return current, nil
}
