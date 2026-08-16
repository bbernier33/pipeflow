package pipeflow

import "context"

type StageItem interface {
	Run(goCtx context.Context, ctx *Context, input any) (any, error)
}
