package pipeflow

type StageItem interface {
	Run(ctx *Context, input any) (any, error)
}
