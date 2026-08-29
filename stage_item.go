package pipeflow

import "context"

// StageItem is an executable unit within a Stage. Step, ConcurrentSteps,
// Parallel, and Subflow are the built-in implementations.
type StageItem interface {
	Run(goCtx context.Context, ctx *Context, input any) (any, error)
}
