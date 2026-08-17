package pipeflow

type FailurePolicy int

const (
	WaitAll FailurePolicy = iota
	FailFast
)
