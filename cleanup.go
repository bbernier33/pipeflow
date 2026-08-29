package pipeflow

import (
	"context"
	"fmt"
	"runtime/debug"
)

// Finalization describes the execution outcome before cleanup errors are added.
type Finalization struct {
	RunID    string
	Pipeline string
	Status   Status
	Err      error
}

// Finalizer releases resources after Pipeline work has stopped.
type Finalizer func(context.Context, Finalization) error

type namedFinalizer struct {
	name string
	fn   Finalizer
}

// CleanupError identifies a failed Pipeline finalizer.
type CleanupError struct {
	Pipeline string
	Name     string
	Err      error
}

func (e *CleanupError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("pipeflow: pipeline %q cleanup %q: %v", e.Pipeline, e.Name, e.Err)
}

func (e *CleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PanicError represents a panic recovered from user execution or cleanup code.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("panic: %v", e.Value)
}

func recoverPanic(err *error) {
	if value := recover(); value != nil {
		*err = &PanicError{Value: value, Stack: debug.Stack()}
	}
}

func runFinalizer(ctx context.Context, finalizer Finalizer, result Finalization) (err error) {
	defer recoverPanic(&err)
	return finalizer(ctx, result)
}
