package pipeflow

import (
	"errors"
	"fmt"
	"strings"
)

// ExecutionError describes where an error occurred during Pipeflow execution.
// Err remains available through errors.Is, errors.As, and errors.Unwrap.
type ExecutionError struct {
	Pipeline string
	Stage    string
	Step     string
	Parallel string
	Branch   string
	Subflow  string
	Poll     int
	Attempt  int
	Err      error
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	location := make([]string, 0, 6)
	if e.Pipeline != "" {
		location = append(location, fmt.Sprintf("pipeline %q", e.Pipeline))
	}
	if e.Stage != "" {
		location = append(location, fmt.Sprintf("stage %q", e.Stage))
	}
	if e.Subflow != "" {
		location = append(location, fmt.Sprintf("subflow %q", e.Subflow))
	}
	if e.Parallel != "" {
		location = append(location, fmt.Sprintf("parallel %q", e.Parallel))
	}
	if e.Branch != "" {
		location = append(location, fmt.Sprintf("branch %q", e.Branch))
	}
	if e.Step != "" {
		location = append(location, fmt.Sprintf("step %q", e.Step))
	}
	if e.Poll > 0 {
		location = append(location, fmt.Sprintf("poll %d", e.Poll))
	}
	if e.Attempt > 0 {
		location = append(location, fmt.Sprintf("attempt %d", e.Attempt))
	}
	prefix := "pipeflow execution failed"
	if len(location) > 0 {
		prefix = "pipeflow: " + strings.Join(location, ", ")
	}
	if e.Err == nil {
		return prefix
	}
	return prefix + ": " + e.Err.Error()
}

func annotateExecutionSubflow(err error, subflow string) error {
	if err == nil {
		return nil
	}
	if direct, ok := err.(*ExecutionError); ok {
		annotated := *direct
		if annotated.Subflow == "" {
			annotated.Subflow = subflow
		}
		return &annotated
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		annotated := make([]error, len(children))
		for i, child := range children {
			annotated[i] = annotateExecutionSubflow(child, subflow)
		}
		return errors.Join(annotated...)
	}
	return &ExecutionError{Subflow: subflow, Err: err}
}

func annotateExecutionBranch(err error, parallel, branch string) error {
	if err == nil {
		return nil
	}
	if direct, ok := err.(*ExecutionError); ok {
		annotated := *direct
		if annotated.Parallel == "" {
			annotated.Parallel = parallel
		}
		if annotated.Branch == "" {
			annotated.Branch = branch
		}
		return &annotated
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		annotated := make([]error, len(children))
		for i, child := range children {
			annotated[i] = annotateExecutionBranch(child, parallel, branch)
		}
		return errors.Join(annotated...)
	}
	return &ExecutionError{Parallel: parallel, Branch: branch, Err: err}
}

func annotateExecutionPoll(err error, poll int) error {
	if err == nil || poll == 0 {
		return err
	}
	if direct, ok := err.(*ExecutionError); ok {
		annotated := *direct
		if annotated.Poll == 0 {
			annotated.Poll = poll
		}
		return &annotated
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		annotated := make([]error, len(children))
		for i, child := range children {
			annotated[i] = annotateExecutionPoll(child, poll)
		}
		return errors.Join(annotated...)
	}
	return err
}

func (e *ExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func annotateExecutionError(err error, pipeline, stage, step string, attempt int) error {
	if err == nil {
		return nil
	}

	var executionErr *ExecutionError
	if errors.As(err, &executionErr) {
		// Handle an ExecutionError itself here. Joined errors are handled below so
		// every branch receives the enclosing location.
		if direct, ok := err.(*ExecutionError); ok {
			annotated := *direct
			if annotated.Pipeline == "" {
				annotated.Pipeline = pipeline
			}
			if annotated.Stage == "" {
				annotated.Stage = stage
			}
			if annotated.Step == "" {
				annotated.Step = step
			}
			if annotated.Attempt == 0 {
				annotated.Attempt = attempt
			}
			return &annotated
		}
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		annotated := make([]error, len(children))
		for i, child := range children {
			annotated[i] = annotateExecutionError(child, pipeline, stage, step, attempt)
		}
		return errors.Join(annotated...)
	}

	return &ExecutionError{
		Pipeline: pipeline,
		Stage:    stage,
		Step:     step,
		Attempt:  attempt,
		Err:      err,
	}
}
