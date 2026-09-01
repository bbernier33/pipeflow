package pipeflow_test

import (
	"context"
	"errors"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
)

// Compile-time assignments intentionally pin the v1 method signatures used by
// callers outside the pipeflow package.
var (
	_ func(string, ...pipeflow.Stage) pipeflow.Pipeline                                   = pipeflow.NewPipeline
	_ func(string, ...pipeflow.StageItem) pipeflow.Stage                                  = pipeflow.NewStage
	_ func(string, any, ...pipeflow.StepOption) *pipeflow.Step                            = pipeflow.NewStep
	_ func(string, any, ...pipeflow.StepOption) *pipeflow.Step                            = pipeflow.NewSourceStep
	_ func(string, any, ...pipeflow.StepOption) *pipeflow.Step                            = pipeflow.NewSinkStep
	_ func(*pipeflow.Step) pipeflow.StepRole                                              = (*pipeflow.Step).Role
	_ func([]*pipeflow.Step, ...pipeflow.ConcurrentStepsOption) *pipeflow.ConcurrentSteps = pipeflow.NewConcurrentSteps
	_ func(string, ...*pipeflow.Step) pipeflow.Branch                                     = pipeflow.NewBranch
	_ func(string, []pipeflow.Branch, ...pipeflow.ParallelOption) *pipeflow.Parallel      = pipeflow.NewParallel
	_ func(string, ...pipeflow.Stage) *pipeflow.Subflow                                   = pipeflow.NewSubflow

	_ func(*pipeflow.Pipeline, context.Context, ...any) (any, error)                     = (*pipeflow.Pipeline).Run
	_ func(*pipeflow.Pipeline, context.Context, ...any) (any, pipeflow.RunReport, error) = (*pipeflow.Pipeline).RunWithReport
	_ func(*pipeflow.Pipeline, context.Context, ...any) (*pipeflow.Execution, error)     = (*pipeflow.Pipeline).Start
	_ func(*pipeflow.Pipeline) error                                                     = (*pipeflow.Pipeline).Validate
	_ func(*pipeflow.Pipeline, any) error                                                = (*pipeflow.Pipeline).ValidateInput
	_ func(pipeflow.Pipeline) pipeflow.Description                                       = pipeflow.Pipeline.Describe
	_ func([]byte) (pipeflow.Config, error)                                              = pipeflow.ParseConfigYAML
	_ func(pipeflow.Pipeline, pipeflow.Config) (pipeflow.Pipeline, error)                = pipeflow.Pipeline.WithConfig
	_ func(pipeflow.Pipeline) (pipeflow.EffectivePipelineConfig, bool)                   = pipeflow.Pipeline.EffectiveConfig
	_ pipeflow.StepOption                                                                = pipeflow.WithPollPredicate(func(bool) bool { return true })

	_ func(*pipeflow.Execution) pipeflow.Status                  = (*pipeflow.Execution).Status
	_ func(*pipeflow.Execution) pipeflow.CurrentExecution        = (*pipeflow.Execution).Current
	_ func(*pipeflow.Execution) pipeflow.ExecutionState          = (*pipeflow.Execution).State
	_ func(*pipeflow.Execution) pipeflow.RunReport               = (*pipeflow.Execution).Report
	_ func(*pipeflow.Execution) <-chan struct{}                  = (*pipeflow.Execution).Done
	_ func(*pipeflow.Execution) (any, pipeflow.RunReport, error) = (*pipeflow.Execution).Wait

	_ pipeflow.StageItem       = (*pipeflow.Step)(nil)
	_ pipeflow.StageItem       = (*pipeflow.ConcurrentSteps)(nil)
	_ pipeflow.StageItem       = (*pipeflow.Parallel)(nil)
	_ pipeflow.StageItem       = (*pipeflow.Subflow)(nil)
	_ pipeflow.Logger          = pipeflow.DefaultLogger{}
	_ pipeflow.RetryAfterError = retryAfterContractError{}
)

type retryAfterContractError struct{}

func (retryAfterContractError) Error() string             { return "retry" }
func (retryAfterContractError) RetryAfter() time.Duration { return 0 }

func Example_v1APIContract() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() (int, error) { return 21, nil }),
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil },
			pipeflow.WithRetry(pipeflow.RetryPolicy{MaxAttempts: 3}),
			pipeflow.WithTimeout(time.Second)),
		pipeflow.NewStep("store", func(value int) error { return nil }),
	)).WithLifecycleHook(func(pipeflow.LifecycleEvent) {})
	pipeline = pipeline.WithObserver(pipeflow.ObserverFuncs{Trace: func(pipeflow.TraceEvent) {}})

	output, report, err := pipeline.RunWithReport(context.Background())
	_, _, _ = output, report, err

	var executionErr *pipeflow.ExecutionError
	_ = errors.As(err, &executionErr)
	// Output:
}
