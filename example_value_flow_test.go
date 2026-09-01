package pipeflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bbernier33/pipeflow"
)

func ExamplePipeline_Run_valueFlow() {
	pipeline := pipeflow.NewPipeline("orders",
		pipeflow.NewStage("prepare",
			pipeflow.NewStep("load", func() (int, error) { return 20, nil }),
			pipeflow.NewStep("audit", func(total int) error {
				fmt.Println("audited", total)
				return nil
			}),
			pipeflow.NewStep("tax", func(total int) (int, error) { return total + 2, nil }),
		),
		pipeflow.NewStage("send",
			pipeflow.NewStep("email", func(total int) error {
				fmt.Println("emailed", total)
				return nil
			}),
		),
	)

	output, err := pipeline.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("result", output)

	// Output:
	// audited 20
	// emailed 22
	// result 22
}

func ExamplePipeline_ValidateInput() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("validate", func(orderID int) error { return nil }),
	))

	fmt.Println(pipeline.Validate())
	fmt.Println(pipeline.ValidateInput(42))
	fmt.Println(pipeline.ValidateInput("wrong") != nil)

	// Output:
	// <nil>
	// <nil>
	// true
}

func ExamplePipeline_Describe() {
	pipeline := pipeflow.NewPipeline("Invoice Processing",
		pipeflow.NewStage("Ingest",
			pipeflow.NewStep("Read", func() error { return nil }),
			pipeflow.NewStep("Parse", func() error { return nil }),
		),
		pipeflow.NewStage("Output",
			pipeflow.NewStep("Store", func() error { return nil }),
		),
	)

	fmt.Println(pipeline.Describe())

	// Output:
	// Invoice Processing
	// ├── Ingest
	// │   ├── Read
	// │   └── Parse
	// └── Output
	//     └── Store
}

func ExampleExecutionError() {
	cause := errors.New("database unavailable")
	pipeline := pipeflow.NewPipeline("orders",
		pipeflow.NewStage("output",
			pipeflow.NewStep("save", func() error { return cause }),
		),
	)

	_, err := pipeline.Run(context.Background())
	var executionErr *pipeflow.ExecutionError
	if errors.As(err, &executionErr) {
		fmt.Println(executionErr.Pipeline, executionErr.Stage, executionErr.Step, executionErr.Attempt)
	}
	fmt.Println(errors.Is(err, cause))

	// Output:
	// orders output save 1
	// true
}

func ExamplePipeline_RunWithReport() {
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("work", pipeflow.NewStep("produce", func() (int, error) { return 42, nil })),
	)

	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(output, report.Status, report.Stages[0].Steps[0].Status)

	// Output:
	// 42 completed completed
}

func ExampleWithResultMetadata() {
	pipeline := pipeflow.NewPipeline("example", pipeflow.NewStage("import",
		pipeflow.NewStep("records", func() (int, error) { return 42, nil },
			pipeflow.WithResultMetadata(func(count int) pipeflow.ResultMetadata {
				return pipeflow.ResultMetadata{"records_processed": count, "source": "gmail"}
			})),
	))

	output, report, err := pipeline.RunWithReport(context.Background())
	metadata := report.Stages[0].Steps[0].Metadata
	fmt.Println(output, metadata["records_processed"], metadata["source"], err)

	// Output:
	// 42 42 gmail <nil>
}

func ExamplePipeline_Start() {
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("work", pipeflow.NewStep("produce", func() (int, error) { return 42, nil })),
	)

	execution, err := pipeline.Start(context.Background())
	if err != nil {
		panic(err)
	}
	output, report, err := execution.Wait()
	if err != nil {
		panic(err)
	}
	fmt.Println(output, report.Status)

	// Output:
	// 42 completed
}

func ExampleExecution_State() {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("prepare", pipeflow.NewStep("load", func() error { return nil })),
		pipeflow.NewStage("process", pipeflow.NewStep("wait", func() error {
			close(started)
			<-release
			return nil
		})),
		pipeflow.NewStage("output", pipeflow.NewStep("save", func() error { return nil })),
	)

	execution, _ := pipeline.Start(context.Background())
	<-started
	state := execution.State()
	for _, stage := range state.Stages {
		fmt.Println(stage.Name, stage.Status)
	}
	close(release)
	_, _, _ = execution.Wait()

	// Output:
	// prepare completed
	// process running
	// output pending
}

func Example_withTimeout() {
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("work",
			pipeflow.NewStep("produce", func() (int, error) { return 42, nil }, pipeflow.WithTimeout(time.Second)),
		).WithTimeout(2*time.Second),
	).WithTimeout(3 * time.Second)

	output, err := pipeline.Run(context.Background())
	fmt.Println(output, err)

	// Output:
	// 42 <nil>
}

func Example_withPolling() {
	polls := 0
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("wait",
			pipeflow.NewStep("ready", func() (int, error) {
				polls++
				return polls, nil
			}, pipeflow.WithPolling(pipeflow.PollPolicy{
				MaxPolls: 3,
				Until:    func(value int) bool { return value == 2 },
			})),
		),
	)

	output, err := pipeline.Run(context.Background())
	fmt.Println(output, polls, err)

	// Output:
	// 2 2 <nil>
}

func ExampleWithCondition() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() (int, error) { return 20, nil }),
		pipeflow.NewStep("discount", func(total int) (int, error) { return total - 5, nil },
			pipeflow.WithCondition(func(total int) bool { return total >= 100 })),
		pipeflow.NewStep("send", func(total int) error {
			fmt.Println(total)
			return nil
		}),
	))

	output, err := pipeline.Run(context.Background())
	fmt.Println(output, err)

	// Output:
	// 20
	// 20 <nil>
}

func Example_withRetryBackoff() {
	temporary := errors.New("temporarily unavailable")
	attempts := 0
	pipeline := pipeflow.NewPipeline("example",
		pipeflow.NewStage("fetch",
			pipeflow.NewStep("users", func() (int, error) {
				attempts++
				if attempts < 3 {
					return 0, temporary
				}
				return 42, nil
			}, pipeflow.WithRetry(pipeflow.RetryPolicy{
				MaxAttempts: 3,
				Delay:       time.Millisecond,
				Backoff:     pipeflow.ExponentialBackoff,
				MaxDelay:    10 * time.Millisecond,
				RetryIf:     func(err error) bool { return errors.Is(err, temporary) },
			})),
		),
	)

	output, err := pipeline.Run(context.Background())
	fmt.Println(output, attempts, err)

	// Output:
	// 42 3 <nil>
}

func ExampleWithRateLimit() {
	policy := pipeflow.RateLimitPolicy{
		Key:           "provider-api",
		MaxCalls:      10,
		Interval:      time.Second,
		MaxConcurrent: 2,
	}
	pipeline := pipeflow.NewPipeline("example", pipeflow.NewStage("call",
		pipeflow.NewStep("request", func() error { return nil }, pipeflow.WithRateLimit(policy)),
	))

	_, err := pipeline.Run(context.Background())
	fmt.Println(err)

	// Output:
	// <nil>
}

func ExamplePipeline_WithLifecycleHook() {
	pipeline := pipeflow.NewPipeline("orders",
		pipeflow.NewStage("process", pipeflow.NewStep("charge", func() error { return nil })),
	).WithLifecycleHook(func(event pipeflow.LifecycleEvent) {
		if event.Type == pipeflow.StepCompleted {
			fmt.Println(event.Pipeline, event.Stage, event.Step, event.Status)
		}
	})

	_, err := pipeline.Run(context.Background())
	fmt.Println(err)

	// Output:
	// orders process charge completed
	// <nil>
}

func ExamplePipeline_WithObserver() {
	pipeline := pipeflow.NewPipeline("orders",
		pipeflow.NewStage("prepare", pipeflow.NewStep("load", func() (int, error) { return 42, nil })),
	).WithObserver(pipeflow.ObserverFuncs{
		Trace: func(event pipeflow.TraceEvent) {
			if event.Scope == pipeflow.ObservationStep && event.Phase == pipeflow.ObservationCompleted {
				fmt.Println(event.Location.Step, event.Status)
			}
		},
	})

	_, _ = pipeline.Run(context.Background())
	// Output:
	// load completed
}

func ExampleNewSourceStep() {
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("flow",
		pipeflow.NewSourceStep("load", func() (int, error) { return 20, nil }),
		pipeflow.NewStep("price", func(value int) (int, error) { return value + 1, nil }),
		pipeflow.NewSinkStep("store", func(value int) error {
			fmt.Println("stored", value)
			return nil
		}),
	))

	output, _ := pipeline.Run(context.Background())
	fmt.Println("output", output)
	// Output:
	// stored 21
	// output 21
}

func ExamplePipeline_Finally() {
	pipeline := pipeflow.NewPipeline("temporary-worker",
		pipeflow.NewStage("work", pipeflow.NewStep("process", func() error { return nil })),
	).Finally("stop worker", func(ctx context.Context, result pipeflow.Finalization) error {
		fmt.Println("cleanup", result.Status, ctx.Err())
		return nil
	})

	_, report, err := pipeline.RunWithReport(context.Background())
	fmt.Println(report.Status, report.Cleanups[0].Name, err)

	// Output:
	// cleanup completed <nil>
	// completed stop worker <nil>
}

func ExampleNewParallel() {
	pipeline := pipeflow.NewPipeline("providers", pipeflow.NewStage("lookup",
		pipeflow.NewParallel("accounts", []pipeflow.Branch{
			pipeflow.NewBranch("gmail", pipeflow.NewStep("lookup", func(id int) (string, error) {
				return fmt.Sprintf("gmail-%d", id), nil
			})),
			pipeflow.NewBranch("microsoft", pipeflow.NewStep("lookup", func(id int) (string, error) {
				return fmt.Sprintf("microsoft-%d", id), nil
			})),
		}),
		pipeflow.NewStep("join", func(results pipeflow.ParallelResults) error {
			fmt.Println(results[0].Name, results[0].Value)
			fmt.Println(results[1].Name, results[1].Value)
			return nil
		}),
	))

	_, err := pipeline.Run(context.Background(), 7)
	fmt.Println(err)

	// Output:
	// gmail gmail-7
	// microsoft microsoft-7
	// <nil>
}

func ExampleNewSubflow() {
	prepare := pipeflow.NewSubflow("prepare",
		pipeflow.NewStage("normalize", pipeflow.NewStep("trim", func(value string) (string, error) {
			return strings.TrimSpace(value), nil
		})),
		pipeflow.NewStage("measure", pipeflow.NewStep("length", func(value string) (int, error) {
			return len(value), nil
		})),
	)
	pipeline := pipeflow.NewPipeline("example", pipeflow.NewStage("process",
		prepare,
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))

	output, err := pipeline.Run(context.Background(), "  abc  ")
	fmt.Println(output, err)

	// Output:
	// 6 <nil>
}

func ExamplePipeline_WithBackground() {
	pipeline := pipeflow.NewPipeline("service",
		pipeflow.NewStage("work", pipeflow.NewStep("produce", func() (int, error) { return 42, nil })),
	).WithBackground("heartbeat", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	output, report, err := pipeline.RunWithReport(context.Background())
	fmt.Println(output, report.Backgrounds[0].Name, report.Backgrounds[0].Status, err)

	// Output:
	// 42 heartbeat completed <nil>
}

func Example_scopedBranchContext() {
	shared := pipeflow.NewContext()
	shared.Set("prefix", "account")
	branchStep := func(label string) *pipeflow.Step {
		return pipeflow.NewStep("scope", func(_ context.Context, branchCtx *pipeflow.Context, input any) (any, error) {
			prefix, _ := branchCtx.Get("prefix")
			branchCtx.Set("local", label)
			return fmt.Sprintf("%s-%s", prefix, label), nil
		})
	}
	parallel := pipeflow.NewParallel("providers", []pipeflow.Branch{
		pipeflow.NewBranch("gmail", branchStep("gmail")),
		pipeflow.NewBranch("microsoft", branchStep("microsoft")),
	})

	output, err := parallel.Run(context.Background(), shared, nil)
	results := output.(pipeflow.ParallelResults)
	_, merged := shared.Get("local")
	fmt.Println(results[0].Value, results[1].Value, merged, err)

	// Output:
	// account-gmail account-microsoft false <nil>
}
