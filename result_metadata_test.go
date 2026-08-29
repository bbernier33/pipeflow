package pipeflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStepResultMetadataDoesNotChangeValueFlow(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (int, error) { return 21, nil }, WithResultMetadata(func(value int) ResultMetadata {
			return ResultMetadata{"records_processed": value, "source": "gmail"}
		})),
		NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	))

	output, report, err := pipeline.RunWithReport(context.Background())
	metadata := report.Stages[0].Steps[0].Metadata
	if err != nil || output != 42 || metadata["records_processed"] != 21 || metadata["source"] != "gmail" {
		t.Fatalf("RunWithReport = (%v, %#v, %v)", output, report, err)
	}
}

func TestPassThroughStepAndStageResultMetadata(t *testing.T) {
	stage := NewStage("stage",
		NewStep("observe", func(value int) error { return nil }, WithResultMetadata(func(value int) ResultMetadata {
			return ResultMetadata{"observed": value}
		})),
	).WithResultMetadata(func(value int) ResultMetadata {
		return ResultMetadata{"final": value}
	})
	pipeline := NewPipeline("pipeline", stage)

	output, report, err := pipeline.RunWithReport(context.Background(), 7)
	if err != nil || output != 7 || report.Stages[0].Steps[0].Metadata["observed"] != 7 || report.Stages[0].Metadata["final"] != 7 {
		t.Fatalf("RunWithReport = (%v, %#v, %v)", output, report, err)
	}
}

func TestResultMetadataRunsOnceAfterRetriesAndPolling(t *testing.T) {
	actions, extracted := 0, 0
	step := NewStep("work", func() (int, error) {
		actions++
		if actions == 1 {
			return 0, errors.New("retry")
		}
		return actions, nil
	}, WithRetry(RetryPolicy{MaxAttempts: 2}),
		WithPolling(PollPolicy{MaxPolls: 2, Until: func(value int) bool { return value == 3 }}),
		WithResultMetadata(func(value int) ResultMetadata {
			extracted++
			return ResultMetadata{"final_call": value}
		}),
	)
	pipeline := NewPipeline("pipeline", NewStage("stage", step))

	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil || output != 3 || actions != 3 || extracted != 1 || report.Stages[0].Steps[0].Metadata["final_call"] != 3 {
		t.Fatalf("RunWithReport = (%v, %#v, %v), actions=%d extracted=%d", output, report, err, actions, extracted)
	}
}

func TestSkippedAndFailedStepsHaveNoResultMetadata(t *testing.T) {
	wantErr := errors.New("failed")
	t.Run("skipped", func(t *testing.T) {
		called := false
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("skip", func() error { return nil },
			WithCondition(func() bool { return false }),
			WithResultMetadata(func() ResultMetadata { called = true; return ResultMetadata{"bad": true} }),
		)))
		_, report, err := pipeline.RunWithReport(context.Background())
		if err != nil || called || report.Stages[0].Steps[0].Metadata != nil {
			t.Fatalf("report = %#v, err = %v, called = %v", report, err, called)
		}
	})
	t.Run("failed", func(t *testing.T) {
		called := false
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("fail", func() error { return wantErr },
			WithResultMetadata(func() ResultMetadata { called = true; return ResultMetadata{"bad": true} }),
		)))
		_, report, err := pipeline.RunWithReport(context.Background())
		if !errors.Is(err, wantErr) || called || report.Stages[0].Steps[0].Metadata != nil {
			t.Fatalf("report = %#v, err = %v, called = %v", report, err, called)
		}
	})
}

func TestResultMetadataSupportsNilZeroAndScalarTypes(t *testing.T) {
	type item struct{}
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("nil", func() (*item, error) { return nil, nil }, WithResultMetadata(func(value *item) ResultMetadata {
			return ResultMetadata{"nil": value == nil, "count": 0, "ratio": 1.5, "at": time.Unix(1, 0)}
		})),
	))
	_, report, err := pipeline.RunWithReport(context.Background())
	metadata := report.Stages[0].Steps[0].Metadata
	if err != nil || metadata["nil"] != true || metadata["count"] != 0 {
		t.Fatalf("metadata = %#v, err = %v", metadata, err)
	}
}

func TestResultMetadataValidationAndRuntimeTypeErrors(t *testing.T) {
	t.Run("invalid signature", func(t *testing.T) {
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() (int, error) { return 1, nil }, WithResultMetadata(func(int) map[string]any { return nil }))))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "result metadata extractor must be") {
			t.Fatalf("Validate error = %v", err)
		}
	})
	t.Run("static mismatch", func(t *testing.T) {
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() (int, error) { return 1, nil }, WithResultMetadata(func(string) ResultMetadata { return nil }))))
		if err := pipeline.Validate(); err == nil || !strings.Contains(err.Error(), "extractor expects string") {
			t.Fatalf("Validate error = %v", err)
		}
	})
	t.Run("dynamic mismatch", func(t *testing.T) {
		step := NewStep("legacy", func(_ context.Context, _ *Context, _ any) (any, error) { return 1, nil }, WithResultMetadata(func(string) ResultMetadata { return nil }))
		pipeline := NewPipeline("pipeline", NewStage("stage", step))
		_, err := pipeline.Run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "cannot use int as string") {
			t.Fatalf("Run error = %v", err)
		}
	})
	stage := NewStage("stage", NewStep("produce", func() (int, error) { return 1, nil })).WithResultMetadata(func(string) ResultMetadata { return nil })
	if err := stage.Validate(); err == nil || !strings.Contains(err.Error(), "extractor expects string") {
		t.Fatalf("Stage Validate error = %v", err)
	}
}

func TestResultMetadataRejectsPayloadShapedValues(t *testing.T) {
	tests := []ResultMetadata{
		{"slice": []int{1}},
		{"map": map[string]int{"one": 1}},
		{"struct": struct{ Value int }{1}},
		{"": true},
		{"large": strings.Repeat("x", maxMetadataString+1)},
	}
	tooMany := ResultMetadata{}
	for i := 0; i <= maxMetadataEntries; i++ {
		tooMany[string(rune('a'+i))] = i
	}
	tests = append(tests, tooMany)
	for _, metadata := range tests {
		pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil }, WithResultMetadata(func() ResultMetadata { return metadata }))))
		_, report, err := pipeline.RunWithReport(context.Background())
		if err == nil || report.Stages[0].Steps[0].Status != StatusFailed || report.Stages[0].Steps[0].Metadata != nil {
			t.Fatalf("metadata %#v: report = %#v, err = %v", metadata, report, err)
		}
	}
}

func TestResultMetadataPanicBecomesStructuredFailure(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() (int, error) { return 1, nil }, WithResultMetadata(func(int) ResultMetadata {
		panic("metadata panic")
	}))))
	_, report, err := pipeline.RunWithReport(context.Background())
	var panicErr *PanicError
	var executionErr *ExecutionError
	if !errors.As(err, &panicErr) || !errors.As(err, &executionErr) || executionErr.Step != "step" || report.Stages[0].Steps[0].Status != StatusFailed {
		t.Fatalf("report = %#v, err = %v", report, err)
	}
}

func TestResultMetadataInParallelAndSubflowReports(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("host",
		NewParallel("parallel", []Branch{NewBranch("branch", NewStep("produce", func() (int, error) { return 1, nil }, WithResultMetadata(func(int) ResultMetadata {
			return ResultMetadata{"branch": true}
		})))}),
		NewSubflow("subflow", NewStage("nested", NewStep("count", func(ParallelResults) (int, error) { return 1, nil })).WithResultMetadata(func(int) ResultMetadata {
			return ResultMetadata{"stage": true}
		})),
	))
	_, report, err := pipeline.RunWithReport(context.Background())
	branchMetadata := report.Stages[0].Parallels[0].Branches[0].Steps[0].Metadata
	nestedMetadata := report.Stages[0].Subflows[0].Stages[0].Metadata
	if err != nil || branchMetadata["branch"] != true || nestedMetadata["stage"] != true {
		t.Fatalf("report = %#v, err = %v", report, err)
	}
}

func TestResultMetadataReportSnapshotsAreIndependent(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("pipeline", NewStage("stage",
		NewStep("produce", func() (int, error) { return 1, nil }, WithResultMetadata(func(int) ResultMetadata { return ResultMetadata{"count": 1} })),
		NewStep("wait", func(int) error { close(started); <-release; return nil }),
	))
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	snapshot := execution.Report()
	snapshot.Stages[0].Steps[0].Metadata["count"] = 99
	if got := execution.Report().Stages[0].Steps[0].Metadata["count"]; got != 1 {
		t.Fatalf("snapshot mutation leaked: %v", got)
	}
	close(release)
	_, _, _ = execution.Wait()
}
