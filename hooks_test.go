package pipeflow

import "testing"

func TestPiplelineStartedHookRuns(t *testing.T) {
	ctx := NewContext()

	pipeline := NewPipeline("Test Pipeline")

	called := false

	pipeline.OnStarted(func(event PipelineEvent) {
		called = true

		if event.Name != "Test Pipeline" {
			t.Fatalf(
				"expected pipeline name %q, got %q",
				"Test Pipeline",
				event.Name,
			)
		}
		if event.Context != ctx {
			t.Fatalf("expected hook to recevie the pipeline context")
		}
		if event.Context.Status() != StatusRunning {
			t.Fatalf(
				"expected status %q, got %q",
				StatusRunning,
				event.Context.Status(),
			)
		}

	})

	_, err := pipeline.Run(ctx, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !called {
		t.Fatalf("expected started hook to run")
	}

}
