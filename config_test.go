package pipeflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func durationPointer(value time.Duration) *Duration {
	converted := Duration(value)
	return &converted
}

func TestParseConfigYAMLStrictDurations(t *testing.T) {
	config, err := ParseConfigYAML([]byte(`
defaults:
  pipeline:
    timeout: 5s
  step:
    retry:
      max_attempts: 3
      backoff: exponential
pipelines:
  orders:
    timeout: 4s
    stages:
      process:
        timeout: 3s
        steps:
          load:
            timeout: 2s
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Duration(*config.Pipelines["orders"].Stages["process"].Steps["load"].Timeout); got != 2*time.Second {
		t.Fatalf("step timeout=%v", got)
	}
	if _, err := ParseConfigYAML([]byte("defaults:\n  step:\n    typo: true\n")); err == nil || !strings.Contains(err.Error(), "field typo") {
		t.Fatalf("unknown field error=%v", err)
	}
}

func TestConfigPrecedenceAndEffectiveSources(t *testing.T) {
	maxAttempts := 4
	backoff := "exponential"
	config := Config{
		Defaults: ConfigDefaults{
			Pipeline: PipelineSettings{Timeout: durationPointer(10 * time.Second)},
			Stage:    StageSettings{Timeout: durationPointer(9 * time.Second)},
			Step: StepSettings{Timeout: durationPointer(8 * time.Second), Retry: &RetrySettings{
				MaxAttempts: &maxAttempts, Backoff: &backoff,
			}},
		},
		Pipelines: map[string]PipelineConfig{"orders": {
			PipelineSettings: PipelineSettings{Timeout: durationPointer(7 * time.Second)},
			Stages: map[string]StageConfig{"process": {
				StageSettings: StageSettings{Timeout: durationPointer(6 * time.Second)},
				Steps:         map[string]StepSettings{"load": {Timeout: durationPointer(5 * time.Second)}},
			}},
		}},
	}
	originalStep := NewStep("load", func() error { return nil }, WithTimeout(2*time.Second))
	original := NewPipeline("orders", NewStage("process", originalStep))
	configured, err := original.WithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	effective, ok := configured.EffectiveConfig()
	if !ok {
		t.Fatal("effective config missing")
	}
	step := effective.Stages[0].Steps[0]
	if effective.Timeout.Value != 7*time.Second || effective.Timeout.Source != ConfigSourcePipeline ||
		effective.Stages[0].Timeout.Value != 6*time.Second || effective.Stages[0].Timeout.Source != ConfigSourceStage ||
		step.Timeout.Value != 2*time.Second || step.Timeout.Source != ConfigSourceGo ||
		step.Retry.Value.MaxAttempts != 4 || step.Retry.Source != ConfigSourceGlobal {
		t.Fatalf("effective=%#v", effective)
	}
	if original.timeout != 0 || original.stages[0].timeout != 0 || originalStep.timeout != 2*time.Second {
		t.Fatal("original pipeline was mutated")
	}
}

func TestConfiguredRetryAndRateLimitAffectExecution(t *testing.T) {
	maxAttempts, maxCalls := 2, 2
	interval := Duration(time.Millisecond)
	config := Config{Pipelines: map[string]PipelineConfig{"pipeline": {
		Stages: map[string]StageConfig{"stage": {Steps: map[string]StepSettings{"request": {
			Retry:     &RetrySettings{MaxAttempts: &maxAttempts},
			RateLimit: &RateLimitSettings{MaxCalls: &maxCalls, Interval: &interval},
		}}}},
	}}}
	var calls atomic.Int32
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("request", func() error {
		if calls.Add(1) == 1 {
			return errors.New("temporary")
		}
		return nil
	})))
	configured, err := pipeline.WithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := configured.Run(context.Background()); err != nil || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestConfigRejectsUnknownScopes(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil })))
	configs := map[string]Config{
		"pipeline": {Pipelines: map[string]PipelineConfig{"other": {}}},
		"stage": {Pipelines: map[string]PipelineConfig{"pipeline": {
			Stages: map[string]StageConfig{"other": {}},
		}}},
		"step": {Pipelines: map[string]PipelineConfig{"pipeline": {
			Stages: map[string]StageConfig{"stage": {Steps: map[string]StepSettings{"other": {}}}},
		}}},
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			if _, err := pipeline.WithConfig(config); err == nil {
				t.Fatal("error=nil")
			}
		})
	}
}

func TestZeroConfigurationPreservesExecution(t *testing.T) {
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("produce", func() (int, error) { return 42, nil })))
	configured, err := pipeline.WithConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := configured.Run(context.Background())
	if err != nil || output != 42 {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestConfigRejectsInvalidPolicyBeforeExecution(t *testing.T) {
	zero := 0
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() error { return nil })))
	_, err := pipeline.WithConfig(Config{Defaults: ConfigDefaults{Step: StepSettings{
		RateLimit: &RateLimitSettings{MaxCalls: &zero},
	}}})
	if err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Fatalf("error=%v", err)
	}
}

func TestNestedConfigurationAndEffectiveTopology(t *testing.T) {
	maxAttempts := 2
	failFast := "fail_fast"
	nonFatal := "non_fatal"
	every := Duration(time.Millisecond)
	maxPolls := 3
	pipeline := NewPipeline("pipeline", NewStage("outer",
		NewParallel("fanout", []Branch{NewBranch("primary", NewStep("fetch", func() (int, error) { return 1, nil }))}),
		NewSubflow("prepare", NewStage("inner", NewStep("wait", func() (bool, error) { return true, nil }, WithPollPredicate(func(value bool) bool { return value })))),
	)).WithBackground("telemetry", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	config := Config{Pipelines: map[string]PipelineConfig{"pipeline": {
		Backgrounds: map[string]BackgroundSettings{"telemetry": {FailurePolicy: &nonFatal}},
		Stages: map[string]StageConfig{"outer": {
			Parallels: map[string]ParallelConfig{"fanout": {FailurePolicy: &failFast, Branches: map[string]BranchConfig{"primary": {Steps: map[string]StepSettings{"fetch": {Retry: &RetrySettings{MaxAttempts: &maxAttempts}}}}}}},
			Subflows:  map[string]SubflowConfig{"prepare": {Stages: map[string]StageConfig{"inner": {Steps: map[string]StepSettings{"wait": {Polling: &PollingSettings{Every: &every, MaxPolls: &maxPolls}}}}}}},
		}},
	}}}
	configured, err := pipeline.WithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	effective, _ := configured.EffectiveConfig()
	stage := effective.Stages[0]
	if effective.Backgrounds[0].FailurePolicy.Value != BackgroundNonFatal || stage.Parallels[0].FailurePolicy.Value != FailFast || stage.Parallels[0].Branches[0].Steps[0].Retry.Value.MaxAttempts != 2 || stage.Subflows[0].Stages[0].Steps[0].Polling.Value.Every != time.Millisecond {
		t.Fatalf("effective=%#v", effective)
	}
	// Snapshot mutation must not affect the configured Pipeline.
	effective.Stages[0].Subflows[0].Stages[0].Name = "changed"
	again, _ := configured.EffectiveConfig()
	if again.Stages[0].Subflows[0].Stages[0].Name != "inner" {
		t.Fatal("effective snapshot mutation leaked")
	}
}

func TestPollingYAMLRequiresGoPredicate(t *testing.T) {
	every := Duration(time.Millisecond)
	pipeline := NewPipeline("pipeline", NewStage("stage", NewStep("step", func() (bool, error) { return true, nil })))
	_, err := pipeline.WithConfig(Config{Pipelines: map[string]PipelineConfig{"pipeline": {Stages: map[string]StageConfig{"stage": {Steps: map[string]StepSettings{"step": {Polling: &PollingSettings{Every: &every}}}}}}}})
	if err == nil || !strings.Contains(err.Error(), "WithPollPredicate") {
		t.Fatalf("error=%v", err)
	}
}

func TestConfiguredPipelineCannotBeConfiguredAgain(t *testing.T) {
	pipeline := NewPipeline("pipeline")
	configured, err := pipeline.WithConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = configured.WithConfig(Config{}); err == nil || !strings.Contains(err.Error(), "already configured") {
		t.Fatalf("error=%v", err)
	}
}
