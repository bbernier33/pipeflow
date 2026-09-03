package pipeflow

import (
	"context"
	"reflect"
	"testing"
)

func TestStepRoleConstructorsShareValueFlowEngine(t *testing.T) {
	source := NewSourceStep("read", func() (int, error) { return 20, nil })
	normal := NewStep("normalize", func(v int) (int, error) { return v + 1, nil })
	sink := NewSinkStep("write", func(v int) error {
		if v != 21 {
			t.Fatalf("sink input = %d", v)
		}
		return nil
	})
	pipeline := NewPipeline("roles", NewStage("flow", source, normal, sink))

	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != 21 {
		t.Fatalf("output = %v, want pass-through 21", output)
	}
	if got := []StepRole{source.Role(), normal.Role(), sink.Role()}; !reflect.DeepEqual(got, []StepRole{StepRoleSource, StepRoleNormal, StepRoleSink}) {
		t.Fatalf("roles = %v", got)
	}
	steps := report.Stages[0].Steps
	if steps[0].Role != StepRoleSource || steps[1].Role != StepRoleNormal || steps[2].Role != StepRoleSink {
		t.Fatalf("report roles = %+v", steps)
	}
}

func TestStepRolesAppearInDescriptionAndLiveState(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("roles", NewStage("flow",
		NewSourceStep("read", func() (int, error) { close(started); <-release; return 1, nil }),
		NewSinkStep("write", func(int) error { return nil }),
	))
	description := pipeline.Describe()
	if description.Children[0].Children[0].Role != StepRoleSource || description.Children[0].Children[1].Role != StepRoleSink {
		t.Fatalf("description = %+v", description)
	}
	execution, err := pipeline.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	state := execution.State()
	if state.Stages[0].Steps[0].Role != StepRoleSource || state.Stages[0].Steps[1].Role != StepRoleSink {
		t.Fatalf("state = %+v", state)
	}
	if current := execution.Current(); len(current.Stages) != 1 || current.Stages[0].Steps[0].Role != StepRoleSource {
		t.Fatalf("current = %+v", current)
	}
	close(release)
	if _, _, err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestStepRoleYAMLAndGoPrecedence(t *testing.T) {
	config, err := ParseConfigYAML([]byte(`
pipelines:
  roles:
    stages:
      flow:
        steps:
          configured:
            role: source
          explicit:
            role: source
`))
	if err != nil {
		t.Fatal(err)
	}
	pipeline, err := NewPipeline("roles", NewStage("flow",
		NewStep("configured", func() error { return nil }),
		NewSinkStep("explicit", func() error { return nil }),
	)).WithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	description := pipeline.Describe()
	if description.Children[0].Children[0].Role != StepRoleSource || description.Children[0].Children[1].Role != StepRoleSink {
		t.Fatalf("description = %+v", description)
	}
	effective, ok := pipeline.EffectiveConfig()
	if !ok {
		t.Fatal("missing effective config")
	}
	steps := effective.Stages[0].Steps
	if steps[0].Role.Value != StepRoleSource || steps[0].Role.Source != ConfigSourceStep || steps[1].Role.Value != StepRoleSink || steps[1].Role.Source != ConfigSourceGo {
		t.Fatalf("effective roles = %+v", steps)
	}
}

func TestStepRoleYAMLRejectsUnknownRole(t *testing.T) {
	config, err := ParseConfigYAML([]byte(`pipelines: {roles: {stages: {flow: {steps: {step: {role: queue}}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewPipeline("roles", NewStage("flow", NewStep("step", func() error { return nil }))).WithConfig(config)
	if err == nil {
		t.Fatal("expected invalid role error")
	}
}

func TestRoleConstructorsPreserveNormalStepOptions(t *testing.T) {
	step := NewSourceStep("source", func() (int, error) { return 1, nil }, WithTimeout(1), WithRetry(RetryPolicy{MaxAttempts: 2}))
	if step.Role() != StepRoleSource || step.timeout != 1 || step.retryPolicy.MaxAttempts != 2 {
		t.Fatalf("step = %+v", step)
	}
}

func TestOrdinaryFunctionIsReusableAcrossRoles(t *testing.T) {
	normalize := func(value int) (int, error) { return value + 1, nil }
	for name, step := range map[string]*Step{
		"normal": NewStep("normalize", normalize),
		"source": NewSourceStep("normalize", normalize),
		"sink":   NewSinkStep("normalize", normalize),
	} {
		output, err := step.Run(context.Background(), NewContext(), 1)
		if err != nil || output != 2 {
			t.Fatalf("%s: output=%v err=%v", name, output, err)
		}
	}
}

func TestStepRoleAppearsInLifecycleAndObservation(t *testing.T) {
	var lifecycleRole, observationRole StepRole
	pipeline := NewPipeline("roles", NewStage("flow", NewSourceStep("read", func() (int, error) { return 1, nil }))).
		WithLifecycleHook(func(event LifecycleEvent) {
			if event.Step == "read" {
				lifecycleRole = event.Role
			}
		}).
		WithObserver(ObserverFuncs{Trace: func(event TraceEvent) {
			if event.Location.Step == "read" {
				observationRole = event.Location.Role
			}
		}})
	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lifecycleRole != StepRoleSource || observationRole != StepRoleSource {
		t.Fatalf("lifecycle=%q observation=%q", lifecycleRole, observationRole)
	}
}
