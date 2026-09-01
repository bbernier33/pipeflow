package pipeflow

import (
	"fmt"
	"reflect"
)

func (p *Pipeline) validateStructure() error {
	if p.name == "" {
		return fmt.Errorf("pipeflow: pipeline name cannot be empty")
	}
	seenBackgrounds := make(map[string]struct{}, len(p.backgrounds))
	for _, task := range p.backgrounds {
		if task.name == "" {
			return fmt.Errorf("pipeflow: background name cannot be empty")
		}
		if task.fn == nil {
			return fmt.Errorf("pipeflow: background %q has a nil function", task.name)
		}
		if task.failurePolicy != BackgroundFatal && task.failurePolicy != BackgroundNonFatal {
			return fmt.Errorf("pipeflow: background %q has invalid failure policy %d", task.name, task.failurePolicy)
		}
		if _, exists := seenBackgrounds[task.name]; exists {
			return fmt.Errorf("pipeflow: duplicate background name %q", task.name)
		}
		seenBackgrounds[task.name] = struct{}{}
	}
	seenFinalizers := make(map[string]struct{}, len(p.finalizers))
	for _, finalizer := range p.finalizers {
		if finalizer.name == "" {
			return fmt.Errorf("pipeflow: finalizer name cannot be empty")
		}
		if _, exists := seenFinalizers[finalizer.name]; exists {
			return fmt.Errorf("pipeflow: duplicate finalizer name %q", finalizer.name)
		}
		seenFinalizers[finalizer.name] = struct{}{}
	}
	return validateStageDefinitions("pipeline "+fmt.Sprintf("%q", p.name), p.stages)
}

func validateStageDefinitions(scope string, stages []Stage) error {
	seen := make(map[string]struct{}, len(stages))
	for i := range stages {
		stage := &stages[i]
		if stage.name == "" {
			return fmt.Errorf("pipeflow: %s has a stage with an empty name", scope)
		}
		if _, exists := seen[stage.name]; exists {
			return fmt.Errorf("pipeflow: %s has duplicate stage name %q", scope, stage.name)
		}
		seen[stage.name] = struct{}{}
		if stage.configErr != nil {
			return stage.configErr
		}
		if err := validateStageItems(stage); err != nil {
			return err
		}
	}
	return nil
}

func validateStageItems(stage *Stage) error {
	seenSteps := make(map[string]struct{})
	seenParallels := make(map[string]struct{})
	seenSubflows := make(map[string]struct{})
	validateStep := func(step *Step) error {
		if step == nil {
			return fmt.Errorf("pipeflow: stage %q contains a nil step", stage.name)
		}
		if step.name == "" {
			return fmt.Errorf("pipeflow: stage %q contains a step with an empty name", stage.name)
		}
		if !step.role.valid() {
			return fmt.Errorf("pipeflow: step %q has invalid role %q", step.name, step.role)
		}
		if _, exists := seenSteps[step.name]; exists {
			return fmt.Errorf("pipeflow: stage %q has duplicate step name %q", stage.name, step.name)
		}
		seenSteps[step.name] = struct{}{}
		return step.configErr
	}
	for _, item := range stage.items {
		if item == nil || isNilInterface(item) {
			return fmt.Errorf("pipeflow: stage %q contains a nil item", stage.name)
		}
		switch typed := item.(type) {
		case *Step:
			if err := validateStep(typed); err != nil {
				return err
			}
		case *ConcurrentSteps:
			if typed.configErr != nil {
				return fmt.Errorf("pipeflow: stage %q: %w", stage.name, typed.configErr)
			}
			if typed.failurePolicy != WaitAll && typed.failurePolicy != FailFast {
				return fmt.Errorf("pipeflow: stage %q has concurrent steps with invalid failure policy %d", stage.name, typed.failurePolicy)
			}
			for _, step := range typed.steps {
				if err := validateStep(step); err != nil {
					return err
				}
			}
		case *Parallel:
			if typed.configErr != nil {
				return typed.configErr
			}
			if typed.name == "" {
				return fmt.Errorf("pipeflow: stage %q contains a parallel group with an empty name", stage.name)
			}
			if _, exists := seenParallels[typed.name]; exists {
				return fmt.Errorf("pipeflow: stage %q has duplicate parallel name %q", stage.name, typed.name)
			}
			seenParallels[typed.name] = struct{}{}
			if typed.failurePolicy != WaitAll && typed.failurePolicy != FailFast {
				return fmt.Errorf("pipeflow: parallel %q has invalid failure policy %d", typed.name, typed.failurePolicy)
			}
			seenBranches := make(map[string]struct{}, len(typed.branches))
			for _, branch := range typed.branches {
				if branch.name == "" {
					return fmt.Errorf("pipeflow: parallel %q contains a branch with an empty name", typed.name)
				}
				if _, exists := seenBranches[branch.name]; exists {
					return fmt.Errorf("pipeflow: parallel %q has duplicate branch name %q", typed.name, branch.name)
				}
				seenBranches[branch.name] = struct{}{}
				seenBranchSteps := make(map[string]struct{}, len(branch.steps))
				for _, step := range branch.steps {
					if step == nil {
						return fmt.Errorf("pipeflow: parallel %q branch %q contains a nil step", typed.name, branch.name)
					}
					if step.name == "" {
						return fmt.Errorf("pipeflow: parallel %q branch %q contains a step with an empty name", typed.name, branch.name)
					}
					if !step.role.valid() {
						return fmt.Errorf("pipeflow: step %q has invalid role %q", step.name, step.role)
					}
					if _, exists := seenBranchSteps[step.name]; exists {
						return fmt.Errorf("pipeflow: parallel %q branch %q has duplicate step name %q", typed.name, branch.name, step.name)
					}
					seenBranchSteps[step.name] = struct{}{}
					if step.configErr != nil {
						return step.configErr
					}
				}
			}
		case *Subflow:
			if typed.name == "" {
				return fmt.Errorf("pipeflow: stage %q contains a subflow with an empty name", stage.name)
			}
			if _, exists := seenSubflows[typed.name]; exists {
				return fmt.Errorf("pipeflow: stage %q has duplicate subflow name %q", stage.name, typed.name)
			}
			seenSubflows[typed.name] = struct{}{}
			if err := validateStageDefinitions("subflow "+fmt.Sprintf("%q", typed.name), typed.stages); err != nil {
				return err
			}
		}
	}
	return nil
}

func isNilInterface(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
