package pipeflow

import "fmt"

// StepRole describes where a Step sits at an application boundary. Roles are
// semantic metadata; every role uses the same Step execution engine.
type StepRole string

const (
	StepRoleNormal StepRole = "normal"
	StepRoleSource StepRole = "source"
	StepRoleSink   StepRole = "sink"
)

func (r StepRole) valid() bool {
	return r == StepRoleNormal || r == StepRoleSource || r == StepRoleSink
}

func parseStepRole(value string) (StepRole, error) {
	role := StepRole(value)
	if !role.valid() {
		return "", fmt.Errorf("pipeflow: unknown step role %q", value)
	}
	return role, nil
}

// NewSourceStep creates a normal Step carrying the source role.
func NewSourceStep(name string, action any, options ...StepOption) *Step {
	return newStep(name, action, StepRoleSource, true, options...)
}

// NewSinkStep creates a normal Step carrying the sink role.
func NewSinkStep(name string, action any, options ...StepOption) *Step {
	return newStep(name, action, StepRoleSink, true, options...)
}

// Role returns the Step's semantic role.
func (s *Step) Role() StepRole {
	if s == nil || !s.role.valid() {
		return StepRoleNormal
	}
	return s.role
}
