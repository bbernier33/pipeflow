package readiness

import (
	"context"
	"errors"
	"fmt"
)

// IntegrationLevel identifies the declared environment/boundary combination.
type IntegrationLevel int

const (
	IntegrationSafeSample IntegrationLevel = iota + 1
	IntegrationRealIngress
	IntegrationStagingOutput
	IntegrationSafeEnvironment
	IntegrationProduction
)

const (
	BoundaryRealIngress       BoundaryKind = "real_ingress"
	BoundaryStagingSink       BoundaryKind = "staging_sink"
	BoundaryProductionIngress BoundaryKind = "production_ingress"
	BoundaryProductionSink    BoundaryKind = "production_sink"
)

// RealIngress declares an external source in a safe test environment.
func RealIngress(name string) Boundary { return Boundary{Name: name, Kind: BoundaryRealIngress} }

// StagingSink declares a development/staging output destination.
func StagingSink(name string) Boundary { return Boundary{Name: name, Kind: BoundaryStagingSink} }

// ProductionIngress declares an explicitly production-connected source.
func ProductionIngress(name string) Boundary {
	return Boundary{Name: name, Kind: BoundaryProductionIngress}
}

// ProductionSink declares an explicitly production-connected output destination.
func ProductionSink(name string) Boundary { return Boundary{Name: name, Kind: BoundaryProductionSink} }

// ErrIntegrationBoundaryDenied identifies a boundary not allowed by policy.
var ErrIntegrationBoundaryDenied = errors.New("readiness: integration boundary denied")

// IntegrationPolicy is an explicit name-and-kind allowlist for real, staging,
// and production boundaries. Sample ingress and safe sinks need no grant.
// This attests to caller-controlled wiring; it cannot sandbox arbitrary Go code.
type IntegrationPolicy struct{ Allowed []Boundary }

// Integration composes one existing Scenario with an integration level and
// boundary policy. It never automatically advances to another level.
type Integration struct {
	scenario   Scenario
	level      IntegrationLevel
	boundaries []Boundary
	policy     IntegrationPolicy
}

// NewIntegration creates an integration verification. By default all real,
// staging, and production boundaries are denied before Pipeline execution.
func NewIntegration(scenario Scenario, level IntegrationLevel, boundaries ...Boundary) Integration {
	return Integration{scenario: scenario, level: level, boundaries: append([]Boundary(nil), boundaries...)}
}

// WithPolicy returns a copy using a detached allowlist.
func (i Integration) WithPolicy(policy IntegrationPolicy) Integration {
	i.policy = IntegrationPolicy{Allowed: append([]Boundary(nil), policy.Allowed...)}
	return i
}

// IntegrationResult retains level, boundaries, contracts, and normal execution
// evidence without adding business values or credentials.
type IntegrationResult struct {
	Level IntegrationLevel
	EndToEndResult
}

// Run validates the level and grants, then reuses E2E contract preflight,
// Scenario expectations, reports, coverage, and timing.
func (i Integration) Run(ctx context.Context) IntegrationResult {
	e := EndToEnd{scenario: i.scenario, boundaries: i.boundaries}
	result := e.run(ctx, func(boundaries []Boundary) error { return validateIntegration(i.level, boundaries, i.policy) })
	return IntegrationResult{Level: i.level, EndToEndResult: result}
}

func validateIntegration(level IntegrationLevel, boundaries []Boundary, policy IntegrationPolicy) error {
	if level < IntegrationSafeSample || level > IntegrationProduction {
		return fmt.Errorf("readiness: invalid integration level %d", level)
	}
	grants := make(map[Boundary]bool, len(policy.Allowed))
	for _, grant := range policy.Allowed {
		if grant.Name == "" || !integrationKind(grant.Kind) {
			return fmt.Errorf("readiness: invalid integration grant %q (%s)", grant.Name, grant.Kind)
		}
		if grants[grant] {
			return fmt.Errorf("readiness: duplicate integration grant %q (%s)", grant.Name, grant.Kind)
		}
		grants[grant] = true
	}
	ingress, sinks, production := 0, 0, 0
	names := make(map[string]bool, len(boundaries))
	for _, boundary := range boundaries {
		if boundary.Name == "" || !integrationKind(boundary.Kind) {
			return fmt.Errorf("readiness: invalid integration boundary %q (%s)", boundary.Name, boundary.Kind)
		}
		if names[boundary.Name] {
			return fmt.Errorf("readiness: duplicate integration boundary %q", boundary.Name)
		}
		names[boundary.Name] = true
		switch boundary.Kind {
		case BoundarySampleIngress, BoundaryRealIngress, BoundaryProductionIngress:
			ingress++
		default:
			sinks++
		}
		if boundary.Kind == BoundaryProductionIngress || boundary.Kind == BoundaryProductionSink {
			production++
		}
		allowed := false
		switch level {
		case IntegrationSafeSample:
			allowed = boundary.Kind == BoundarySampleIngress || boundary.Kind == BoundarySafeSink
		case IntegrationRealIngress:
			allowed = boundary.Kind == BoundaryRealIngress || boundary.Kind == BoundarySafeSink
		case IntegrationStagingOutput:
			allowed = boundary.Kind == BoundarySampleIngress || boundary.Kind == BoundaryStagingSink
		case IntegrationSafeEnvironment:
			allowed = boundary.Kind == BoundaryRealIngress || boundary.Kind == BoundaryStagingSink
		case IntegrationProduction:
			allowed = true
		}
		if !allowed {
			return fmt.Errorf("readiness: boundary %q (%s) incompatible with integration level %d", boundary.Name, boundary.Kind, level)
		}
		if boundary.Kind != BoundarySampleIngress && boundary.Kind != BoundarySafeSink && !grants[boundary] {
			return fmt.Errorf("%w: %q (%s)", ErrIntegrationBoundaryDenied, boundary.Name, boundary.Kind)
		}
	}
	if ingress != 1 || sinks < 1 {
		return fmt.Errorf("readiness: integration requires exactly one ingress and at least one sink; got %d/%d", ingress, sinks)
	}
	if level == IntegrationProduction && production == 0 {
		return errors.New("readiness: production integration requires a declared production boundary")
	}
	return nil
}

func integrationKind(kind BoundaryKind) bool {
	switch kind {
	case BoundarySampleIngress, BoundarySafeSink, BoundaryRealIngress, BoundaryStagingSink, BoundaryProductionIngress, BoundaryProductionSink:
		return true
	default:
		return false
	}
}
