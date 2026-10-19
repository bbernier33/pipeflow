package pipeflow

import (
	"fmt"
	"reflect"
)

// FlowContractStatus describes whether a producer value can be passed to a
// consumer. Unknown means the producer type cannot be known before execution.
type FlowContractStatus string

const (
	FlowContractCompatible   FlowContractStatus = "compatible"
	FlowContractIncompatible FlowContractStatus = "incompatible"
	FlowContractUnknown      FlowContractStatus = "unknown"
)

// FlowEndpoint identifies one side of a value-flow boundary.
type FlowEndpoint struct {
	Kind DescriptionKind
	Path string
	Name string
	Type string
}

// FlowBoundary describes one statically inspectable producer-to-consumer
// contract. It contains type names only and never a business value.
type FlowBoundary struct {
	Producer FlowEndpoint
	Consumer FlowEndpoint
	Status   FlowContractStatus
	Error    error
}

// FlowReport contains a Pipeline's definition and statically inspectable value
// contracts. Error is non-nil when the definition or a known boundary is invalid.
type FlowReport struct {
	Topology   Description
	Boundaries []FlowBoundary
	Error      error
}

// InspectFlow validates and describes statically knowable Pipeline contracts.
// An initial input contract is unknown.
func (p *Pipeline) InspectFlow() FlowReport {
	return p.inspectFlow(nil, false)
}

// InspectFlowInput validates and describes Pipeline contracts for the supplied
// initial value. A nil value is treated as a known nil input.
func (p *Pipeline) InspectFlowInput(input any) FlowReport {
	return p.inspectFlow(reflect.TypeOf(input), true)
}

type flowSource struct {
	endpoint FlowEndpoint
	typeOf   reflect.Type
	known    bool
}

type flowInspector struct {
	report FlowReport
}

func (p *Pipeline) inspectFlow(initial reflect.Type, known bool) FlowReport {
	inspector := flowInspector{report: FlowReport{Topology: p.Describe()}}
	if err := p.validateStructure(); err != nil {
		inspector.report.Error = err
		return inspector.report
	}
	if err := validateSharedRateLimits(p.stages); err != nil {
		inspector.report.Error = err
		return inspector.report
	}
	source := flowSource{
		endpoint: FlowEndpoint{Kind: DescriptionPipeline, Path: p.name + "/input", Name: "input", Type: inspectedTypeName(initial, known)},
		typeOf:   initial,
		known:    known,
	}
	var err error
	for i := range p.stages {
		source, err = inspector.inspectStage(p.name+"/"+p.stages[i].name, &p.stages[i], source)
		if err != nil {
			inspector.report.Error = err
			return inspector.report
		}
	}
	return inspector.report
}

func (i *flowInspector) inspectStage(path string, stage *Stage, source flowSource) (flowSource, error) {
	for index, item := range stage.items {
		switch typed := item.(type) {
		case *Step:
			var err error
			source, err = i.inspectStep(path+"/"+typed.name, typed, source)
			if err != nil {
				return flowSource{}, fmt.Errorf("pipeflow: stage %q: %w", stage.name, err)
			}
		case *ConcurrentSteps:
			for _, step := range typed.steps {
				if _, err := i.inspectStep(path+fmt.Sprintf("/concurrent[%d]/", index)+step.name, step, source); err != nil {
					return flowSource{}, fmt.Errorf("pipeflow: stage %q: %w", stage.name, err)
				}
			}
			typeOf := reflect.TypeOf([]any{})
			source = flowSource{endpoint: FlowEndpoint{Kind: DescriptionConcurrent, Path: path + fmt.Sprintf("/concurrent[%d]", index), Name: "ConcurrentSteps", Type: typeOf.String()}, typeOf: typeOf, known: true}
		case *Parallel:
			for _, branch := range typed.branches {
				branchSource := source
				for _, step := range branch.steps {
					var err error
					branchSource, err = i.inspectStep(path+"/"+typed.name+"/"+branch.name+"/"+step.name, step, branchSource)
					if err != nil {
						return flowSource{}, fmt.Errorf("pipeflow: stage %q: parallel %q branch %q: %w", stage.name, typed.name, branch.name, err)
					}
				}
			}
			typeOf := reflect.TypeOf(ParallelResults{})
			source = flowSource{endpoint: FlowEndpoint{Kind: DescriptionParallel, Path: path + "/" + typed.name, Name: typed.name, Type: typeOf.String()}, typeOf: typeOf, known: true}
		case *Subflow:
			for subIndex := range typed.stages {
				var err error
				subStage := &typed.stages[subIndex]
				source, err = i.inspectStage(path+"/"+typed.name+"/"+subStage.name, subStage, source)
				if err != nil {
					return flowSource{}, fmt.Errorf("pipeflow: stage %q: subflow %q: %w", stage.name, typed.name, err)
				}
			}
		default:
			source = flowSource{endpoint: FlowEndpoint{Kind: DescriptionCustomItem, Path: path + fmt.Sprintf("/item[%d]", index), Name: fmt.Sprintf("item[%d]", index)}, known: false}
		}
	}
	return source, nil
}

func (i *flowInspector) inspectStep(path string, step *Step, source flowSource) (flowSource, error) {
	consumer := FlowEndpoint{Kind: DescriptionStep, Path: path, Name: step.name, Type: inspectedTypeName(step.inputType, step.inputType != nil)}
	if step.inputType != nil {
		boundary := FlowBoundary{Producer: source.endpoint, Consumer: consumer, Status: FlowContractUnknown}
		if source.known {
			if assignableFlowType(source.typeOf, step.inputType) {
				boundary.Status = FlowContractCompatible
			} else {
				boundary.Status = FlowContractIncompatible
				boundary.Error = fmt.Errorf("step %q expects %s but previous output is %s", step.name, step.inputType, typeName(source.typeOf))
			}
		}
		i.report.Boundaries = append(i.report.Boundaries, boundary)
	}
	nextType, nextKnown, err := step.validateFlow(source.typeOf, source.known)
	if err != nil {
		return flowSource{}, err
	}
	if step.flow == flowReplace && nextKnown {
		return flowSource{endpoint: FlowEndpoint{Kind: DescriptionStep, Path: path, Name: step.name, Type: typeName(nextType)}, typeOf: nextType, known: true}, nil
	}
	if !nextKnown {
		return flowSource{endpoint: FlowEndpoint{Kind: DescriptionStep, Path: path, Name: step.name}, known: false}, nil
	}
	return source, nil
}

func assignableFlowType(producer, consumer reflect.Type) bool {
	if producer == nil {
		return isNilable(consumer)
	}
	return producer.AssignableTo(consumer)
}

func inspectedTypeName(value reflect.Type, known bool) string {
	if !known {
		return ""
	}
	return typeName(value)
}
