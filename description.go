package pipeflow

import (
	"fmt"
	"strings"
)

// DescriptionKind identifies one definition node in a Pipeline description.
type DescriptionKind string

const (
	DescriptionPipeline    DescriptionKind = "pipeline"
	DescriptionStage       DescriptionKind = "stage"
	DescriptionStep        DescriptionKind = "step"
	DescriptionConcurrent  DescriptionKind = "concurrent_steps"
	DescriptionParallel    DescriptionKind = "parallel"
	DescriptionBranch      DescriptionKind = "branch"
	DescriptionSubflow     DescriptionKind = "subflow"
	DescriptionCustomItem  DescriptionKind = "custom_item"
	DescriptionInvalidItem DescriptionKind = "invalid_item"
)

// Description is a definition-only Pipeline tree. It contains no functions,
// flowing values, result metadata, reports, or mutable execution state.
type Description struct {
	Kind     DescriptionKind
	Name     string
	Type     string
	Children []Description
}

// Describe returns a fresh structural description of the Pipeline.
func (p Pipeline) Describe() Description {
	root := Description{Kind: DescriptionPipeline, Name: p.name, Children: make([]Description, len(p.stages))}
	for i := range p.stages {
		root.Children[i] = describeStage(p.stages[i])
	}
	return root
}

func describeStage(stage Stage) Description {
	node := Description{Kind: DescriptionStage, Name: stage.name, Children: make([]Description, 0, len(stage.items))}
	for _, item := range stage.items {
		node.Children = append(node.Children, describeStageItem(item))
	}
	return node
}

func describeStageItem(item StageItem) Description {
	if item == nil || isNilInterface(item) {
		return Description{Kind: DescriptionInvalidItem, Type: fmt.Sprintf("%T", item)}
	}
	switch typed := item.(type) {
	case *Step:
		return describeStep(typed)
	case *ConcurrentSteps:
		node := Description{Kind: DescriptionConcurrent, Children: make([]Description, len(typed.steps))}
		for i, step := range typed.steps {
			node.Children[i] = describeStep(step)
		}
		return node
	case *Parallel:
		node := Description{Kind: DescriptionParallel, Name: typed.name, Children: make([]Description, len(typed.branches))}
		for i, branch := range typed.branches {
			branchNode := Description{Kind: DescriptionBranch, Name: branch.name, Children: make([]Description, len(branch.steps))}
			for j, step := range branch.steps {
				branchNode.Children[j] = describeStep(step)
			}
			node.Children[i] = branchNode
		}
		return node
	case *Subflow:
		node := Description{Kind: DescriptionSubflow, Name: typed.name, Children: make([]Description, len(typed.stages))}
		for i := range typed.stages {
			node.Children[i] = describeStage(typed.stages[i])
		}
		return node
	default:
		return Description{Kind: DescriptionCustomItem, Type: fmt.Sprintf("%T", item)}
	}
}

func describeStep(step *Step) Description {
	if step == nil {
		return Description{Kind: DescriptionInvalidItem, Type: "*pipeflow.Step"}
	}
	return Description{Kind: DescriptionStep, Name: step.name}
}

// String renders the definition as a compact Unicode tree.
func (d Description) String() string {
	var builder strings.Builder
	builder.WriteString(d.label())
	for i := range d.Children {
		builder.WriteByte('\n')
		renderDescription(&builder, d.Children[i], "", i == len(d.Children)-1)
	}
	return builder.String()
}

func renderDescription(builder *strings.Builder, node Description, prefix string, last bool) {
	builder.WriteString(prefix)
	if last {
		builder.WriteString("└── ")
	} else {
		builder.WriteString("├── ")
	}
	builder.WriteString(node.label())
	childPrefix := prefix
	if last {
		childPrefix += "    "
	} else {
		childPrefix += "│   "
	}
	for i := range node.Children {
		builder.WriteByte('\n')
		renderDescription(builder, node.Children[i], childPrefix, i == len(node.Children)-1)
	}
}

func (d Description) label() string {
	if d.Name != "" {
		return d.Name
	}
	if d.Type != "" {
		return d.Type
	}
	switch d.Kind {
	case DescriptionPipeline:
		return "<unnamed pipeline>"
	case DescriptionStage:
		return "<unnamed stage>"
	case DescriptionStep:
		return "<unnamed step>"
	case DescriptionConcurrent:
		return "ConcurrentSteps"
	case DescriptionParallel:
		return "<unnamed parallel>"
	case DescriptionBranch:
		return "<unnamed branch>"
	case DescriptionSubflow:
		return "<unnamed subflow>"
	default:
		return "<invalid item>"
	}
}
