package pipeflow

type Step struct {
	name string
}

func NewStep(name string) Step {
	return Step{
		name: name,
	}
}
