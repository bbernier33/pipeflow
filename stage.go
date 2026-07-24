package pipeflow

type Stage struct {
	name  string
	steps []Step
}

func NewStage(name string, steps ...Step) Stage {
	return Stage{
		name:  name,
		steps: steps,
	}
}
