package pipeflow

type Pipeline struct {
	name   string
	stages []Stage
}

func NewPipeline(name string, stages ...Stage) Pipeline {
	return Pipeline{
		name:   name,
		stages: stages,
	}
}

func (p *Pipeline) Run(input any) (any, error) {
	current := input

	for _, stage := range p.stages {
		output, err := stage.Run(current)
		if err != nil {
			return nil, err
		}
		current = output
	}
	return current, nil
}
