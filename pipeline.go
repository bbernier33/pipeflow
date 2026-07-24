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

func (p *Pipeline) Run() error {
	for _, stage := range p.stages {
		err := stage.Run()
		if err != nil {
			return err
		}
	}
	return nil
}
