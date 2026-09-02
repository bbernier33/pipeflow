package obs_test

import (
	"context"
	"fmt"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
)

func ExampleCollector() {
	collector := obs.NewCollector(obs.Options{RunCapacity: 100})
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() (int, error) { return 21, nil }),
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	)).WithObserver(collector)
	_, _ = pipeline.Run(context.Background())
	snapshot := collector.Snapshot()
	fmt.Println(snapshot.Pipelines[0].Name, snapshot.Pipelines[0].Health.Health)
	// Output: orders healthy
}

func ExampleCollector_TrackWorker() {
	collector := obs.NewCollector(obs.Options{})
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("accept", func(value int) error { return nil }),
	))
	worker, _ := pipeline.StartWorker(context.Background(), pipeflow.WorkerOptions{Workers: 2, Buffer: 10})
	untrack, _ := collector.TrackWorker("primary", worker)

	snapshot := collector.Snapshot()
	fmt.Println(snapshot.Workers[0].Name, snapshot.Workers[0].Concurrency)
	fmt.Println(snapshot.Queues[0].Capacity)

	untrack()
	worker.Close()
	_ = worker.Wait()
	// Output:
	// primary 2
	// 10
}
