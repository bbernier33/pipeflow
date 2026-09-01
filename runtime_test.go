package pipeflow

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerRunsFinitePipelineWithoutFunctionChanges(t *testing.T) {
	normalize := func(value int) (int, error) { return value * 2, nil }
	pipeline := NewPipeline("work", NewStage("process", NewStep("normalize", normalize)))
	finite, err := pipeline.Run(context.Background(), 3)
	if err != nil || finite != 6 {
		t.Fatalf("finite=%v err=%v", finite, err)
	}
	worker, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: 2, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	work, err := worker.Submit(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	worker.Close()
	output, report, err := work.Wait()
	if err != nil || output != 8 || report.RunID == "" || report.Status != StatusCompleted {
		t.Fatalf("output=%v report=%+v err=%v", output, report, err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRunFailureDoesNotStopRuntime(t *testing.T) {
	pipeline := NewPipeline("work", NewStage("process", NewStep("run", func(value int) (int, error) {
		if value == 2 {
			return 0, errors.New("item failed")
		}
		return value, nil
	})))
	worker, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: 1, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := worker.Submit(context.Background(), 1)
	failing, _ := worker.Submit(context.Background(), 2)
	last, _ := worker.Submit(context.Background(), 3)
	worker.Close()
	if output, _, err := first.Wait(); err != nil || output != 1 {
		t.Fatalf("first=%v err=%v", output, err)
	}
	if _, report, err := failing.Wait(); err == nil || report.Status != StatusFailed {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if output, _, err := last.Wait(); err != nil || output != 3 {
		t.Fatalf("last=%v err=%v", output, err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatalf("item failure killed Worker: %v", err)
	}
}

func TestWorkerBufferFullBlocksSubmitAndCloseDrains(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	pipeline := NewPipeline("work", NewStage("process", NewStep("block", func(value int) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return value, nil
	})))
	worker, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: 1, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := worker.Submit(context.Background(), 1)
	<-started
	second, _ := worker.Submit(context.Background(), 2)
	accepted := make(chan *Work, 1)
	go func() { work, _ := worker.Submit(context.Background(), 3); accepted <- work }()
	select {
	case <-accepted:
		t.Fatal("Submit did not apply backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	var third *Work
	select {
	case third = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("blocked Submit did not resume")
	}
	worker.Close()
	for _, work := range []*Work{first, second, third} {
		if output, _, err := work.Wait(); err != nil || output == nil {
			t.Fatalf("output=%v err=%v", output, err)
		}
	}
	if err := worker.Wait(); err != nil || calls.Load() != 3 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestWorkerCancelCompletesActiveAndQueuedHandles(t *testing.T) {
	started := make(chan struct{})
	pipeline := NewPipeline("work", NewStage("process", NewStep("wait", func(ctx context.Context, _ *Context, _ any) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})))
	worker, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: 1, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	active, _ := worker.Submit(context.Background(), nil)
	<-started
	queued, _ := worker.Submit(context.Background(), nil)
	worker.Cancel()
	for _, work := range []*Work{active, queued} {
		select {
		case <-work.Done():
		case <-time.After(time.Second):
			t.Fatal("Work did not complete on cancellation")
		}
	}
	if err := worker.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v", err)
	}
}

func TestStreamContinuesAfterItemFailureAndSingleWorkerOrdersResults(t *testing.T) {
	inputs := make(chan any, 3)
	inputs <- 1
	inputs <- 2
	inputs <- 3
	close(inputs)
	pipeline := NewPipeline("stream", NewStage("process", NewStep("run", func(value int) (int, error) {
		if value == 2 {
			return 0, errors.New("bad item")
		}
		return value * 10, nil
	})))
	stream, err := pipeline.StartStream(context.Background(), inputs, StreamOptions{Workers: 1, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	var results []StreamResult
	for result := range stream.Results() {
		results = append(results, result)
	}
	if err := stream.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 || results[0].Sequence != 1 || results[1].Sequence != 2 || results[2].Sequence != 3 || results[1].Err == nil || results[2].Output != 30 {
		t.Fatalf("results = %+v", results)
	}
}

func TestStreamMultipleWorkersMayCompleteUnorderedWithoutLoss(t *testing.T) {
	inputs := make(chan any, 3)
	inputs <- 1
	inputs <- 2
	inputs <- 3
	close(inputs)
	pipeline := NewPipeline("stream", NewStage("process", NewStep("run", func(value int) (int, error) {
		if value == 1 {
			time.Sleep(30 * time.Millisecond)
		}
		return value, nil
	})))
	stream, err := pipeline.StartStream(context.Background(), inputs, StreamOptions{Workers: 2, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	var outputs []int
	for result := range stream.Results() {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		outputs = append(outputs, result.Output.(int))
	}
	sort.Ints(outputs)
	if len(outputs) != 3 || outputs[0] != 1 || outputs[2] != 3 {
		t.Fatalf("outputs=%v", outputs)
	}
}

func TestRuntimeConfigurationValidation(t *testing.T) {
	pipeline := NewPipeline("runtime")
	if _, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: -1}); err == nil {
		t.Fatal("expected Worker validation error")
	}
	inputs := make(chan any)
	if _, err := pipeline.StartStream(context.Background(), inputs, StreamOptions{FailurePolicy: "drop"}); err == nil {
		t.Fatal("expected Stream validation error")
	}
}

func TestZeroBufferQueueIsRendezvous(t *testing.T) {
	queue := newBoundedQueue[int](0)
	enqueued := make(chan error, 1)
	go func() { enqueued <- queue.enqueue(context.Background(), 7) }()
	select {
	case <-enqueued:
		t.Fatal("zero-capacity enqueue completed without a receiver")
	case <-time.After(20 * time.Millisecond):
	}
	value, ok := queue.dequeue(context.Background())
	if !ok || value != 7 {
		t.Fatalf("value=%d ok=%v", value, ok)
	}
	if err := <-enqueued; err != nil {
		t.Fatal(err)
	}
	queue.close()
}

func TestWorkerMaxInFlightBlocksAdmissionAndStateIsPayloadFree(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := NewPipeline("bounded", NewStage("work", NewStep("wait", func(value int) (int, error) {
		if value == 1 {
			close(started)
			<-release
		}
		return value, nil
	})))
	worker, err := pipeline.StartWorker(context.Background(), WorkerOptions{Workers: 1, Buffer: 4, MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := worker.Submit(context.Background(), 1)
	<-started
	second, _ := worker.Submit(context.Background(), 2)
	accepted := make(chan *Work, 1)
	go func() { work, _ := worker.Submit(context.Background(), 3); accepted <- work }()
	select {
	case <-accepted:
		t.Fatal("max in-flight did not block")
	case <-time.After(20 * time.Millisecond):
	}
	state := worker.State()
	if state.Status != StatusRunning || state.Submitted != 2 || state.InFlight != 2 || state.Active != 1 || state.QueueDepth != 1 {
		t.Fatalf("state=%+v", state)
	}
	close(release)
	third := <-accepted
	worker.Close()
	for _, work := range []*Work{first, second, third} {
		if _, _, err := work.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if final := worker.State(); final.Completed != 3 || final.Failed != 0 {
		t.Fatalf("final=%+v", final)
	}
}

func TestStreamStopPolicyStopsAfterItemFailure(t *testing.T) {
	inputs := make(chan any, 3)
	inputs <- 1
	inputs <- 2
	inputs <- 3
	close(inputs)
	pipeline := NewPipeline("stream", NewStage("work", NewStep("run", func(value int) error {
		if value == 1 {
			return errors.New("stop")
		}
		time.Sleep(50 * time.Millisecond)
		return nil
	})))
	stream, err := pipeline.StartStream(context.Background(), inputs, StreamOptions{Workers: 1, Buffer: 2, FailurePolicy: StreamStop})
	if err != nil {
		t.Fatal(err)
	}
	var sawFailure bool
	for result := range stream.Results() {
		if result.Err != nil {
			sawFailure = true
		}
	}
	if !sawFailure || !errors.Is(stream.Wait(), context.Canceled) {
		t.Fatalf("failure=%v wait=%v", sawFailure, stream.Wait())
	}
}
