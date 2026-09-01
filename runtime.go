package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// WorkerOptions configures a bounded, in-memory Worker runtime.
type WorkerOptions struct {
	Workers     int
	Buffer      int
	MaxInFlight int
}

// Work is one finite Pipeline run submitted to a Worker.
type Work struct {
	done   chan struct{}
	mu     sync.Mutex
	output any
	report RunReport
	err    error
}

func newWork() *Work                  { return &Work{done: make(chan struct{})} }
func (w *Work) Done() <-chan struct{} { return w.done }
func (w *Work) Wait() (any, RunReport, error) {
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output, w.report, w.err
}
func (w *Work) finish(output any, report RunReport, err error) {
	w.mu.Lock()
	w.output, w.report, w.err = output, report, err
	w.mu.Unlock()
	close(w.done)
}

type workRequest struct {
	input any
	work  *Work
}

// Worker repeatedly runs one finite Pipeline. Its queue is bounded and Submit
// blocks when full; queued values are memory-only and are never durable.
type Worker struct {
	pipeline       Pipeline
	ctx            context.Context
	cancel         context.CancelFunc
	queue          *boundedQueue[workRequest]
	done           chan struct{}
	parent         context.Context
	workerCount    int
	bufferCapacity int
	maxInFlight    int
	slots          chan struct{}
	status         atomicStatus
	submitted      atomic.Uint64
	completed      atomic.Uint64
	failed         atomic.Uint64
	active         atomic.Int64
}

type atomicStatus struct{ value atomic.Value }

func (s *atomicStatus) store(status Status) { s.value.Store(status) }
func (s *atomicStatus) load() Status {
	if v := s.value.Load(); v != nil {
		return v.(Status)
	}
	return StatusPending
}

// WorkerState is a payload-free snapshot of one Worker runtime.
type WorkerState struct {
	Pipeline       string
	Status         Status
	Workers        int
	BufferCapacity int
	MaxInFlight    int
	QueueDepth     int
	InFlight       int
	Active         int
	Submitted      uint64
	Completed      uint64
	Failed         uint64
}

// StartWorker starts a long-lived Worker over the existing finite execution engine.
func (p Pipeline) StartWorker(ctx context.Context, options WorkerOptions) (*Worker, error) {
	if ctx == nil {
		return nil, errors.New("pipeflow: Worker context cannot be nil")
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if options.Workers == 0 {
		options.Workers = 1
	}
	if options.Workers < 0 {
		return nil, errors.New("pipeflow: Worker count cannot be negative")
	}
	if options.Buffer < 0 {
		return nil, errors.New("pipeflow: Worker buffer cannot be negative")
	}
	if options.MaxInFlight < 0 {
		return nil, errors.New("pipeflow: Worker max in-flight cannot be negative")
	}
	if options.MaxInFlight > 0 && options.MaxInFlight < options.Workers {
		return nil, errors.New("pipeflow: Worker max in-flight cannot be less than worker count")
	}
	runtimeCtx, cancel := context.WithCancel(ctx)
	w := &Worker{pipeline: p, ctx: runtimeCtx, cancel: cancel, queue: newBoundedQueue[workRequest](options.Buffer), done: make(chan struct{}), parent: ctx, workerCount: options.Workers, bufferCapacity: options.Buffer, maxInFlight: options.MaxInFlight}
	if options.MaxInFlight > 0 {
		w.slots = make(chan struct{}, options.MaxInFlight)
	}
	w.status.store(StatusRunning)
	var workers sync.WaitGroup
	workers.Add(options.Workers)
	for i := 0; i < options.Workers; i++ {
		go func() {
			defer workers.Done()
			for {
				request, ok := w.queue.dequeue(runtimeCtx)
				if !ok {
					return
				}
				w.active.Add(1)
				output, report, err := w.pipeline.RunWithReport(runtimeCtx, request.input)
				request.work.finish(output, report, err)
				w.active.Add(-1)
				w.completed.Add(1)
				if err != nil {
					w.failed.Add(1)
				}
				w.releaseSlot()
			}
		}()
	}
	go func() {
		select {
		case <-ctx.Done():
			cancel()
			w.queue.close()
		case <-w.done:
		}
	}()
	go func() {
		workers.Wait()
		w.queue.close()
		for {
			request, ok := w.queue.dequeue(context.Background())
			if !ok {
				break
			}
			request.work.finish(nil, RunReport{}, runtimeCtx.Err())
			w.completed.Add(1)
			w.failed.Add(1)
			w.releaseSlot()
		}
		if runtimeCtx.Err() != nil {
			w.status.store(statusForError(runtimeCtx.Err()))
		} else {
			w.status.store(StatusCompleted)
		}
		close(w.done)
	}()
	return w, nil
}

// Submit blocks while the bounded queue is full. Cancellation never drops a
// successfully accepted item without completing its Work handle.
func (w *Worker) Submit(ctx context.Context, input any) (*Work, error) {
	if w == nil {
		return nil, errors.New("pipeflow: nil Worker")
	}
	if ctx == nil {
		return nil, errors.New("pipeflow: Submit context cannot be nil")
	}
	work := newWork()
	if w.slots != nil {
		select {
		case w.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		}
	}
	if err := w.queue.enqueue(ctx, workRequest{input: input, work: work}); err != nil {
		w.releaseSlot()
		return nil, err
	}
	w.submitted.Add(1)
	return work, nil
}

func (w *Worker) releaseSlot() {
	if w.slots != nil {
		<-w.slots
	}
}

// State returns a read-only, payload-free runtime snapshot.
func (w *Worker) State() WorkerState {
	if w == nil {
		return WorkerState{}
	}
	submitted, completed := w.submitted.Load(), w.completed.Load()
	return WorkerState{Pipeline: w.pipeline.name, Status: w.status.load(), Workers: w.workerCount, BufferCapacity: w.bufferCapacity, MaxInFlight: w.maxInFlight, QueueDepth: w.queue.len(), InFlight: int(submitted - completed), Active: int(w.active.Load()), Submitted: submitted, Completed: completed, Failed: w.failed.Load()}
}

// Close stops intake and gracefully drains accepted work.
func (w *Worker) Close() {
	if w != nil {
		w.queue.close()
	}
}

// Cancel stops intake and cancels active and queued work.
func (w *Worker) Cancel() {
	if w != nil {
		w.cancel()
		w.queue.close()
	}
}
func (w *Worker) Done() <-chan struct{} { return w.done }

// Wait waits for runtime shutdown. Item failures are returned by Work.Wait and
// do not fail the Worker. Parent cancellation is returned as runtime shutdown.
func (w *Worker) Wait() error {
	<-w.done
	if err := w.parent.Err(); err != nil {
		return err
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	return nil
}

// StreamItemFailurePolicy determines whether one failed item stops a Stream.
type StreamItemFailurePolicy string

const (
	StreamContinue StreamItemFailurePolicy = "continue"
	StreamStop     StreamItemFailurePolicy = "stop_stream"
)

type StreamOptions struct {
	Workers       int
	Buffer        int
	MaxInFlight   int
	FailurePolicy StreamItemFailurePolicy
}

// StreamResult is one transient item result. RunReport remains payload-free;
// Output is delivered only to the explicit Stream consumer.
type StreamResult struct {
	Sequence uint64
	Output   any
	Report   RunReport
	Err      error
}

type Stream struct {
	results       chan StreamResult
	done          chan struct{}
	cancel        context.CancelFunc
	worker        *Worker
	failurePolicy StreamItemFailurePolicy
	errMu         sync.Mutex
	err           error
}

type StreamState struct {
	Worker        WorkerState
	FailurePolicy StreamItemFailurePolicy
}

func (s *Stream) State() StreamState {
	if s == nil {
		return StreamState{}
	}
	return StreamState{Worker: s.worker.State(), FailurePolicy: s.failurePolicy}
}

func (s *Stream) Results() <-chan StreamResult { return s.results }
func (s *Stream) Done() <-chan struct{}        { return s.done }
func (s *Stream) Cancel() {
	if s != nil {
		s.cancel()
	}
}
func (s *Stream) Wait() error { <-s.done; s.errMu.Lock(); defer s.errMu.Unlock(); return s.err }

// StartStream continuously submits input values to the same finite Pipeline
// engine used by Run and Worker. Closing inputs gracefully drains the Stream.
func (p Pipeline) StartStream(ctx context.Context, inputs <-chan any, options StreamOptions) (*Stream, error) {
	if ctx == nil {
		return nil, errors.New("pipeflow: Stream context cannot be nil")
	}
	if inputs == nil {
		return nil, errors.New("pipeflow: Stream inputs cannot be nil")
	}
	if options.FailurePolicy == "" {
		options.FailurePolicy = StreamContinue
	}
	if options.FailurePolicy != StreamContinue && options.FailurePolicy != StreamStop {
		return nil, fmt.Errorf("pipeflow: invalid Stream failure policy %q", options.FailurePolicy)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	worker, err := p.StartWorker(streamCtx, WorkerOptions{Workers: options.Workers, Buffer: options.Buffer, MaxInFlight: options.MaxInFlight})
	if err != nil {
		cancel()
		return nil, err
	}
	s := &Stream{results: make(chan StreamResult), done: make(chan struct{}), cancel: cancel, worker: worker, failurePolicy: options.FailurePolicy}
	go s.run(streamCtx, inputs, worker, options.FailurePolicy)
	return s, nil
}

func (s *Stream) run(ctx context.Context, inputs <-chan any, worker *Worker, policy StreamItemFailurePolicy) {
	defer close(s.done)
	defer close(s.results)
	defer s.cancel()
	type sequencedWork struct {
		sequence uint64
		work     *Work
	}
	pending := make(chan sequencedWork)
	collectorDone := make(chan struct{})
	emit := func(result StreamResult) {
		select {
		case s.results <- result:
		case <-ctx.Done():
		}
		if result.Err != nil && policy == StreamStop {
			worker.Cancel()
			s.cancel()
		}
	}
	go func() {
		defer close(collectorDone)
		if workerCount := streamWorkerCount(worker); workerCount == 1 {
			for item := range pending {
				output, report, err := item.work.Wait()
				emit(StreamResult{Sequence: item.sequence, Output: output, Report: report, Err: err})
			}
			return
		}
		var waits sync.WaitGroup
		for item := range pending {
			item := item
			waits.Add(1)
			go func() {
				defer waits.Done()
				output, report, err := item.work.Wait()
				emit(StreamResult{Sequence: item.sequence, Output: output, Report: report, Err: err})
			}()
		}
		waits.Wait()
	}()
	var sequence uint64
	feedDone := false
	for !feedDone {
		select {
		case <-ctx.Done():
			feedDone = true
		case input, ok := <-inputs:
			if !ok {
				feedDone = true
				break
			}
			sequence++
			work, err := worker.Submit(ctx, input)
			if err != nil {
				feedDone = true
				break
			}
			select {
			case pending <- sequencedWork{sequence: sequence, work: work}:
			case <-ctx.Done():
				feedDone = true
			}
		}
	}
	worker.Close()
	close(pending)
	<-collectorDone
	workerErr := worker.Wait()
	if ctx.Err() != nil {
		s.err = ctx.Err()
	} else {
		s.err = workerErr
	}
}

func streamWorkerCount(worker *Worker) int { return worker.workerCount }

var errQueueClosed = errors.New("pipeflow: runtime is closed")

type boundedQueue[T any] struct {
	mu       sync.Mutex
	items    []T
	capacity int
	closed   bool
	waiters  int
	changed  chan struct{}
}

func newBoundedQueue[T any](capacity int) *boundedQueue[T] {
	return &boundedQueue[T]{capacity: capacity, changed: make(chan struct{})}
}
func (q *boundedQueue[T]) signal() { close(q.changed); q.changed = make(chan struct{}) }
func (q *boundedQueue[T]) enqueue(ctx context.Context, value T) error {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return errQueueClosed
		}
		if len(q.items) < q.capacity || (q.capacity == 0 && q.waiters > 0) {
			q.items = append(q.items, value)
			q.signal()
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (q *boundedQueue[T]) dequeue(ctx context.Context) (T, bool) {
	var zero T
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			value := q.items[0]
			q.items[0] = zero
			q.items = q.items[1:]
			q.signal()
			q.mu.Unlock()
			return value, true
		}
		if q.closed {
			q.mu.Unlock()
			return zero, false
		}
		q.waiters++
		if q.waiters == 1 {
			q.signal()
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		q.mu.Lock()
		q.waiters--
		q.mu.Unlock()
		if ctx.Err() != nil {
			return zero, false
		}
	}
}
func (q *boundedQueue[T]) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.signal()
	}
	q.mu.Unlock()
}
func (q *boundedQueue[T]) len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }
