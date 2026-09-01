package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type BackgroundFailurePolicy int

const (
	BackgroundFatal BackgroundFailurePolicy = iota
	BackgroundNonFatal
)

type BackgroundFunc func(context.Context) error

type BackgroundOption func(*backgroundTask)

func WithBackgroundFailurePolicy(policy BackgroundFailurePolicy) BackgroundOption {
	return func(task *backgroundTask) { task.failurePolicy = policy; task.failurePolicySet = true }
}

type backgroundTask struct {
	name             string
	fn               BackgroundFunc
	failurePolicy    BackgroundFailurePolicy
	failurePolicySet bool
}

// BackgroundError identifies a failed fatal background task.
type BackgroundError struct {
	Pipeline string
	Name     string
	Err      error
}

func (e *BackgroundError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("pipeflow: pipeline %q background %q: %v", e.Pipeline, e.Name, e.Err)
}

func (e *BackgroundError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type backgroundResult struct {
	err            error
	contextWasDone bool
	hookFailure    bool
}

type backgroundManager struct {
	pipeline   string
	tasks      []backgroundTask
	ctx        context.Context
	cancel     context.CancelFunc
	recorder   *runRecorder
	dispatcher *lifecycleDispatcher
	results    []backgroundResult
	wg         sync.WaitGroup
}

func startBackgrounds(parent context.Context, cancelExecution context.CancelFunc, pipeline string, tasks []backgroundTask, recorder *runRecorder, dispatcher *lifecycleDispatcher) *backgroundManager {
	manager := &backgroundManager{
		pipeline: pipeline, tasks: tasks, ctx: parent, cancel: cancelExecution,
		recorder: recorder, dispatcher: dispatcher, results: make([]backgroundResult, len(tasks)),
	}
	manager.wg.Add(len(tasks))
	for i := range tasks {
		recorder.startBackground(i)
		if hookErr := dispatcher.emitBackground(BackgroundStarted, tasks[i].name, StatusRunning, nil); hookErr != nil {
			recorder.finishBackground(i, hookErr)
			manager.results[i] = backgroundResult{err: hookErr, hookFailure: true}
			manager.wg.Done()
			cancelExecution()
			continue
		}
		go manager.run(i)
	}
	return manager
}

func (m *backgroundManager) run(index int) {
	defer m.wg.Done()
	task := m.tasks[index]
	err := runBackground(m.ctx, task.fn)
	contextWasDone := m.ctx.Err() != nil
	eventEmitted := !expectedBackgroundCancellation(err, contextWasDone)
	var hookErr error
	if eventEmitted {
		if err != nil {
			hookErr = m.dispatcher.emitBackground(BackgroundFailed, task.name, statusForError(err), err)
		} else {
			hookErr = m.dispatcher.emitBackground(BackgroundCompleted, task.name, StatusCompleted, nil)
		}
	}
	if hookErr != nil {
		err = errors.Join(err, hookErr)
	}
	m.recorder.finishBackground(index, err)
	m.results[index] = backgroundResult{err: err, contextWasDone: contextWasDone, hookFailure: hookErr != nil}
	if err != nil && task.failurePolicy == BackgroundFatal && !expectedBackgroundCancellation(err, contextWasDone) {
		m.cancel()
	}
}

func (m *backgroundManager) stopAndWait(mainErr, parentErr error) error {
	m.cancel()
	m.wg.Wait()
	var fatal []error
	for i, result := range m.results {
		task := m.tasks[i]
		if expectedBackgroundCancellation(result.err, result.contextWasDone) {
			var hookErr error
			if mainErr == nil && parentErr == nil {
				m.recorder.normalizeBackgroundCompletion(i)
				hookErr = m.dispatcher.emitBackground(BackgroundCompleted, task.name, StatusCompleted, nil)
			} else {
				hookErr = m.dispatcher.emitBackground(BackgroundFailed, task.name, statusForError(result.err), result.err)
			}
			if hookErr != nil {
				m.recorder.finishBackground(i, hookErr)
				fatal = append(fatal, &BackgroundError{Pipeline: m.pipeline, Name: task.name, Err: hookErr})
			}
			continue
		}
		if result.err == nil {
			continue
		}
		if task.failurePolicy == BackgroundFatal || result.hookFailure {
			fatal = append(fatal, &BackgroundError{Pipeline: m.pipeline, Name: task.name, Err: result.err})
		}
	}
	if len(fatal) == 0 {
		return mainErr
	}
	if mainErr != nil && !(errors.Is(mainErr, context.Canceled) && parentErr == nil) {
		fatal = append([]error{mainErr}, fatal...)
	}
	return errors.Join(fatal...)
}

func expectedBackgroundCancellation(err error, contextWasDone bool) bool {
	return contextWasDone && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

func runBackground(ctx context.Context, fn BackgroundFunc) (err error) {
	defer recoverPanic(&err)
	return fn(ctx)
}
