package pipeflow

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

// ObservationScope identifies a unit in Pipeflow's execution hierarchy.
type ObservationScope string

const (
	ObservationPipeline   ObservationScope = "pipeline"
	ObservationStage      ObservationScope = "stage"
	ObservationStep       ObservationScope = "step"
	ObservationAttempt    ObservationScope = "attempt"
	ObservationParallel   ObservationScope = "parallel"
	ObservationBranch     ObservationScope = "branch"
	ObservationSubflow    ObservationScope = "subflow"
	ObservationBackground ObservationScope = "background"
)

// ObservationPhase describes a timestamped execution transition.
type ObservationPhase string

const (
	ObservationStarted   ObservationPhase = "started"
	ObservationCompleted ObservationPhase = "completed"
	ObservationFailed    ObservationPhase = "failed"
	ObservationSkipped   ObservationPhase = "skipped"
	ObservationFinalized ObservationPhase = "finalized"
)

// ObservationLocation identifies an execution unit without carrying business data.
type ObservationLocation struct {
	RunID, Pipeline, Stage, Step, Parallel, Branch, Subflow, Background string
	Attempt, Poll                                                       int
}

// ObservationError classifies a failure without exposing its message or payload.
type ObservationError struct {
	Type  string
	Panic bool
}

// TraceEvent is a timestamped, payload-free execution fact.
type TraceEvent struct {
	Scope      ObservationScope
	Phase      ObservationPhase
	Location   ObservationLocation
	Status     Status
	OccurredAt time.Time
	Duration   time.Duration
	Error      *ObservationError
}

// MetricSample is a raw numeric sample. Aggregation, rates, and percentiles are
// deliberately responsibilities of observation consumers.
type MetricSample struct {
	Name       string
	Scope      ObservationScope
	Location   ObservationLocation
	Status     Status
	Value      float64
	Unit       string
	OccurredAt time.Time
}

// ProfileSample attributes elapsed execution time to one hierarchy location.
type ProfileSample struct {
	Location   ObservationLocation
	Scope      ObservationScope
	Duration   time.Duration
	Status     Status
	OccurredAt time.Time
}

// Observer consumes Pipeflow telemetry. Calls are serialized, and implementations
// should return promptly. Observer panics are recovered and never affect execution.
type Observer interface {
	ObserveTrace(TraceEvent)
	ObserveMetric(MetricSample)
	ObserveProfile(ProfileSample)
}

// ObserverFuncs makes it convenient to consume only the families an application needs.
type ObserverFuncs struct {
	Trace   func(TraceEvent)
	Metric  func(MetricSample)
	Profile func(ProfileSample)
}

func (o ObserverFuncs) ObserveTrace(v TraceEvent) {
	if o.Trace != nil {
		o.Trace(v)
	}
}
func (o ObserverFuncs) ObserveMetric(v MetricSample) {
	if o.Metric != nil {
		o.Metric(v)
	}
}
func (o ObserverFuncs) ObserveProfile(v ProfileSample) {
	if o.Profile != nil {
		o.Profile(v)
	}
}

type observationDispatcher struct {
	observer Observer
	mu       sync.Mutex
	starts   map[observationKey]time.Time
}

type observationKey struct {
	scope    ObservationScope
	location ObservationLocation
}

func newObservationDispatcher(observer Observer) *observationDispatcher {
	if observer == nil {
		return nil
	}
	return &observationDispatcher{observer: observer, starts: make(map[observationKey]time.Time)}
}

func (d *observationDispatcher) emit(scope ObservationScope, phase ObservationPhase, location ObservationLocation, status Status, err error) {
	if d == nil {
		return
	}
	now := time.Now()
	key := observationKey{scope: scope, location: location}
	d.mu.Lock()
	var duration time.Duration
	if phase == ObservationStarted {
		d.starts[key] = now
	} else if start, ok := d.starts[key]; ok {
		duration = now.Sub(start)
		delete(d.starts, key)
	}
	event := TraceEvent{Scope: scope, Phase: phase, Location: location, Status: status, OccurredAt: now, Duration: duration, Error: classifyObservationError(err)}
	safeObserve(func() { d.observer.ObserveTrace(event) })
	if phase == ObservationCompleted || phase == ObservationFailed || phase == ObservationSkipped {
		safeObserve(func() {
			d.observer.ObserveMetric(MetricSample{Name: "pipeflow.execution.count", Scope: scope, Location: location, Status: status, Value: 1, Unit: "count", OccurredAt: now})
		})
		safeObserve(func() {
			d.observer.ObserveMetric(MetricSample{Name: "pipeflow.execution.duration", Scope: scope, Location: location, Status: status, Value: float64(duration), Unit: "nanoseconds", OccurredAt: now})
		})
		safeObserve(func() {
			d.observer.ObserveProfile(ProfileSample{Location: location, Scope: scope, Duration: duration, Status: status, OccurredAt: now})
		})
	}
	d.mu.Unlock()
}

func safeObserve(fn func()) { defer func() { _ = recover() }(); fn() }

func classifyObservationError(err error) *ObservationError {
	if err == nil {
		return nil
	}
	t := reflect.TypeOf(err)
	result := &ObservationError{Type: t.String()}
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		result.Panic = true
	}
	return result
}
