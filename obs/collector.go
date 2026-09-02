package obs

import (
	"fmt"
	"sort"
	"sync"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
)

type Options struct {
	TraceCapacity   int
	MetricCapacity  int
	ProfileCapacity int
	RunCapacity     int
}

func (o Options) normalized() Options {
	if o.TraceCapacity <= 0 {
		o.TraceCapacity = 1000
	}
	if o.MetricCapacity <= 0 {
		o.MetricCapacity = 1000
	}
	if o.ProfileCapacity <= 0 {
		o.ProfileCapacity = 1000
	}
	if o.RunCapacity <= 0 {
		o.RunCapacity = 250
	}
	return o
}

// Source is the read-only boundary consumed by future transports and clients.
type Source interface{ Snapshot() Snapshot }

type Health string

const (
	HealthUnknown  Health = "unknown"
	HealthHealthy  Health = "healthy"
	HealthBusy     Health = "busy"
	HealthDegraded Health = "degraded"
)

type HealthEvidence struct {
	Health  Health
	Reasons []string
}

type RunView struct {
	RunID, Pipeline    string
	Status             pipeflow.Status
	StartedAt, EndedAt time.Time
	Duration           time.Duration
}

type PipelineView struct {
	Name                                   string
	Health                                 HealthEvidence
	ActiveRuns, Started, Completed, Failed int64
	LastStatus                             pipeflow.Status
	LastActivity                           time.Time
}

type ErrorGroup struct {
	Pipeline, Stage, Step, Type string
	Panic                       bool
	Occurrences                 int64
	FirstSeen, LastSeen         time.Time
}

type MetricAggregate struct {
	Name                  string
	Scope                 pipeflow.ObservationScope
	Pipeline, Stage, Step string
	Status                pipeflow.Status
	Samples               int64
	Sum                   float64
	Unit                  string
	FirstSeen, LastSeen   time.Time
}

type ProfileAggregate struct {
	Scope                 pipeflow.ObservationScope
	Pipeline, Stage, Step string
	Samples               int64
	Total                 time.Duration
	Maximum               time.Duration
}

type Snapshot struct {
	CapturedAt                                     time.Time
	Pipelines                                      []PipelineView
	Runs                                           []RunView
	Errors                                         []ErrorGroup
	Metrics                                        []MetricAggregate
	Profiles                                       []ProfileAggregate
	Traces                                         []pipeflow.TraceEvent
	RecentMetrics                                  []pipeflow.MetricSample
	RecentProfiles                                 []pipeflow.ProfileSample
	DroppedTraces, DroppedMetrics, DroppedProfiles uint64
}

type runKey string
type metricKey struct {
	name                  string
	scope                 pipeflow.ObservationScope
	pipeline, stage, step string
	status                pipeflow.Status
	unit                  string
}
type profileKey struct {
	scope                 pipeflow.ObservationScope
	pipeline, stage, step string
}
type errorKey struct {
	pipeline, stage, step, kind string
	panic                       bool
}

type pipelineState struct{ view PipelineView }

// Collector is a concurrency-safe in-process telemetry consumer.
type Collector struct {
	mu                                             sync.RWMutex
	options                                        Options
	traces                                         []pipeflow.TraceEvent
	metrics                                        []pipeflow.MetricSample
	profiles                                       []pipeflow.ProfileSample
	droppedTraces, droppedMetrics, droppedProfiles uint64
	runs                                           map[runKey]RunView
	runOrder                                       []runKey
	pipelines                                      map[string]*pipelineState
	metricAggregates                               map[metricKey]MetricAggregate
	profileAggregates                              map[profileKey]ProfileAggregate
	errors                                         map[errorKey]ErrorGroup
}

func NewCollector(options Options) *Collector {
	return &Collector{options: options.normalized(), runs: make(map[runKey]RunView), pipelines: make(map[string]*pipelineState), metricAggregates: make(map[metricKey]MetricAggregate), profileAggregates: make(map[profileKey]ProfileAggregate), errors: make(map[errorKey]ErrorGroup)}
}

func (c *Collector) ObserveTrace(event pipeflow.TraceEvent) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces, c.droppedTraces = appendBounded(c.traces, event, c.options.TraceCapacity, c.droppedTraces)
	c.observeRun(event)
	if event.Error != nil {
		key := errorKey{pipeline: event.Location.Pipeline, stage: event.Location.Stage, step: event.Location.Step, kind: event.Error.Type, panic: event.Error.Panic}
		group := c.errors[key]
		if group.Occurrences == 0 {
			group = ErrorGroup{Pipeline: key.pipeline, Stage: key.stage, Step: key.step, Type: key.kind, Panic: key.panic, FirstSeen: event.OccurredAt}
		}
		group.Occurrences++
		group.LastSeen = event.OccurredAt
		c.errors[key] = group
	}
}

func (c *Collector) observeRun(event pipeflow.TraceEvent) {
	if event.Scope != pipeflow.ObservationPipeline || event.Location.RunID == "" {
		return
	}
	key := runKey(event.Location.RunID)
	run, exists := c.runs[key]
	state := c.pipelines[event.Location.Pipeline]
	if state == nil {
		state = &pipelineState{view: PipelineView{Name: event.Location.Pipeline}}
		c.pipelines[event.Location.Pipeline] = state
	}
	state.view.LastActivity, state.view.LastStatus = event.OccurredAt, event.Status
	if event.Phase == pipeflow.ObservationFinalized {
		return
	}
	if event.Phase == pipeflow.ObservationStarted {
		if !exists {
			c.ensureRunCapacity()
			c.runOrder = append(c.runOrder, key)
			state.view.Started++
			state.view.ActiveRuns++
		}
		run = RunView{RunID: event.Location.RunID, Pipeline: event.Location.Pipeline, Status: event.Status, StartedAt: event.OccurredAt}
	} else {
		if !exists {
			c.ensureRunCapacity()
			c.runOrder = append(c.runOrder, key)
			run = RunView{RunID: event.Location.RunID, Pipeline: event.Location.Pipeline}
		}
		run.Status, run.EndedAt, run.Duration = event.Status, event.OccurredAt, event.Duration
		if exists && run.StartedAt.IsZero() == false && event.Phase != pipeflow.ObservationFinalized {
			if state.view.ActiveRuns > 0 {
				state.view.ActiveRuns--
			}
			if event.Phase == pipeflow.ObservationCompleted {
				state.view.Completed++
			} else if event.Phase == pipeflow.ObservationFailed {
				state.view.Failed++
			}
		}
	}
	c.runs[key] = run
}

func (c *Collector) ensureRunCapacity() {
	if len(c.runOrder) < c.options.RunCapacity {
		return
	}
	for i, key := range c.runOrder {
		if c.runs[key].EndedAt.IsZero() {
			continue
		}
		delete(c.runs, key)
		c.runOrder = append(c.runOrder[:i], c.runOrder[i+1:]...)
		return
	}
	// Active runs are never discarded merely to satisfy historical retention.
}

func (c *Collector) ObserveMetric(sample pipeflow.MetricSample) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics, c.droppedMetrics = appendBounded(c.metrics, sample, c.options.MetricCapacity, c.droppedMetrics)
	key := metricKey{name: sample.Name, scope: sample.Scope, pipeline: sample.Location.Pipeline, stage: sample.Location.Stage, step: sample.Location.Step, status: sample.Status, unit: sample.Unit}
	agg := c.metricAggregates[key]
	if agg.Samples == 0 {
		agg = MetricAggregate{Name: key.name, Scope: key.scope, Pipeline: key.pipeline, Stage: key.stage, Step: key.step, Status: key.status, Unit: key.unit, FirstSeen: sample.OccurredAt}
	}
	agg.Samples++
	agg.Sum += sample.Value
	agg.LastSeen = sample.OccurredAt
	c.metricAggregates[key] = agg
}

func (c *Collector) ObserveProfile(sample pipeflow.ProfileSample) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.profiles, c.droppedProfiles = appendBounded(c.profiles, sample, c.options.ProfileCapacity, c.droppedProfiles)
	key := profileKey{scope: sample.Scope, pipeline: sample.Location.Pipeline, stage: sample.Location.Stage, step: sample.Location.Step}
	agg := c.profileAggregates[key]
	if agg.Samples == 0 {
		agg = ProfileAggregate{Scope: key.scope, Pipeline: key.pipeline, Stage: key.stage, Step: key.step}
	}
	agg.Samples++
	agg.Total += sample.Duration
	if sample.Duration > agg.Maximum {
		agg.Maximum = sample.Duration
	}
	c.profileAggregates[key] = agg
}

func appendBounded[T any](values []T, value T, capacity int, dropped uint64) ([]T, uint64) {
	if len(values) >= capacity {
		copy(values, values[1:])
		values[len(values)-1] = value
		return values, dropped + 1
	}
	return append(values, value), dropped
}

func (c *Collector) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{CapturedAt: time.Now()}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := Snapshot{CapturedAt: time.Now(), Traces: append([]pipeflow.TraceEvent(nil), c.traces...), RecentMetrics: append([]pipeflow.MetricSample(nil), c.metrics...), RecentProfiles: append([]pipeflow.ProfileSample(nil), c.profiles...), DroppedTraces: c.droppedTraces, DroppedMetrics: c.droppedMetrics, DroppedProfiles: c.droppedProfiles}
	for _, state := range c.pipelines {
		view := state.view
		view.Health = deriveHealth(view)
		result.Pipelines = append(result.Pipelines, view)
	}
	for _, run := range c.runs {
		result.Runs = append(result.Runs, run)
	}
	for _, group := range c.errors {
		result.Errors = append(result.Errors, group)
	}
	for _, aggregate := range c.metricAggregates {
		result.Metrics = append(result.Metrics, aggregate)
	}
	for _, aggregate := range c.profileAggregates {
		result.Profiles = append(result.Profiles, aggregate)
	}
	sortSnapshot(&result)
	return result
}

func deriveHealth(view PipelineView) HealthEvidence {
	if view.ActiveRuns > 0 {
		return HealthEvidence{Health: HealthBusy, Reasons: []string{fmt.Sprintf("%d active run(s)", view.ActiveRuns)}}
	}
	if view.Failed > 0 {
		return HealthEvidence{Health: HealthDegraded, Reasons: []string{fmt.Sprintf("%d observed failed run(s)", view.Failed)}}
	}
	if view.Completed > 0 {
		return HealthEvidence{Health: HealthHealthy, Reasons: []string{fmt.Sprintf("%d observed completed run(s)", view.Completed)}}
	}
	return HealthEvidence{Health: HealthUnknown, Reasons: []string{"no completed run evidence"}}
}

func sortSnapshot(s *Snapshot) {
	sort.Slice(s.Pipelines, func(i, j int) bool { return s.Pipelines[i].Name < s.Pipelines[j].Name })
	sort.Slice(s.Runs, func(i, j int) bool { return s.Runs[i].StartedAt.Before(s.Runs[j].StartedAt) })
	sort.Slice(s.Errors, func(i, j int) bool {
		a, b := s.Errors[i], s.Errors[j]
		if a.Pipeline != b.Pipeline {
			return a.Pipeline < b.Pipeline
		}
		if a.Stage != b.Stage {
			return a.Stage < b.Stage
		}
		if a.Step != b.Step {
			return a.Step < b.Step
		}
		return a.Type < b.Type
	})
	sort.Slice(s.Metrics, func(i, j int) bool {
		a, b := s.Metrics[i], s.Metrics[j]
		if a.Pipeline != b.Pipeline {
			return a.Pipeline < b.Pipeline
		}
		return a.Name < b.Name
	})
	sort.Slice(s.Profiles, func(i, j int) bool { return s.Profiles[i].Total > s.Profiles[j].Total })
}
