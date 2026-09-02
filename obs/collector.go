package obs

import (
	"errors"
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
	Health  Health   `json:"health"`
	Reasons []string `json:"reasons"`
}

type RunView struct {
	RunID     string          `json:"run_id"`
	Pipeline  string          `json:"pipeline"`
	Status    pipeflow.Status `json:"status"`
	StartedAt time.Time       `json:"started_at"`
	EndedAt   time.Time       `json:"ended_at"`
	Duration  time.Duration   `json:"duration_ns"`
}

type PipelineView struct {
	Name         string          `json:"name"`
	Health       HealthEvidence  `json:"health"`
	ActiveRuns   int64           `json:"active_runs"`
	Started      int64           `json:"started"`
	Completed    int64           `json:"completed"`
	Failed       int64           `json:"failed"`
	LastStatus   pipeflow.Status `json:"last_status"`
	LastActivity time.Time       `json:"last_activity"`
}

type ErrorGroup struct {
	Pipeline    string    `json:"pipeline"`
	Stage       string    `json:"stage,omitempty"`
	Step        string    `json:"step,omitempty"`
	Type        string    `json:"type"`
	Panic       bool      `json:"panic"`
	Occurrences int64     `json:"occurrences"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
}

type MetricAggregate struct {
	Name      string                    `json:"name"`
	Scope     pipeflow.ObservationScope `json:"scope"`
	Pipeline  string                    `json:"pipeline"`
	Stage     string                    `json:"stage,omitempty"`
	Step      string                    `json:"step,omitempty"`
	Status    pipeflow.Status           `json:"status"`
	Samples   int64                     `json:"samples"`
	Sum       float64                   `json:"sum"`
	Unit      string                    `json:"unit"`
	FirstSeen time.Time                 `json:"first_seen"`
	LastSeen  time.Time                 `json:"last_seen"`
}

type ProfileAggregate struct {
	Scope    pipeflow.ObservationScope `json:"scope"`
	Pipeline string                    `json:"pipeline"`
	Stage    string                    `json:"stage,omitempty"`
	Step     string                    `json:"step,omitempty"`
	Samples  int64                     `json:"samples"`
	Total    time.Duration             `json:"total_ns"`
	Maximum  time.Duration             `json:"maximum_ns"`
}

// WorkerView is a read-only operational view of a tracked Core Worker.
type WorkerView struct {
	Name        string          `json:"name"`
	Pipeline    string          `json:"pipeline"`
	Status      pipeflow.Status `json:"status"`
	Concurrency int             `json:"concurrency"`
	Active      int             `json:"active"`
	InFlight    int             `json:"in_flight"`
	Submitted   uint64          `json:"submitted"`
	Completed   uint64          `json:"completed"`
	Failed      uint64          `json:"failed"`
}

// QueueView is the payload-free queue portion of a tracked Worker runtime.
type QueueView struct {
	Name        string  `json:"name"`
	Worker      string  `json:"worker"`
	Pipeline    string  `json:"pipeline"`
	Depth       int     `json:"depth"`
	Capacity    int     `json:"capacity"`
	MaxInFlight int     `json:"max_in_flight"`
	InFlight    int     `json:"in_flight"`
	Utilization float64 `json:"utilization"`
}

type RecoveryView struct {
	Pipeline     string          `json:"pipeline"`
	Stage        string          `json:"stage"`
	Step         string          `json:"step"`
	Name         string          `json:"name"`
	Activations  uint64          `json:"activations"`
	Completed    uint64          `json:"completed"`
	Failed       uint64          `json:"failed"`
	RetryStep    uint64          `json:"retry_step"`
	FailStep     uint64          `json:"fail_step"`
	FailStage    uint64          `json:"fail_stage"`
	FailPipeline uint64          `json:"fail_pipeline"`
	LastStatus   pipeflow.Status `json:"last_status"`
	LastSeen     time.Time       `json:"last_seen"`
}

type CircuitView struct {
	Dependency    string                `json:"dependency"`
	State         pipeflow.CircuitState `json:"state"`
	Events        uint64                `json:"events"`
	Failures      uint64                `json:"failures"`
	ShortCircuits uint64                `json:"short_circuits"`
	Probes        uint64                `json:"probes"`
	LastSeen      time.Time             `json:"last_seen"`
}

type IdempotencyView struct {
	Guard               string    `json:"guard"`
	Executed            uint64    `json:"executed"`
	DuplicateCompleted  uint64    `json:"duplicate_completed"`
	DuplicateInProgress uint64    `json:"duplicate_in_progress"`
	FailedReleasable    uint64    `json:"failed_releasable"`
	StoreFailures       uint64    `json:"store_failures"`
	LastSeen            time.Time `json:"last_seen"`
}

// DefinitionView is immutable Pipeline topology and optional resolved config.
type DefinitionView struct {
	Name        string
	Description pipeflow.Description
	Effective   *pipeflow.EffectivePipelineConfig
}

type Snapshot struct {
	CapturedAt                                     time.Time
	Pipelines                                      []PipelineView
	Runs                                           []RunView
	Errors                                         []ErrorGroup
	Metrics                                        []MetricAggregate
	Profiles                                       []ProfileAggregate
	Workers                                        []WorkerView
	Queues                                         []QueueView
	Recoveries                                     []RecoveryView
	Circuits                                       []CircuitView
	Idempotency                                    []IdempotencyView
	Definitions                                    []DefinitionView
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
type trackedWorker struct {
	worker *pipeflow.Worker
}
type trackedDefinition struct{ view DefinitionView }

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
	workers                                        map[string]*trackedWorker
	recoveries                                     map[string]RecoveryView
	circuits                                       map[string]CircuitView
	idempotency                                    map[string]IdempotencyView
	definitions                                    map[string]*trackedDefinition
}

func NewCollector(options Options) *Collector {
	return &Collector{options: options.normalized(), runs: make(map[runKey]RunView), pipelines: make(map[string]*pipelineState), metricAggregates: make(map[metricKey]MetricAggregate), profileAggregates: make(map[profileKey]ProfileAggregate), errors: make(map[errorKey]ErrorGroup), workers: make(map[string]*trackedWorker), recoveries: make(map[string]RecoveryView), circuits: make(map[string]CircuitView), idempotency: make(map[string]IdempotencyView), definitions: make(map[string]*trackedDefinition)}
}

// TrackPipeline adds immutable topology and effective configuration to future
// snapshots. It never observes or controls executions.
func (c *Collector) TrackPipeline(pipeline pipeflow.Pipeline) (func(), error) {
	if c == nil {
		return nil, errors.New("pipeflow obs: nil Collector")
	}
	if err := pipeline.Validate(); err != nil {
		return nil, fmt.Errorf("pipeflow obs: track Pipeline: %w", err)
	}
	description := pipeline.Describe()
	view := DefinitionView{Name: description.Name, Description: description}
	if effective, ok := pipeline.EffectiveConfig(); ok {
		view.Effective = &effective
	}
	registration := &trackedDefinition{view: view}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.definitions[view.Name]; exists {
		return nil, fmt.Errorf("pipeflow obs: Pipeline %q is already tracked", view.Name)
	}
	c.definitions[view.Name] = registration
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.definitions[view.Name] == registration {
			delete(c.definitions, view.Name)
		}
	}, nil
}

// TrackWorker adds a named Core Worker to future operational snapshots. The
// returned function removes exactly this registration and is safe to call more
// than once.
func (c *Collector) TrackWorker(name string, worker *pipeflow.Worker) (func(), error) {
	if c == nil {
		return nil, errors.New("pipeflow obs: nil Collector")
	}
	if name == "" {
		return nil, errors.New("pipeflow obs: Worker name cannot be empty")
	}
	if worker == nil {
		return nil, errors.New("pipeflow obs: nil Worker")
	}
	registration := &trackedWorker{worker: worker}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.workers[name]; exists {
		return nil, fmt.Errorf("pipeflow obs: Worker %q is already tracked", name)
	}
	c.workers[name] = registration
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.workers[name] == registration {
			delete(c.workers, name)
		}
	}, nil
}

func (c *Collector) ObserveTrace(event pipeflow.TraceEvent) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces, c.droppedTraces = appendBounded(c.traces, event, c.options.TraceCapacity, c.droppedTraces)
	c.observeRun(event)
	c.observeResilience(event)
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

func (c *Collector) observeResilience(event pipeflow.TraceEvent) {
	switch event.Scope {
	case pipeflow.ObservationRecovery:
		key := event.Location.Pipeline + "\x00" + event.Location.Stage + "\x00" + event.Location.Step + "\x00" + event.Location.Recovery
		view := c.recoveries[key]
		view.Pipeline, view.Stage, view.Step, view.Name = event.Location.Pipeline, event.Location.Stage, event.Location.Step, event.Location.Recovery
		if event.Phase == pipeflow.ObservationStarted {
			view.Activations++
		}
		if event.Phase == pipeflow.ObservationCompleted {
			view.Completed++
		}
		if event.Phase == pipeflow.ObservationFailed {
			view.Failed++
		}
		switch event.Location.RecoveryDecision {
		case pipeflow.RecoveryRetryStep:
			view.RetryStep++
		case pipeflow.RecoveryFailStep:
			view.FailStep++
		case pipeflow.RecoveryFailStage:
			view.FailStage++
		case pipeflow.RecoveryFailPipeline:
			view.FailPipeline++
		}
		view.LastStatus, view.LastSeen = event.Status, event.OccurredAt
		c.recoveries[key] = view
	case pipeflow.ObservationCircuit:
		view := c.circuits[event.Location.Dependency]
		view.Dependency, view.State, view.LastSeen = event.Location.Dependency, event.Location.CircuitState, event.OccurredAt
		view.Events++
		if event.Phase == pipeflow.ObservationFailed {
			view.Failures++
		}
		if event.Location.ShortCircuited {
			view.ShortCircuits++
		}
		if event.Location.Probe {
			view.Probes++
		}
		c.circuits[view.Dependency] = view
	case pipeflow.ObservationIdempotency:
		view := c.idempotency[event.Location.Guard]
		view.Guard, view.LastSeen = event.Location.Guard, event.OccurredAt
		switch event.Location.IdempotencyOutcome {
		case pipeflow.IdempotencyExecuted:
			view.Executed++
		case pipeflow.IdempotencyDuplicateCompleted:
			view.DuplicateCompleted++
		case pipeflow.IdempotencyDuplicateInProgress:
			view.DuplicateInProgress++
		case pipeflow.IdempotencyFailedReleasable:
			view.FailedReleasable++
		case pipeflow.IdempotencyStoreFailure:
			view.StoreFailures++
		}
		c.idempotency[view.Guard] = view
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
	for name, registration := range c.workers {
		state := registration.worker.State()
		result.Workers = append(result.Workers, WorkerView{Name: name, Pipeline: state.Pipeline, Status: state.Status, Concurrency: state.Workers, Active: state.Active, InFlight: state.InFlight, Submitted: state.Submitted, Completed: state.Completed, Failed: state.Failed})
		utilization := 0.0
		if state.BufferCapacity > 0 {
			utilization = float64(state.QueueDepth) / float64(state.BufferCapacity)
		}
		result.Queues = append(result.Queues, QueueView{Name: name + ".queue", Worker: name, Pipeline: state.Pipeline, Depth: state.QueueDepth, Capacity: state.BufferCapacity, MaxInFlight: state.MaxInFlight, InFlight: state.InFlight, Utilization: utilization})
	}
	for _, view := range c.recoveries {
		result.Recoveries = append(result.Recoveries, view)
	}
	for _, view := range c.circuits {
		result.Circuits = append(result.Circuits, view)
	}
	for _, view := range c.idempotency {
		result.Idempotency = append(result.Idempotency, view)
	}
	for _, registration := range c.definitions {
		result.Definitions = append(result.Definitions, cloneDefinition(registration.view))
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
	sort.Slice(s.Workers, func(i, j int) bool { return s.Workers[i].Name < s.Workers[j].Name })
	sort.Slice(s.Queues, func(i, j int) bool { return s.Queues[i].Name < s.Queues[j].Name })
	sort.Slice(s.Recoveries, func(i, j int) bool { return s.Recoveries[i].Name < s.Recoveries[j].Name })
	sort.Slice(s.Circuits, func(i, j int) bool { return s.Circuits[i].Dependency < s.Circuits[j].Dependency })
	sort.Slice(s.Idempotency, func(i, j int) bool { return s.Idempotency[i].Guard < s.Idempotency[j].Guard })
	sort.Slice(s.Definitions, func(i, j int) bool { return s.Definitions[i].Name < s.Definitions[j].Name })
}

func cloneDefinition(view DefinitionView) DefinitionView {
	view.Description = cloneDescription(view.Description)
	if view.Effective != nil {
		copy := cloneEffective(*view.Effective)
		view.Effective = &copy
	}
	return view
}

func cloneEffective(value pipeflow.EffectivePipelineConfig) pipeflow.EffectivePipelineConfig {
	value.Stages = append([]pipeflow.EffectiveStageConfig(nil), value.Stages...)
	for i := range value.Stages {
		value.Stages[i] = cloneEffectiveStage(value.Stages[i])
	}
	value.Backgrounds = append([]pipeflow.EffectiveBackgroundConfig(nil), value.Backgrounds...)
	return value
}

func cloneEffectiveStage(value pipeflow.EffectiveStageConfig) pipeflow.EffectiveStageConfig {
	value.Steps = append([]pipeflow.EffectiveStepConfig(nil), value.Steps...)
	value.Parallels = append([]pipeflow.EffectiveParallelConfig(nil), value.Parallels...)
	for i := range value.Parallels {
		value.Parallels[i].Branches = append([]pipeflow.EffectiveBranchConfig(nil), value.Parallels[i].Branches...)
		for j := range value.Parallels[i].Branches {
			value.Parallels[i].Branches[j].Steps = append([]pipeflow.EffectiveStepConfig(nil), value.Parallels[i].Branches[j].Steps...)
		}
	}
	value.Subflows = append([]pipeflow.EffectiveSubflowConfig(nil), value.Subflows...)
	for i := range value.Subflows {
		value.Subflows[i].Stages = append([]pipeflow.EffectiveStageConfig(nil), value.Subflows[i].Stages...)
		for j := range value.Subflows[i].Stages {
			value.Subflows[i].Stages[j] = cloneEffectiveStage(value.Subflows[i].Stages[j])
		}
	}
	return value
}

func cloneDescription(value pipeflow.Description) pipeflow.Description {
	value.Children = append([]pipeflow.Description(nil), value.Children...)
	for i := range value.Children {
		value.Children[i] = cloneDescription(value.Children[i])
	}
	return value
}
