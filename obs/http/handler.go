// Package obshttp exposes read-only Observation snapshots over HTTP.
package obshttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
	"github.com/bbernier33/pipeflow/obs/history"
)

const SchemaVersion = "pipeflow.obs.http/v1"

type Authorizer func(*http.Request) bool

type HistorySource interface {
	Query(history.Query) ([]obs.Snapshot, error)
}

type Options struct {
	Authorize Authorizer
	History   HistorySource
}

type Handler struct {
	source    obs.Source
	authorize Authorizer
	history   HistorySource
}

func NewHandler(source obs.Source, options Options) (*Handler, error) {
	if source == nil {
		return nil, errors.New("pipeflow obs http: nil source")
	}
	return &Handler{source: source, authorize: options.Authorize, history: options.History}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if h.authorize != nil && !h.authorize(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.URL.Path == "/healthz" {
		writeJSON(w, r, http.StatusOK, map[string]any{"schema": SchemaVersion, "status": "ok"})
		return
	}
	if r.URL.Path == "/v1/history" {
		h.serveHistory(w, r)
		return
	}
	var snapshot obs.Snapshot
	if !safeSnapshot(h.source, &snapshot) {
		writeError(w, r, http.StatusInternalServerError, "snapshot_unavailable")
		return
	}
	switch r.URL.Path {
	case "/v1/snapshot":
		writeJSON(w, r, http.StatusOK, snapshotDocument(snapshot))
	case "/v1/health":
		writeJSON(w, r, http.StatusOK, healthDocument{Schema: SchemaVersion, CapturedAt: snapshot.CapturedAt, Pipelines: snapshot.Pipelines})
	case "/v1/workers":
		writeJSON(w, r, http.StatusOK, workerDocument{Schema: SchemaVersion, CapturedAt: snapshot.CapturedAt, Workers: snapshot.Workers})
	case "/v1/queues":
		writeJSON(w, r, http.StatusOK, queueDocument{Schema: SchemaVersion, CapturedAt: snapshot.CapturedAt, Queues: snapshot.Queues})
	case "/v1/resilience":
		writeJSON(w, r, http.StatusOK, resilienceDocument{Schema: SchemaVersion, CapturedAt: snapshot.CapturedAt, Recoveries: snapshot.Recoveries, Circuits: snapshot.Circuits, Idempotency: snapshot.Idempotency})
	case "/v1/config":
		writeJSON(w, r, http.StatusOK, configDocument{Schema: SchemaVersion, CapturedAt: snapshot.CapturedAt, Pipelines: wireDefinitions(snapshot.Definitions)})
	default:
		writeError(w, r, http.StatusNotFound, "not_found")
	}
}

func (h *Handler) serveHistory(w http.ResponseWriter, r *http.Request) {
	if h.history == nil {
		writeError(w, r, http.StatusNotFound, "history_unavailable")
		return
	}
	query := history.Query{Limit: 100, NewestFirst: true}
	var err error
	if value := r.URL.Query().Get("from"); value != "" {
		query.From, err = time.Parse(time.RFC3339Nano, value)
	}
	if err == nil {
		if value := r.URL.Query().Get("to"); value != "" {
			query.To, err = time.Parse(time.RFC3339Nano, value)
		}
	}
	if err == nil {
		if value := r.URL.Query().Get("limit"); value != "" {
			query.Limit, err = strconv.Atoi(value)
		}
	}
	if err != nil || query.Limit < 1 || query.Limit > 1000 {
		writeError(w, r, http.StatusBadRequest, "invalid_history_query")
		return
	}
	if order := r.URL.Query().Get("order"); order == "asc" {
		query.NewestFirst = false
	} else if order != "" && order != "desc" {
		writeError(w, r, http.StatusBadRequest, "invalid_history_query")
		return
	}
	var snapshots []obs.Snapshot
	if !safeHistoryQuery(h.history, query, &snapshots) {
		writeError(w, r, http.StatusInternalServerError, "history_unavailable")
		return
	}
	document := historyDocument{Schema: SchemaVersion, Snapshots: make([]snapshotWire, 0, len(snapshots))}
	for _, snapshot := range snapshots {
		document.Snapshots = append(document.Snapshots, snapshotDocument(snapshot))
	}
	writeJSON(w, r, http.StatusOK, document)
}

func safeHistoryQuery(source HistorySource, query history.Query, target *[]obs.Snapshot) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	values, err := source.Query(query)
	if err != nil {
		return false
	}
	*target = values
	return true
}

func safeSnapshot(source obs.Source, target *obs.Snapshot) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	*target = source.Snapshot()
	return true
}

type healthDocument struct {
	Schema     string             `json:"schema"`
	CapturedAt time.Time          `json:"captured_at"`
	Pipelines  []obs.PipelineView `json:"pipelines"`
}
type workerDocument struct {
	Schema     string           `json:"schema"`
	CapturedAt time.Time        `json:"captured_at"`
	Workers    []obs.WorkerView `json:"workers"`
}
type queueDocument struct {
	Schema     string          `json:"schema"`
	CapturedAt time.Time       `json:"captured_at"`
	Queues     []obs.QueueView `json:"queues"`
}
type resilienceDocument struct {
	Schema      string                `json:"schema"`
	CapturedAt  time.Time             `json:"captured_at"`
	Recoveries  []obs.RecoveryView    `json:"recoveries"`
	Circuits    []obs.CircuitView     `json:"circuits"`
	Idempotency []obs.IdempotencyView `json:"idempotency"`
}
type historyDocument struct {
	Schema    string         `json:"schema"`
	Snapshots []snapshotWire `json:"snapshots"`
}
type configDocument struct {
	Schema     string           `json:"schema"`
	CapturedAt time.Time        `json:"captured_at"`
	Pipelines  []definitionWire `json:"pipelines"`
}
type definitionWire struct {
	Name        string                 `json:"name"`
	Description descriptionWire        `json:"description"`
	Effective   *effectivePipelineWire `json:"effective_config,omitempty"`
}
type descriptionWire struct {
	Kind     pipeflow.DescriptionKind `json:"kind"`
	Name     string                   `json:"name,omitempty"`
	Type     string                   `json:"type,omitempty"`
	Role     pipeflow.StepRole        `json:"role,omitempty"`
	Children []descriptionWire        `json:"children,omitempty"`
}
type effectiveValueWire[T any] struct {
	Value  T                     `json:"value"`
	Source pipeflow.ConfigSource `json:"source"`
}
type effectivePipelineWire struct {
	Name        string                            `json:"name"`
	Timeout     effectiveValueWire[time.Duration] `json:"timeout"`
	Stages      []effectiveStageWire              `json:"stages"`
	Backgrounds []effectiveBackgroundWire         `json:"backgrounds"`
}
type effectiveStageWire struct {
	Name      string                            `json:"name"`
	Timeout   effectiveValueWire[time.Duration] `json:"timeout"`
	Steps     []effectiveStepWire               `json:"steps"`
	Parallels []effectiveParallelWire           `json:"parallels"`
	Subflows  []effectiveSubflowWire            `json:"subflows"`
}
type effectiveParallelWire struct {
	Name          string                     `json:"name"`
	FailurePolicy effectiveValueWire[string] `json:"failure_policy"`
	Branches      []effectiveBranchWire      `json:"branches"`
}
type effectiveBranchWire struct {
	Name  string              `json:"name"`
	Steps []effectiveStepWire `json:"steps"`
}
type effectiveSubflowWire struct {
	Name   string               `json:"name"`
	Stages []effectiveStageWire `json:"stages"`
}
type effectiveBackgroundWire struct {
	Name          string                     `json:"name"`
	FailurePolicy effectiveValueWire[string] `json:"failure_policy"`
}
type retryWire struct {
	MaxAttempts int           `json:"max_attempts"`
	Delay       time.Duration `json:"delay_ns"`
	Backoff     string        `json:"backoff"`
	MaxDelay    time.Duration `json:"max_delay_ns"`
	Jitter      float64       `json:"jitter"`
}
type pollingWire struct {
	Every    time.Duration `json:"every_ns"`
	MaxPolls int           `json:"max_polls"`
	Timeout  time.Duration `json:"timeout_ns"`
}
type rateLimitWire struct {
	Key           string        `json:"key,omitempty"`
	MaxCalls      int           `json:"max_calls"`
	Interval      time.Duration `json:"interval_ns"`
	MaxConcurrent int           `json:"max_concurrent"`
}
type effectiveStepWire struct {
	Name      string                                `json:"name"`
	Role      effectiveValueWire[pipeflow.StepRole] `json:"role"`
	Timeout   effectiveValueWire[time.Duration]     `json:"timeout"`
	Retry     effectiveValueWire[retryWire]         `json:"retry"`
	Polling   effectiveValueWire[pollingWire]       `json:"polling"`
	RateLimit effectiveValueWire[rateLimitWire]     `json:"rate_limit"`
}
type snapshotWire struct {
	Schema          string                 `json:"schema"`
	CapturedAt      time.Time              `json:"captured_at"`
	Pipelines       []obs.PipelineView     `json:"pipelines"`
	Runs            []obs.RunView          `json:"runs"`
	Errors          []obs.ErrorGroup       `json:"errors"`
	Metrics         []obs.MetricAggregate  `json:"metrics"`
	Profiles        []obs.ProfileAggregate `json:"profiles"`
	Workers         []obs.WorkerView       `json:"workers"`
	Queues          []obs.QueueView        `json:"queues"`
	Recoveries      []obs.RecoveryView     `json:"recoveries"`
	Circuits        []obs.CircuitView      `json:"circuits"`
	Idempotency     []obs.IdempotencyView  `json:"idempotency"`
	Definitions     []definitionWire       `json:"definitions"`
	Traces          []traceWire            `json:"traces"`
	RecentMetrics   []metricWire           `json:"recent_metrics"`
	RecentProfiles  []profileWire          `json:"recent_profiles"`
	DroppedTraces   uint64                 `json:"dropped_traces"`
	DroppedMetrics  uint64                 `json:"dropped_metrics"`
	DroppedProfiles uint64                 `json:"dropped_profiles"`
}
type locationWire struct {
	RunID              string                      `json:"run_id,omitempty"`
	Pipeline           string                      `json:"pipeline,omitempty"`
	Stage              string                      `json:"stage,omitempty"`
	Step               string                      `json:"step,omitempty"`
	Parallel           string                      `json:"parallel,omitempty"`
	Branch             string                      `json:"branch,omitempty"`
	Subflow            string                      `json:"subflow,omitempty"`
	Background         string                      `json:"background,omitempty"`
	Recovery           string                      `json:"recovery,omitempty"`
	Role               pipeflow.StepRole           `json:"role,omitempty"`
	Attempt            int                         `json:"attempt,omitempty"`
	Poll               int                         `json:"poll,omitempty"`
	RecoveryAttempt    int                         `json:"recovery_attempt,omitempty"`
	RecoveryDecision   pipeflow.RecoveryDecision   `json:"recovery_decision,omitempty"`
	Dependency         string                      `json:"dependency,omitempty"`
	Guard              string                      `json:"guard,omitempty"`
	CircuitState       pipeflow.CircuitState       `json:"circuit_state,omitempty"`
	IdempotencyOutcome pipeflow.IdempotencyOutcome `json:"idempotency_outcome,omitempty"`
	Probe              bool                        `json:"probe,omitempty"`
	ShortCircuited     bool                        `json:"short_circuited,omitempty"`
}
type errorWire struct {
	Type  string `json:"type"`
	Panic bool   `json:"panic"`
}
type traceWire struct {
	Scope      pipeflow.ObservationScope `json:"scope"`
	Phase      pipeflow.ObservationPhase `json:"phase"`
	Location   locationWire              `json:"location"`
	Status     pipeflow.Status           `json:"status"`
	OccurredAt time.Time                 `json:"occurred_at"`
	Duration   time.Duration             `json:"duration_ns"`
	Error      *errorWire                `json:"error,omitempty"`
}
type metricWire struct {
	Name       string                    `json:"name"`
	Scope      pipeflow.ObservationScope `json:"scope"`
	Location   locationWire              `json:"location"`
	Status     pipeflow.Status           `json:"status"`
	Value      float64                   `json:"value"`
	Unit       string                    `json:"unit"`
	OccurredAt time.Time                 `json:"occurred_at"`
}
type profileWire struct {
	Location   locationWire              `json:"location"`
	Scope      pipeflow.ObservationScope `json:"scope"`
	Duration   time.Duration             `json:"duration_ns"`
	Status     pipeflow.Status           `json:"status"`
	OccurredAt time.Time                 `json:"occurred_at"`
}

func snapshotDocument(s obs.Snapshot) snapshotWire {
	w := snapshotWire{Schema: SchemaVersion, CapturedAt: s.CapturedAt, Pipelines: s.Pipelines, Runs: s.Runs, Errors: s.Errors, Metrics: s.Metrics, Profiles: s.Profiles, Workers: s.Workers, Queues: s.Queues, Recoveries: s.Recoveries, Circuits: s.Circuits, Idempotency: s.Idempotency, Definitions: wireDefinitions(s.Definitions), DroppedTraces: s.DroppedTraces, DroppedMetrics: s.DroppedMetrics, DroppedProfiles: s.DroppedProfiles}
	for _, v := range s.Traces {
		e := (*errorWire)(nil)
		if v.Error != nil {
			e = &errorWire{Type: v.Error.Type, Panic: v.Error.Panic}
		}
		w.Traces = append(w.Traces, traceWire{Scope: v.Scope, Phase: v.Phase, Location: wireLocation(v.Location), Status: v.Status, OccurredAt: v.OccurredAt, Duration: v.Duration, Error: e})
	}
	for _, v := range s.RecentMetrics {
		w.RecentMetrics = append(w.RecentMetrics, metricWire{Name: v.Name, Scope: v.Scope, Location: wireLocation(v.Location), Status: v.Status, Value: v.Value, Unit: v.Unit, OccurredAt: v.OccurredAt})
	}
	for _, v := range s.RecentProfiles {
		w.RecentProfiles = append(w.RecentProfiles, profileWire{Location: wireLocation(v.Location), Scope: v.Scope, Duration: v.Duration, Status: v.Status, OccurredAt: v.OccurredAt})
	}
	return w
}

func wireDefinitions(values []obs.DefinitionView) []definitionWire {
	result := make([]definitionWire, 0, len(values))
	for _, value := range values {
		wire := definitionWire{Name: value.Name, Description: wireDescription(value.Description)}
		if value.Effective != nil {
			effective := wireEffective(*value.Effective)
			wire.Effective = &effective
		}
		result = append(result, wire)
	}
	return result
}
func wireDescription(value pipeflow.Description) descriptionWire {
	wire := descriptionWire{Kind: value.Kind, Name: value.Name, Type: value.Type, Role: value.Role}
	for _, child := range value.Children {
		wire.Children = append(wire.Children, wireDescription(child))
	}
	return wire
}
func wireEffective(value pipeflow.EffectivePipelineConfig) effectivePipelineWire {
	wire := effectivePipelineWire{Name: value.Name, Timeout: effectiveValueWire[time.Duration]{Value: value.Timeout.Value, Source: value.Timeout.Source}}
	for _, stage := range value.Stages {
		wire.Stages = append(wire.Stages, wireStage(stage))
	}
	for _, background := range value.Backgrounds {
		wire.Backgrounds = append(wire.Backgrounds, effectiveBackgroundWire{Name: background.Name, FailurePolicy: effectiveValueWire[string]{Value: backgroundPolicyName(background.FailurePolicy.Value), Source: background.FailurePolicy.Source}})
	}
	return wire
}
func wireStage(value pipeflow.EffectiveStageConfig) effectiveStageWire {
	wire := effectiveStageWire{Name: value.Name, Timeout: effectiveValueWire[time.Duration]{Value: value.Timeout.Value, Source: value.Timeout.Source}}
	for _, step := range value.Steps {
		wire.Steps = append(wire.Steps, wireStep(step))
	}
	for _, parallel := range value.Parallels {
		p := effectiveParallelWire{Name: parallel.Name, FailurePolicy: effectiveValueWire[string]{Value: failurePolicyName(parallel.FailurePolicy.Value), Source: parallel.FailurePolicy.Source}}
		for _, branch := range parallel.Branches {
			b := effectiveBranchWire{Name: branch.Name}
			for _, step := range branch.Steps {
				b.Steps = append(b.Steps, wireStep(step))
			}
			p.Branches = append(p.Branches, b)
		}
		wire.Parallels = append(wire.Parallels, p)
	}
	for _, subflow := range value.Subflows {
		s := effectiveSubflowWire{Name: subflow.Name}
		for _, stage := range subflow.Stages {
			s.Stages = append(s.Stages, wireStage(stage))
		}
		wire.Subflows = append(wire.Subflows, s)
	}
	return wire
}
func wireStep(value pipeflow.EffectiveStepConfig) effectiveStepWire {
	return effectiveStepWire{Name: value.Name, Role: effectiveValueWire[pipeflow.StepRole]{Value: value.Role.Value, Source: value.Role.Source}, Timeout: effectiveValueWire[time.Duration]{Value: value.Timeout.Value, Source: value.Timeout.Source}, Retry: effectiveValueWire[retryWire]{Value: retryWire{MaxAttempts: value.Retry.Value.MaxAttempts, Delay: value.Retry.Value.Delay, Backoff: backoffName(value.Retry.Value.Backoff), MaxDelay: value.Retry.Value.MaxDelay, Jitter: value.Retry.Value.Jitter}, Source: value.Retry.Source}, Polling: effectiveValueWire[pollingWire]{Value: pollingWire{Every: value.Polling.Value.Every, MaxPolls: value.Polling.Value.MaxPolls, Timeout: value.Polling.Value.Timeout}, Source: value.Polling.Source}, RateLimit: effectiveValueWire[rateLimitWire]{Value: rateLimitWire{Key: value.RateLimit.Value.Key, MaxCalls: value.RateLimit.Value.MaxCalls, Interval: value.RateLimit.Value.Interval, MaxConcurrent: value.RateLimit.Value.MaxConcurrent}, Source: value.RateLimit.Source}}
}

func failurePolicyName(value pipeflow.FailurePolicy) string {
	switch value {
	case pipeflow.WaitAll:
		return "wait_all"
	case pipeflow.FailFast:
		return "fail_fast"
	default:
		return fmt.Sprintf("unknown_%d", value)
	}
}
func backgroundPolicyName(value pipeflow.BackgroundFailurePolicy) string {
	switch value {
	case pipeflow.BackgroundFatal:
		return "fatal"
	case pipeflow.BackgroundNonFatal:
		return "non_fatal"
	default:
		return fmt.Sprintf("unknown_%d", value)
	}
}
func backoffName(value pipeflow.BackoffStrategy) string {
	switch value {
	case pipeflow.FixedBackoff:
		return "fixed"
	case pipeflow.ExponentialBackoff:
		return "exponential"
	default:
		return fmt.Sprintf("unknown_%d", value)
	}
}
func wireLocation(v pipeflow.ObservationLocation) locationWire {
	return locationWire{RunID: v.RunID, Pipeline: v.Pipeline, Stage: v.Stage, Step: v.Step, Parallel: v.Parallel, Branch: v.Branch, Subflow: v.Subflow, Background: v.Background, Recovery: v.Recovery, Role: v.Role, Attempt: v.Attempt, Poll: v.Poll, RecoveryAttempt: v.RecoveryAttempt, RecoveryDecision: v.RecoveryDecision, Dependency: v.Dependency, Guard: v.Guard, CircuitState: v.CircuitState, IdempotencyOutcome: v.IdempotencyOutcome, Probe: v.Probe, ShortCircuited: v.ShortCircuited}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code string) {
	writeJSON(w, r, status, map[string]any{"schema": SchemaVersion, "error": code})
}
func writeJSON(w http.ResponseWriter, r *http.Request, status int, value any) {
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}
