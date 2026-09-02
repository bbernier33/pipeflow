// Package obshttp exposes read-only Observation snapshots over HTTP.
package obshttp

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
)

const SchemaVersion = "pipeflow.obs.http/v1"

type Authorizer func(*http.Request) bool

type Options struct{ Authorize Authorizer }

type Handler struct {
	source    obs.Source
	authorize Authorizer
}

func NewHandler(source obs.Source, options Options) (*Handler, error) {
	if source == nil {
		return nil, errors.New("pipeflow obs http: nil source")
	}
	return &Handler{source: source, authorize: options.Authorize}, nil
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
	default:
		writeError(w, r, http.StatusNotFound, "not_found")
	}
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
	Traces          []traceWire            `json:"traces"`
	RecentMetrics   []metricWire           `json:"recent_metrics"`
	RecentProfiles  []profileWire          `json:"recent_profiles"`
	DroppedTraces   uint64                 `json:"dropped_traces"`
	DroppedMetrics  uint64                 `json:"dropped_metrics"`
	DroppedProfiles uint64                 `json:"dropped_profiles"`
}
type locationWire struct {
	RunID           string            `json:"run_id,omitempty"`
	Pipeline        string            `json:"pipeline,omitempty"`
	Stage           string            `json:"stage,omitempty"`
	Step            string            `json:"step,omitempty"`
	Parallel        string            `json:"parallel,omitempty"`
	Branch          string            `json:"branch,omitempty"`
	Subflow         string            `json:"subflow,omitempty"`
	Background      string            `json:"background,omitempty"`
	Recovery        string            `json:"recovery,omitempty"`
	Role            pipeflow.StepRole `json:"role,omitempty"`
	Attempt         int               `json:"attempt,omitempty"`
	Poll            int               `json:"poll,omitempty"`
	RecoveryAttempt int               `json:"recovery_attempt,omitempty"`
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
	w := snapshotWire{Schema: SchemaVersion, CapturedAt: s.CapturedAt, Pipelines: s.Pipelines, Runs: s.Runs, Errors: s.Errors, Metrics: s.Metrics, Profiles: s.Profiles, Workers: s.Workers, Queues: s.Queues, DroppedTraces: s.DroppedTraces, DroppedMetrics: s.DroppedMetrics, DroppedProfiles: s.DroppedProfiles}
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
func wireLocation(v pipeflow.ObservationLocation) locationWire {
	return locationWire{RunID: v.RunID, Pipeline: v.Pipeline, Stage: v.Stage, Step: v.Step, Parallel: v.Parallel, Branch: v.Branch, Subflow: v.Subflow, Background: v.Background, Recovery: v.Recovery, Role: v.Role, Attempt: v.Attempt, Poll: v.Poll, RecoveryAttempt: v.RecoveryAttempt}
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
