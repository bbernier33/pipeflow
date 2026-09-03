package obs

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type AnalysisOptions struct {
	QueueWarningUtilization  float64
	QueueCriticalUtilization float64
	WorkerSaturation         float64
	ProfileTimeShare         float64
	PipelineFailureRate      float64
	MinimumPipelineRuns      uint64
	RepeatedRecoveryCount    uint64
	IdempotencyDuplicateRate float64
	MinimumIdempotencyEvents uint64
}

func (o AnalysisOptions) normalized() AnalysisOptions {
	if o.QueueWarningUtilization == 0 {
		o.QueueWarningUtilization = .75
	}
	if o.QueueCriticalUtilization == 0 {
		o.QueueCriticalUtilization = .95
	}
	if o.WorkerSaturation == 0 {
		o.WorkerSaturation = .9
	}
	if o.ProfileTimeShare == 0 {
		o.ProfileTimeShare = .4
	}
	if o.PipelineFailureRate == 0 {
		o.PipelineFailureRate = .1
	}
	if o.MinimumPipelineRuns == 0 {
		o.MinimumPipelineRuns = 5
	}
	if o.RepeatedRecoveryCount == 0 {
		o.RepeatedRecoveryCount = 3
	}
	if o.IdempotencyDuplicateRate == 0 {
		o.IdempotencyDuplicateRate = .2
	}
	if o.MinimumIdempotencyEvents == 0 {
		o.MinimumIdempotencyEvents = 5
	}
	return o
}

type Evidence struct {
	Metric    string  `json:"metric"`
	Observed  float64 `json:"observed"`
	Threshold float64 `json:"threshold,omitempty"`
	Unit      string  `json:"unit"`
}

type Finding struct {
	Code       string     `json:"code"`
	Severity   Severity   `json:"severity"`
	Summary    string     `json:"summary"`
	Pipeline   string     `json:"pipeline,omitempty"`
	Stage      string     `json:"stage,omitempty"`
	Step       string     `json:"step,omitempty"`
	Worker     string     `json:"worker,omitempty"`
	Dependency string     `json:"dependency,omitempty"`
	Guard      string     `json:"guard,omitempty"`
	Evidence   []Evidence `json:"evidence"`
}

type Analysis struct {
	CapturedAt        time.Time `json:"captured_at"`
	Findings          []Finding `json:"findings"`
	PrimaryBottleneck *Finding  `json:"primary_bottleneck,omitempty"`
}

// Analyze derives conservative operational findings from one immutable
// Snapshot. It performs no I/O and cannot affect execution or collection.
func Analyze(snapshot Snapshot, options AnalysisOptions) (Analysis, error) {
	options = options.normalized()
	if err := validateAnalysisOptions(options); err != nil {
		return Analysis{}, err
	}
	result := Analysis{CapturedAt: snapshot.CapturedAt}
	result.Findings = append(result.Findings, analyzeQueues(snapshot, options)...)
	result.Findings = append(result.Findings, analyzeWorkers(snapshot, options)...)
	result.Findings = append(result.Findings, analyzeProfiles(snapshot, options)...)
	result.Findings = append(result.Findings, analyzePipelines(snapshot, options)...)
	result.Findings = append(result.Findings, analyzeResilience(snapshot, options)...)
	sort.SliceStable(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		if severityRank(a.Severity) != severityRank(b.Severity) {
			return severityRank(a.Severity) > severityRank(b.Severity)
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return findingLocation(a) < findingLocation(b)
	})
	for i := range result.Findings {
		if bottleneckCandidate(result.Findings[i].Code) {
			candidate := result.Findings[i]
			result.PrimaryBottleneck = &candidate
			break
		}
	}
	return result, nil
}

func validateAnalysisOptions(o AnalysisOptions) error {
	for name, value := range map[string]float64{"queue warning utilization": o.QueueWarningUtilization, "queue critical utilization": o.QueueCriticalUtilization, "worker saturation": o.WorkerSaturation, "profile time share": o.ProfileTimeShare, "pipeline failure rate": o.PipelineFailureRate, "idempotency duplicate rate": o.IdempotencyDuplicateRate} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
			return fmt.Errorf("pipeflow obs: analysis %s must be greater than zero and at most one", name)
		}
	}
	if o.QueueWarningUtilization > o.QueueCriticalUtilization {
		return errors.New("pipeflow obs: queue warning utilization cannot exceed critical utilization")
	}
	return nil
}

func analyzeQueues(snapshot Snapshot, o AnalysisOptions) []Finding {
	var findings []Finding
	for _, queue := range snapshot.Queues {
		if queue.Capacity <= 0 || queue.Utilization < o.QueueWarningUtilization {
			continue
		}
		severity, threshold := SeverityWarning, o.QueueWarningUtilization
		if queue.Utilization >= o.QueueCriticalUtilization {
			severity, threshold = SeverityCritical, o.QueueCriticalUtilization
		}
		findings = append(findings, Finding{Code: "queue_pressure", Severity: severity, Summary: "queue utilization is near its configured capacity", Pipeline: queue.Pipeline, Worker: queue.Worker, Evidence: []Evidence{{Metric: "queue.utilization", Observed: queue.Utilization, Threshold: threshold, Unit: "ratio"}, {Metric: "queue.depth", Observed: float64(queue.Depth), Unit: "items"}}})
	}
	return findings
}

func analyzeWorkers(snapshot Snapshot, o AnalysisOptions) []Finding {
	var findings []Finding
	for _, worker := range snapshot.Workers {
		if worker.Concurrency <= 0 {
			continue
		}
		ratio := float64(worker.Active) / float64(worker.Concurrency)
		if ratio < o.WorkerSaturation || worker.InFlight <= worker.Active {
			continue
		}
		findings = append(findings, Finding{Code: "worker_saturation", Severity: SeverityWarning, Summary: "all or most workers are active while work remains in flight", Pipeline: worker.Pipeline, Worker: worker.Name, Evidence: []Evidence{{Metric: "worker.active_ratio", Observed: ratio, Threshold: o.WorkerSaturation, Unit: "ratio"}, {Metric: "worker.in_flight", Observed: float64(worker.InFlight), Unit: "items"}}})
	}
	return findings
}

func analyzeProfiles(snapshot Snapshot, o AnalysisOptions) []Finding {
	byPipeline := make(map[string][]ProfileAggregate)
	for _, profile := range snapshot.Profiles {
		if profile.Scope == pipeflow.ObservationStep && profile.Samples > 0 {
			byPipeline[profile.Pipeline] = append(byPipeline[profile.Pipeline], profile)
		}
	}
	var findings []Finding
	for pipeline, profiles := range byPipeline {
		if len(profiles) < 2 {
			continue
		}
		var total time.Duration
		for _, profile := range profiles {
			total += profile.Total
		}
		if total <= 0 {
			continue
		}
		top := profiles[0]
		for _, profile := range profiles[1:] {
			if profile.Total > top.Total {
				top = profile
			}
		}
		share := float64(top.Total) / float64(total)
		if share < o.ProfileTimeShare {
			continue
		}
		findings = append(findings, Finding{Code: "step_time_concentration", Severity: SeverityWarning, Summary: "one Step accounts for a large share of observed Step execution time", Pipeline: pipeline, Stage: top.Stage, Step: top.Step, Evidence: []Evidence{{Metric: "profile.time_share", Observed: share, Threshold: o.ProfileTimeShare, Unit: "ratio"}, {Metric: "profile.average_duration", Observed: float64(top.Total) / float64(top.Samples), Unit: "nanoseconds"}}})
	}
	return findings
}

func analyzePipelines(snapshot Snapshot, o AnalysisOptions) []Finding {
	var findings []Finding
	for _, pipeline := range snapshot.Pipelines {
		terminal := uint64(pipeline.Completed + pipeline.Failed)
		if terminal < o.MinimumPipelineRuns {
			continue
		}
		rate := float64(pipeline.Failed) / float64(terminal)
		if rate >= o.PipelineFailureRate {
			findings = append(findings, Finding{Code: "pipeline_failure_rate", Severity: SeverityWarning, Summary: "observed Pipeline failure rate exceeds its analysis threshold", Pipeline: pipeline.Name, Evidence: []Evidence{{Metric: "pipeline.failure_rate", Observed: rate, Threshold: o.PipelineFailureRate, Unit: "ratio"}, {Metric: "pipeline.terminal_runs", Observed: float64(terminal), Unit: "runs"}}})
		}
	}
	return findings
}

func analyzeResilience(snapshot Snapshot, o AnalysisOptions) []Finding {
	var findings []Finding
	for _, circuit := range snapshot.Circuits {
		if circuit.State == pipeflow.CircuitOpen || circuit.State == pipeflow.CircuitHalfOpen {
			severity := SeverityCritical
			if circuit.State == pipeflow.CircuitHalfOpen {
				severity = SeverityWarning
			}
			findings = append(findings, Finding{Code: "circuit_not_closed", Severity: severity, Summary: "dependency circuit is not closed", Dependency: circuit.Dependency, Evidence: []Evidence{{Metric: "circuit.short_circuits", Observed: float64(circuit.ShortCircuits), Unit: "events"}}})
		}
	}
	for _, recovery := range snapshot.Recoveries {
		if recovery.Failed > 0 {
			findings = append(findings, Finding{Code: "recovery_failures", Severity: SeverityCritical, Summary: "Operational Recovery has failed", Pipeline: recovery.Pipeline, Stage: recovery.Stage, Step: recovery.Step, Evidence: []Evidence{{Metric: "recovery.failures", Observed: float64(recovery.Failed), Unit: "events"}}})
		} else if recovery.Activations >= o.RepeatedRecoveryCount {
			findings = append(findings, Finding{Code: "repeated_recovery", Severity: SeverityWarning, Summary: "Operational Recovery is activating repeatedly", Pipeline: recovery.Pipeline, Stage: recovery.Stage, Step: recovery.Step, Evidence: []Evidence{{Metric: "recovery.activations", Observed: float64(recovery.Activations), Threshold: float64(o.RepeatedRecoveryCount), Unit: "events"}}})
		}
	}
	for _, guard := range snapshot.Idempotency {
		if guard.StoreFailures > 0 {
			findings = append(findings, Finding{Code: "idempotency_store_failures", Severity: SeverityCritical, Summary: "idempotency storage operations have failed", Guard: guard.Guard, Evidence: []Evidence{{Metric: "idempotency.store_failures", Observed: float64(guard.StoreFailures), Unit: "events"}}})
		}
		total := guard.Executed + guard.DuplicateCompleted + guard.DuplicateInProgress + guard.FailedReleasable + guard.StoreFailures
		duplicates := guard.DuplicateCompleted + guard.DuplicateInProgress
		if total >= o.MinimumIdempotencyEvents && float64(duplicates)/float64(total) >= o.IdempotencyDuplicateRate {
			findings = append(findings, Finding{Code: "idempotency_duplicate_rate", Severity: SeverityWarning, Summary: "idempotency duplicate rate exceeds its analysis threshold", Guard: guard.Guard, Evidence: []Evidence{{Metric: "idempotency.duplicate_rate", Observed: float64(duplicates) / float64(total), Threshold: o.IdempotencyDuplicateRate, Unit: "ratio"}}})
		}
	}
	return findings
}

func severityRank(value Severity) int {
	if value == SeverityCritical {
		return 3
	}
	if value == SeverityWarning {
		return 2
	}
	return 1
}
func findingLocation(value Finding) string {
	return value.Pipeline + "\x00" + value.Stage + "\x00" + value.Step + "\x00" + value.Worker + "\x00" + value.Dependency + "\x00" + value.Guard
}
func bottleneckCandidate(code string) bool {
	return code == "queue_pressure" || code == "worker_saturation" || code == "step_time_concentration" || code == "circuit_not_closed"
}
