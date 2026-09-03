package obs_test

import (
	"reflect"
	"testing"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
)

func TestAnalyzeAttributesOperationalBottlenecks(t *testing.T) {
	snapshot := obs.Snapshot{
		CapturedAt: time.Now(),
		Queues:     []obs.QueueView{{Name: "primary.queue", Worker: "primary", Pipeline: "orders", Depth: 96, Capacity: 100, Utilization: .96}},
		Workers:    []obs.WorkerView{{Name: "primary", Pipeline: "orders", Concurrency: 4, Active: 4, InFlight: 12}},
		Profiles: []obs.ProfileAggregate{
			{Scope: pipeflow.ObservationStep, Pipeline: "orders", Stage: "process", Step: "enrich", Samples: 10, Total: 800 * time.Millisecond},
			{Scope: pipeflow.ObservationStep, Pipeline: "orders", Stage: "process", Step: "save", Samples: 10, Total: 200 * time.Millisecond},
		},
	}
	original := snapshot.Queues[0]
	analysis, err := obs.Analyze(snapshot, obs.AnalysisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.Findings) != 3 {
		t.Fatalf("findings=%+v", analysis.Findings)
	}
	if analysis.PrimaryBottleneck == nil || analysis.PrimaryBottleneck.Code != "queue_pressure" || analysis.PrimaryBottleneck.Severity != obs.SeverityCritical {
		t.Fatalf("primary=%+v", analysis.PrimaryBottleneck)
	}
	if !reflect.DeepEqual(snapshot.Queues[0], original) {
		t.Fatal("analysis mutated snapshot")
	}
}

func TestAnalyzeResilienceAndFailureEvidence(t *testing.T) {
	snapshot := obs.Snapshot{
		CapturedAt:  time.Now(),
		Pipelines:   []obs.PipelineView{{Name: "orders", Completed: 7, Failed: 3}},
		Circuits:    []obs.CircuitView{{Dependency: "payments", State: pipeflow.CircuitOpen, ShortCircuits: 2}},
		Recoveries:  []obs.RecoveryView{{Pipeline: "orders", Stage: "charge", Step: "send", Name: "refresh", Activations: 4}},
		Idempotency: []obs.IdempotencyView{{Guard: "charges", Executed: 6, DuplicateCompleted: 3, StoreFailures: 1}},
	}
	analysis, err := obs.Analyze(snapshot, obs.AnalysisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"circuit_not_closed": false, "pipeline_failure_rate": false, "repeated_recovery": false, "idempotency_store_failures": false, "idempotency_duplicate_rate": false}
	for _, finding := range analysis.Findings {
		if _, ok := want[finding.Code]; ok {
			want[finding.Code] = true
		}
	}
	for code, found := range want {
		if !found {
			t.Errorf("missing %s in %+v", code, analysis.Findings)
		}
	}
	if analysis.PrimaryBottleneck == nil || analysis.PrimaryBottleneck.Code != "circuit_not_closed" {
		t.Fatalf("primary=%+v", analysis.PrimaryBottleneck)
	}
}

func TestAnalyzeAvoidsClaimsWithoutEnoughEvidence(t *testing.T) {
	analysis, err := obs.Analyze(obs.Snapshot{Idempotency: []obs.IdempotencyView{{Guard: "g", Executed: 1, DuplicateCompleted: 1}}}, obs.AnalysisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.Findings) != 0 || analysis.PrimaryBottleneck != nil {
		t.Fatalf("analysis=%+v", analysis)
	}
}

func TestAnalyzeValidatesThresholds(t *testing.T) {
	if _, err := obs.Analyze(obs.Snapshot{}, obs.AnalysisOptions{QueueWarningUtilization: .9, QueueCriticalUtilization: .8}); err == nil {
		t.Fatal("expected threshold ordering error")
	}
	if _, err := obs.Analyze(obs.Snapshot{}, obs.AnalysisOptions{WorkerSaturation: 2}); err == nil {
		t.Fatal("expected range error")
	}
}
