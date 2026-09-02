package obs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
)

func TestCollectorBuildsOperationalSnapshot(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() (int, error) { return 2, nil }),
		pipeflow.NewStep("double", func(value int) (int, error) { return value * 2, nil }),
	)).WithObserver(collector)
	output, err := pipeline.Run(context.Background())
	if err != nil || output != 4 {
		t.Fatalf("output=%v err=%v", output, err)
	}
	snapshot := collector.Snapshot()
	if len(snapshot.Pipelines) != 1 || snapshot.Pipelines[0].Health.Health != obs.HealthHealthy || snapshot.Pipelines[0].Completed != 1 {
		t.Fatalf("pipelines=%+v", snapshot.Pipelines)
	}
	if len(snapshot.Runs) != 1 || snapshot.Runs[0].Status != pipeflow.StatusCompleted || snapshot.Runs[0].Duration < 0 {
		t.Fatalf("runs=%+v", snapshot.Runs)
	}
	if len(snapshot.Traces) == 0 || len(snapshot.Metrics) == 0 || len(snapshot.Profiles) == 0 {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
	}
}

func TestCollectorGroupsPayloadFreeErrorsByStructure(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	secret := errors.New("customer-secret-value")
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("charge", pipeflow.NewStep("gateway", func() error { return secret }))).WithObserver(collector)
	for range 2 {
		_, _ = pipeline.Run(context.Background())
	}
	snapshot := collector.Snapshot()
	var stepGroup *obs.ErrorGroup
	for i := range snapshot.Errors {
		if snapshot.Errors[i].Pipeline == "orders" && snapshot.Errors[i].Stage == "charge" && snapshot.Errors[i].Step == "gateway" && snapshot.Errors[i].Type == "*errors.errorString" {
			stepGroup = &snapshot.Errors[i]
			break
		}
	}
	if stepGroup == nil || stepGroup.Occurrences != 2 {
		t.Fatalf("errors=%+v", snapshot.Errors)
	}
	for _, trace := range snapshot.Traces {
		if trace.Error != nil && trace.Error.Type == secret.Error() {
			t.Fatal("error message leaked as classification")
		}
	}
}

func TestCollectorBoundsRawEvidenceAndCountsDrops(t *testing.T) {
	collector := obs.NewCollector(obs.Options{TraceCapacity: 2, MetricCapacity: 2, ProfileCapacity: 2, RunCapacity: 2})
	location := pipeflow.ObservationLocation{RunID: "r", Pipeline: "p"}
	for i := 0; i < 5; i++ {
		collector.ObserveTrace(pipeflow.TraceEvent{Scope: pipeflow.ObservationStep, Phase: pipeflow.ObservationCompleted, Location: location, OccurredAt: time.Now()})
		collector.ObserveMetric(pipeflow.MetricSample{Name: "count", Scope: pipeflow.ObservationStep, Location: location, Value: 1, Unit: "count", OccurredAt: time.Now()})
		collector.ObserveProfile(pipeflow.ProfileSample{Scope: pipeflow.ObservationStep, Location: location, Duration: time.Millisecond, OccurredAt: time.Now()})
	}
	snapshot := collector.Snapshot()
	if len(snapshot.Traces) != 2 || snapshot.DroppedTraces != 3 || len(snapshot.RecentMetrics) != 2 || snapshot.DroppedMetrics != 3 || len(snapshot.RecentProfiles) != 2 || snapshot.DroppedProfiles != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if len(snapshot.Metrics) != 1 || snapshot.Metrics[0].Samples != 5 || snapshot.Metrics[0].Sum != 5 {
		t.Fatalf("metrics=%+v", snapshot.Metrics)
	}
}

func TestCollectorSupportsConcurrentApplicationsAndSnapshots(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	var wg sync.WaitGroup
	for pipelineIndex := 0; pipelineIndex < 4; pipelineIndex++ {
		pipeline := pipeflow.NewPipeline(string(rune('a'+pipelineIndex)), pipeflow.NewStage("work", pipeflow.NewStep("step", func() error { return nil }))).WithObserver(collector)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = pipeline.Run(context.Background()); _ = collector.Snapshot() }()
		}
	}
	wg.Wait()
	snapshot := collector.Snapshot()
	if len(snapshot.Pipelines) != 4 {
		t.Fatalf("pipelines=%+v", snapshot.Pipelines)
	}
	for _, pipeline := range snapshot.Pipelines {
		if pipeline.Completed != 20 || pipeline.ActiveRuns != 0 {
			t.Fatalf("pipeline=%+v", pipeline)
		}
	}
}

func TestCollectorTracksWorkerAndQueueState(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pipeline := pipeflow.NewPipeline("jobs", pipeflow.NewStage("work",
		pipeflow.NewStep("block", func(value int) error {
			if value == 1 {
				close(started)
				<-release
			}
			return nil
		}),
	))
	worker, err := pipeline.StartWorker(context.Background(), pipeflow.WorkerOptions{Workers: 1, Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	collector := obs.NewCollector(obs.Options{})
	untrack, err := collector.TrackWorker("primary", worker)
	if err != nil {
		t.Fatal(err)
	}
	first, err := worker.Submit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := worker.Submit(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}

	snapshot := collector.Snapshot()
	if len(snapshot.Workers) != 1 || snapshot.Workers[0].Name != "primary" || snapshot.Workers[0].Active != 1 || snapshot.Workers[0].InFlight != 2 {
		t.Fatalf("workers=%+v", snapshot.Workers)
	}
	if len(snapshot.Queues) != 1 || snapshot.Queues[0].Depth != 1 || snapshot.Queues[0].Capacity != 2 || snapshot.Queues[0].Utilization != 0.5 {
		t.Fatalf("queues=%+v", snapshot.Queues)
	}

	close(release)
	worker.Close()
	if _, _, err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatal(err)
	}
	untrack()
	untrack()
	if snapshot := collector.Snapshot(); len(snapshot.Workers) != 0 || len(snapshot.Queues) != 0 {
		t.Fatalf("untracked snapshot=%+v", snapshot)
	}
}

func TestTrackWorkerValidatesRegistration(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	if _, err := collector.TrackWorker("", nil); err == nil {
		t.Fatal("expected empty name error")
	}
	if _, err := collector.TrackWorker("worker", nil); err == nil {
		t.Fatal("expected nil Worker error")
	}
}

func TestCollectorDerivesResilienceViews(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})

	var recoveryCalls int
	recovery := pipeflow.NewRecoveryStage("refresh",
		pipeflow.NewStep("repair", func(pipeflow.Failure) error { return nil }),
		pipeflow.NewStep("decide", func(pipeflow.Failure) (pipeflow.RecoveryDecision, error) { return pipeflow.RecoveryRetryStep, nil }),
	)
	recoveryPipeline := pipeflow.NewPipeline("recovery-p", pipeflow.NewStage("s",
		pipeflow.NewStep("fetch", func() error {
			recoveryCalls++
			if recoveryCalls == 1 {
				return errors.New("expired")
			}
			return nil
		}).WithRecovery(recovery, pipeflow.RecoveryPolicy{MaxAttempts: 1}),
	)).WithObserver(collector)
	if _, err := recoveryPipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	dependencyErr := errors.New("dependency")
	breaker, err := pipeflow.NewCircuitBreaker("provider", pipeflow.CircuitBreakerPolicy{FailureThreshold: 1, ObservationWindow: time.Minute, OpenDuration: time.Minute, HalfOpenMaxProbes: 1, IsFailure: func(err error) bool { return errors.Is(err, dependencyErr) }})
	if err != nil {
		t.Fatal(err)
	}
	circuitPipeline := pipeflow.NewPipeline("circuit-p", pipeflow.NewStage("s", pipeflow.NewStep("call", func() error { return dependencyErr }).WithCircuitBreaker(breaker))).WithObserver(collector)
	_, _ = circuitPipeline.Run(context.Background())
	_, _ = circuitPipeline.Run(context.Background())

	guard, err := pipeflow.NewIdempotencyGuard("payments", pipeflow.NewMemoryIdempotencyStore(), func(value string) string { return value })
	if err != nil {
		t.Fatal(err)
	}
	idempotentPipeline := pipeflow.NewPipeline("idempotency-p", pipeflow.NewStage("s", pipeflow.NewStep("charge", func(string) error { return nil }).WithIdempotencyGuard(guard))).WithObserver(collector)
	if _, err := idempotentPipeline.Run(context.Background(), "order-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := idempotentPipeline.Run(context.Background(), "order-1"); err != nil {
		t.Fatal(err)
	}

	snapshot := collector.Snapshot()
	if len(snapshot.Recoveries) != 1 || snapshot.Recoveries[0].Activations != 1 || snapshot.Recoveries[0].RetryStep != 1 {
		t.Fatalf("recoveries=%+v", snapshot.Recoveries)
	}
	if len(snapshot.Circuits) != 1 || snapshot.Circuits[0].State != pipeflow.CircuitOpen || snapshot.Circuits[0].Failures != 1 || snapshot.Circuits[0].ShortCircuits != 1 {
		t.Fatalf("circuits=%+v", snapshot.Circuits)
	}
	if len(snapshot.Idempotency) != 1 || snapshot.Idempotency[0].Executed != 1 || snapshot.Idempotency[0].DuplicateCompleted != 1 {
		t.Fatalf("idempotency=%+v", snapshot.Idempotency)
	}
}

func TestCollectorSnapshotIsDetached(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	collector.ObserveTrace(pipeflow.TraceEvent{Scope: pipeflow.ObservationPipeline, Phase: pipeflow.ObservationStarted, Location: pipeflow.ObservationLocation{RunID: "r", Pipeline: "p"}, Status: pipeflow.StatusRunning, OccurredAt: time.Now()})
	first := collector.Snapshot()
	first.Pipelines[0].Name = "changed"
	first.Traces[0].Location.Pipeline = "changed"
	second := collector.Snapshot()
	if second.Pipelines[0].Name != "p" || second.Traces[0].Location.Pipeline != "p" {
		t.Fatalf("snapshot aliases collector: %+v", second)
	}
}

func TestCollectorTracksDetachedPipelineDefinitionAndEffectiveConfig(t *testing.T) {
	base := pipeflow.NewPipeline("orders", pipeflow.NewStage("process",
		pipeflow.NewStep("load", func() error { return nil }),
	))
	config, err := pipeflow.ParseConfigYAML([]byte("pipelines:\n  orders:\n    timeout: 3s\n    stages:\n      process:\n        steps:\n          load:\n            timeout: 250ms\n"))
	if err != nil {
		t.Fatal(err)
	}
	configured, err := base.WithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	collector := obs.NewCollector(obs.Options{})
	untrack, err := collector.TrackPipeline(configured)
	if err != nil {
		t.Fatal(err)
	}

	snapshot := collector.Snapshot()
	if len(snapshot.Definitions) != 1 || snapshot.Definitions[0].Description.Children[0].Children[0].Name != "load" {
		t.Fatalf("definitions=%+v", snapshot.Definitions)
	}
	effective := snapshot.Definitions[0].Effective
	if effective == nil || effective.Timeout.Value != 3*time.Second || effective.Timeout.Source != pipeflow.ConfigSourcePipeline || effective.Stages[0].Steps[0].Timeout.Value != 250*time.Millisecond {
		t.Fatalf("effective=%+v", effective)
	}
	snapshot.Definitions[0].Description.Children[0].Children[0].Name = "mutated"
	snapshot.Definitions[0].Effective.Stages[0].Steps[0].Name = "mutated"
	again := collector.Snapshot().Definitions[0]
	if again.Description.Children[0].Children[0].Name != "load" || again.Effective.Stages[0].Steps[0].Name != "load" {
		t.Fatal("definition snapshot was not detached")
	}

	if _, err := collector.TrackPipeline(configured); err == nil {
		t.Fatal("expected duplicate registration error")
	}
	untrack()
	untrack()
	if len(collector.Snapshot().Definitions) != 0 {
		t.Fatal("definition remained after untrack")
	}
}

func TestCollectorTracksUnconfiguredTopologyWithoutEffectiveConfig(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	pipeline := pipeflow.NewPipeline("plain", pipeflow.NewStage("stage", pipeflow.NewStep("step", func() error { return nil })))
	if _, err := collector.TrackPipeline(pipeline); err != nil {
		t.Fatal(err)
	}
	definition := collector.Snapshot().Definitions[0]
	if definition.Name != "plain" || definition.Effective != nil {
		t.Fatalf("definition=%+v", definition)
	}
}
