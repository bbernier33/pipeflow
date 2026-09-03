package pipeflow

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type observationCollector struct {
	mu       sync.Mutex
	traces   []TraceEvent
	metrics  []MetricSample
	profiles []ProfileSample
}

func (c *observationCollector) ObserveTrace(v TraceEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, v)
}
func (c *observationCollector) ObserveMetric(v MetricSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = append(c.metrics, v)
}
func (c *observationCollector) ObserveProfile(v ProfileSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.profiles = append(c.profiles, v)
}

func TestObserverEmitsTraceMetricsProfileAndAttempts(t *testing.T) {
	collector := &observationCollector{}
	pipeline := NewPipeline("orders", NewStage("prepare",
		NewStep("produce", func() (int, error) { return 41, nil }),
		NewStep("increment", func(v int) (int, error) { return v + 1, nil }),
	)).WithObserver(collector)

	output, err := pipeline.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != 42 {
		t.Fatalf("output = %v, want 42", output)
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if len(collector.traces) == 0 || len(collector.metrics) == 0 || len(collector.profiles) == 0 {
		t.Fatalf("families: traces=%d metrics=%d profiles=%d", len(collector.traces), len(collector.metrics), len(collector.profiles))
	}
	foundAttempt := false
	for _, event := range collector.traces {
		if event.Scope == ObservationAttempt && event.Location.Step == "increment" && event.Location.Attempt == 1 {
			foundAttempt = true
		}
		if event.Location.RunID == "" || event.Location.Pipeline != "orders" {
			t.Fatalf("bad identity: %+v", event.Location)
		}
	}
	if !foundAttempt {
		t.Fatal("missing attempt trace")
	}
	for _, sample := range collector.metrics {
		if sample.Name != "pipeflow.execution.count" && sample.Name != "pipeflow.execution.duration" {
			t.Fatalf("unexpected metric %q", sample.Name)
		}
	}
}

func TestObserverPanicNeverChangesExecution(t *testing.T) {
	pipeline := NewPipeline("safe", NewStage("stage", NewStep("step", func() (string, error) { return "ok", nil }))).WithObserver(ObserverFuncs{
		Trace:   func(TraceEvent) { panic("observer unavailable") },
		Metric:  func(MetricSample) { panic("observer unavailable") },
		Profile: func(ProfileSample) { panic("observer unavailable") },
	})
	output, report, err := pipeline.RunWithReport(context.Background())
	if err != nil {
		t.Fatalf("observer changed error: %v", err)
	}
	if output != "ok" || report.Status != StatusCompleted {
		t.Fatalf("output=%v status=%s", output, report.Status)
	}
}

func TestObservationFailureIsClassifiedWithoutMessage(t *testing.T) {
	collector := &observationCollector{}
	secret := "customer-secret-123"
	pipeline := NewPipeline("safe", NewStage("stage", NewStep("step", func() error { return errors.New(secret) }))).WithObserver(collector)
	_, _ = pipeline.Run(context.Background())
	collector.mu.Lock()
	defer collector.mu.Unlock()
	found := false
	for _, event := range collector.traces {
		if event.Error != nil {
			found = true
			if reflect.ValueOf(*event.Error).FieldByName("Message").IsValid() {
				t.Fatal("observation error exposes a message field")
			}
			if event.Error.Type == secret {
				t.Fatal("observation exposed error message")
			}
		}
	}
	if !found {
		t.Fatal("missing classified failure")
	}
}

func TestObservationDisabledPreservesExecution(t *testing.T) {
	pipeline := NewPipeline("plain", NewStage("stage", NewStep("step", func(v int) (int, error) { return v * 2, nil })))
	output, report, err := pipeline.RunWithReport(context.Background(), 21)
	if err != nil || output != 42 || report.Status != StatusCompleted {
		t.Fatalf("output=%v status=%s err=%v", output, report.Status, err)
	}
	if pipeline.observer != nil {
		t.Fatal("observation should be disabled by default")
	}
}

func TestObserverFuncsMaySelectOneFamily(t *testing.T) {
	var traces int
	pipeline := NewPipeline("one", NewStage("stage", NewStep("step", func() error { return nil }))).WithObserver(ObserverFuncs{Trace: func(TraceEvent) { traces++ }})
	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if traces == 0 {
		t.Fatal("trace callback not called")
	}
}
