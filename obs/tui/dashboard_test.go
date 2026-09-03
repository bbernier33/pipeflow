package tui_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bbernier33/pipeflow/obs"
	obshttp "github.com/bbernier33/pipeflow/obs/http"
	"github.com/bbernier33/pipeflow/obs/tui"
)

type notifyBuffer struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	once    sync.Once
	written chan struct{}
}

func newNotifyBuffer() *notifyBuffer { return &notifyBuffer{written: make(chan struct{})} }
func (b *notifyBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buffer.Write(value)
	b.once.Do(func() { close(b.written) })
	return n, err
}
func (b *notifyBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }

func TestRenderDashboard(t *testing.T) {
	dashboard := obshttp.Dashboard{RetrievedAt: time.Now(), Snapshot: obshttp.OperationalSnapshot{CapturedAt: time.Now(), Pipelines: []obs.PipelineView{{Name: "orders", Health: obs.HealthEvidence{Health: obs.HealthBusy}, ActiveRuns: 1}}, Workers: []obs.WorkerView{{Name: "primary", Active: 2, Concurrency: 2, InFlight: 3}}, Queues: []obs.QueueView{{Name: "primary.queue", Depth: 1, Capacity: 10, Utilization: .1}}, Resources: obs.ResourceView{Samples: 1, Latest: &obs.ResourceSample{HeapAlloc: 1024, HeapInUse: 2048, Goroutines: 4}}}, Analysis: obs.Analysis{Findings: []obs.Finding{{Code: "worker_saturation", Severity: obs.SeverityWarning, Summary: "workers busy", Pipeline: "orders"}}}}
	var output bytes.Buffer
	if err := tui.Render(&output, dashboard, tui.Options{}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"PIPEFLOW OPERATIONS", "orders", "primary.queue", "1.0KiB", "worker_saturation", "WARNING"} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("missing %q in:\n%s", expected, output.String())
		}
	}
	if strings.Contains(output.String(), "\x1b[") {
		t.Fatal("unexpected ANSI output")
	}
}

func TestRenderSupportsColorAndClear(t *testing.T) {
	var output bytes.Buffer
	if err := tui.Render(&output, obshttp.Dashboard{}, tui.Options{Color: true, ClearScreen: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "\x1b[H\x1b[2J") || !strings.Contains(output.String(), "\x1b[1;36m") {
		t.Fatalf("output=%q", output.String())
	}
}

func TestRunStopsCleanlyAfterCancellation(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	handler, err := obshttp.NewHandler(collector, obshttp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	output := newNotifyBuffer()
	done := make(chan error, 1)
	go func() {
		done <- tui.Run(ctx, output, obshttp.Client{BaseURL: server.URL, HTTPClient: server.Client()}, tui.Options{Refresh: time.Hour})
	}()
	<-output.written
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "PIPEFLOW OPERATIONS") {
		t.Fatalf("output=%s", output.String())
	}
}
