// Package tui renders a read-only terminal view of remote Pipeflow operations.
package tui

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	obshttp "github.com/bbernier33/pipeflow/obs/http"
)

type Options struct {
	Refresh     time.Duration
	Color       bool
	ClearScreen bool
}

func (o Options) normalized() Options {
	if o.Refresh <= 0 {
		o.Refresh = 2 * time.Second
	}
	return o
}

// Run refreshes until ctx is cancelled. Fetch failures are rendered and
// retried; cancellation is a normal clean shutdown.
func Run(ctx context.Context, output io.Writer, client obshttp.Client, options Options) error {
	if ctx == nil {
		return fmt.Errorf("pipeflow obs tui: nil context")
	}
	if output == nil {
		return fmt.Errorf("pipeflow obs tui: nil output")
	}
	options = options.normalized()
	ticker := time.NewTicker(options.Refresh)
	defer ticker.Stop()
	for {
		dashboard, err := client.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			renderFailure(output, err, options)
		} else if err := Render(output, dashboard, options); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Render writes one deterministic dashboard frame.
func Render(output io.Writer, dashboard obshttp.Dashboard, options Options) error {
	if output == nil {
		return fmt.Errorf("pipeflow obs tui: nil output")
	}
	var b strings.Builder
	if options.ClearScreen {
		b.WriteString("\x1b[H\x1b[2J")
	}
	fmt.Fprintf(&b, "%s\n", style("PIPEFLOW OPERATIONS", "1;36", options.Color))
	fmt.Fprintf(&b, "Captured: %s  Retrieved: %s\n", formatTime(dashboard.Snapshot.CapturedAt), formatTime(dashboard.RetrievedAt))
	fmt.Fprintf(&b, "Pipelines: %d  Active runs: %d  Findings: %d\n", len(dashboard.Snapshot.Pipelines), activeRuns(dashboard), len(dashboard.Analysis.Findings))
	section(&b, "PIPELINES")
	if len(dashboard.Snapshot.Pipelines) == 0 {
		b.WriteString("  No observed pipelines\n")
	}
	for _, pipeline := range dashboard.Snapshot.Pipelines {
		fmt.Fprintf(&b, "  %-24s %-10s active=%-3d completed=%-6d failed=%d\n", pipeline.Name, strings.ToUpper(string(pipeline.Health.Health)), pipeline.ActiveRuns, pipeline.Completed, pipeline.Failed)
	}
	if len(dashboard.Snapshot.Workers) > 0 || len(dashboard.Snapshot.Queues) > 0 {
		section(&b, "WORKERS / QUEUES")
		for _, worker := range dashboard.Snapshot.Workers {
			fmt.Fprintf(&b, "  %-20s %-10s active=%d/%d in-flight=%d failed=%d\n", worker.Name, worker.Status, worker.Active, worker.Concurrency, worker.InFlight, worker.Failed)
		}
		for _, queue := range dashboard.Snapshot.Queues {
			fmt.Fprintf(&b, "  %-20s depth=%d/%d utilization=%5.1f%%\n", queue.Name, queue.Depth, queue.Capacity, queue.Utilization*100)
		}
	}
	if dashboard.Snapshot.Resources.Latest != nil {
		resource := dashboard.Snapshot.Resources.Latest
		section(&b, "GO RUNTIME")
		fmt.Fprintf(&b, "  heap=%s heap-in-use=%s stack=%s goroutines=%d GC=%d pause=%s\n", formatBytes(resource.HeapAlloc), formatBytes(resource.HeapInUse), formatBytes(resource.StackInUse), resource.Goroutines, resource.GCCycles, resource.LastGCPause)
	}
	if len(dashboard.Snapshot.Circuits) > 0 || len(dashboard.Snapshot.Recoveries) > 0 || len(dashboard.Snapshot.Idempotency) > 0 {
		section(&b, "RESILIENCE")
		for _, circuit := range dashboard.Snapshot.Circuits {
			fmt.Fprintf(&b, "  circuit %-18s state=%-10s failures=%d short-circuits=%d\n", circuit.Dependency, circuit.State, circuit.Failures, circuit.ShortCircuits)
		}
		for _, recovery := range dashboard.Snapshot.Recoveries {
			fmt.Fprintf(&b, "  recovery %-17s activations=%d completed=%d failed=%d\n", recovery.Name, recovery.Activations, recovery.Completed, recovery.Failed)
		}
		for _, guard := range dashboard.Snapshot.Idempotency {
			fmt.Fprintf(&b, "  idempotency %-14s executed=%d duplicates=%d conflicts=%d store-failures=%d\n", guard.Guard, guard.Executed, guard.DuplicateCompleted, guard.DuplicateInProgress, guard.StoreFailures)
		}
	}
	section(&b, "EXPLAIN")
	if dashboard.Analysis.PrimaryBottleneck != nil {
		fmt.Fprintf(&b, "  Primary: %s - %s\n", dashboard.Analysis.PrimaryBottleneck.Code, dashboard.Analysis.PrimaryBottleneck.Summary)
	}
	if len(dashboard.Analysis.Findings) == 0 {
		b.WriteString("  No configured analysis threshold crossed\n")
	}
	for _, finding := range dashboard.Analysis.Findings {
		fmt.Fprintf(&b, "  %s %-28s %s%s\n", severityLabel(string(finding.Severity), options.Color), finding.Code, location(finding.Pipeline, finding.Stage, finding.Step, finding.Worker, finding.Dependency, finding.Guard), finding.Summary)
	}
	if dashboard.Snapshot.DroppedTraces+dashboard.Snapshot.DroppedMetrics+dashboard.Snapshot.DroppedProfiles+dashboard.Snapshot.DroppedResources > 0 {
		section(&b, "RETENTION")
		fmt.Fprintf(&b, "  dropped trace=%d metric=%d profile=%d resource=%d\n", dashboard.Snapshot.DroppedTraces, dashboard.Snapshot.DroppedMetrics, dashboard.Snapshot.DroppedProfiles, dashboard.Snapshot.DroppedResources)
	}
	_, err := io.WriteString(output, b.String())
	return err
}

func renderFailure(output io.Writer, err error, options Options) {
	var b strings.Builder
	if options.ClearScreen {
		b.WriteString("\x1b[H\x1b[2J")
	}
	fmt.Fprintf(&b, "%s\n  %v\n  Retrying in %s...\n", style("PIPEFLOW OPERATIONS - DISCONNECTED", "1;31", options.Color), err, options.Refresh)
	_, _ = io.WriteString(output, b.String())
}
func section(b *strings.Builder, title string) { fmt.Fprintf(b, "\n-- %s --\n", title) }
func style(value, code string, enabled bool) string {
	if !enabled {
		return value
	}
	return "\x1b[" + code + "m" + value + "\x1b[0m"
}
func severityLabel(value string, color bool) string {
	code := "36"
	if value == "warning" {
		code = "33"
	}
	if value == "critical" {
		code = "31"
	}
	return style(strings.ToUpper(value), code, color)
}
func formatTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.Local().Format("2006-01-02 15:04:05")
}
func formatBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	amount := float64(value)
	unit := units[0]
	for i := 1; i < len(units) && amount >= 1024; i++ {
		amount /= 1024
		unit = units[i]
	}
	return fmt.Sprintf("%.1f%s", amount, unit)
}
func activeRuns(d obshttp.Dashboard) int64 {
	var total int64
	for _, pipeline := range d.Snapshot.Pipelines {
		total += pipeline.ActiveRuns
	}
	return total
}
func location(values ...string) string {
	var present []string
	for _, value := range values {
		if value != "" {
			present = append(present, value)
		}
	}
	if len(present) == 0 {
		return ""
	}
	return "[" + strings.Join(present, "/") + "] "
}
