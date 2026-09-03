package readiness

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbernier33/pipeflow"
)

// LoadConfig defines a finite, bounded load run.
type LoadConfig struct {
	Runs            int
	Concurrency     int
	StartsPerSecond float64
}

// LoadInput returns the input for a zero-based run index.
type LoadInput func(index int) (any, error)

// LoadCheck evaluates aggregate, payload-free load evidence.
type LoadCheck func(LoadSummary) Assessment

// LoadExpectation names an aggregate load requirement.
type LoadExpectation struct {
	Name  string
	Check LoadCheck
}

// ExpectLoad creates a named aggregate load expectation.
func ExpectLoad(name string, check LoadCheck) LoadExpectation {
	return LoadExpectation{Name: name, Check: check}
}

// LoadPlan executes a Scenario repeatedly with bounded concurrency.
type LoadPlan struct {
	scenario     Scenario
	config       LoadConfig
	input        LoadInput
	expectations []LoadExpectation
}

// NewLoadPlan creates a finite load plan. Runs and Concurrency must be positive.
func NewLoadPlan(scenario Scenario, config LoadConfig) LoadPlan {
	return LoadPlan{scenario: scenario, config: config}
}

// Inputs supplies deterministic per-run inputs. Without a factory, the
// Scenario's existing input configuration is reused.
func (p LoadPlan) Inputs(input LoadInput) LoadPlan {
	p.input = input
	return p
}

// Expect appends aggregate load expectations in declaration order.
func (p LoadPlan) Expect(expectations ...LoadExpectation) LoadPlan {
	p.expectations = append(append([]LoadExpectation(nil), p.expectations...), expectations...)
	return p
}

// LoadRunResult is one payload-free Scenario execution result.
type LoadRunResult struct {
	Index    int
	Verdict  Verdict
	Duration time.Duration
	Report   pipeflow.RunReport
	Error    error
}

// LoadSummary contains aggregate load measurements.
type LoadSummary struct {
	Requested        int
	Started          int
	Completed        int
	PipelineFailures int
	ScenarioFailures int
	Reviews          int
	MaxConcurrent    int
	WallDuration     time.Duration
	Throughput       float64
	MinLatency       time.Duration
	MeanLatency      time.Duration
	P50Latency       time.Duration
	P95Latency       time.Duration
	P99Latency       time.Duration
	MaxLatency       time.Duration
}

// LoadResult contains per-run summaries and aggregate checks.
type LoadResult struct {
	Verdict Verdict
	Summary LoadSummary
	Runs    []LoadRunResult
	Checks  []CheckResult
	Error   error
}

// Run executes the bounded load plan and waits for every started run.
func (p LoadPlan) Run(ctx context.Context) LoadResult {
	if err := p.validate(); err != nil {
		return LoadResult{Verdict: VerdictFail, Error: err}
	}
	if ctx == nil {
		return LoadResult{Verdict: VerdictFail, Error: errors.New("readiness: context cannot be nil")}
	}
	startedAt := time.Now()
	runs := make([]LoadRunResult, p.config.Runs)
	jobs := make(chan int)
	var workers sync.WaitGroup
	var active atomic.Int64
	var maximum atomic.Int64
	var started atomic.Int64

	workers.Add(p.config.Concurrency)
	for worker := 0; worker < p.config.Concurrency; worker++ {
		go func() {
			defer workers.Done()
			for index := range jobs {
				current := active.Add(1)
				updateMaximum(&maximum, current)
				started.Add(1)
				runs[index] = p.runOne(ctx, index)
				active.Add(-1)
			}
		}()
	}

	submitErr := p.submit(ctx, jobs)
	close(jobs)
	workers.Wait()
	completedAt := time.Now()
	startedCount := int(started.Load())
	runs = runs[:startedCount]
	sort.Slice(runs, func(a, b int) bool { return runs[a].Index < runs[b].Index })

	result := LoadResult{Verdict: VerdictPass, Runs: runs}
	result.Summary = summarizeLoad(p.config.Runs, runs, int(maximum.Load()), completedAt.Sub(startedAt))
	for _, run := range runs {
		result.Verdict = combine(result.Verdict, run.Verdict)
	}
	for _, expectation := range p.expectations {
		checked := evaluateLoadCheck(expectation, result.Summary)
		result.Checks = append(result.Checks, checked)
		result.Verdict = combine(result.Verdict, checked.Verdict)
	}
	if submitErr != nil {
		result.Error = submitErr
		result.Verdict = VerdictFail
	}
	return result
}

func (p LoadPlan) validate() error {
	if p.config.Runs < 1 {
		return errors.New("readiness: load runs must be positive")
	}
	if p.config.Concurrency < 1 {
		return errors.New("readiness: load concurrency must be positive")
	}
	if p.config.StartsPerSecond < 0 || math.IsNaN(p.config.StartsPerSecond) || math.IsInf(p.config.StartsPerSecond, 0) {
		return errors.New("readiness: load starts per second must be finite and non-negative")
	}
	base := p.scenario
	base.input = nil
	base.hasInput = false
	if err := base.validate(); err != nil {
		return fmt.Errorf("readiness: load scenario: %w", err)
	}
	seen := make(map[string]struct{}, len(p.expectations))
	for _, expectation := range p.expectations {
		if expectation.Name == "" {
			return errors.New("readiness: load expectation name cannot be empty")
		}
		if expectation.Check == nil {
			return fmt.Errorf("readiness: load expectation %q has a nil check", expectation.Name)
		}
		if _, exists := seen[expectation.Name]; exists {
			return fmt.Errorf("readiness: duplicate load expectation %q", expectation.Name)
		}
		seen[expectation.Name] = struct{}{}
	}
	return nil
}

func (p LoadPlan) submit(ctx context.Context, jobs chan<- int) error {
	var ticker *time.Ticker
	if p.config.StartsPerSecond > 0 {
		interval := time.Duration(float64(time.Second) / p.config.StartsPerSecond)
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}
	for index := 0; index < p.config.Runs; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ticker != nil && index > 0 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case jobs <- index:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p LoadPlan) runOne(ctx context.Context, index int) LoadRunResult {
	scenario := p.scenario
	if p.input != nil {
		input, err := invokeLoadInput(p.input, index)
		if err != nil {
			return LoadRunResult{Index: index, Verdict: VerdictFail, Error: err}
		}
		scenario = scenario.WithInput(input)
	}
	result := scenario.Run(ctx)
	duration := result.Report.Duration
	if duration == 0 {
		duration = result.Duration
	}
	return LoadRunResult{
		Index: index, Verdict: result.Verdict, Duration: duration, Error: result.Error,
		Report: result.Report,
	}
}

func invokeLoadInput(input LoadInput, index int) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("readiness: load input %d panicked: %v", index, recovered)
		}
	}()
	return input(index)
}

func updateMaximum(maximum *atomic.Int64, current int64) {
	for previous := maximum.Load(); current > previous; previous = maximum.Load() {
		if maximum.CompareAndSwap(previous, current) {
			return
		}
	}
}

func summarizeLoad(requested int, runs []LoadRunResult, maxConcurrent int, wall time.Duration) LoadSummary {
	summary := LoadSummary{Requested: requested, Started: len(runs), Completed: len(runs), MaxConcurrent: maxConcurrent, WallDuration: wall}
	latencies := make([]time.Duration, 0, len(runs))
	var total time.Duration
	for _, run := range runs {
		latencies = append(latencies, run.Duration)
		total += run.Duration
		if run.Report.Error != nil || run.Report.Status == pipeflow.StatusFailed || run.Report.Status == pipeflow.StatusTimeout || run.Report.Status == pipeflow.StatusCancelled {
			summary.PipelineFailures++
		}
		if run.Verdict == VerdictFail {
			summary.ScenarioFailures++
		} else if run.Verdict == VerdictReview {
			summary.Reviews++
		}
	}
	if wall > 0 {
		summary.Throughput = float64(len(runs)) / wall.Seconds()
	}
	if len(latencies) == 0 {
		return summary
	}
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	summary.MinLatency = latencies[0]
	summary.MaxLatency = latencies[len(latencies)-1]
	summary.MeanLatency = total / time.Duration(len(latencies))
	summary.P50Latency = percentile(latencies, 0.50)
	summary.P95Latency = percentile(latencies, 0.95)
	summary.P99Latency = percentile(latencies, 0.99)
	return summary
}

func percentile(sorted []time.Duration, percentile float64) time.Duration {
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func evaluateLoadCheck(expectation LoadExpectation, summary LoadSummary) (result CheckResult) {
	result.Name = expectation.Name
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Verdict = VerdictFail
			result.Observed = "load expectation panicked"
			result.Error = fmt.Errorf("readiness: load expectation %q panicked: %v", expectation.Name, recovered)
		}
	}()
	assessment := expectation.Check(summary)
	result.Verdict, result.Observed = assessment.Verdict, assessment.Observed
	if result.Verdict != VerdictPass && result.Verdict != VerdictFail && result.Verdict != VerdictReview {
		result.Verdict = VerdictFail
		result.Error = fmt.Errorf("readiness: load expectation %q returned invalid verdict %q", expectation.Name, assessment.Verdict)
	}
	return result
}

// P95Within requires p95 Pipeline execution latency not to exceed maximum.
func P95Within(maximum time.Duration) LoadExpectation {
	return ExpectLoad("p95 latency", func(summary LoadSummary) Assessment {
		if summary.P95Latency <= maximum {
			return Passed(fmt.Sprintf("p95 %s <= %s", summary.P95Latency, maximum))
		}
		return Failed(fmt.Sprintf("p95 %s > %s", summary.P95Latency, maximum))
	})
}

// PipelineFailureRateAtMost limits the fraction of runs with failed, timed out,
// or cancelled Pipeline reports.
func PipelineFailureRateAtMost(maximum float64) LoadExpectation {
	return ExpectLoad("pipeline failure rate", func(summary LoadSummary) Assessment {
		rate := 0.0
		if summary.Completed > 0 {
			rate = float64(summary.PipelineFailures) / float64(summary.Completed)
		}
		if maximum >= 0 && maximum <= 1 && rate <= maximum {
			return Passed(fmt.Sprintf("failure rate %.4f <= %.4f", rate, maximum))
		}
		return Failed(fmt.Sprintf("failure rate %.4f > %.4f", rate, maximum))
	})
}

// ThroughputAtLeast requires a minimum completed-runs-per-second rate.
func ThroughputAtLeast(minimum float64) LoadExpectation {
	return ExpectLoad("throughput", func(summary LoadSummary) Assessment {
		if minimum >= 0 && summary.Throughput >= minimum {
			return Passed(fmt.Sprintf("throughput %.2f/s >= %.2f/s", summary.Throughput, minimum))
		}
		return Failed(fmt.Sprintf("throughput %.2f/s < %.2f/s", summary.Throughput, minimum))
	})
}
