package readiness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bbernier33/pipeflow"
)

// PressureConfig defines a finite Worker queue/backpressure exercise.
type PressureConfig struct {
	Items                 int
	Worker                pipeflow.WorkerOptions
	SubmitTimeout         time.Duration
	BackpressureThreshold time.Duration
	SampleInterval        time.Duration
}

// PressureInput supplies one zero-based item input.
type PressureInput func(index int) (any, error)

// PressureCheck evaluates aggregate, payload-free pressure evidence.
type PressureCheck func(PressureSummary) Assessment

// PressureExpectation names one pressure requirement.
type PressureExpectation struct {
	Name  string
	Check PressureCheck
}

// ExpectPressure creates a named pressure expectation.
func ExpectPressure(name string, check PressureCheck) PressureExpectation {
	return PressureExpectation{Name: name, Check: check}
}

// PressurePlan drives a real Pipeflow Worker under bounded pressure.
type PressurePlan struct {
	scenario     Scenario
	config       PressureConfig
	input        PressureInput
	expectations []PressureExpectation
}

// NewPressurePlan creates a Worker pressure plan.
func NewPressurePlan(scenario Scenario, config PressureConfig) PressurePlan {
	return PressurePlan{scenario: scenario, config: config}
}

// Inputs supplies deterministic inputs. Without it, the Scenario input is reused.
func (p PressurePlan) Inputs(input PressureInput) PressurePlan {
	p.input = input
	return p
}

// Expect appends aggregate pressure expectations.
func (p PressurePlan) Expect(expectations ...PressureExpectation) PressurePlan {
	p.expectations = append(append([]PressureExpectation(nil), p.expectations...), expectations...)
	return p
}

// PressureItemResult retains one accepted Work result without its output.
type PressureItemResult struct {
	Index             int
	Accepted          bool
	SubmissionLatency time.Duration
	SubmitError       error
	Scenario          Result
}

// PressureSummary describes observed queue, backpressure, and drain behavior.
type PressureSummary struct {
	Requested            int
	Accepted             int
	Completed            int
	Rejected             int
	PipelineFailures     int
	ScenarioFailures     int
	BlockedSubmissions   int
	MaxSubmissionLatency time.Duration
	MaxQueueDepth        int
	MaxInFlight          int
	MaxActive            int
	CompletionOrder      []int
	Drained              bool
	FinalWorkerStatus    pipeflow.Status
}

// PressureResult contains per-item evidence and aggregate checks.
type PressureResult struct {
	Verdict Verdict
	Summary PressureSummary
	Items   []PressureItemResult
	Checks  []CheckResult
	Error   error
}

type acceptedPressureWork struct {
	index   int
	latency time.Duration
	work    *pipeflow.Work
}

// Run submits every item, closes intake, and waits for graceful drain.
func (p PressurePlan) Run(ctx context.Context) PressureResult {
	config, err := p.validatedConfig()
	if err != nil {
		return PressureResult{Verdict: VerdictFail, Error: err}
	}
	if ctx == nil {
		return PressureResult{Verdict: VerdictFail, Error: errors.New("readiness: context cannot be nil")}
	}
	worker, err := p.scenario.pipeline.StartWorker(ctx, config.Worker)
	if err != nil {
		return PressureResult{Verdict: VerdictFail, Error: fmt.Errorf("readiness: start pressure Worker: %w", err)}
	}

	maximums := &pressureMaximums{}
	stopSampling := make(chan struct{})
	samplingDone := make(chan struct{})
	go sampleWorker(worker, config.SampleInterval, maximums, stopSampling, samplingDone)

	items := make([]PressureItemResult, 0, config.Items)
	accepted := make([]acceptedPressureWork, 0, config.Items)
	for index := 0; index < config.Items; index++ {
		input, inputErr := p.pressureInput(index)
		if inputErr != nil {
			items = append(items, PressureItemResult{Index: index, SubmitError: inputErr})
			continue
		}
		submitCtx := ctx
		cancel := func() {}
		if config.SubmitTimeout > 0 {
			submitCtx, cancel = context.WithTimeout(ctx, config.SubmitTimeout)
		}
		started := time.Now()
		work, submitErr := worker.Submit(submitCtx, input)
		latency := time.Since(started)
		cancel()
		item := PressureItemResult{Index: index, Accepted: submitErr == nil, SubmissionLatency: latency, SubmitError: submitErr}
		items = append(items, item)
		if submitErr == nil {
			accepted = append(accepted, acceptedPressureWork{index: index, latency: latency, work: work})
		}
		maximums.observe(worker.State())
		if ctx.Err() != nil {
			break
		}
	}
	worker.Close()

	for _, acceptedWork := range accepted {
		output, report, workErr := acceptedWork.work.Wait()
		scenarioResult := p.evaluateWork(output, report, workErr)
		for itemIndex := range items {
			if items[itemIndex].Index == acceptedWork.index {
				items[itemIndex].Scenario = scenarioResult
				break
			}
		}
	}
	waitErr := worker.Wait()
	close(stopSampling)
	<-samplingDone
	maximums.observe(worker.State())

	result := PressureResult{Verdict: VerdictPass, Items: items}
	result.Summary = summarizePressure(config, items, maximums.snapshot(), worker.State())
	for _, item := range items {
		if item.SubmitError != nil {
			result.Verdict = VerdictFail
			continue
		}
		result.Verdict = combine(result.Verdict, item.Scenario.Verdict)
	}
	for _, expectation := range p.expectations {
		checked := evaluatePressureCheck(expectation, result.Summary)
		result.Checks = append(result.Checks, checked)
		result.Verdict = combine(result.Verdict, checked.Verdict)
	}
	if waitErr != nil {
		result.Error = waitErr
		result.Verdict = VerdictFail
	}
	return result
}

func (p PressurePlan) validatedConfig() (PressureConfig, error) {
	config := p.config
	if config.Items < 1 {
		return config, errors.New("readiness: pressure items must be positive")
	}
	if config.SubmitTimeout < 0 || config.BackpressureThreshold < 0 || config.SampleInterval < 0 {
		return config, errors.New("readiness: pressure durations cannot be negative")
	}
	if config.BackpressureThreshold == 0 {
		config.BackpressureThreshold = time.Millisecond
	}
	if config.SampleInterval == 0 {
		config.SampleInterval = time.Millisecond
	}
	base := p.scenario
	base.input, base.hasInput = nil, false
	if err := base.validate(); err != nil {
		return config, fmt.Errorf("readiness: pressure scenario: %w", err)
	}
	seen := make(map[string]struct{}, len(p.expectations))
	for _, expectation := range p.expectations {
		if expectation.Name == "" || expectation.Check == nil {
			return config, errors.New("readiness: pressure expectations require a name and check")
		}
		if _, exists := seen[expectation.Name]; exists {
			return config, fmt.Errorf("readiness: duplicate pressure expectation %q", expectation.Name)
		}
		seen[expectation.Name] = struct{}{}
	}
	return config, nil
}

func (p PressurePlan) pressureInput(index int) (value any, err error) {
	if p.input == nil {
		return p.scenario.input, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("readiness: pressure input %d panicked: %v", index, recovered)
		}
	}()
	return p.input(index)
}

func (p PressurePlan) evaluateWork(output any, report pipeflow.RunReport, runErr error) Result {
	result := Result{Name: p.scenario.name, Arrangement: p.scenario.arrangement, Verdict: VerdictPass, StartedAt: report.StartedAt, EndedAt: report.EndedAt, Duration: report.Duration, Report: report}
	observation := Observation{Output: output, Report: report, Err: runErr}
	for _, expectation := range p.scenario.expectations {
		checked := evaluate(expectation, observation)
		result.Checks = append(result.Checks, checked)
		result.Verdict = combine(result.Verdict, checked.Verdict)
	}
	return result
}

type pressureMaximums struct {
	mu       sync.Mutex
	queue    int
	inFlight int
	active   int
}

func (m *pressureMaximums) observe(state pipeflow.WorkerState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state.QueueDepth > m.queue {
		m.queue = state.QueueDepth
	}
	if state.InFlight > m.inFlight {
		m.inFlight = state.InFlight
	}
	if state.Active > m.active {
		m.active = state.Active
	}
}

type pressurePeaks struct {
	queue    int
	inFlight int
	active   int
}

func (m *pressureMaximums) snapshot() pressurePeaks {
	m.mu.Lock()
	defer m.mu.Unlock()
	return pressurePeaks{queue: m.queue, inFlight: m.inFlight, active: m.active}
}

func sampleWorker(worker *pipeflow.Worker, interval time.Duration, maximums *pressureMaximums, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		maximums.observe(worker.State())
		select {
		case <-ticker.C:
		case <-stop:
			return
		}
	}
}

func summarizePressure(config PressureConfig, items []PressureItemResult, maximums pressurePeaks, final pipeflow.WorkerState) PressureSummary {
	summary := PressureSummary{Requested: config.Items, MaxQueueDepth: maximums.queue, MaxInFlight: maximums.inFlight, MaxActive: maximums.active, FinalWorkerStatus: final.Status}
	type completion struct {
		index int
		ended time.Time
	}
	var completed []completion
	for _, item := range items {
		if !item.Accepted {
			summary.Rejected++
			continue
		}
		summary.Accepted++
		if item.SubmissionLatency >= config.BackpressureThreshold {
			summary.BlockedSubmissions++
		}
		if item.SubmissionLatency > summary.MaxSubmissionLatency {
			summary.MaxSubmissionLatency = item.SubmissionLatency
		}
		if item.Scenario.Report.RunID != "" {
			summary.Completed++
			completed = append(completed, completion{index: item.Index, ended: item.Scenario.Report.EndedAt})
		}
		if item.Scenario.Report.Error != nil {
			summary.PipelineFailures++
		}
		if item.Scenario.Verdict == VerdictFail {
			summary.ScenarioFailures++
		}
	}
	sort.SliceStable(completed, func(a, b int) bool {
		if completed[a].ended.Equal(completed[b].ended) {
			return completed[a].index < completed[b].index
		}
		return completed[a].ended.Before(completed[b].ended)
	})
	for _, item := range completed {
		summary.CompletionOrder = append(summary.CompletionOrder, item.index)
	}
	summary.Drained = summary.Accepted == summary.Completed && final.InFlight == 0 && final.QueueDepth == 0
	return summary
}

func evaluatePressureCheck(expectation PressureExpectation, summary PressureSummary) (result CheckResult) {
	result.Name = expectation.Name
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Verdict = VerdictFail
			result.Observed = "pressure expectation panicked"
			result.Error = fmt.Errorf("readiness: pressure expectation %q panicked: %v", expectation.Name, recovered)
		}
	}()
	assessment := expectation.Check(summary)
	result.Verdict, result.Observed = assessment.Verdict, assessment.Observed
	if result.Verdict != VerdictPass && result.Verdict != VerdictFail && result.Verdict != VerdictReview {
		result.Verdict = VerdictFail
		result.Error = fmt.Errorf("readiness: pressure expectation %q returned invalid verdict %q", expectation.Name, assessment.Verdict)
	}
	return result
}

// BackpressureObserved requires at least one materially blocked submission.
func BackpressureObserved() PressureExpectation {
	return ExpectPressure("backpressure observed", func(summary PressureSummary) Assessment {
		if summary.BlockedSubmissions > 0 {
			return Passed(fmt.Sprintf("%d submissions blocked", summary.BlockedSubmissions))
		}
		return Failed("no submission crossed the backpressure threshold")
	})
}

// AllAcceptedWorkDrained requires every accepted item to complete before shutdown.
func AllAcceptedWorkDrained() PressureExpectation {
	return ExpectPressure("accepted work drained", func(summary PressureSummary) Assessment {
		if summary.Drained {
			return Passed(fmt.Sprintf("all %d accepted items completed", summary.Accepted))
		}
		return Failed(fmt.Sprintf("accepted=%d completed=%d", summary.Accepted, summary.Completed))
	})
}

// QueueDepthAtMost checks the observed bounded queue depth.
func QueueDepthAtMost(maximum int) PressureExpectation {
	return ExpectPressure("queue depth bound", func(summary PressureSummary) Assessment {
		if maximum >= 0 && summary.MaxQueueDepth <= maximum {
			return Passed(fmt.Sprintf("max queue depth %d <= %d", summary.MaxQueueDepth, maximum))
		}
		return Failed(fmt.Sprintf("max queue depth %d > %d", summary.MaxQueueDepth, maximum))
	})
}

// CompletionOrderPreserved requires completion in submission-index order.
func CompletionOrderPreserved() PressureExpectation {
	return ExpectPressure("completion order preserved", func(summary PressureSummary) Assessment {
		for index, completed := range summary.CompletionOrder {
			if completed != index {
				return Failed(fmt.Sprintf("completion position %d contains item %d", index, completed))
			}
		}
		return Passed("completion order matches submission order")
	})
}
