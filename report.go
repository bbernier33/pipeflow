package pipeflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RunReport contains execution facts and never retains flowing business values.
type RunReport struct {
	RunID       string
	Pipeline    string
	Status      Status
	StartedAt   time.Time
	EndedAt     time.Time
	Duration    time.Duration
	Stages      []StageReport
	Cleanups    []CleanupReport
	Backgrounds []BackgroundReport
	Error       error
}

type BackgroundReport struct {
	Name          string
	FailurePolicy BackgroundFailurePolicy
	Status        Status
	StartedAt     time.Time
	EndedAt       time.Time
	Duration      time.Duration
	Error         error
}

type CleanupReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Error     error
}

type StageReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Steps     []StepReport
	Parallels []ParallelReport
	Subflows  []SubflowReport
	Error     error
	Metadata  ResultMetadata
}

// SubflowReport contains a reusable group's nested Stage reports.
type SubflowReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Stages    []StageReport
	Error     error
}

type ParallelReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Branches  []BranchReport
	Error     error
}

type BranchReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Steps     []StepReport
	Error     error
}

type StepReport struct {
	Name      string
	Status    Status
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Attempts  []AttemptReport
	Polls     []PollReport
	Error     error
	Metadata  ResultMetadata
}

type PollReport struct {
	Poll      int
	Status    Status
	Complete  bool
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Attempts  []AttemptReport
	Error     error
}

type AttemptReport struct {
	Attempt    int
	Status     Status
	StartedAt  time.Time
	EndedAt    time.Time
	Duration   time.Duration
	RetryDelay time.Duration
	Error      error
}

type runRecorder struct {
	mu              sync.Mutex
	report          RunReport
	nestedRecorders map[subflowReportKey]*runRecorder
}

type subflowReportKey struct{ stage, subflow int }

func newRunRecorder(pipeline string, stages []Stage, backgrounds []backgroundTask) (*runRecorder, error) {
	runID, err := newRunID()
	if err != nil {
		return nil, fmt.Errorf("pipeflow: create run ID: %w", err)
	}
	report := buildRunReport(runID, pipeline, stages, backgrounds)
	return &runRecorder{report: report, nestedRecorders: make(map[subflowReportKey]*runRecorder)}, nil
}

func newNestedRecorder(runID, pipeline string, stages []Stage) *runRecorder {
	return &runRecorder{report: buildRunReport(runID, pipeline, stages, nil), nestedRecorders: make(map[subflowReportKey]*runRecorder)}
}

func buildRunReport(runID, pipeline string, stages []Stage, backgrounds []backgroundTask) RunReport {
	report := RunReport{RunID: runID, Pipeline: pipeline, Status: StatusPending}
	for _, task := range backgrounds {
		report.Backgrounds = append(report.Backgrounds, BackgroundReport{Name: task.name, FailurePolicy: task.failurePolicy, Status: StatusPending})
	}
	report.Stages = buildStageReports(stages)
	return report
}

func buildStageReports(stages []Stage) []StageReport {
	reports := make([]StageReport, 0, len(stages))
	for _, stage := range stages {
		stageReport := StageReport{Name: stage.name, Status: StatusPending}
		for _, item := range stage.items {
			switch typed := item.(type) {
			case *Step:
				stageReport.Steps = append(stageReport.Steps, StepReport{Name: typed.name, Status: StatusPending})
			case *ConcurrentSteps:
				for _, step := range typed.steps {
					stageReport.Steps = append(stageReport.Steps, StepReport{Name: step.name, Status: StatusPending})
				}
			case *Parallel:
				parallelReport := ParallelReport{Name: typed.name, Status: StatusPending}
				for _, branch := range typed.branches {
					branchReport := BranchReport{Name: branch.name, Status: StatusPending}
					for _, step := range branch.steps {
						branchReport.Steps = append(branchReport.Steps, StepReport{Name: step.name, Status: StatusPending})
					}
					parallelReport.Branches = append(parallelReport.Branches, branchReport)
				}
				stageReport.Parallels = append(stageReport.Parallels, parallelReport)
			case *Subflow:
				if typed != nil {
					stageReport.Subflows = append(stageReport.Subflows, SubflowReport{Name: typed.name, Status: StatusPending, Stages: buildStageReports(typed.stages)})
				}
			}
		}
		reports = append(reports, stageReport)
	}
	return reports
}

func (r *runRecorder) runIdentity() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.report.RunID, r.report.Pipeline
}

func (r *runRecorder) startSubflow(stage, subflow int, nested *runRecorder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Subflows[subflow]
	report.Status = StatusRunning
	report.StartedAt = time.Now()
	r.nestedRecorders[subflowReportKey{stage: stage, subflow: subflow}] = nested
}

func (r *runRecorder) finishSubflow(stage, subflow int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Subflows[subflow]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = annotateExecutionSubflow(err, report.Name)
}

func (r *runRecorder) startBackground(index int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Backgrounds[index]
	report.Status = StatusRunning
	report.StartedAt = time.Now()
}

func (r *runRecorder) finishBackground(index int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Backgrounds[index]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = err
}

func (r *runRecorder) normalizeBackgroundCompletion(index int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Backgrounds[index]
	report.Status = StatusCompleted
	report.Error = nil
}

type stepReportPath struct {
	stage, step                  int
	parallel, branch, branchStep int
	inBranch                     bool
}

func topLevelStepPath(stage, step int) stepReportPath {
	return stepReportPath{stage: stage, step: step}
}

func branchStepPath(stage, parallel, branch, step int) stepReportPath {
	return stepReportPath{stage: stage, parallel: parallel, branch: branch, branchStep: step, inBranch: true}
}

func (r *runRecorder) stepReport(path stepReportPath) *StepReport {
	if path.inBranch {
		return &r.report.Stages[path.stage].Parallels[path.parallel].Branches[path.branch].Steps[path.branchStep]
	}
	return &r.report.Stages[path.stage].Steps[path.step]
}

func (r *runRecorder) stepIdentity(path stepReportPath) (stage, step string) {
	stageReport := &r.report.Stages[path.stage]
	report := r.stepReport(path)
	return stageReport.Name, report.Name
}

func (r *runRecorder) annotateStepPath(err error, path stepReportPath, attempt int) error {
	stage, step := r.stepIdentity(path)
	err = annotateExecutionError(err, r.report.Pipeline, stage, step, attempt)
	if path.inBranch {
		parallel := r.report.Stages[path.stage].Parallels[path.parallel]
		err = annotateExecutionBranch(err, parallel.Name, parallel.Branches[path.branch].Name)
	}
	return err
}

func newRunID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (r *runRecorder) startRun() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Status = StatusRunning
	r.report.StartedAt = time.Now()
}

func (r *runRecorder) finishRun(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.report.Status = statusForError(err)
	r.report.EndedAt = now
	r.report.Duration = now.Sub(r.report.StartedAt)
	r.report.Error = err
	for i := range r.report.Backgrounds {
		if r.report.Backgrounds[i].Status == StatusPending {
			r.report.Backgrounds[i].Status = StatusSkipped
		}
	}
	markPendingSkipped(r.report.Stages)
}

func (r *runRecorder) startCleanup(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Cleanups = append(r.report.Cleanups, CleanupReport{Name: name, Status: StatusRunning, StartedAt: time.Now()})
	return len(r.report.Cleanups) - 1
}

func (r *runRecorder) finishCleanup(index int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Cleanups[index]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = err
}

func (r *runRecorder) startStage(stage int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage]
	report.Status = StatusRunning
	report.StartedAt = time.Now()
}

func (r *runRecorder) finishStage(stage int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = annotateExecutionError(err, r.report.Pipeline, report.Name, "", 0)
	for i := range report.Steps {
		if report.Steps[i].Status == StatusPending {
			report.Steps[i].Status = StatusSkipped
		}
	}
	for i := range report.Parallels {
		if report.Parallels[i].Status == StatusPending {
			report.Parallels[i].Status = StatusSkipped
			for j := range report.Parallels[i].Branches {
				report.Parallels[i].Branches[j].Status = StatusSkipped
				for k := range report.Parallels[i].Branches[j].Steps {
					report.Parallels[i].Branches[j].Steps[k].Status = StatusSkipped
				}
			}
		}
	}
	for i := range report.Subflows {
		if report.Subflows[i].Status == StatusPending {
			report.Subflows[i].Status = StatusSkipped
		}
	}
}

func (r *runRecorder) setStageMetadata(stage int, metadata ResultMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Stages[stage].Metadata = metadata
}

func (r *runRecorder) startParallel(stage, parallel int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Parallels[parallel]
	report.Status = StatusRunning
	report.StartedAt = time.Now()
}

func (r *runRecorder) finishParallel(stage, parallel int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Parallels[parallel]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = annotateExecutionError(err, r.report.Pipeline, r.report.Stages[stage].Name, "", 0)
}

func (r *runRecorder) startBranch(stage, parallel, branch int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Parallels[parallel].Branches[branch]
	report.Status = StatusRunning
	report.StartedAt = time.Now()
}

func (r *runRecorder) finishBranch(stage, parallel, branch int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &r.report.Stages[stage].Parallels[parallel].Branches[branch]
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = annotateExecutionError(err, r.report.Pipeline, r.report.Stages[stage].Name, "", 0)
	for i := range report.Steps {
		if report.Steps[i].Status == StatusPending {
			report.Steps[i].Status = StatusSkipped
		}
	}
}

func (r *runRecorder) startStep(path stepReportPath) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	report.Status = StatusRunning
	report.StartedAt = time.Now()
}

func (r *runRecorder) skipStep(path stepReportPath) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	report.Status = StatusSkipped
	report.StartedAt = time.Time{}
	report.EndedAt = time.Time{}
	report.Duration = 0
	report.Error = nil
}

func (r *runRecorder) finishStep(path stepReportPath, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	now := time.Now()
	report.Status = statusForError(err)
	report.EndedAt = now
	report.Duration = now.Sub(report.StartedAt)
	report.Error = r.annotateStepPath(err, path, 0)
}

func (r *runRecorder) setStepMetadata(path stepReportPath, metadata ResultMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepReport(path).Metadata = metadata
}

func (r *runRecorder) startPoll(path stepReportPath, poll int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	report.Polls = append(report.Polls, PollReport{Poll: poll, Status: StatusRunning, StartedAt: time.Now()})
}

func (r *runRecorder) finishPoll(path stepReportPath, poll int, complete bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	pollReport := &report.Polls[len(report.Polls)-1]
	now := time.Now()
	pollReport.Status = statusForError(err)
	pollReport.Complete = complete
	pollReport.EndedAt = now
	pollReport.Duration = now.Sub(pollReport.StartedAt)
	pollReport.Error = annotateExecutionPoll(r.annotateStepPath(err, path, 0), poll)
}

func (r *runRecorder) startAttempt(path stepReportPath, poll, attempt int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	attemptReport := AttemptReport{Attempt: attempt, Status: StatusRunning, StartedAt: time.Now()}
	report.Attempts = append(report.Attempts, attemptReport)
	if poll > 0 {
		pollReport := &report.Polls[len(report.Polls)-1]
		pollReport.Attempts = append(pollReport.Attempts, attemptReport)
	}
}

func (r *runRecorder) finishAttempt(path stepReportPath, poll, attempt int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	attemptReport := &report.Attempts[len(report.Attempts)-1]
	now := time.Now()
	attemptReport.Status = statusForError(err)
	attemptReport.EndedAt = now
	attemptReport.Duration = now.Sub(attemptReport.StartedAt)
	attemptReport.Error = r.annotateStepPath(err, path, attempt)
	attemptReport.Error = annotateExecutionPoll(attemptReport.Error, poll)
	if poll > 0 {
		pollAttempt := &report.Polls[len(report.Polls)-1].Attempts[len(report.Polls[len(report.Polls)-1].Attempts)-1]
		*pollAttempt = *attemptReport
	}
}

func (r *runRecorder) setAttemptRetryDelay(path stepReportPath, poll int, delay time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := r.stepReport(path)
	report.Attempts[len(report.Attempts)-1].RetryDelay = delay
	if poll > 0 {
		pollReport := &report.Polls[len(report.Polls)-1]
		pollReport.Attempts[len(pollReport.Attempts)-1].RetryDelay = delay
	}
}

func (r *runRecorder) snapshot() RunReport {
	r.mu.Lock()
	result := r.report
	result.Cleanups = append([]CleanupReport(nil), r.report.Cleanups...)
	result.Backgrounds = append([]BackgroundReport(nil), r.report.Backgrounds...)
	result.Stages = make([]StageReport, len(r.report.Stages))
	for i, stage := range r.report.Stages {
		result.Stages[i] = stage
		result.Stages[i].Metadata = cloneResultMetadata(stage.Metadata)
		result.Stages[i].Steps = make([]StepReport, len(stage.Steps))
		for j, step := range stage.Steps {
			result.Stages[i].Steps[j] = step
			result.Stages[i].Steps[j].Metadata = cloneResultMetadata(step.Metadata)
			result.Stages[i].Steps[j].Attempts = append([]AttemptReport(nil), step.Attempts...)
			result.Stages[i].Steps[j].Polls = make([]PollReport, len(step.Polls))
			for k, poll := range step.Polls {
				result.Stages[i].Steps[j].Polls[k] = poll
				result.Stages[i].Steps[j].Polls[k].Attempts = append([]AttemptReport(nil), poll.Attempts...)
			}
		}
		result.Stages[i].Parallels = make([]ParallelReport, len(stage.Parallels))
		for j, parallel := range stage.Parallels {
			result.Stages[i].Parallels[j] = parallel
			result.Stages[i].Parallels[j].Branches = make([]BranchReport, len(parallel.Branches))
			for k, branch := range parallel.Branches {
				result.Stages[i].Parallels[j].Branches[k] = branch
				result.Stages[i].Parallels[j].Branches[k].Steps = make([]StepReport, len(branch.Steps))
				for l, step := range branch.Steps {
					result.Stages[i].Parallels[j].Branches[k].Steps[l] = cloneStepReport(step)
				}
			}
		}
		result.Stages[i].Subflows = make([]SubflowReport, len(stage.Subflows))
		for j, subflow := range stage.Subflows {
			result.Stages[i].Subflows[j] = cloneSubflowReport(subflow)
		}
	}
	nested := make(map[subflowReportKey]*runRecorder, len(r.nestedRecorders))
	for key, recorder := range r.nestedRecorders {
		nested[key] = recorder
	}
	r.mu.Unlock()
	for key, recorder := range nested {
		child := recorder.snapshot()
		result.Stages[key.stage].Subflows[key.subflow].Stages = child.Stages
	}
	updateLiveDurations(&result, time.Now())
	return result
}

func cloneSubflowReport(subflow SubflowReport) SubflowReport {
	result := subflow
	result.Stages = make([]StageReport, len(subflow.Stages))
	for i, stage := range subflow.Stages {
		result.Stages[i] = stage
		result.Stages[i].Metadata = cloneResultMetadata(stage.Metadata)
		result.Stages[i].Steps = make([]StepReport, len(stage.Steps))
		for j, step := range stage.Steps {
			result.Stages[i].Steps[j] = cloneStepReport(step)
		}
		result.Stages[i].Parallels = make([]ParallelReport, len(stage.Parallels))
		for j, parallel := range stage.Parallels {
			result.Stages[i].Parallels[j] = parallel
			result.Stages[i].Parallels[j].Branches = make([]BranchReport, len(parallel.Branches))
			for k, branch := range parallel.Branches {
				result.Stages[i].Parallels[j].Branches[k] = branch
				result.Stages[i].Parallels[j].Branches[k].Steps = make([]StepReport, len(branch.Steps))
				for l, step := range branch.Steps {
					result.Stages[i].Parallels[j].Branches[k].Steps[l] = cloneStepReport(step)
				}
			}
		}
		result.Stages[i].Subflows = make([]SubflowReport, len(stage.Subflows))
		for j, nested := range stage.Subflows {
			result.Stages[i].Subflows[j] = cloneSubflowReport(nested)
		}
	}
	return result
}

func cloneStepReport(step StepReport) StepReport {
	result := step
	result.Metadata = cloneResultMetadata(step.Metadata)
	result.Attempts = append([]AttemptReport(nil), step.Attempts...)
	result.Polls = make([]PollReport, len(step.Polls))
	for i, poll := range step.Polls {
		result.Polls[i] = poll
		result.Polls[i].Attempts = append([]AttemptReport(nil), poll.Attempts...)
	}
	return result
}

func cloneResultMetadata(metadata ResultMetadata) ResultMetadata {
	if metadata == nil {
		return nil
	}
	result := make(ResultMetadata, len(metadata))
	for key, value := range metadata {
		result[key] = value
	}
	return result
}

func updateLiveDurations(report *RunReport, now time.Time) {
	if report.Status == StatusRunning {
		report.Duration = now.Sub(report.StartedAt)
	}
	for i := range report.Backgrounds {
		if report.Backgrounds[i].Status == StatusRunning {
			report.Backgrounds[i].Duration = now.Sub(report.Backgrounds[i].StartedAt)
		}
	}
	for i := range report.Stages {
		stage := &report.Stages[i]
		if stage.Status == StatusRunning {
			stage.Duration = now.Sub(stage.StartedAt)
		}
		for j := range stage.Steps {
			updateLiveStep(&stage.Steps[j], now)
		}
		for j := range stage.Parallels {
			parallel := &stage.Parallels[j]
			if parallel.Status == StatusRunning {
				parallel.Duration = now.Sub(parallel.StartedAt)
			}
			for k := range parallel.Branches {
				branch := &parallel.Branches[k]
				if branch.Status == StatusRunning {
					branch.Duration = now.Sub(branch.StartedAt)
				}
				for l := range branch.Steps {
					updateLiveStep(&branch.Steps[l], now)
				}
			}
		}
		for j := range stage.Subflows {
			subflow := &stage.Subflows[j]
			if subflow.Status == StatusRunning {
				subflow.Duration = now.Sub(subflow.StartedAt)
			}
			updateLiveStageDurations(subflow.Stages, now)
		}
	}
}

func updateLiveStageDurations(stages []StageReport, now time.Time) {
	for i := range stages {
		stage := &stages[i]
		if stage.Status == StatusRunning {
			stage.Duration = now.Sub(stage.StartedAt)
		}
		for j := range stage.Steps {
			updateLiveStep(&stage.Steps[j], now)
		}
		for j := range stage.Parallels {
			parallel := &stage.Parallels[j]
			if parallel.Status == StatusRunning {
				parallel.Duration = now.Sub(parallel.StartedAt)
			}
			for k := range parallel.Branches {
				branch := &parallel.Branches[k]
				if branch.Status == StatusRunning {
					branch.Duration = now.Sub(branch.StartedAt)
				}
				for l := range branch.Steps {
					updateLiveStep(&branch.Steps[l], now)
				}
			}
		}
		for j := range stage.Subflows {
			subflow := &stage.Subflows[j]
			if subflow.Status == StatusRunning {
				subflow.Duration = now.Sub(subflow.StartedAt)
			}
			updateLiveStageDurations(subflow.Stages, now)
		}
	}
}

func updateLiveStep(step *StepReport, now time.Time) {
	if step.Status == StatusRunning {
		step.Duration = now.Sub(step.StartedAt)
	}
	for i := range step.Attempts {
		if step.Attempts[i].Status == StatusRunning {
			step.Attempts[i].Duration = now.Sub(step.Attempts[i].StartedAt)
		}
	}
	for i := range step.Polls {
		poll := &step.Polls[i]
		if poll.Status == StatusRunning {
			poll.Duration = now.Sub(poll.StartedAt)
		}
		for j := range poll.Attempts {
			if poll.Attempts[j].Status == StatusRunning {
				poll.Attempts[j].Duration = now.Sub(poll.Attempts[j].StartedAt)
			}
		}
	}
}

func statusForError(err error) Status {
	if err == nil {
		return StatusCompleted
	}
	if hasBusinessError(err) {
		return StatusFailed
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return StatusTimeout
	}
	if errors.Is(err, context.Canceled) {
		return StatusCancelled
	}
	return StatusFailed
}

func hasBusinessError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if hasBusinessError(child) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return hasBusinessError(wrapped.Unwrap())
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func markPendingSkipped(stages []StageReport) {
	for i := range stages {
		if stages[i].Status == StatusPending {
			stages[i].Status = StatusSkipped
			for j := range stages[i].Steps {
				stages[i].Steps[j].Status = StatusSkipped
			}
			for j := range stages[i].Parallels {
				stages[i].Parallels[j].Status = StatusSkipped
				for k := range stages[i].Parallels[j].Branches {
					stages[i].Parallels[j].Branches[k].Status = StatusSkipped
					for l := range stages[i].Parallels[j].Branches[k].Steps {
						stages[i].Parallels[j].Branches[k].Steps[l].Status = StatusSkipped
					}
				}
			}
			for j := range stages[i].Subflows {
				stages[i].Subflows[j].Status = StatusSkipped
				markPendingSkipped(stages[i].Subflows[j].Stages)
			}
		}
	}
}
