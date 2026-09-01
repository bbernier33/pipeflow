package pipeflow

import (
	"sync"
	"time"
)

// Execution is a read-only, run-scoped handle for an asynchronously running Pipeline.
type Execution struct {
	recorder *runRecorder
	done     chan struct{}

	mu     sync.Mutex
	output any
	report RunReport
	err    error
}

// CurrentExecution describes only the execution units active at snapshot time.
type CurrentExecution struct {
	Pipeline    string
	Status      Status
	Duration    time.Duration
	Stages      []CurrentStage
	Backgrounds []CurrentBackground
}

type CurrentBackground struct {
	Name          string
	FailurePolicy BackgroundFailurePolicy
	Status        Status
	Duration      time.Duration
}

type CurrentStage struct {
	Name      string
	Status    Status
	Duration  time.Duration
	Steps     []CurrentStep
	Parallels []CurrentParallel
	Subflows  []CurrentSubflow
}

type CurrentSubflow struct {
	Name     string
	Status   Status
	Duration time.Duration
	Stages   []CurrentStage
}

type CurrentParallel struct {
	Name     string
	Status   Status
	Duration time.Duration
	Branches []CurrentBranch
}

type CurrentBranch struct {
	Name     string
	Status   Status
	Duration time.Duration
	Steps    []CurrentStep
}

type CurrentStep struct {
	Name            string
	Role            StepRole
	Status          Status
	Duration        time.Duration
	Attempt         int
	AttemptDuration time.Duration
	Poll            int
}

// ExecutionState is a read-only, payload-free snapshot of the complete run
// topology, including pending, active, and completed execution units.
type ExecutionState struct {
	RunID       string
	Pipeline    string
	Status      Status
	Duration    time.Duration
	Stages      []StageState
	Backgrounds []BackgroundState
}

type BackgroundState struct {
	Name          string
	FailurePolicy BackgroundFailurePolicy
	Status        Status
	Duration      time.Duration
}

type StageState struct {
	Name      string
	Status    Status
	Duration  time.Duration
	Steps     []StepState
	Parallels []ParallelState
	Subflows  []SubflowState
}

type SubflowState struct {
	Name     string
	Status   Status
	Duration time.Duration
	Stages   []StageState
}

type ParallelState struct {
	Name     string
	Status   Status
	Duration time.Duration
	Branches []BranchState
}

type BranchState struct {
	Name     string
	Status   Status
	Duration time.Duration
	Steps    []StepState
}

type StepState struct {
	Name            string
	Role            StepRole
	Status          Status
	Duration        time.Duration
	Attempt         int
	AttemptDuration time.Duration
	Poll            int
}

// Status returns the run's current status.
func (e *Execution) Status() Status {
	return e.recorder.snapshot().Status
}

// Report returns an immutable snapshot of the run's current report.
func (e *Execution) Report() RunReport {
	return e.recorder.snapshot()
}

// State returns a lightweight snapshot of every execution unit. Unlike
// Current, it includes pending and completed siblings as well as active work.
func (e *Execution) State() ExecutionState {
	report := e.recorder.snapshot()
	state := ExecutionState{RunID: report.RunID, Pipeline: report.Pipeline, Status: report.Status, Duration: report.Duration}
	state.Backgrounds = make([]BackgroundState, len(report.Backgrounds))
	for i, background := range report.Backgrounds {
		state.Backgrounds[i] = BackgroundState{
			Name: background.Name, FailurePolicy: background.FailurePolicy,
			Status: background.Status, Duration: background.Duration,
		}
	}
	state.Stages = stageStates(report.Stages)
	return state
}

func stageStates(reports []StageReport) []StageState {
	states := make([]StageState, len(reports))
	for i, report := range reports {
		state := StageState{Name: report.Name, Status: report.Status, Duration: report.Duration}
		state.Steps = make([]StepState, len(report.Steps))
		for j, step := range report.Steps {
			state.Steps[j] = stepState(step)
		}
		state.Parallels = make([]ParallelState, len(report.Parallels))
		for j, parallel := range report.Parallels {
			parallelState := ParallelState{Name: parallel.Name, Status: parallel.Status, Duration: parallel.Duration}
			parallelState.Branches = make([]BranchState, len(parallel.Branches))
			for k, branch := range parallel.Branches {
				branchState := BranchState{Name: branch.Name, Status: branch.Status, Duration: branch.Duration}
				branchState.Steps = make([]StepState, len(branch.Steps))
				for l, step := range branch.Steps {
					branchState.Steps[l] = stepState(step)
				}
				parallelState.Branches[k] = branchState
			}
			state.Parallels[j] = parallelState
		}
		state.Subflows = make([]SubflowState, len(report.Subflows))
		for j, subflow := range report.Subflows {
			state.Subflows[j] = SubflowState{
				Name: subflow.Name, Status: subflow.Status, Duration: subflow.Duration,
				Stages: stageStates(subflow.Stages),
			}
		}
		states[i] = state
	}
	return states
}

func stepState(report StepReport) StepState {
	state := StepState{Name: report.Name, Role: report.Role, Status: report.Status, Duration: report.Duration}
	if count := len(report.Polls); count > 0 && report.Polls[count-1].Status == StatusRunning {
		state.Poll = report.Polls[count-1].Poll
	}
	if count := len(report.Attempts); count > 0 && report.Attempts[count-1].Status == StatusRunning {
		state.Attempt = report.Attempts[count-1].Attempt
		state.AttemptDuration = report.Attempts[count-1].Duration
	}
	return state
}

// Current returns the currently active stages and steps.
func (e *Execution) Current() CurrentExecution {
	report := e.recorder.snapshot()
	current := CurrentExecution{Pipeline: report.Pipeline, Status: report.Status, Duration: report.Duration}
	for _, background := range report.Backgrounds {
		if background.Status == StatusRunning {
			current.Backgrounds = append(current.Backgrounds, CurrentBackground{
				Name: background.Name, FailurePolicy: background.FailurePolicy,
				Status: background.Status, Duration: background.Duration,
			})
		}
	}
	for _, stage := range report.Stages {
		if stage.Status != StatusRunning {
			continue
		}
		currentStage := CurrentStage{Name: stage.Name, Status: stage.Status, Duration: stage.Duration}
		for _, step := range stage.Steps {
			if step.Status != StatusRunning {
				continue
			}
			currentStep := CurrentStep{Name: step.Name, Role: step.Role, Status: step.Status, Duration: step.Duration}
			if count := len(step.Polls); count > 0 {
				currentStep.Poll = step.Polls[count-1].Poll
			}
			if count := len(step.Attempts); count > 0 && step.Attempts[count-1].Status == StatusRunning {
				currentStep.Attempt = step.Attempts[count-1].Attempt
				currentStep.AttemptDuration = step.Attempts[count-1].Duration
			}
			currentStage.Steps = append(currentStage.Steps, currentStep)
		}
		for _, parallel := range stage.Parallels {
			if parallel.Status != StatusRunning {
				continue
			}
			currentParallel := CurrentParallel{Name: parallel.Name, Status: parallel.Status, Duration: parallel.Duration}
			for _, branch := range parallel.Branches {
				if branch.Status != StatusRunning {
					continue
				}
				currentBranch := CurrentBranch{Name: branch.Name, Status: branch.Status, Duration: branch.Duration}
				for _, step := range branch.Steps {
					if step.Status == StatusRunning {
						currentBranch.Steps = append(currentBranch.Steps, currentStepFromReport(step))
					}
				}
				currentParallel.Branches = append(currentParallel.Branches, currentBranch)
			}
			currentStage.Parallels = append(currentStage.Parallels, currentParallel)
		}
		for _, subflow := range stage.Subflows {
			if subflow.Status == StatusRunning {
				currentStage.Subflows = append(currentStage.Subflows, currentSubflowFromReport(subflow))
			}
		}
		current.Stages = append(current.Stages, currentStage)
	}
	return current
}

func currentSubflowFromReport(report SubflowReport) CurrentSubflow {
	current := CurrentSubflow{Name: report.Name, Status: report.Status, Duration: report.Duration}
	for _, stage := range report.Stages {
		if stage.Status != StatusRunning {
			continue
		}
		active := CurrentStage{Name: stage.Name, Status: stage.Status, Duration: stage.Duration}
		for _, step := range stage.Steps {
			if step.Status == StatusRunning {
				active.Steps = append(active.Steps, currentStepFromReport(step))
			}
		}
		for _, parallel := range stage.Parallels {
			if parallel.Status != StatusRunning {
				continue
			}
			activeParallel := CurrentParallel{Name: parallel.Name, Status: parallel.Status, Duration: parallel.Duration}
			for _, branch := range parallel.Branches {
				if branch.Status != StatusRunning {
					continue
				}
				activeBranch := CurrentBranch{Name: branch.Name, Status: branch.Status, Duration: branch.Duration}
				for _, step := range branch.Steps {
					if step.Status == StatusRunning {
						activeBranch.Steps = append(activeBranch.Steps, currentStepFromReport(step))
					}
				}
				activeParallel.Branches = append(activeParallel.Branches, activeBranch)
			}
			active.Parallels = append(active.Parallels, activeParallel)
		}
		for _, subflow := range stage.Subflows {
			if subflow.Status == StatusRunning {
				active.Subflows = append(active.Subflows, currentSubflowFromReport(subflow))
			}
		}
		current.Stages = append(current.Stages, active)
	}
	return current
}

func currentStepFromReport(step StepReport) CurrentStep {
	current := CurrentStep{Name: step.Name, Role: step.Role, Status: step.Status, Duration: step.Duration}
	if count := len(step.Polls); count > 0 {
		current.Poll = step.Polls[count-1].Poll
	}
	if count := len(step.Attempts); count > 0 && step.Attempts[count-1].Status == StatusRunning {
		current.Attempt = step.Attempts[count-1].Attempt
		current.AttemptDuration = step.Attempts[count-1].Duration
	}
	return current
}

// Done closes when pipeline execution finishes.
func (e *Execution) Done() <-chan struct{} {
	return e.done
}

// Wait waits for completion and returns the same outcome as RunWithReport.
func (e *Execution) Wait() (any, RunReport, error) {
	<-e.done
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.output, e.report, e.err
}

func (e *Execution) finish(output any, report RunReport, err error) {
	e.mu.Lock()
	e.output = output
	e.report = report
	e.err = err
	e.mu.Unlock()
	close(e.done)
}
