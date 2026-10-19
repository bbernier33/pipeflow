package readiness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
)

// ChaosCase identifies one independently reproducible experiment.
type ChaosCase struct {
	Index int
	Seed  int64
}

// Random returns a fresh private generator starting at this case's seed.
// Draw conditions before starting concurrent work; rand.Rand is not thread-safe.
func (c ChaosCase) Random() *rand.Rand { return rand.New(rand.NewSource(c.Seed)) }

// ChaosEvidence holds exactly one existing readiness result. It must not hold
// generated inputs, functions, or production credentials. Use the underlying
// Scenario arrangement to describe the generated conditions without payloads.
type ChaosEvidence struct {
	Scenario *Result
	Load     *LoadResult
	Pressure *PressureResult
}

// ChaosExperiment builds fresh state and executes a case using existing
// readiness plans. Return exactly one result in ChaosEvidence. The caller owns
// safe external boundaries, fault selection, and explicit expectations.
type ChaosExperiment func(context.Context, ChaosCase) (ChaosEvidence, error)

// ChaosPlan runs finite seeded experiments sequentially. It stores no random
// generator or execution state, so replay does not depend on previous cases.
type ChaosPlan struct {
	seed       int64
	cases      int
	experiment ChaosExperiment
}

// NewChaosPlan creates a finite plan. Cases must be positive. For independent
// replay the experiment must construct fresh guards, stores, and fault wrappers.
func NewChaosPlan(seed int64, cases int, experiment ChaosExperiment) ChaosPlan {
	return ChaosPlan{seed: seed, cases: cases, experiment: experiment}
}

// ChaosCaseResult retains the seed and normal payload-free readiness evidence.
type ChaosCaseResult struct {
	Case     ChaosCase
	Verdict  Verdict
	Evidence ChaosEvidence
	Error    error
}

// ChaosResult aggregates cases with FAIL > REVIEW > PASS precedence.
type ChaosResult struct {
	Seed      int64
	Requested int
	Verdict   Verdict
	Cases     []ChaosCaseResult
	Error     error
}

// Run executes cases in index order, stopping on parent cancellation. Individual
// experiment failures remain inspectable and do not prevent subsequent cases.
func (p ChaosPlan) Run(ctx context.Context) ChaosResult {
	result := ChaosResult{Seed: p.seed, Requested: p.cases, Verdict: VerdictFail}
	if err := p.validate(ctx); err != nil {
		result.Error = err
		return result
	}
	result.Verdict = VerdictPass
	for index := 0; index < p.cases; index++ {
		if err := ctx.Err(); err != nil {
			result.Error = err
			result.Verdict = VerdictFail
			return result
		}
		trial := p.RunCase(ctx, index)
		result.Cases = append(result.Cases, trial)
		result.Verdict = combine(result.Verdict, trial.Verdict)
	}
	if err := ctx.Err(); err != nil {
		result.Error = err
		result.Verdict = VerdictFail
	}
	return result
}

// RunCase replays the case at index using the plan seed and current experiment
// definition. It never executes preceding cases. Index must be in [0, cases).
func (p ChaosPlan) RunCase(ctx context.Context, index int) (result ChaosCaseResult) {
	result = ChaosCaseResult{Case: ChaosCase{Index: index, Seed: chaosSeed(p.seed, index)}, Verdict: VerdictFail}
	if err := p.validate(ctx); err != nil {
		result.Error = err
		return result
	}
	if index < 0 || index >= p.cases {
		result.Error = fmt.Errorf("readiness: chaos case index %d outside [0, %d)", index, p.cases)
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Error = err
		return result
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Verdict = VerdictFail
			result.Error = fmt.Errorf("readiness: chaos case %d seed %d panicked: %v\n%s", index, result.Case.Seed, recovered, debug.Stack())
		}
	}()
	evidence, err := p.experiment(ctx, result.Case)
	result.Evidence = evidence
	if err != nil {
		result.Error = err
		return result
	}
	result.Verdict, result.Error = evidence.outcome()
	if err := ctx.Err(); err != nil {
		result.Verdict = VerdictFail
		result.Error = err
	}
	return result
}

func (p ChaosPlan) validate(ctx context.Context) error {
	if p.cases < 1 {
		return errors.New("readiness: chaos cases must be positive")
	}
	if p.experiment == nil {
		return errors.New("readiness: chaos experiment cannot be nil")
	}
	if ctx == nil {
		return errors.New("readiness: context cannot be nil")
	}
	return nil
}

func (e ChaosEvidence) outcome() (Verdict, error) {
	count := 0
	var verdict Verdict
	var err error
	if e.Scenario != nil {
		count++
		verdict, err = e.Scenario.Verdict, e.Scenario.Error
	}
	if e.Load != nil {
		count++
		verdict, err = e.Load.Verdict, e.Load.Error
	}
	if e.Pressure != nil {
		count++
		verdict, err = e.Pressure.Verdict, e.Pressure.Error
	}
	if count != 1 {
		return VerdictFail, errors.New("readiness: chaos experiment must return exactly one Scenario, Load, or Pressure result")
	}
	if err != nil {
		return VerdictFail, err
	}
	if verdict != VerdictPass && verdict != VerdictFail && verdict != VerdictReview {
		return VerdictFail, fmt.Errorf("readiness: chaos evidence contains invalid verdict %q", verdict)
	}
	return verdict, nil
}

// SplitMix64 maps seed/index to stable per-case seeds using specified integer
// arithmetic rather than consuming a shared pseudo-random sequence.
func chaosSeed(seed int64, index int) int64 {
	value := uint64(seed) + 0x9e3779b97f4a7c15*(uint64(index)+1)
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}
