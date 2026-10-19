package readiness

import (
	"fmt"
	"strings"
)

// ReadinessStatus is a verdict over the declared evidence and policy, not proof
// of production safety or authorization to enable production boundaries.
type ReadinessStatus string

const (
	ReadinessReady             ReadinessStatus = "READY"
	ReadinessReadyWithWarnings ReadinessStatus = "READY WITH WARNINGS"
	ReadinessNotReady          ReadinessStatus = "NOT READY"
	ReadinessReviewRequired    ReadinessStatus = "REVIEW REQUIRED"
)

// ReadinessCategory identifies a required evidence family. Scenario-based
// failure/recovery/circuit/idempotency categories are explicitly caller-labelled.
type ReadinessCategory string

const (
	CategoryContracts        ReadinessCategory = "contracts"
	CategoryScenario         ReadinessCategory = "scenario"
	CategoryEndToEnd         ReadinessCategory = "end_to_end"
	CategoryPayloadMutation  ReadinessCategory = "payload_mutation"
	CategorySemanticMutation ReadinessCategory = "semantic_mutation"
	CategoryLoad             ReadinessCategory = "load"
	CategoryPressure         ReadinessCategory = "pressure"
	CategoryFailureInjection ReadinessCategory = "failure_injection"
	CategoryRecovery         ReadinessCategory = "recovery"
	CategoryCircuit          ReadinessCategory = "circuit"
	CategoryIdempotency      ReadinessCategory = "idempotency"
	CategoryChaos            ReadinessCategory = "chaos"
	CategoryIntegration      ReadinessCategory = "integration"
	CategoryCoverage         ReadinessCategory = "coverage"
)

// ReadinessPolicy controls the aggregate assessment. Empty Required defaults to
// contracts, end-to-end, and coverage. A nonempty list replaces that baseline.
// Critical gaps and failures always block.
type ReadinessPolicy struct {
	Required             []ReadinessCategory
	WarningsAsFailures   bool
	AllowNonCriticalGaps bool
}

// ReadinessEvidence is an aggregation input. Result must be a supported result
// VALUE (Result, ContractResult, EndToEndResult, MutationPlanResult,
// SemanticPlanResult, LoadResult, PressureResult, ChaosResult, IntegrationResult,
// or CoverageResult). Category may be omitted for type-based inference. Result
// is consumed synchronously and is not retained in the aggregate report.
type ReadinessEvidence struct {
	Name     string
	Category ReadinessCategory
	Result   any
	Warnings []string
}

// ReadinessItem is a detached metadata summary, retaining checks, run identity,
// review items, gaps, and tool errors without retaining business outputs.
type ReadinessItem struct {
	Name        string
	Category    ReadinessCategory
	Verdict     Verdict
	Checks      []CheckResult
	RunIDs      []string
	ReviewItems []string
	Warnings    []string
	Gaps        []CoverageGap
	Error       error
	Summary     []string
}

// ReadinessReport is a deterministic assessment of already-executed evidence.
// Passed/Failed/Reviews count evidence items. Warnings counts warning messages.
type ReadinessReport struct {
	Status        ReadinessStatus
	Required      []ReadinessCategory
	Items         []ReadinessItem
	Missing       []ReadinessCategory
	Passed        int
	Failed        int
	Reviews       int
	Warnings      int
	UntestedPaths int
	CriticalGaps  int
	Error         error
}

// BuildReadinessReport aggregates evidence without executing pipelines or
// authorizing boundaries. PASS means the supplied requirements were satisfied.
// Use evidence from the same application revision and declare its required
// categories explicitly; category labels alone do not prove a test's adequacy.
func BuildReadinessReport(policy ReadinessPolicy, evidence ...ReadinessEvidence) ReadinessReport {
	report := ReadinessReport{Status: ReadinessNotReady, Required: append([]ReadinessCategory(nil), policy.Required...)}
	if len(report.Required) == 0 {
		report.Required = []ReadinessCategory{CategoryContracts, CategoryEndToEnd, CategoryCoverage}
	}
	required := make(map[ReadinessCategory]bool)
	for _, category := range report.Required {
		if !validReadinessCategory(category) || required[category] {
			report.Error = fmt.Errorf("readiness: invalid or duplicate required category %q", category)
			return report
		}
		required[category] = true
	}
	seen := make(map[string]bool, len(evidence))
	present := make(map[ReadinessCategory]bool)
	for _, input := range evidence {
		if input.Name == "" || seen[input.Name] {
			report.Error = fmt.Errorf("readiness: empty or duplicate evidence name %q", input.Name)
			return report
		}
		seen[input.Name] = true
		item := ReadinessItem{Name: input.Name, Warnings: append([]string(nil), input.Warnings...)}
		inferred := collectReadinessFacts(&item, input.Result, policy)
		item.Category = inferred
		if input.Category != "" {
			if input.Category != inferred && !(inferred == CategoryScenario && scenarioReadinessCategory(input.Category)) {
				item.Verdict = VerdictFail
				item.Error = fmt.Errorf("readiness: category %q does not match evidence type %T", input.Category, input.Result)
			}
			item.Category = input.Category
		}
		if !validReadinessCategory(item.Category) {
			item.Verdict = VerdictFail
			if item.Error == nil {
				item.Error = fmt.Errorf("readiness: invalid evidence category %q", item.Category)
			}
		}
		if item.Error != nil {
			item.Verdict = VerdictFail
		}
		if !validReadinessVerdict(item.Verdict) {
			item.Error = fmt.Errorf("readiness: evidence %q has invalid verdict %q", input.Name, item.Verdict)
			item.Verdict = VerdictFail
		}
		for _, gap := range item.Gaps {
			report.UntestedPaths++
			if gap.Critical {
				report.CriticalGaps++
				item.Verdict = VerdictFail
			}
		}
		if item.Verdict == VerdictReview && len(item.ReviewItems) == 0 {
			item.ReviewItems = append(item.ReviewItems, "evidence requires review")
		}
		present[item.Category] = true
		report.Items = append(report.Items, item)
		report.Warnings += len(item.Warnings)
		switch item.Verdict {
		case VerdictPass:
			report.Passed++
		case VerdictFail:
			report.Failed++
		case VerdictReview:
			report.Reviews++
		}
	}
	for _, category := range report.Required {
		if !present[category] {
			report.Missing = append(report.Missing, category)
		}
	}
	switch {
	case report.Failed > 0 || len(report.Missing) > 0 || report.CriticalGaps > 0 || policy.WarningsAsFailures && report.Warnings > 0:
		report.Status = ReadinessNotReady
	case report.Reviews > 0:
		report.Status = ReadinessReviewRequired
	case report.Warnings > 0:
		report.Status = ReadinessReadyWithWarnings
	default:
		report.Status = ReadinessReady
	}
	return report
}

func collectReadinessFacts(item *ReadinessItem, value any, policy ReadinessPolicy) ReadinessCategory {
	switch result := value.(type) {
	case Result:
		collectScenarioFacts(item, result)
		return CategoryScenario
	case ContractResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("%d inspected contract boundaries", len(result.Boundaries)))
		return CategoryContracts
	case EndToEndResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		if result.Scenario.Report.RunID != "" {
			collectScenarioFacts(item, result.Scenario)
		}
		item.Verdict = combineFactVerdict(item.Verdict, result.Contracts.Verdict)
		return CategoryEndToEnd
	case IntegrationResult:
		collectReadinessFacts(item, result.EndToEndResult, policy)
		item.Summary = append(item.Summary, fmt.Sprintf("integration level %d", result.Level))
		return CategoryIntegration
	case CoverageResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Gaps = append([]CoverageGap(nil), result.Gaps...)
		item.Warnings = append(item.Warnings, result.Warnings...)
		item.Summary = append(item.Summary, fmt.Sprintf("%d Step paths, %d required behaviors, %d gaps", len(result.Paths), len(result.Requirements), len(result.Gaps)))
		allow := policy.AllowNonCriticalGaps && len(result.Gaps) > 0 && len(result.Warnings) == 0 && result.Error == nil && result.Verdict == VerdictReview
		for _, gap := range result.Gaps {
			if gap.Critical {
				allow = false
			}
		}
		if allow {
			item.Verdict = VerdictPass
		}
		for _, gap := range result.Gaps {
			message := fmt.Sprintf("untested %s: %s", gap.Path, gap.Behavior)
			if allow {
				item.Warnings = append(item.Warnings, message)
			} else {
				item.ReviewItems = append(item.ReviewItems, message)
				if gap.Critical {
					item.Verdict = VerdictFail
				} else {
					item.Verdict = combineFactVerdict(item.Verdict, VerdictReview)
				}
			}
		}
		return CategoryCoverage
	case LoadResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("load: completed=%d/%d, pipeline failures=%d, throughput=%.2f/s, p95=%s", result.Summary.Completed, result.Summary.Requested, result.Summary.PipelineFailures, result.Summary.Throughput, result.Summary.P95Latency))
		collectChecks(item, result.Checks)
		for _, run := range result.Runs {
			item.Verdict = combineFactVerdict(item.Verdict, run.Verdict)
			if run.Report.RunID != "" {
				item.RunIDs = append(item.RunIDs, run.Report.RunID)
			}
			if run.Error != nil {
				item.Error = run.Error
			}
		}
		return CategoryLoad
	case PressureResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("pressure: accepted=%d, completed=%d, rejected=%d, max queue=%d, drained=%t", result.Summary.Accepted, result.Summary.Completed, result.Summary.Rejected, result.Summary.MaxQueueDepth, result.Summary.Drained))
		collectChecks(item, result.Checks)
		for _, trial := range result.Items {
			if trial.SubmitError != nil {
				item.Error = trial.SubmitError
			}
			if trial.Accepted {
				collectScenarioFacts(item, trial.Scenario)
			}
		}
		return CategoryPressure
	case MutationPlanResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("%d payload mutation cases", len(result.Cases)))
		for _, trial := range result.Cases {
			collectScenarioFacts(item, trial.Scenario)
		}
		return CategoryPayloadMutation
	case SemanticPlanResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("%d semantic cases", len(result.Cases)))
		for _, trial := range result.Cases {
			// Expected rejection can legitimately override RunSucceeds in this API.
			item.Verdict = combineFactVerdict(item.Verdict, trial.Verdict)
			if trial.Scenario.Report.RunID != "" {
				item.RunIDs = append(item.RunIDs, trial.Scenario.Report.RunID)
			}
			if trial.Error != nil {
				item.Error = trial.Error
			}
			if trial.Scenario.Error != nil {
				item.Error = trial.Scenario.Error
			}
			for _, check := range trial.Scenario.Checks {
				if check.Error != nil {
					item.Error = check.Error
				}
			}
			if trial.Check != nil {
				collectChecks(item, []CheckResult{*trial.Check})
			}
			if trial.Verdict == VerdictReview {
				item.ReviewItems = append(item.ReviewItems, fmt.Sprintf("%s (%s): expected=%s observed=%s", trial.Name, trial.Path, trial.Expected, trial.Observed))
			}
		}
		return CategorySemanticMutation
	case ChaosResult:
		item.Verdict, item.Error = result.Verdict, result.Error
		item.Summary = append(item.Summary, fmt.Sprintf("chaos: seed=%d, cases=%d/%d", result.Seed, len(result.Cases), result.Requested))
		for _, trial := range result.Cases {
			item.Summary = append(item.Summary, fmt.Sprintf("case %d seed=%d: %s", trial.Case.Index, trial.Case.Seed, trial.Verdict))
			item.Verdict = combineFactVerdict(item.Verdict, trial.Verdict)
			if trial.Error != nil {
				item.Error = trial.Error
			}
			child := ReadinessItem{}
			if trial.Evidence.Scenario != nil {
				collectReadinessFacts(&child, *trial.Evidence.Scenario, policy)
			}
			if trial.Evidence.Load != nil {
				collectReadinessFacts(&child, *trial.Evidence.Load, policy)
			}
			if trial.Evidence.Pressure != nil {
				collectReadinessFacts(&child, *trial.Evidence.Pressure, policy)
			}
			if child.Verdict != "" {
				item.Verdict = combineFactVerdict(item.Verdict, child.Verdict)
			}
			if child.Error != nil {
				item.Error = child.Error
			}
			item.RunIDs = append(item.RunIDs, child.RunIDs...)
			item.Checks = append(item.Checks, child.Checks...)
			item.ReviewItems = append(item.ReviewItems, child.ReviewItems...)
		}
		return CategoryChaos
	default:
		item.Verdict = VerdictFail
		item.Error = fmt.Errorf("readiness: unsupported aggregate evidence type %T", value)
		return ""
	}
}

func collectScenarioFacts(item *ReadinessItem, result Result) {
	if !validReadinessVerdict(result.Verdict) {
		item.Error = fmt.Errorf("readiness: Scenario has invalid verdict %q", result.Verdict)
	}
	item.Verdict = combineFactVerdict(item.Verdict, result.Verdict)
	if result.Arrangement != "" {
		item.Summary = append(item.Summary, "arrange: "+result.Arrangement)
	}
	if result.Error != nil {
		item.Error = result.Error
	}
	if result.Report.RunID != "" {
		item.RunIDs = append(item.RunIDs, result.Report.RunID)
	}
	collectChecks(item, result.Checks)
}

func collectChecks(item *ReadinessItem, checks []CheckResult) {
	item.Checks = append(item.Checks, checks...)
	for _, check := range checks {
		item.Verdict = combineFactVerdict(item.Verdict, check.Verdict)
		if check.Error != nil {
			item.Error = check.Error
		}
		if check.Verdict == VerdictReview {
			item.ReviewItems = append(item.ReviewItems, check.Name+": "+check.Observed)
		}
	}
}

func combineFactVerdict(current, next Verdict) Verdict {
	if !validReadinessVerdict(next) {
		return VerdictFail
	}
	if current == "" {
		return next
	}
	return combine(current, next)
}

func validReadinessVerdict(verdict Verdict) bool {
	return verdict == VerdictPass || verdict == VerdictFail || verdict == VerdictReview
}
func scenarioReadinessCategory(category ReadinessCategory) bool {
	return category == CategoryScenario || category == CategoryFailureInjection || category == CategoryRecovery || category == CategoryCircuit || category == CategoryIdempotency
}
func validReadinessCategory(category ReadinessCategory) bool {
	switch category {
	case CategoryContracts, CategoryScenario, CategoryEndToEnd, CategoryPayloadMutation, CategorySemanticMutation, CategoryLoad, CategoryPressure, CategoryFailureInjection, CategoryRecovery, CategoryCircuit, CategoryIdempotency, CategoryChaos, CategoryIntegration, CategoryCoverage:
		return true
	default:
		return false
	}
}

// String renders a concise human summary with explicit missing evidence and gaps.
func (r ReadinessReport) String() string {
	var out strings.Builder
	fmt.Fprintf(&out, "PIPEFLOW PRODUCTION READINESS\nRESULT: %s\n", r.Status)
	for _, item := range r.Items {
		fmt.Fprintf(&out, "%s [%s]: %s\n", item.Name, item.Category, strings.ToUpper(string(item.Verdict)))
		for _, summary := range item.Summary {
			fmt.Fprintf(&out, "  %s\n", summary)
		}
		for _, check := range item.Checks {
			fmt.Fprintf(&out, "  check %s: %s - %s\n", check.Name, strings.ToUpper(string(check.Verdict)), check.Observed)
		}
		if item.Error != nil {
			fmt.Fprintf(&out, "  error: %v\n", item.Error)
		}
		for _, warning := range item.Warnings {
			fmt.Fprintf(&out, "  warning: %s\n", warning)
		}
		for _, review := range item.ReviewItems {
			fmt.Fprintf(&out, "  review: %s\n", review)
		}
		for _, gap := range item.Gaps {
			fmt.Fprintf(&out, "  untested: %s (%s), critical=%t\n", gap.Path, gap.Behavior, gap.Critical)
		}
	}
	for _, category := range r.Missing {
		fmt.Fprintf(&out, "MISSING: %s\n", category)
	}
	if r.Error != nil {
		fmt.Fprintf(&out, "ERROR: %v\n", r.Error)
	}
	fmt.Fprintf(&out, "Passed: %d  Failed: %d  Review: %d  Warnings: %d  Untested: %d  Critical gaps: %d\n", r.Passed, r.Failed, r.Reviews, r.Warnings, r.UntestedPaths, r.CriticalGaps)
	return out.String()
}
