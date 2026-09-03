package readiness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

// MutationKind identifies one type-aware payload change.
type MutationKind string

const (
	MutationZero       MutationKind = "zero"
	MutationNil        MutationKind = "nil"
	MutationEmpty      MutationKind = "empty"
	MutationTruncated  MutationKind = "truncated"
	MutationUppercase  MutationKind = "uppercase"
	MutationLowercase  MutationKind = "lowercase"
	MutationMixedCase  MutationKind = "mixed_case"
	MutationTypeChange MutationKind = "type_change"
	MutationMissingKey MutationKind = "missing_key"
	MutationExtraKey   MutationKind = "extra_key"
	MutationReordered  MutationKind = "reordered"
	MutationDuplicate  MutationKind = "duplicate"
)

// Mutation describes and carries one generated input. Value may contain
// business data and should be treated as transient test input.
type Mutation struct {
	Name         string
	Path         string
	Kind         MutationKind
	OriginalType string
	MutatedType  string
	Value        any
}

// MutationSummary is the payload-free form retained in test results.
type MutationSummary struct {
	Name         string
	Path         string
	Kind         MutationKind
	OriginalType string
	MutatedType  string
}

// MutationOption configures bounded mutation generation.
type MutationOption func(*mutationConfig)

type mutationConfig struct {
	maxDepth int
	maxCases int
	err      error
}

// WithMutationMaxDepth limits recursive field/element inspection. The default
// is 4. Depth must be positive.
func WithMutationMaxDepth(depth int) MutationOption {
	return func(config *mutationConfig) {
		if depth < 1 {
			config.err = errors.New("readiness: mutation max depth must be positive")
			return
		}
		config.maxDepth = depth
	}
}

// WithMutationMaxCases limits the number of generated cases. The default is
// 100. Cases are selected in deterministic traversal order.
func WithMutationMaxCases(count int) MutationOption {
	return func(config *mutationConfig) {
		if count < 1 {
			config.err = errors.New("readiness: mutation max cases must be positive")
			return
		}
		config.maxCases = count
	}
}

// GenerateMutations produces deterministic, single-change cases appropriate
// to the sample's Go shape. It never modifies sample.
func GenerateMutations(sample any, options ...MutationOption) ([]Mutation, error) {
	config := mutationConfig{maxDepth: 4, maxCases: 100}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.err != nil {
		return nil, config.err
	}
	root := reflect.ValueOf(sample)
	candidates := mutateValue(root, "$", 0, config.maxDepth)
	if len(candidates) > config.maxCases {
		candidates = candidates[:config.maxCases]
	}
	mutations := make([]Mutation, 0, len(candidates))
	for _, candidate := range candidates {
		mutations = append(mutations, Mutation{
			Name:         candidate.name,
			Path:         candidate.path,
			Kind:         candidate.kind,
			OriginalType: candidate.originalType,
			MutatedType:  candidate.mutatedType,
			Value:        candidate.value.Interface(),
		})
	}
	return mutations, nil
}

type mutationCandidate struct {
	name         string
	path         string
	kind         MutationKind
	originalType string
	mutatedType  string
	value        reflect.Value
}

func mutateValue(value reflect.Value, path string, depth, maxDepth int) []mutationCandidate {
	if !value.IsValid() {
		return nil
	}
	candidates := directMutations(value, path)
	if depth >= maxDepth {
		return candidates
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			for _, child := range mutateValue(value.Elem(), path, depth+1, maxDepth) {
				wrapped := reflect.New(value.Type()).Elem()
				wrapped.Set(child.value)
				child.value = wrapped
				candidates = append(candidates, child)
			}
		}
	case reflect.Pointer:
		if !value.IsNil() {
			for _, child := range mutateValue(value.Elem(), path, depth+1, maxDepth) {
				wrapped := reflect.New(value.Type().Elem())
				wrapped.Elem().Set(child.value)
				child.value = wrapped
				candidates = append(candidates, child)
			}
		}
	case reflect.Struct:
		for field := 0; field < value.NumField(); field++ {
			definition := value.Type().Field(field)
			if !definition.IsExported() || !value.Field(field).CanInterface() {
				continue
			}
			fieldPath := path + "." + definition.Name
			for _, child := range mutateValue(value.Field(field), fieldPath, depth+1, maxDepth) {
				copyValue := reflect.New(value.Type()).Elem()
				copyValue.Set(value)
				copyValue.Field(field).Set(child.value)
				child.value = copyValue
				candidates = append(candidates, child)
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			itemPath := fmt.Sprintf("%s[%d]", path, index)
			for _, child := range mutateValue(value.Index(index), itemPath, depth+1, maxDepth) {
				copyValue := cloneSequence(value)
				copyValue.Index(index).Set(child.value)
				child.value = copyValue
				candidates = append(candidates, child)
			}
		}
	case reflect.Map:
		if value.Type().Key().Kind() == reflect.String {
			keys := value.MapKeys()
			sort.Slice(keys, func(a, b int) bool { return keys[a].String() < keys[b].String() })
			for _, key := range keys {
				entry := value.MapIndex(key)
				entryPath := fmt.Sprintf("%s[%q]", path, key.String())
				for _, child := range mutateValue(entry, entryPath, depth+1, maxDepth) {
					copyValue := cloneMap(value)
					copyValue.SetMapIndex(key, child.value)
					child.value = copyValue
					candidates = append(candidates, child)
				}
			}
		}
	}
	return candidates
}

func directMutations(value reflect.Value, path string) []mutationCandidate {
	if value.Kind() == reflect.Interface && !value.IsNil() {
		return interfaceMutations(value, path)
	}
	makeCandidate := func(kind MutationKind, changed reflect.Value) mutationCandidate {
		return mutationCandidate{name: fmt.Sprintf("%s at %s", strings.ReplaceAll(string(kind), "_", " "), path), path: path, kind: kind, originalType: typeOfValue(value), mutatedType: typeOfValue(changed), value: changed}
	}
	var candidates []mutationCandidate
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if !value.IsNil() {
			candidates = append(candidates, makeCandidate(MutationNil, reflect.Zero(value.Type())))
		}
	}
	switch value.Kind() {
	case reflect.String:
		text := value.String()
		for _, change := range stringChanges(text) {
			changed := reflect.New(value.Type()).Elem()
			changed.SetString(change.value)
			candidates = append(candidates, makeCandidate(change.kind, changed))
		}
	case reflect.Bool:
		if value.Bool() {
			candidates = append(candidates, makeCandidate(MutationZero, reflect.Zero(value.Type())))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Int() != 0 {
			candidates = append(candidates, makeCandidate(MutationZero, reflect.Zero(value.Type())))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if value.Uint() != 0 {
			candidates = append(candidates, makeCandidate(MutationZero, reflect.Zero(value.Type())))
		}
	case reflect.Float32, reflect.Float64:
		if value.Float() != 0 {
			candidates = append(candidates, makeCandidate(MutationZero, reflect.Zero(value.Type())))
		}
	case reflect.Struct:
		zero := reflect.Zero(value.Type())
		if !value.IsZero() {
			candidates = append(candidates, makeCandidate(MutationZero, zero))
		}
	case reflect.Slice:
		if value.Len() > 0 {
			candidates = append(candidates, makeCandidate(MutationEmpty, reflect.MakeSlice(value.Type(), 0, 0)))
			reversed := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
			for index := 0; index < value.Len(); index++ {
				reversed.Index(index).Set(value.Index(value.Len() - 1 - index))
			}
			if value.Len() > 1 {
				candidates = append(candidates, makeCandidate(MutationReordered, reversed))
			}
			duplicated := reflect.MakeSlice(value.Type(), value.Len()+1, value.Len()+1)
			reflect.Copy(duplicated, value)
			duplicated.Index(value.Len()).Set(value.Index(0))
			candidates = append(candidates, makeCandidate(MutationDuplicate, duplicated))
		}
	case reflect.Map:
		if value.Type().Key().Kind() == reflect.String && value.Len() > 0 {
			keys := value.MapKeys()
			sort.Slice(keys, func(a, b int) bool { return keys[a].String() < keys[b].String() })
			missing := cloneMap(value)
			missing.SetMapIndex(keys[0], reflect.Value{})
			candidates = append(candidates, mutationCandidate{name: fmt.Sprintf("missing key %q at %s", keys[0].String(), path), path: fmt.Sprintf("%s[%q]", path, keys[0].String()), kind: MutationMissingKey, originalType: typeOfValue(value), mutatedType: typeOfValue(value), value: missing})
			extraKey := "__pipeflow_unexpected"
			if !value.MapIndex(reflect.ValueOf(extraKey).Convert(value.Type().Key())).IsValid() {
				extra := cloneMap(value)
				extra.SetMapIndex(reflect.ValueOf(extraKey).Convert(value.Type().Key()), reflect.Zero(value.Type().Elem()))
				candidates = append(candidates, makeCandidate(MutationExtraKey, extra))
			}
		}
	}
	return candidates
}

type stringChange struct {
	kind  MutationKind
	value string
}

func stringChanges(value string) []stringChange {
	seen := map[string]struct{}{value: {}}
	add := func(changes *[]stringChange, kind MutationKind, changed string) {
		if _, exists := seen[changed]; exists {
			return
		}
		seen[changed] = struct{}{}
		*changes = append(*changes, stringChange{kind: kind, value: changed})
	}
	var changes []stringChange
	add(&changes, MutationEmpty, "")
	runes := []rune(value)
	if len(runes) > 1 {
		add(&changes, MutationTruncated, string(runes[:len(runes)/2]))
	}
	add(&changes, MutationUppercase, strings.ToUpper(value))
	add(&changes, MutationLowercase, strings.ToLower(value))
	runes = []rune(value)
	for index, current := range runes {
		if index%2 == 0 {
			runes[index] = unicode.ToUpper(current)
		} else {
			runes[index] = unicode.ToLower(current)
		}
	}
	add(&changes, MutationMixedCase, string(runes))
	return changes
}

func interfaceMutations(value reflect.Value, path string) []mutationCandidate {
	actual := value.Elem()
	var replacements []any
	switch actual.Kind() {
	case reflect.String:
		replacements = []any{0, false}
	case reflect.Bool:
		replacements = []any{fmt.Sprint(actual.Bool())}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		replacements = []any{fmt.Sprint(actual.Interface())}
	}
	mutations := make([]mutationCandidate, 0, len(replacements))
	for _, replacement := range replacements {
		changed := reflect.New(value.Type()).Elem()
		changed.Set(reflect.ValueOf(replacement))
		mutations = append(mutations, mutationCandidate{name: fmt.Sprintf("type change at %s", path), path: path, kind: MutationTypeChange, originalType: typeOfValue(value), mutatedType: typeOfValue(changed), value: changed})
	}
	return mutations
}

func cloneSequence(value reflect.Value) reflect.Value {
	if value.Kind() == reflect.Array {
		copyValue := reflect.New(value.Type()).Elem()
		copyValue.Set(value)
		return copyValue
	}
	copyValue := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
	reflect.Copy(copyValue, value)
	return copyValue
}

func cloneMap(value reflect.Value) reflect.Value {
	copyValue := reflect.MakeMapWithSize(value.Type(), value.Len()+1)
	iterator := value.MapRange()
	for iterator.Next() {
		copyValue.SetMapIndex(iterator.Key(), iterator.Value())
	}
	return copyValue
}

func typeOfValue(value reflect.Value) string {
	if !value.IsValid() {
		return "nil"
	}
	if value.Kind() == reflect.Interface && !value.IsNil() {
		return value.Elem().Type().String()
	}
	return value.Type().String()
}

// MutationPlan runs one copy of a Scenario for every generated mutation.
type MutationPlan struct {
	scenario Scenario
	sample   any
	options  []MutationOption
}

// NewMutationPlan creates a bounded type-aware mutation execution plan.
func NewMutationPlan(scenario Scenario, sample any, options ...MutationOption) MutationPlan {
	return MutationPlan{scenario: scenario, sample: sample, options: append([]MutationOption(nil), options...)}
}

// MutationCaseResult retains a mutation descriptor and its payload-free
// Scenario result.
type MutationCaseResult struct {
	Mutation MutationSummary
	Scenario Result
}

// MutationPlanResult is the aggregate outcome of all generated mutation cases.
type MutationPlanResult struct {
	Verdict Verdict
	Cases   []MutationCaseResult
	Error   error
}

// Run generates mutations and executes the Scenario once per case in order.
func (p MutationPlan) Run(ctx context.Context) MutationPlanResult {
	mutations, err := GenerateMutations(p.sample, p.options...)
	if err != nil {
		return MutationPlanResult{Verdict: VerdictFail, Error: err}
	}
	if len(mutations) == 0 {
		return MutationPlanResult{Verdict: VerdictReview, Error: errors.New("readiness: sample produced no applicable mutations")}
	}
	result := MutationPlanResult{Verdict: VerdictPass, Cases: make([]MutationCaseResult, 0, len(mutations))}
	for _, mutation := range mutations {
		if ctx == nil {
			result.Verdict = VerdictFail
			result.Error = errors.New("readiness: context cannot be nil")
			return result
		}
		if err := ctx.Err(); err != nil {
			result.Verdict = VerdictFail
			result.Error = err
			return result
		}
		scenarioResult := p.scenario.WithInput(mutation.Value).Run(ctx)
		result.Cases = append(result.Cases, MutationCaseResult{
			Mutation: MutationSummary{Name: mutation.Name, Path: mutation.Path, Kind: mutation.Kind, OriginalType: mutation.OriginalType, MutatedType: mutation.MutatedType},
			Scenario: scenarioResult,
		})
		result.Verdict = combine(result.Verdict, scenarioResult.Verdict)
	}
	return result
}
