package pipeflow

import (
	"bytes"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigSource identifies the winning layer for one effective setting.
type ConfigSource string

const (
	ConfigSourceDefault  ConfigSource = "core_default"
	ConfigSourceGlobal   ConfigSource = "yaml_default"
	ConfigSourcePipeline ConfigSource = "yaml_pipeline"
	ConfigSourceStage    ConfigSource = "yaml_stage"
	ConfigSourceStep     ConfigSource = "yaml_step"
	ConfigSourceGo       ConfigSource = "go"
)

// Duration is a YAML duration represented using Go duration syntax.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("pipeflow: duration must be a string")
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("pipeflow: invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// Config is an optional immutable execution-policy configuration document.
type Config struct {
	Defaults  ConfigDefaults            `yaml:"defaults"`
	Pipelines map[string]PipelineConfig `yaml:"pipelines"`
}

type ConfigDefaults struct {
	Pipeline PipelineSettings `yaml:"pipeline"`
	Stage    StageSettings    `yaml:"stage"`
	Step     StepSettings     `yaml:"step"`
}

type PipelineConfig struct {
	PipelineSettings `yaml:",inline"`
	Stages           map[string]StageConfig        `yaml:"stages"`
	Backgrounds      map[string]BackgroundSettings `yaml:"backgrounds"`
}

type StageConfig struct {
	StageSettings `yaml:",inline"`
	Steps         map[string]StepSettings   `yaml:"steps"`
	Parallels     map[string]ParallelConfig `yaml:"parallels"`
	Subflows      map[string]SubflowConfig  `yaml:"subflows"`
}

type ParallelConfig struct {
	FailurePolicy *string                 `yaml:"failure_policy"`
	Branches      map[string]BranchConfig `yaml:"branches"`
}
type BranchConfig struct {
	Steps map[string]StepSettings `yaml:"steps"`
}
type SubflowConfig struct {
	Stages map[string]StageConfig `yaml:"stages"`
}
type BackgroundSettings struct {
	FailurePolicy *string `yaml:"failure_policy"`
}

type PipelineSettings struct {
	Timeout *Duration `yaml:"timeout"`
}
type StageSettings struct {
	Timeout *Duration `yaml:"timeout"`
}

type StepSettings struct {
	Role      *string            `yaml:"role"`
	Timeout   *Duration          `yaml:"timeout"`
	Retry     *RetrySettings     `yaml:"retry"`
	Polling   *PollingSettings   `yaml:"polling"`
	RateLimit *RateLimitSettings `yaml:"rate_limit"`
}

type PollingSettings struct {
	Every    *Duration `yaml:"every"`
	MaxPolls *int      `yaml:"max_polls"`
	Timeout  *Duration `yaml:"timeout"`
}

type RetrySettings struct {
	MaxAttempts *int      `yaml:"max_attempts"`
	Delay       *Duration `yaml:"delay"`
	Backoff     *string   `yaml:"backoff"`
	MaxDelay    *Duration `yaml:"max_delay"`
	Jitter      *float64  `yaml:"jitter"`
}

type RateLimitSettings struct {
	Key           *string   `yaml:"key"`
	MaxCalls      *int      `yaml:"max_calls"`
	Interval      *Duration `yaml:"interval"`
	MaxConcurrent *int      `yaml:"max_concurrent"`
}

// ParseConfigYAML parses a strict Pipeflow configuration document.
func ParseConfigYAML(data []byte) (Config, error) {
	var config Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("pipeflow: parse configuration: %w", err)
	}
	return config, nil
}

// EffectiveValue contains a resolved setting and the layer that supplied it.
type EffectiveValue[T any] struct {
	Value  T
	Source ConfigSource
}

type EffectivePipelineConfig struct {
	Name        string
	Timeout     EffectiveValue[time.Duration]
	Stages      []EffectiveStageConfig
	Backgrounds []EffectiveBackgroundConfig
}

type EffectiveStageConfig struct {
	Name      string
	Timeout   EffectiveValue[time.Duration]
	Steps     []EffectiveStepConfig
	Parallels []EffectiveParallelConfig
	Subflows  []EffectiveSubflowConfig
}

type EffectiveParallelConfig struct {
	Name          string
	FailurePolicy EffectiveValue[FailurePolicy]
	Branches      []EffectiveBranchConfig
}
type EffectiveBranchConfig struct {
	Name  string
	Steps []EffectiveStepConfig
}
type EffectiveSubflowConfig struct {
	Name   string
	Stages []EffectiveStageConfig
}
type EffectiveBackgroundConfig struct {
	Name          string
	FailurePolicy EffectiveValue[BackgroundFailurePolicy]
}

type EffectiveStepConfig struct {
	Name      string
	Role      EffectiveValue[StepRole]
	Timeout   EffectiveValue[time.Duration]
	Retry     EffectiveValue[EffectiveRetrySettings]
	Polling   EffectiveValue[EffectivePollingSettings]
	RateLimit EffectiveValue[RateLimitPolicy]
}

type EffectiveRetrySettings struct {
	MaxAttempts int
	Delay       time.Duration
	Backoff     BackoffStrategy
	MaxDelay    time.Duration
	Jitter      float64
}
type EffectivePollingSettings struct {
	Every    time.Duration
	MaxPolls int
	Timeout  time.Duration
}

// WithConfig resolves config into a detached Pipeline copy. Explicit Go
// options always win, regardless of when this method is called.
func (p Pipeline) WithConfig(config Config) (Pipeline, error) {
	if p.effectiveConfig != nil {
		return Pipeline{}, fmt.Errorf("pipeflow: pipeline %q is already configured", p.name)
	}
	if err := p.Validate(); err != nil {
		return Pipeline{}, err
	}
	configured := clonePipeline(p)
	pipelineConfig, hasPipeline := config.Pipelines[p.name]
	if err := validateConfigNames(config, configured, pipelineConfig, hasPipeline); err != nil {
		return Pipeline{}, err
	}
	effective := EffectivePipelineConfig{Name: configured.name}
	configured.timeout, effective.Timeout = resolveDuration(configured.timeout, configured.timeoutSet,
		durationCandidate{config.Defaults.Pipeline.Timeout, ConfigSourceGlobal},
		durationCandidate{pipelineConfig.Timeout, ConfigSourcePipeline})
	effective.Stages = applyStagesConfig(configured.stages, config.Defaults, pipelineConfig.Stages)
	for i := range configured.backgrounds {
		task := &configured.backgrounds[i]
		settings := pipelineConfig.Backgrounds[task.name]
		policy, source := task.failurePolicy, ConfigSourceDefault
		if settings.FailurePolicy != nil {
			parsed, err := parseBackgroundFailurePolicy(*settings.FailurePolicy)
			if err != nil {
				return Pipeline{}, err
			}
			policy, source = parsed, ConfigSourcePipeline
		}
		if task.failurePolicySet {
			policy, source = task.failurePolicy, ConfigSourceGo
		}
		task.failurePolicy = policy
		effective.Backgrounds = append(effective.Backgrounds, EffectiveBackgroundConfig{Name: task.name, FailurePolicy: EffectiveValue[BackgroundFailurePolicy]{Value: policy, Source: source}})
	}
	if err := configured.Validate(); err != nil {
		return Pipeline{}, err
	}
	if err := validateConfiguredPolicies(configured); err != nil {
		return Pipeline{}, err
	}
	configured.effectiveConfig = &effective
	return configured, nil
}

func validateConfiguredPolicies(p Pipeline) error {
	if p.timeout < 0 {
		return fmt.Errorf("pipeflow: pipeline %q timeout cannot be negative", p.name)
	}
	var validateStages func([]Stage) error
	validateStep := func(step *Step) error {
		if step.timeout < 0 {
			return fmt.Errorf("pipeflow: step %q timeout cannot be negative", step.name)
		}
		if step.retryPolicy != nil {
			r := step.retryPolicy
			if r.MaxAttempts <= 0 || r.Delay < 0 || r.MaxDelay < 0 || r.Jitter < 0 || r.Jitter > 1 {
				return fmt.Errorf("pipeflow: step %q has invalid retry configuration", step.name)
			}
		}
		if step.pollPolicy != nil && (step.pollPolicy.every < 0 || step.pollPolicy.maxPolls < 0 || step.pollPolicy.timeout < 0) {
			return fmt.Errorf("pipeflow: step %q has invalid polling configuration", step.name)
		}
		return nil
	}
	validateStages = func(stages []Stage) error {
		for _, stage := range stages {
			if stage.timeout < 0 {
				return fmt.Errorf("pipeflow: stage %q timeout cannot be negative", stage.name)
			}
			for _, item := range stage.items {
				switch typed := item.(type) {
				case *Step:
					if err := validateStep(typed); err != nil {
						return err
					}
				case *ConcurrentSteps:
					for _, s := range typed.steps {
						if err := validateStep(s); err != nil {
							return err
						}
					}
				case *Parallel:
					for _, b := range typed.branches {
						for _, s := range b.steps {
							if err := validateStep(s); err != nil {
								return err
							}
						}
					}
				case *Subflow:
					if err := validateStages(typed.stages); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return validateStages(p.stages)
}

// EffectiveConfig returns a detached snapshot of the resolved configuration.
// The second result is false when WithConfig has not been called.
func (p Pipeline) EffectiveConfig() (EffectivePipelineConfig, bool) {
	if p.effectiveConfig == nil {
		return EffectivePipelineConfig{}, false
	}
	copy := *p.effectiveConfig
	copy.Stages = append([]EffectiveStageConfig(nil), copy.Stages...)
	for i := range copy.Stages {
		copy.Stages[i] = cloneEffectiveStage(copy.Stages[i])
	}
	copy.Backgrounds = append([]EffectiveBackgroundConfig(nil), copy.Backgrounds...)
	return copy, true
}

func cloneEffectiveStage(stage EffectiveStageConfig) EffectiveStageConfig {
	stage.Steps = append([]EffectiveStepConfig(nil), stage.Steps...)
	stage.Parallels = append([]EffectiveParallelConfig(nil), stage.Parallels...)
	for i := range stage.Parallels {
		stage.Parallels[i].Branches = append([]EffectiveBranchConfig(nil), stage.Parallels[i].Branches...)
		for j := range stage.Parallels[i].Branches {
			stage.Parallels[i].Branches[j].Steps = append([]EffectiveStepConfig(nil), stage.Parallels[i].Branches[j].Steps...)
		}
	}
	stage.Subflows = append([]EffectiveSubflowConfig(nil), stage.Subflows...)
	for i := range stage.Subflows {
		stage.Subflows[i].Stages = append([]EffectiveStageConfig(nil), stage.Subflows[i].Stages...)
		for j := range stage.Subflows[i].Stages {
			stage.Subflows[i].Stages[j] = cloneEffectiveStage(stage.Subflows[i].Stages[j])
		}
	}
	return stage
}

func applyStagesConfig(stages []Stage, defaults ConfigDefaults, configs map[string]StageConfig) []EffectiveStageConfig {
	effective := make([]EffectiveStageConfig, 0, len(stages))
	for i := range stages {
		stage := &stages[i]
		config := configs[stage.name]
		entry := EffectiveStageConfig{Name: stage.name}
		stage.timeout, entry.Timeout = resolveDuration(stage.timeout, stage.timeoutSet, durationCandidate{defaults.Stage.Timeout, ConfigSourceGlobal}, durationCandidate{config.Timeout, ConfigSourceStage})
		applyStageItemsConfig(stage, defaults, config, &entry)
		effective = append(effective, entry)
	}
	return effective
}

type durationCandidate struct {
	value  *Duration
	source ConfigSource
}

func resolveDuration(goValue time.Duration, goSet bool, candidates ...durationCandidate) (time.Duration, EffectiveValue[time.Duration]) {
	value, source := time.Duration(0), ConfigSourceDefault
	for _, candidate := range candidates {
		if candidate.value != nil {
			value, source = time.Duration(*candidate.value), candidate.source
		}
	}
	if goSet {
		value, source = goValue, ConfigSourceGo
	}
	return value, EffectiveValue[time.Duration]{Value: value, Source: source}
}

func applyStageItemsConfig(stage *Stage, defaults ConfigDefaults, config StageConfig, effective *EffectiveStageConfig) {
	for _, item := range stage.items {
		switch typed := item.(type) {
		case *Step:
			effective.Steps = append(effective.Steps, applyStepSettings(typed, defaults.Step, config.Steps[typed.name]))
		case *ConcurrentSteps:
			for _, step := range typed.steps {
				effective.Steps = append(effective.Steps, applyStepSettings(step, defaults.Step, config.Steps[step.name]))
			}
		case *Parallel:
			pc := config.Parallels[typed.name]
			policy, source := typed.failurePolicy, ConfigSourceDefault
			if pc.FailurePolicy != nil {
				parsed, err := parseFailurePolicy(*pc.FailurePolicy)
				if err != nil {
					typed.failurePolicy = FailurePolicy(-1)
					typed.configErr = err
				} else {
					policy, source = parsed, ConfigSourceStage
				}
			}
			if typed.failurePolicySet {
				policy, source = typed.failurePolicy, ConfigSourceGo
			}
			typed.failurePolicy = policy
			ep := EffectiveParallelConfig{Name: typed.name, FailurePolicy: EffectiveValue[FailurePolicy]{Value: policy, Source: source}}
			for i := range typed.branches {
				branch := &typed.branches[i]
				bc := pc.Branches[branch.name]
				eb := EffectiveBranchConfig{Name: branch.name}
				for _, step := range branch.steps {
					eb.Steps = append(eb.Steps, applyStepSettings(step, defaults.Step, bc.Steps[step.name]))
				}
				ep.Branches = append(ep.Branches, eb)
			}
			effective.Parallels = append(effective.Parallels, ep)
		case *Subflow:
			sc := config.Subflows[typed.name]
			effective.Subflows = append(effective.Subflows, EffectiveSubflowConfig{Name: typed.name, Stages: applyStagesConfig(typed.stages, defaults, sc.Stages)})
		}
	}
}

func applyStepSettings(step *Step, defaults, override StepSettings) EffectiveStepConfig {
	effective := EffectiveStepConfig{Name: step.name}
	role, roleSource := StepRoleNormal, ConfigSourceDefault
	if defaults.Role != nil {
		parsed, err := parseStepRole(*defaults.Role)
		if err != nil {
			step.configErr = err
		} else {
			role, roleSource = parsed, ConfigSourceGlobal
		}
	}
	if override.Role != nil {
		parsed, err := parseStepRole(*override.Role)
		if err != nil {
			step.configErr = err
		} else {
			role, roleSource = parsed, ConfigSourceStep
		}
	}
	if step.roleSet {
		role, roleSource = step.Role(), ConfigSourceGo
	}
	step.role = role
	effective.Role = EffectiveValue[StepRole]{Value: role, Source: roleSource}
	step.timeout, effective.Timeout = resolveDuration(step.timeout, step.timeoutSet,
		durationCandidate{defaults.Timeout, ConfigSourceGlobal},
		durationCandidate{override.Timeout, ConfigSourceStep})
	retrySettings, configuredRetrySource := defaults.Retry, ConfigSourceGlobal
	if override.Retry != nil {
		merged := mergeRetrySettings(defaults.Retry, override.Retry)
		retrySettings, configuredRetrySource = &merged, ConfigSourceStep
	}
	if retrySettings != nil && !step.retrySet {
		policy, err := retryPolicyFromSettings(*retrySettings)
		if err != nil {
			step.configErr = err
		} else {
			step.retryPolicy = &policy
		}
	}
	retry := RetryPolicy{MaxAttempts: 1}
	retrySource := ConfigSourceDefault
	if step.retryPolicy != nil {
		retry = *step.retryPolicy
		if step.retrySet {
			retrySource = ConfigSourceGo
		}
	}
	if step.retryPolicy != nil && !step.retrySet {
		retrySource = configuredRetrySource
	}
	effective.Retry = EffectiveValue[EffectiveRetrySettings]{Value: EffectiveRetrySettings{MaxAttempts: retry.MaxAttempts, Delay: retry.Delay, Backoff: retry.Backoff, MaxDelay: retry.MaxDelay, Jitter: retry.Jitter}, Source: retrySource}
	if step.pollPolicy != nil {
		pollSource := ConfigSourceGo
		settings, configuredSource := defaults.Polling, ConfigSourceGlobal
		if override.Polling != nil {
			merged := mergePollingSettings(defaults.Polling, override.Polling)
			settings, configuredSource = &merged, ConfigSourceStep
		}
		if settings != nil && !step.pollingSet {
			if settings.Every != nil {
				step.pollPolicy.every = time.Duration(*settings.Every)
			}
			if settings.MaxPolls != nil {
				step.pollPolicy.maxPolls = *settings.MaxPolls
			}
			if settings.Timeout != nil {
				step.pollPolicy.timeout = time.Duration(*settings.Timeout)
			}
			pollSource = configuredSource
		}
		effective.Polling = EffectiveValue[EffectivePollingSettings]{Value: EffectivePollingSettings{Every: step.pollPolicy.every, MaxPolls: step.pollPolicy.maxPolls, Timeout: step.pollPolicy.timeout}, Source: pollSource}
	} else {
		effective.Polling.Source = ConfigSourceDefault
		if defaults.Polling != nil || override.Polling != nil {
			step.configErr = fmt.Errorf("pipeflow: step %q polling configuration requires WithPollPredicate", step.name)
		}
	}
	if step.rateLimit != nil {
		source := ConfigSourceStep
		if step.rateLimitSet {
			source = ConfigSourceGo
		}
		effective.RateLimit = EffectiveValue[RateLimitPolicy]{Value: *step.rateLimit, Source: source}
	} else {
		rateSettings, source := defaults.RateLimit, ConfigSourceGlobal
		if override.RateLimit != nil {
			merged := mergeRateLimitSettings(defaults.RateLimit, override.RateLimit)
			rateSettings, source = &merged, ConfigSourceStep
		}
		if rateSettings != nil {
			policy := rateLimitPolicyFromSettings(*rateSettings)
			if err := policy.validate(step.name); err != nil {
				step.configErr = err
			} else {
				step.rateLimit = &policy
			}
			effective.RateLimit = EffectiveValue[RateLimitPolicy]{Value: policy, Source: source}
		} else {
			effective.RateLimit.Source = ConfigSourceDefault
		}
	}
	return effective
}

func mergePollingSettings(base, override *PollingSettings) PollingSettings {
	var merged PollingSettings
	if base != nil {
		merged = *base
	}
	if override.Every != nil {
		merged.Every = override.Every
	}
	if override.MaxPolls != nil {
		merged.MaxPolls = override.MaxPolls
	}
	if override.Timeout != nil {
		merged.Timeout = override.Timeout
	}
	return merged
}

func mergeRetrySettings(base, override *RetrySettings) RetrySettings {
	var merged RetrySettings
	if base != nil {
		merged = *base
	}
	if override.MaxAttempts != nil {
		merged.MaxAttempts = override.MaxAttempts
	}
	if override.Delay != nil {
		merged.Delay = override.Delay
	}
	if override.Backoff != nil {
		merged.Backoff = override.Backoff
	}
	if override.MaxDelay != nil {
		merged.MaxDelay = override.MaxDelay
	}
	if override.Jitter != nil {
		merged.Jitter = override.Jitter
	}
	return merged
}

func mergeRateLimitSettings(base, override *RateLimitSettings) RateLimitSettings {
	var merged RateLimitSettings
	if base != nil {
		merged = *base
	}
	if override.Key != nil {
		merged.Key = override.Key
	}
	if override.MaxCalls != nil {
		merged.MaxCalls = override.MaxCalls
	}
	if override.Interval != nil {
		merged.Interval = override.Interval
	}
	if override.MaxConcurrent != nil {
		merged.MaxConcurrent = override.MaxConcurrent
	}
	return merged
}

func rateLimitPolicyFromSettings(settings RateLimitSettings) RateLimitPolicy {
	var policy RateLimitPolicy
	if settings.Key != nil {
		policy.Key = *settings.Key
	}
	if settings.MaxCalls != nil {
		policy.MaxCalls = *settings.MaxCalls
	}
	if settings.Interval != nil {
		policy.Interval = time.Duration(*settings.Interval)
	}
	if settings.MaxConcurrent != nil {
		policy.MaxConcurrent = *settings.MaxConcurrent
	}
	return policy
}

func parseFailurePolicy(value string) (FailurePolicy, error) {
	switch value {
	case "wait_all":
		return WaitAll, nil
	case "fail_fast":
		return FailFast, nil
	default:
		return 0, fmt.Errorf("pipeflow: unknown failure policy %q", value)
	}
}
func parseBackgroundFailurePolicy(value string) (BackgroundFailurePolicy, error) {
	switch value {
	case "fatal":
		return BackgroundFatal, nil
	case "non_fatal":
		return BackgroundNonFatal, nil
	default:
		return 0, fmt.Errorf("pipeflow: unknown background failure policy %q", value)
	}
}

func retryPolicyFromSettings(settings RetrySettings) (RetryPolicy, error) {
	policy := RetryPolicy{MaxAttempts: 1}
	if settings.MaxAttempts != nil {
		policy.MaxAttempts = *settings.MaxAttempts
	}
	if settings.Delay != nil {
		policy.Delay = time.Duration(*settings.Delay)
	}
	if settings.MaxDelay != nil {
		policy.MaxDelay = time.Duration(*settings.MaxDelay)
	}
	if settings.Jitter != nil {
		policy.Jitter = *settings.Jitter
	}
	if settings.Backoff != nil {
		switch *settings.Backoff {
		case "fixed":
			policy.Backoff = FixedBackoff
		case "exponential":
			policy.Backoff = ExponentialBackoff
		default:
			return RetryPolicy{}, fmt.Errorf("pipeflow: unknown retry backoff %q", *settings.Backoff)
		}
	}
	return policy, nil
}

func validateConfigNames(config Config, pipeline Pipeline, selected PipelineConfig, exists bool) error {
	if !exists && len(config.Pipelines) > 0 {
		return fmt.Errorf("pipeflow: configuration has no entry for pipeline %q", pipeline.name)
	}
	knownStages := map[string]bool{}
	for _, stage := range pipeline.stages {
		knownStages[stage.name] = true
	}
	for name, stageConfig := range selected.Stages {
		if !knownStages[name] {
			return fmt.Errorf("pipeflow: pipeline %q configuration references unknown stage %q", pipeline.name, name)
		}
		for _, stage := range pipeline.stages {
			if stage.name == name {
				if err := validateStageConfigNames(stage, stageConfig); err != nil {
					return err
				}
			}
		}
	}
	knownBackgrounds := map[string]bool{}
	for _, task := range pipeline.backgrounds {
		knownBackgrounds[task.name] = true
	}
	for name := range selected.Backgrounds {
		if !knownBackgrounds[name] {
			return fmt.Errorf("pipeflow: configuration references unknown background %q", name)
		}
	}
	return nil
}

func validateStageConfigNames(stage Stage, config StageConfig) error {
	steps := map[string]bool{}
	parallels := map[string]*Parallel{}
	subflows := map[string]*Subflow{}
	for _, item := range stage.items {
		switch typed := item.(type) {
		case *Step:
			steps[typed.name] = true
		case *ConcurrentSteps:
			for _, s := range typed.steps {
				steps[s.name] = true
			}
		case *Parallel:
			parallels[typed.name] = typed
		case *Subflow:
			subflows[typed.name] = typed
		}
	}
	for name := range config.Steps {
		if !steps[name] {
			return fmt.Errorf("pipeflow: stage %q configuration references unknown step %q", stage.name, name)
		}
	}
	for name, pc := range config.Parallels {
		parallel := parallels[name]
		if parallel == nil {
			return fmt.Errorf("pipeflow: stage %q configuration references unknown parallel %q", stage.name, name)
		}
		branches := map[string]Branch{}
		for _, b := range parallel.branches {
			branches[b.name] = b
		}
		for branchName, bc := range pc.Branches {
			branch, ok := branches[branchName]
			if !ok {
				return fmt.Errorf("pipeflow: parallel %q configuration references unknown branch %q", name, branchName)
			}
			branchSteps := map[string]bool{}
			for _, s := range branch.steps {
				branchSteps[s.name] = true
			}
			for step := range bc.Steps {
				if !branchSteps[step] {
					return fmt.Errorf("pipeflow: parallel %q branch %q configuration references unknown step %q", name, branchName, step)
				}
			}
		}
	}
	for name, sc := range config.Subflows {
		sub := subflows[name]
		if sub == nil {
			return fmt.Errorf("pipeflow: stage %q configuration references unknown subflow %q", stage.name, name)
		}
		known := map[string]Stage{}
		for _, nested := range sub.stages {
			known[nested.name] = nested
		}
		for nestedName, nestedConfig := range sc.Stages {
			nested, ok := known[nestedName]
			if !ok {
				return fmt.Errorf("pipeflow: subflow %q configuration references unknown stage %q", name, nestedName)
			}
			if err := validateStageConfigNames(nested, nestedConfig); err != nil {
				return err
			}
		}
	}
	return nil
}

func collectStepNames(items []StageItem, names map[string]bool) {
	for _, item := range items {
		switch typed := item.(type) {
		case *Step:
			names[typed.name] = true
		case *ConcurrentSteps:
			for _, s := range typed.steps {
				names[s.name] = true
			}
		case *Parallel:
			// Branch-local Steps require an explicit branch path in configuration.
		}
	}
}

func clonePipeline(p Pipeline) Pipeline {
	clone := p
	clone.stages = cloneStages(p.stages)
	clone.effectiveConfig = nil
	return clone
}
func cloneStages(stages []Stage) []Stage {
	out := make([]Stage, len(stages))
	for i, s := range stages {
		out[i] = s
		out[i].items = make([]StageItem, len(s.items))
		for j, item := range s.items {
			out[i].items[j] = cloneStageItem(item)
		}
	}
	return out
}
func cloneStageItem(item StageItem) StageItem {
	switch typed := item.(type) {
	case *Step:
		if typed == nil {
			return (*Step)(nil)
		}
		c := *typed
		if typed.retryPolicy != nil {
			v := *typed.retryPolicy
			c.retryPolicy = &v
		}
		if typed.rateLimit != nil {
			v := *typed.rateLimit
			c.rateLimit = &v
		}
		if typed.pollPolicy != nil {
			v := *typed.pollPolicy
			c.pollPolicy = &v
		}
		return &c
	case *ConcurrentSteps:
		c := *typed
		c.steps = make([]*Step, len(typed.steps))
		for i, s := range typed.steps {
			c.steps[i] = cloneStageItem(s).(*Step)
		}
		return &c
	case *Parallel:
		c := *typed
		c.branches = append([]Branch(nil), typed.branches...)
		for i := range c.branches {
			c.branches[i].steps = make([]*Step, len(typed.branches[i].steps))
			for j, s := range typed.branches[i].steps {
				c.branches[i].steps[j] = cloneStageItem(s).(*Step)
			}
		}
		return &c
	case *Subflow:
		c := *typed
		c.stages = cloneStages(typed.stages)
		return &c
	default:
		return item
	}
}
