package pipeflow

import (
	"fmt"
	"math"
	"time"
)

type StepOption func(*Step)

func WithRetry(policy RetryPolicy) StepOption {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Delay < 0 {
		policy.Delay = 0
	}
	if policy.MaxDelay < 0 {
		policy.MaxDelay = 0
	}
	if policy.Jitter < 0 {
		policy.Jitter = 0
	}
	if policy.Jitter > 1 {
		policy.Jitter = 1
	}
	return func(step *Step) {
		if policy.Backoff != FixedBackoff && policy.Backoff != ExponentialBackoff {
			if step.configErr == nil {
				step.configErr = fmt.Errorf("pipeflow: step %q has invalid retry backoff strategy %d", step.name, policy.Backoff)
			}
			return
		}
		if math.IsNaN(policy.Jitter) {
			if step.configErr == nil {
				step.configErr = fmt.Errorf("pipeflow: step %q retry jitter must be a number", step.name)
			}
			return
		}
		step.retryPolicy = &policy
	}
}

// WithTimeout limits the total Step execution, including retries and delays.
func WithTimeout(timeout time.Duration) StepOption {
	return func(step *Step) {
		if timeout > 0 {
			step.timeout = timeout
		} else {
			step.timeout = 0
		}
	}
}

// WithPolling repeats a successful Step operation until its predicate is true.
func WithPolling(policy PollPolicy) StepOption {
	return func(step *Step) {
		compiled, err := compilePollPolicy(step, policy)
		if err != nil {
			if step.configErr == nil {
				step.configErr = err
			}
			return
		}
		step.pollPolicy = compiled
	}
}

// WithCondition runs a Step only when its predicate returns true. A skipped
// Step preserves its input value.
func WithCondition(condition any) StepOption {
	return func(step *Step) {
		compiled, err := compileCondition(step, condition)
		if err != nil {
			if step.configErr == nil {
				step.configErr = err
			}
			return
		}
		step.condition = compiled
	}
}

// WithRateLimit limits Step invocations within a Pipeline run. The policy is
// applied to every retry and polling invocation.
func WithRateLimit(policy RateLimitPolicy) StepOption {
	return func(step *Step) {
		if err := policy.validate(step.name); err != nil {
			if step.configErr == nil {
				step.configErr = err
			}
			return
		}
		step.rateLimit = &policy
	}
}

// WithResultMetadata extracts small scalar reporting facts from a successful
// Step result without changing the flowing value.
func WithResultMetadata(extractor any) StepOption {
	return func(step *Step) {
		compiled, err := compileMetadataExtractor("step", step.name, extractor, step.outputType)
		if err != nil {
			if step.configErr == nil {
				step.configErr = err
			}
			return
		}
		step.metadata = compiled
	}
}
