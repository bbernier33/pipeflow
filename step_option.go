package pipeflow

type StepOption func(*Step)

func WithRetry(policy RetryPolicy) StepOption {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Delay < 0 {
		policy.Delay = 0
	}
	return func(step *Step) {
		step.retryPolicy = &policy
	}
}
