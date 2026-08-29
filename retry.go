package pipeflow

import (
	"math"
	"math/rand/v2"
	"time"
)

type BackoffStrategy int

const (
	FixedBackoff BackoffStrategy = iota
	ExponentialBackoff
)

type RetryPolicy struct {
	MaxAttempts int
	Delay       time.Duration
	Backoff     BackoffStrategy
	MaxDelay    time.Duration
	Jitter      float64
	RetryIf     func(error) bool
}

func (p RetryPolicy) delayAfter(failedAttempt int) time.Duration {
	delay := p.Delay
	if p.Backoff == ExponentialBackoff {
		for i := 1; i < failedAttempt; i++ {
			if delay > time.Duration(math.MaxInt64)/2 {
				delay = time.Duration(math.MaxInt64)
				break
			}
			delay *= 2
		}
	}
	if p.Jitter > 0 && delay > 0 {
		factor := 1 + ((rand.Float64()*2)-1)*p.Jitter
		delay = time.Duration(float64(delay) * factor)
		if delay < 0 {
			delay = 0
		}
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}
