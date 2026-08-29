package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// RateLimitPolicy controls Step invocation rate and concurrency within one run.
// Steps with the same non-empty Key share a limiter.
type RateLimitPolicy struct {
	Key           string
	MaxCalls      int
	Interval      time.Duration
	MaxConcurrent int
}

func (p RateLimitPolicy) validate(step string) error {
	if p.MaxCalls < 0 || p.Interval < 0 || p.MaxConcurrent < 0 {
		return fmt.Errorf("pipeflow: step %q rate limit values cannot be negative", step)
	}
	if p.MaxCalls == 0 && p.Interval > 0 {
		return fmt.Errorf("pipeflow: step %q rate limit interval requires MaxCalls", step)
	}
	if p.MaxCalls > 0 && p.Interval <= 0 {
		return fmt.Errorf("pipeflow: step %q rate limit MaxCalls requires a positive Interval", step)
	}
	if p.MaxCalls == 0 && p.MaxConcurrent == 0 {
		return fmt.Errorf("pipeflow: step %q rate limit must configure MaxCalls or MaxConcurrent", step)
	}
	return nil
}

// RetryAfterError is implemented by errors that specify a minimum delay before
// another invocation. Pipeflow discovers it through errors.As.
type RetryAfterError interface {
	error
	RetryAfter() time.Duration
}

func retryAfter(err error) (delay time.Duration, errOut error) {
	defer recoverPanic(&errOut)
	var retryAfterErr RetryAfterError
	if errors.As(err, &retryAfterErr) {
		delay = retryAfterErr.RetryAfter()
		if delay > 0 {
			return delay, nil
		}
	}
	return 0, nil
}

type runLimiter struct {
	policy RateLimitPolicy
	mu     sync.Mutex
	tokens float64
	last   time.Time
	until  time.Time
	sem    chan struct{}
}

func newRunLimiter(policy RateLimitPolicy) *runLimiter {
	limiter := &runLimiter{policy: policy, tokens: float64(policy.MaxCalls), last: time.Now()}
	if policy.MaxConcurrent > 0 {
		limiter.sem = make(chan struct{}, policy.MaxConcurrent)
	}
	return limiter
}

func (l *runLimiter) acquire(ctx context.Context) (func(), error) {
	if err := l.acquireToken(ctx); err != nil {
		return nil, err
	}
	if l.sem == nil {
		return func() {}, nil
	}
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *runLimiter) acquireToken(ctx context.Context) error {
	if l.policy.MaxCalls == 0 {
		return nil
	}
	for {
		now := time.Now()
		l.mu.Lock()
		elapsed := now.Sub(l.last)
		if elapsed > 0 {
			refill := float64(elapsed) * float64(l.policy.MaxCalls) / float64(l.policy.Interval)
			l.tokens = math.Min(float64(l.policy.MaxCalls), l.tokens+refill)
			l.last = now
		}
		wait := time.Duration(0)
		if now.Before(l.until) {
			wait = l.until.Sub(now)
		} else if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		} else {
			wait = time.Duration(math.Ceil((1 - l.tokens) * float64(l.policy.Interval) / float64(l.policy.MaxCalls)))
		}
		l.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func (l *runLimiter) observe(err error) error {
	delay, callbackErr := retryAfter(err)
	if callbackErr != nil {
		return callbackErr
	}
	if delay <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	until := time.Now().Add(delay)
	if until.After(l.until) {
		l.until = until
	}
	return nil
}
