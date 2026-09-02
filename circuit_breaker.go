package pipeflow

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("pipeflow: circuit open")

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

type CircuitBreakerPolicy struct {
	FailureThreshold  int
	ObservationWindow time.Duration
	OpenDuration      time.Duration
	HalfOpenMaxProbes int
	IsFailure         func(error) bool
}

type CircuitSnapshot struct {
	Dependency      string
	State           CircuitState
	QualifyingFails int
	OpenedAt        time.Time
	RetryAt         time.Time
	HalfOpenProbes  int
}

type CircuitOpenError struct {
	Dependency string
	State      CircuitState
	RetryAt    time.Time
}

func (e *CircuitOpenError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("pipeflow: dependency %q: %v", e.Dependency, ErrCircuitOpen)
}
func (e *CircuitOpenError) Unwrap() error { return ErrCircuitOpen }

// CircuitBreaker is concurrency-safe, in-process dependency health state.
// Share one instance wherever Steps use the same logical dependency.
type CircuitBreaker struct {
	mu             sync.Mutex
	name           string
	policy         CircuitBreakerPolicy
	state          CircuitState
	failures       []time.Time
	openedAt       time.Time
	retryAt        time.Time
	halfOpenProbes int
	generation     uint64
}

func NewCircuitBreaker(dependency string, policy CircuitBreakerPolicy) (*CircuitBreaker, error) {
	if dependency == "" {
		return nil, fmt.Errorf("pipeflow: circuit dependency cannot be empty")
	}
	if policy.FailureThreshold <= 0 {
		return nil, fmt.Errorf("pipeflow: circuit %q failure threshold must be positive", dependency)
	}
	if policy.ObservationWindow <= 0 {
		return nil, fmt.Errorf("pipeflow: circuit %q observation window must be positive", dependency)
	}
	if policy.OpenDuration <= 0 {
		return nil, fmt.Errorf("pipeflow: circuit %q open duration must be positive", dependency)
	}
	if policy.HalfOpenMaxProbes <= 0 {
		return nil, fmt.Errorf("pipeflow: circuit %q half-open probe limit must be positive", dependency)
	}
	if policy.IsFailure == nil {
		return nil, fmt.Errorf("pipeflow: circuit %q requires a failure predicate", dependency)
	}
	return &CircuitBreaker{name: dependency, policy: policy, state: CircuitClosed}, nil
}

func (b *CircuitBreaker) Snapshot() CircuitSnapshot {
	if b == nil {
		return CircuitSnapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(time.Now())
	return b.snapshotLocked()
}

type circuitPermit struct {
	breaker    *CircuitBreaker
	probe      bool
	generation uint64
	before     CircuitSnapshot
}

func (b *CircuitBreaker) acquire(now time.Time) (circuitPermit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(now)
	before := b.snapshotLocked()
	if b.state == CircuitOpen || (b.state == CircuitHalfOpen && b.halfOpenProbes >= b.policy.HalfOpenMaxProbes) {
		return circuitPermit{}, &CircuitOpenError{Dependency: b.name, State: b.state, RetryAt: b.retryAt}
	}
	permit := circuitPermit{breaker: b, generation: b.generation, before: before}
	if b.state == CircuitHalfOpen {
		b.halfOpenProbes++
		permit.probe = true
	}
	return permit, nil
}

func (p circuitPermit) finish(err error) (CircuitSnapshot, error) {
	b := p.breaker
	if b == nil {
		return CircuitSnapshot{}, nil
	}
	qualifies := false
	var predicateErr error
	if err != nil {
		qualifies, predicateErr = invokeCircuitPredicate(b.policy.IsFailure, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if p.generation != b.generation {
		return b.snapshotLocked(), predicateErr
	}
	if p.probe {
		if b.halfOpenProbes > 0 {
			b.halfOpenProbes--
		}
		if err == nil {
			b.closeLocked()
		} else if qualifies {
			b.openLocked(now)
		}
		return b.snapshotLocked(), predicateErr
	}
	if qualifies {
		b.pruneLocked(now)
		b.failures = append(b.failures, now)
		if len(b.failures) >= b.policy.FailureThreshold {
			b.openLocked(now)
		}
	}
	return b.snapshotLocked(), predicateErr
}

func (b *CircuitBreaker) advance(now time.Time) {
	if b.state == CircuitOpen && !now.Before(b.retryAt) {
		b.state = CircuitHalfOpen
		b.halfOpenProbes = 0
		b.generation++
	}
	if b.state == CircuitClosed {
		b.pruneLocked(now)
	}
}
func (b *CircuitBreaker) pruneLocked(now time.Time) {
	cutoff := now.Add(-b.policy.ObservationWindow)
	first := 0
	for first < len(b.failures) && b.failures[first].Before(cutoff) {
		first++
	}
	if first > 0 {
		b.failures = append([]time.Time(nil), b.failures[first:]...)
	}
}
func (b *CircuitBreaker) openLocked(now time.Time) {
	b.state, b.openedAt, b.retryAt = CircuitOpen, now, now.Add(b.policy.OpenDuration)
	b.halfOpenProbes = 0
	b.generation++
}
func (b *CircuitBreaker) closeLocked() {
	b.state, b.openedAt, b.retryAt = CircuitClosed, time.Time{}, time.Time{}
	b.failures = nil
	b.halfOpenProbes = 0
	b.generation++
}
func (b *CircuitBreaker) snapshotLocked() CircuitSnapshot {
	return CircuitSnapshot{Dependency: b.name, State: b.state, QualifyingFails: len(b.failures), OpenedAt: b.openedAt, RetryAt: b.retryAt, HalfOpenProbes: b.halfOpenProbes}
}
func invokeCircuitPredicate(predicate func(error) bool, err error) (qualifies bool, panicErr error) {
	defer recoverPanic(&panicErr)
	return predicate(err), nil
}

// WithCircuitBreaker protects this Step with shared dependency state.
func (s *Step) WithCircuitBreaker(breaker *CircuitBreaker) *Step {
	if s == nil {
		return nil
	}
	if breaker == nil && s.configErr == nil {
		s.configErr = fmt.Errorf("pipeflow: step %q has nil circuit breaker", s.name)
	}
	s.circuit = breaker
	return s
}
