package pipeflow

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrContextInUse is returned when one Pipeflow Context is used by overlapping runs.
var ErrContextInUse = errors.New("pipeflow: context is already in use by another execution")

type Context struct {
	values map[string]any
	logger Logger

	mu       sync.RWMutex
	metadata *contextMetadata
}

type contextMetadata struct {
	mu           sync.RWMutex
	status       Status
	currentStage string
	currentStep  string
	startedAt    time.Time
	endedAt      time.Time
	limiters     map[rateLimiterKey]*runLimiter
}

type rateLimiterKey struct {
	key  string
	step *Step
}

func NewContext() *Context {
	return NewContextWithLogger(DefaultLogger{})
}

func NewContextWithLogger(logger Logger) *Context {
	if logger == nil {
		logger = DefaultLogger{}
	}

	return &Context{
		values:   make(map[string]any),
		logger:   &synchronizedLogger{logger: logger},
		metadata: &contextMetadata{status: StatusPending},
	}
}

func (c *Context) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[string]any)
	}
	c.values[key] = value
}

func (c *Context) Logger() Logger {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.logger
}

func (c *Context) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, exists := c.values[key]
	return value, exists
}

func (c *Context) Status() Status {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	return metadata.status
}

// CurrentStage is retained for compatibility. Use Execution.Current for
// structured live execution state.
// Deprecated: use Execution.Current.
func (c *Context) CurrentStage() string {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	return metadata.currentStage
}

// CurrentStep is retained for compatibility but cannot represent concurrent
// Steps. Use Execution.Current for structured live execution state.
// Deprecated: use Execution.Current.
func (c *Context) CurrentStep() string {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	return metadata.currentStep
}

// StartedAt returns compatibility execution timing stored on Context.
// Deprecated: use Execution.Report or RunReport.
func (c *Context) StartedAt() time.Time {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	return metadata.startedAt
}

// EndedAt returns compatibility execution timing stored on Context.
// Deprecated: use Execution.Report or RunReport.
func (c *Context) EndedAt() time.Time {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	return metadata.endedAt
}

// Duration returns compatibility execution timing stored on Context.
// Deprecated: use Execution.State, Execution.Report, or RunReport.
func (c *Context) Duration() time.Duration {
	metadata := c.metadataRef()
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()

	if metadata.startedAt.IsZero() {
		return 0
	}
	if metadata.endedAt.IsZero() {
		return time.Since(metadata.startedAt)
	}

	return metadata.endedAt.Sub(metadata.startedAt)
}

func (c *Context) setCurrentStage(name string) {
	metadata := c.metadataRef()
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.currentStage = name
}

func (c *Context) markStarted() error {
	metadata := c.metadataRef()
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	if metadata.status == StatusRunning {
		return ErrContextInUse
	}
	metadata.status = StatusRunning
	metadata.startedAt = time.Now()
	metadata.endedAt = time.Time{}
	metadata.currentStage = ""
	metadata.currentStep = ""
	metadata.limiters = make(map[rateLimiterKey]*runLimiter)
	return nil
}

func (c *Context) rateLimiter(step *Step, policy RateLimitPolicy) (*runLimiter, error) {
	metadata := c.metadataRef()
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	key := rateLimiterKey{key: policy.Key}
	if policy.Key == "" {
		key.step = step
	}
	if metadata.limiters == nil {
		metadata.limiters = make(map[rateLimiterKey]*runLimiter)
	}
	if limiter := metadata.limiters[key]; limiter != nil {
		if limiter.policy != policy {
			return nil, fmt.Errorf("pipeflow: rate limit key %q has conflicting policies", policy.Key)
		}
		return limiter, nil
	}
	limiter := newRunLimiter(policy)
	metadata.limiters[key] = limiter
	return limiter, nil
}

type synchronizedLogger struct {
	mu     sync.Mutex
	logger Logger
}

func (l *synchronizedLogger) Info(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Info(message)
}

func (l *synchronizedLogger) Error(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Error(message)
}

func (c *Context) markCompleted() {
	metadata := c.metadataRef()
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.status = StatusCompleted
	metadata.endedAt = time.Now()
	metadata.currentStage = ""
	metadata.currentStep = ""
}

func (c *Context) markFailed() {
	c.markFinished(StatusFailed)
}

func (c *Context) markFinished(status Status) {
	metadata := c.metadataRef()
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.status = status
	metadata.endedAt = time.Now()
	metadata.currentStage = ""
	metadata.currentStep = ""
}

func (c *Context) metadataRef() *contextMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metadata == nil {
		c.metadata = &contextMetadata{status: StatusPending}
	}
	return c.metadata
}

func (c *Context) branchContexts(count int) []*Context {
	metadata := c.metadataRef()
	c.mu.RLock()
	defer c.mu.RUnlock()
	contexts := make([]*Context, count)
	for i := range contexts {
		values := make(map[string]any, len(c.values))
		for key, value := range c.values {
			values[key] = value
		}
		contexts[i] = &Context{values: values, logger: c.logger, metadata: metadata}
	}
	return contexts
}
