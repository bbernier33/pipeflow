package pipeflow

import (
	"sync"
	"time"
)

type Context struct {
	values map[string]any
	logger Logger

	mu sync.RWMutex

	status       Status
	currentStage string
	currentStep  string
	startedAt    time.Time
	endedAt      time.Time
}

func NewContext() *Context {
	return NewContextWithLogger(DefaultLogger{})
}

func NewContextWithLogger(logger Logger) *Context {
	if logger == nil {
		logger = DefaultLogger{}
	}

	return &Context{
		values: make(map[string]any),
		logger: logger,
		status: StatusPending,
	}
}

func (c *Context) Set(key string, value any) {
	c.values[key] = value
}

func (c *Context) Logger() Logger {
	return c.logger
}

func (c *Context) Get(key string) (any, bool) {
	value, exists := c.values[key]
	return value, exists
}

func (c *Context) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *Context) CurrentStage() string {
	c.mu.RLock()
	defer c.mu.Lock()
	return c.currentStage
}

func (c *Context) CurrentStep() string {
	c.mu.RLock()
	defer c.mu.Lock()
	return c.currentStep
}

func (c *Context) StartedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.startedAt
}

func (c *Context) EndedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.endedAt
}

func (c *Context) Duration() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.startedAt.IsZero() {
		return 0
	}
	if c.endedAt.IsZero() {
		return time.Since(c.startedAt)
	}

	return c.endedAt.Sub(c.startedAt)
}

func (c *Context) setStatus(status Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = status
}

func (c *Context) setCurrentStage(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentStage = name
}

func (c *Context) setCurrentStep(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentStep = name
}

func (c *Context) markStarted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = StatusRunning
	c.startedAt = time.Now()
	c.endedAt = time.Time{}
}

func (c *Context) markCompleted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = StatusCompleted
	c.endedAt = time.Now()
	c.currentStage = ""
	c.currentStep = ""
}

func (c *Context) markFailed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = StatusFailed
	c.endedAt = time.Now()
}
