package pipeflow

import "time"

type RetryPolicy struct {
	MaxAttempts int
	Delay       time.Duration
}
