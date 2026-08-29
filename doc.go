// Package pipeflow executes ordinary Go functions as sequential, observable
// Pipelines composed from Stages and Steps.
//
// Business values flow through function returns. Context is reserved for
// execution-wide shared data, while policies add bounded execution behavior
// such as retry, timeout, polling, rate limiting, parallel branches, managed
// background work, and finalization.
//
// The v1 API is described in API_STABILITY.md. RunReport and live state types
// intentionally contain execution facts rather than flowing business values.
package pipeflow
