package pipeflow

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusSkipped   Status = "skipped"
	StatusCancelled Status = "cancelled"
	StatusTimeout   Status = "timeout"
)
