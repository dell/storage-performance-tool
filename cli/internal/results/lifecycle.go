package results

// StepLifecycle is the stable artifact-requiredness state for one planned engine step.
type StepLifecycle string

// Stable per-step lifecycle values shared by tracking, finalization, and result consumers.
const (
	StepLifecyclePlanned    StepLifecycle = "planned"
	StepLifecycleStarted    StepLifecycle = "started"
	StepLifecycleCompleted  StepLifecycle = "completed"
	StepLifecycleFailed     StepLifecycle = "failed"
	StepLifecycleNotStarted StepLifecycle = "not_started"
)
