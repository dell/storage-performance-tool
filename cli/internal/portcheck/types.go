/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"fmt"
	"time"
)

// ServiceInfo represents information about a service using a port
type ServiceInfo struct {
	IsListening bool           // Whether the port is listening
	IsSpt       bool           // Whether the service appears to be Spt
	Status      *SptStatus     // Spt status if detected (nil if not Spt)
	Container   *ContainerInfo // Docker container info if found (nil if none)
}

// SptStatus represents the status of a running Spt test
type SptStatus struct {
	RunID          string        `json:"runId,omitempty"`
	State          string        `json:"state,omitempty"`          // RUNNING, COMPLETED, FAILED, IDLE
	Progress       float64       `json:"progress,omitempty"`       // Completion percentage (0-100)
	Duration       time.Duration `json:"duration,omitempty"`       // How long the test has been running
	OpsPerSec      int64         `json:"opsPerSec,omitempty"`      // Current operations per second
	Message        string        `json:"message,omitempty"`        // Status message
	CompletedItems int64         `json:"completedItems,omitempty"` // Number of completed operations
	TotalItems     int64         `json:"totalItems,omitempty"`     // Total operations (if known)
	StartTime      time.Time     `json:"startTime,omitempty"`      // When the test started
	EndTime        time.Time     `json:"endTime,omitempty"`        // When the test ended (if completed)
}

// ContainerInfo represents information about a Docker container
type ContainerInfo struct {
	ID      string            `json:"id"`                // Container ID
	Name    string            `json:"name"`              // Container name
	Image   string            `json:"image"`             // Image name
	State   string            `json:"state"`             // Container state (running, exited, etc.)
	Ports   []string          `json:"ports"`             // Port bindings
	Labels  map[string]string `json:"labels,omitempty"`  // Container labels
	Created time.Time         `json:"created,omitempty"` // When container was created
}

// Resolution represents the user's choice for resolving port conflicts
type Resolution int

const (
	// ResolveStop stops the existing test gracefully via its API.
	ResolveStop Resolution = iota
	// ResolveForceStop forcibly stops or removes the conflicting process/container.
	ResolveForceStop
	// ResolveAbort aborts the pending spt operation.
	ResolveAbort
	// ResolveRetry retries the detection after user intervention.
	ResolveRetry
)

func (r Resolution) String() string {
	switch r {
	case ResolveStop:
		return "Stop"
	case ResolveForceStop:
		return "ForceStop"
	case ResolveAbort:
		return "Abort"
	case ResolveRetry:
		return "Retry"
	default:
		return "Unknown"
	}
}

// Error represents an error that can occur during port checking.
type Error struct {
	Operation string // The operation that failed
	Port      string // The port being checked
	Err       error  // The underlying error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("port check failed during %s on port %s: %v", e.Operation, e.Port, e.Err)
	}
	return fmt.Sprintf("port check failed during %s on port %s", e.Operation, e.Port)
}

func (e *Error) Unwrap() error {
	return e.Err
}

// NewError constructs a new portcheck.Error value describing a failure.
func NewError(operation, port string, err error) *Error {
	return &Error{
		Operation: operation,
		Port:      port,
		Err:       err,
	}
}
