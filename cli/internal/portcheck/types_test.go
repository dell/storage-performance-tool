/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"fmt"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestResolution_String(t *testing.T) {
	tests := []struct {
		name     string
		r        Resolution
		expected string
	}{
		{"ResolveStop", ResolveStop, "Stop"},
		{"ResolveForceStop", ResolveForceStop, "ForceStop"},
		{"ResolveAbort", ResolveAbort, "Abort"},
		{"ResolveRetry", ResolveRetry, "Retry"},
		{"Unknown", Resolution(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.r.String()
			if result != tt.expected {
				t.Errorf("Resolution.String() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestPortCheckError(t *testing.T) {
	originalErr := fmt.Errorf("connection refused")

	t.Run("with underlying error", func(t *testing.T) {
		err := NewError("connect", constants.SptAPIPort, originalErr)

		if err.Operation != "connect" {
			t.Errorf("Operation = %v, want %v", err.Operation, "connect")
		}
		if err.Port != constants.SptAPIPort {
			t.Errorf("Port = %v, want %v", err.Port, constants.SptAPIPort)
		}
		if err.Err != originalErr {
			t.Errorf("Err = %v, want %v", err.Err, originalErr)
		}

		expectedMsg := "port check failed during connect on port 9999: connection refused"
		if err.Error() != expectedMsg {
			t.Errorf("Error() = %v, want %v", err.Error(), expectedMsg)
		}

		if err.Unwrap() != originalErr {
			t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), originalErr)
		}
	})

	t.Run("without underlying error", func(t *testing.T) {
		err := NewError("listen", "8080", nil)

		expectedMsg := "port check failed during listen on port 8080"
		if err.Error() != expectedMsg {
			t.Errorf("Error() = %v, want %v", err.Error(), expectedMsg)
		}

		if err.Unwrap() != nil {
			t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), nil)
		}
	})
}

func TestServiceInfo_GetServiceDescription(t *testing.T) {
	tests := []struct {
		name     string
		info     ServiceInfo
		expected string
	}{
		{
			name:     "port available",
			info:     ServiceInfo{IsListening: false},
			expected: "Port is available",
		},
		{
			name: "spt with run info",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					RunID: "test-123",
					State: "RUNNING",
				},
			},
			expected: "Spt test (Run ID: test-123, State: RUNNING)",
		},
		{
			name: "spt without run info",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status:      &SptStatus{},
			},
			expected: "Spt service",
		},
		{
			name: "container service",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       false,
				Container: &ContainerInfo{
					Name:  "web-server",
					Image: "nginx:latest",
				},
			},
			expected: "Container: web-server (nginx:latest)",
		},
		{
			name: "unknown service",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       false,
			},
			expected: "Unknown service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.info.GetServiceDescription()
			if result != tt.expected {
				t.Errorf("GetServiceDescription() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestServiceInfo_IsTestActive(t *testing.T) {
	tests := []struct {
		name     string
		info     ServiceInfo
		expected bool
	}{
		{
			name: "not spt",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       false,
			},
			expected: false,
		},
		{
			name: "spt without status",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status:      nil,
			},
			expected: false,
		},
		{
			name: "running test",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					State: "RUNNING",
				},
			},
			expected: true,
		},
		{
			name: "starting test",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					State: "STARTING",
				},
			},
			expected: true,
		},
		{
			name: "completed test",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					State: "COMPLETED",
				},
			},
			expected: false,
		},
		{
			name: "failed test",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					State: "FAILED",
				},
			},
			expected: false,
		},
		{
			name: "case insensitive running",
			info: ServiceInfo{
				IsListening: true,
				IsSpt:       true,
				Status: &SptStatus{
					State: "running",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.info.IsTestActive()
			if result != tt.expected {
				t.Errorf("IsTestActive() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSptStatus_GetFormattedDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{"zero duration", 0, "unknown"},
		{"30 seconds", 30 * time.Second, "0:30"},
		{"1 minute", 1 * time.Minute, "1:00"},
		{"1 minute 30 seconds", 90 * time.Second, "1:30"},
		{"1 hour", 1 * time.Hour, "1:00:00"},
		{"1 hour 30 minutes", 90 * time.Minute, "1:30:00"},
		{"2 hours 5 minutes 42 seconds", 2*time.Hour + 5*time.Minute + 42*time.Second, "2:05:42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &SptStatus{Duration: tt.duration}
			result := status.GetFormattedDuration()
			if result != tt.expected {
				t.Errorf("GetFormattedDuration() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSptStatus_GetFormattedProgress(t *testing.T) {
	tests := []struct {
		name     string
		status   SptStatus
		expected string
	}{
		{
			name:     "percentage progress",
			status:   SptStatus{Progress: 75.5},
			expected: "75.5%",
		},
		{
			name: "completed/total with calculation",
			status: SptStatus{
				CompletedItems: 250,
				TotalItems:     1000,
			},
			expected: "25.0% (250/1000)",
		},
		{
			name: "only completed items",
			status: SptStatus{
				CompletedItems: 42,
			},
			expected: "42 completed",
		},
		{
			name:     "no progress info",
			status:   SptStatus{},
			expected: "unknown",
		},
		{
			name: "progress overrides completed/total",
			status: SptStatus{
				Progress:       80.0,
				CompletedItems: 200,
				TotalItems:     1000,
			},
			expected: "80.0%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.GetFormattedProgress()
			if result != tt.expected {
				t.Errorf("GetFormattedProgress() = %v, want %v", result, tt.expected)
			}
		})
	}
}
