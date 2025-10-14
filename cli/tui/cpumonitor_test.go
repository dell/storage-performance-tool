/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"testing"
	"time"
)

func TestNewCPUMonitor(t *testing.T) {
	tests := []struct {
		name             string
		interval         time.Duration
		expectedInterval time.Duration
	}{
		{
			name:             "valid interval",
			interval:         500 * time.Millisecond,
			expectedInterval: 500 * time.Millisecond,
		},
		{
			name:             "zero interval defaults to 1 second",
			interval:         0,
			expectedInterval: time.Second,
		},
		{
			name:             "negative interval defaults to 1 second",
			interval:         -100 * time.Millisecond,
			expectedInterval: time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := NewCPUMonitor(tt.interval)

			if monitor == nil {
				t.Fatal("NewCPUMonitor returned nil")
			}

			if monitor.interval != tt.expectedInterval {
				t.Errorf("Expected interval %v, got %v", tt.expectedInterval, monitor.interval)
			}

			if monitor.currentCPU != 0 {
				t.Errorf("Expected initial CPU to be 0, got %f", monitor.currentCPU)
			}

			if monitor.started {
				t.Error("Expected monitor to not be started initially")
			}

			if monitor.stopCh == nil {
				t.Error("Expected stopCh to be initialized")
			}
		})
	}
}

func TestCPUMonitor_StartStop(t *testing.T) {
	monitor := NewCPUMonitor(100 * time.Millisecond)

	// Test starting
	err := monitor.Start()
	if err != nil {
		t.Errorf("Unexpected error starting monitor: %v", err)
	}

	if !monitor.IsStarted() {
		t.Error("Expected monitor to be started")
	}

	// Test double start (should not error)
	err = monitor.Start()
	if err != nil {
		t.Errorf("Unexpected error on double start: %v", err)
	}

	// Give it a moment to collect some data
	time.Sleep(200 * time.Millisecond)

	// CPU should be a reasonable value (0-100)
	cpu := monitor.GetCPUPercent()
	if cpu < 0 || cpu > 100 {
		t.Errorf("Expected CPU percentage 0-100, got %f", cpu)
	}

	// Test stopping
	monitor.Stop()
	if monitor.IsStarted() {
		t.Error("Expected monitor to be stopped")
	}

	// Test double stop (should not panic)
	monitor.Stop()
}

func TestCPUMonitor_GetCPUPercent(t *testing.T) {
	monitor := NewCPUMonitor(50 * time.Millisecond)

	// Before starting, should return 0
	if cpu := monitor.GetCPUPercent(); cpu != 0 {
		t.Errorf("Expected 0 CPU before starting, got %f", cpu)
	}

	// Start monitoring
	err := monitor.Start()
	if err != nil {
		t.Fatalf("Failed to start monitor: %v", err)
	}
	defer monitor.Stop()

	// Wait for a few samples
	time.Sleep(200 * time.Millisecond)

	// Should get valid CPU readings
	cpu := monitor.GetCPUPercent()
	if cpu < 0 || cpu > 100 {
		t.Errorf("Expected CPU percentage 0-100, got %f", cpu)
	}
}

func TestCPUMonitor_ConcurrentAccess(t *testing.T) {
	monitor := NewCPUMonitor(10 * time.Millisecond)

	err := monitor.Start()
	if err != nil {
		t.Fatalf("Failed to start monitor: %v", err)
	}
	defer monitor.Stop()

	// Test concurrent reads and status checks
	done := make(chan bool)
	errors := make(chan error, 100)

	// Start multiple readers
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < 50; j++ {
				cpu := monitor.GetCPUPercent()
				if cpu < 0 || cpu > 100 {
					errors <- fmt.Errorf("invalid CPU reading: %f", cpu)
					return
				}

				if !monitor.IsStarted() {
					errors <- fmt.Errorf("monitor reported not started during test")
					return
				}

				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Check for errors
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestCPUMonitor_IsStarted(t *testing.T) {
	monitor := NewCPUMonitor(100 * time.Millisecond)

	// Initially not started
	if monitor.IsStarted() {
		t.Error("Expected monitor to not be started initially")
	}

	// After start
	monitor.Start()
	if !monitor.IsStarted() {
		t.Error("Expected monitor to be started after Start()")
	}

	// After stop
	monitor.Stop()
	if monitor.IsStarted() {
		t.Error("Expected monitor to not be started after Stop()")
	}
}

func TestCPUMonitor_StopWithoutStart(t *testing.T) {
	monitor := NewCPUMonitor(100 * time.Millisecond)

	// Should not panic or error
	monitor.Stop()

	if monitor.IsStarted() {
		t.Error("Expected monitor to not be started")
	}
}

// TestCPUMonitor_Integration tests the full lifecycle with realistic timing
func TestCPUMonitor_Integration(t *testing.T) {
	monitor := NewCPUMonitor(50 * time.Millisecond)

	// Start and collect samples
	err := monitor.Start()
	if err != nil {
		t.Fatalf("Failed to start monitor: %v", err)
	}

	// Collect CPU readings over time
	var readings []float64
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		cpu := monitor.GetCPUPercent()
		readings = append(readings, cpu)

		// Each reading should be valid
		if cpu < 0 || cpu > 100 {
			t.Errorf("Invalid CPU reading %d: %f", i, cpu)
		}
	}

	monitor.Stop()

	// Verify we got reasonable readings
	// Note: We can't test exact values since CPU usage varies,
	// but we can test that the system is working
	if len(readings) != 5 {
		t.Errorf("Expected 5 readings, got %d", len(readings))
	}

	// After stopping, readings should not change
	finalReading := monitor.GetCPUPercent()
	time.Sleep(100 * time.Millisecond)
	postStopReading := monitor.GetCPUPercent()

	if finalReading != postStopReading {
		t.Error("CPU reading changed after stopping monitor")
	}
}
