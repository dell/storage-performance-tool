/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"sync/atomic"
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

// fakePercent returns a CPU sampler that counts calls and reports value.
func fakePercent(calls *atomic.Int64, value float64) func(time.Duration, bool) ([]float64, error) {
	return func(time.Duration, bool) ([]float64, error) {
		calls.Add(1)
		return []float64{value}, nil
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stopWithin calls Stop and fails the test if it does not return in time.
func stopWithin(t *testing.T, monitor *CPUMonitor) {
	t.Helper()
	returned := make(chan struct{})
	go func() {
		monitor.Stop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}

// TestCPUMonitor_RestartAfterStop checks that Stop joins the loop (no sample
// after it returns), that Start after Stop runs a live loop again, and that a
// double Stop is safe.
func TestCPUMonitor_RestartAfterStop(t *testing.T) {
	monitor := NewCPUMonitor(5 * time.Millisecond)
	var calls atomic.Int64
	monitor.percent = fakePercent(&calls, 25)

	if err := monitor.Start(); err != nil {
		t.Fatalf("Failed to start monitor: %v", err)
	}
	waitFor(t, "first loop to store a reading", func() bool { return monitor.GetCPUPercent() == 25 })

	stopWithin(t, monitor)
	afterStop := calls.Load()
	time.Sleep(50 * time.Millisecond) // ten intervals
	if got := calls.Load(); got != afterStop {
		t.Fatalf("CPU sampled %d times after Stop returned", got-afterStop)
	}

	// Restart with a different reading: only a live second loop can store it.
	var restartCalls atomic.Int64
	monitor.percent = fakePercent(&restartCalls, 75)
	if err := monitor.Start(); err != nil {
		t.Fatalf("Failed to restart monitor: %v", err)
	}
	if !monitor.IsStarted() {
		t.Fatal("Expected monitor to be started after restart")
	}
	waitFor(t, "restarted loop to store a reading", func() bool { return monitor.GetCPUPercent() == 75 })

	stopWithin(t, monitor)
	stopWithin(t, monitor) // double Stop is safe
	if monitor.IsStarted() {
		t.Error("Expected monitor to be stopped")
	}
	afterStop = restartCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if got := restartCalls.Load(); got != afterStop {
		t.Fatalf("CPU sampled %d times after second Stop returned", got-afterStop)
	}
}

// TestCPUMonitor_StopAfterFailedStart checks that Stop does not block when
// Start launched no goroutine because CPU sampling failed, including after an
// earlier successful start and stop.
func TestCPUMonitor_StopAfterFailedStart(t *testing.T) {
	failing := func(time.Duration, bool) ([]float64, error) {
		return nil, fmt.Errorf("cpu sampling unavailable")
	}

	t.Run("fresh monitor", func(t *testing.T) {
		monitor := NewCPUMonitor(5 * time.Millisecond)
		monitor.percent = failing
		if err := monitor.Start(); err != nil {
			t.Fatalf("Start returned an error: %v", err)
		}
		if !monitor.IsStarted() {
			t.Fatal("Expected monitor to report started after a failed probe")
		}
		stopWithin(t, monitor)
		stopWithin(t, monitor)
		if monitor.IsStarted() {
			t.Error("Expected monitor to be stopped")
		}
	})

	t.Run("after a successful start and stop", func(t *testing.T) {
		monitor := NewCPUMonitor(5 * time.Millisecond)
		var calls atomic.Int64
		monitor.percent = fakePercent(&calls, 10)
		if err := monitor.Start(); err != nil {
			t.Fatalf("Failed to start monitor: %v", err)
		}
		stopWithin(t, monitor)

		monitor.percent = failing
		if err := monitor.Start(); err != nil {
			t.Fatalf("Start returned an error: %v", err)
		}
		stopWithin(t, monitor)
		if monitor.IsStarted() {
			t.Error("Expected monitor to be stopped")
		}
	})
}

// gatedPercent returns a sampler that, once a token is put in arm, blocks
// its next call until release is closed, after signalling entered.
func gatedPercent(calls *atomic.Int64, arm, entered, release chan struct{}) func(time.Duration, bool) ([]float64, error) {
	return func(time.Duration, bool) ([]float64, error) {
		calls.Add(1)
		select {
		case <-arm:
			entered <- struct{}{}
			<-release
		default:
		}
		return []float64{42}, nil
	}
}

// TestCPUMonitor_StopWaitsForInFlightUpdate holds one sample open and checks
// that Stop does not return while it is in flight, does not hold the lock
// while it waits, and returns once the sample ends.
func TestCPUMonitor_StopWaitsForInFlightUpdate(t *testing.T) {
	monitor := NewCPUMonitor(5 * time.Millisecond)
	var calls atomic.Int64
	arm := make(chan struct{}, 1)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	monitor.percent = gatedPercent(&calls, arm, entered, release)

	if err := monitor.Start(); err != nil {
		t.Fatalf("Failed to start monitor: %v", err)
	}
	waitFor(t, "a first reading", func() bool { return monitor.GetCPUPercent() == 42 })
	arm <- struct{}{}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("loop never sampled")
	}

	stopped := make(chan struct{})
	go func() {
		monitor.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while an update was in flight")
	case <-time.After(300 * time.Millisecond):
	}

	// Readers must not block while Stop waits.
	read := make(chan bool, 1)
	go func() {
		_ = monitor.GetCPUPercent()
		read <- monitor.IsStarted()
	}()
	select {
	case started := <-read:
		if started {
			t.Error("IsStarted is true while Stop waits")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("readers blocked while Stop waits (lock held across the wait)")
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the update finished")
	}
	after := calls.Load()
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != after {
		t.Fatalf("CPU sampled %d times after Stop returned", got-after)
	}
}
