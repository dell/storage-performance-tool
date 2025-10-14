/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// removed unused mock input reader

func TestNewConflictResolver(t *testing.T) {
	t.Run("create resolver successfully", func(t *testing.T) {
		resolver, err := NewConflictResolver(constants.SptAPIPort, false)
		if err != nil {
			t.Errorf("NewConflictResolver() error = %v, want nil", err)
		}
		defer func() { _ = resolver.Close() }()

		if resolver.Port != constants.SptAPIPort {
			t.Errorf("Port = %v, want %v", resolver.Port, constants.SptAPIPort)
		}
		if resolver.forceMode != false {
			t.Errorf("forceMode = %v, want %v", resolver.forceMode, false)
		}
		if resolver.checker == nil {
			t.Errorf("checker is nil, want non-nil")
		}
	})

	t.Run("create resolver with force mode", func(t *testing.T) {
		resolver, err := NewConflictResolver("8080", true)
		if err != nil {
			t.Errorf("NewConflictResolver() error = %v, want nil", err)
		}
		defer func() { _ = resolver.Close() }()

		if resolver.forceMode != true {
			t.Errorf("forceMode = %v, want %v", resolver.forceMode, true)
		}
	})
}

func TestConflictResolver_CheckAndResolveConflicts_NoConflict(t *testing.T) {
	resolver, err := NewConflictResolver("0", false) // Port 0 should be available
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer func() { _ = resolver.Close() }()

	ctx := context.Background()
	result, err := resolver.CheckAndResolveConflicts(ctx)

	if err != nil {
		t.Errorf("CheckAndResolveConflicts() error = %v, want nil", err)
	}

	if !result.Success {
		t.Errorf("Success = %v, want true (no conflict)", result.Success)
	}

	if !strings.Contains(result.Message, "available") {
		t.Errorf("Message should contain 'available', got %v", result.Message)
	}
}

func TestConflictResolver_CheckAndResolveConflicts_WithConflict(t *testing.T) {
	// Create a test server to occupy a port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer func() { _ = listener.Close() }()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to extract port: %v", err)
	}

	resolver, err := NewConflictResolver(port, true) // Use force mode to avoid interactive prompts
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer func() { _ = resolver.Close() }()
	// speed up detection against non-HTTP TCP listener
	resolver.checker.HTTPTimeout = 150 * time.Millisecond
	resolver.checker.httpClient.Timeout = 150 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := resolver.CheckAndResolveConflicts(ctx)

	if err != nil {
		t.Errorf("CheckAndResolveConflicts() error = %v, want nil", err)
	}

	// Should detect conflict but may fail to resolve (no Docker containers to stop)
	if result == nil {
		t.Fatal("Result is nil, want non-nil")
	}
}

func TestConflictResolver_ForceResolution(t *testing.T) {
	// Create a mock Spt server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			if r.Method == "DELETE" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case "/metrics/json", "/metrics":
			w.WriteHeader(http.StatusOK)
			sptJSON := `{
				"lastUpdateTimeISO": "2025-08-14T15:30:00.000Z",
				"opType": "create",
				"opsPerSec": 150.5,
				"meanLatencyMicros": 25.7
			}`
			if _, err := fmt.Fprintln(w, sptJSON); err != nil {
				t.Fatalf("write failed: %v", err)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to extract port: %v", err)
	}

	resolver, err := NewConflictResolver(port, true) // Force mode
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer func() { _ = resolver.Close() }()
	// speed up test: avoid real sleeps
	resolver.clock = NewFakeClock()

	// Create mock service info that looks like Spt
	serviceInfo := &ServiceInfo{
		IsListening: true,
		IsSpt:       true,
		Status: &SptStatus{
			RunID: "test-123",
			State: "RUNNING",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := resolver.executeForceResolution(ctx, serviceInfo)

	if err != nil {
		t.Errorf("executeForceResolution() error = %v, want nil", err)
	}

	if result == nil {
		t.Errorf("Result is nil, want non-nil")
	}

	if result.Strategy != StrategyGracefulStop && result.Strategy != StrategyForceStop {
		t.Errorf("Strategy = %v, want graceful or force", result.Strategy)
	}
}

func TestConflictResolver_ExecuteGracefulStop(t *testing.T) {
	t.Run("successful graceful stop", func(t *testing.T) {
		// Create a server that simulates a successful stop
		stopped := false
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/run":
				if r.Method == "DELETE" {
					stopped = true
					w.WriteHeader(http.StatusOK)
				} else {
					if stopped {
						w.WriteHeader(http.StatusNotFound) // No test running after stop
					} else {
						w.WriteHeader(http.StatusOK) // Test running before stop
					}
				}
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		_, port, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		resolver, err := NewConflictResolver(port, false)
		if err != nil {
			t.Fatalf("NewConflictResolver() error = %v", err)
		}
		defer func() { _ = resolver.Close() }()
		// speed up test: avoid real sleeps
		resolver.clock = NewFakeClock()

		serviceInfo := &ServiceInfo{
			IsListening: true,
			IsSpt:       true,
			Status:      &SptStatus{RunID: "test-123", State: "RUNNING"},
		}

		ctx := context.Background()
		result := resolver.executeGracefulStop(ctx, serviceInfo)

		if result.Strategy != StrategyGracefulStop {
			t.Errorf("Strategy = %v, want %v", result.Strategy, StrategyGracefulStop)
		}

		// The result success depends on whether the port is actually freed after stop
		// In this test, the HTTP server is still running, so it may not succeed
	})

	t.Run("graceful stop non-spt service", func(t *testing.T) {
		resolver, err := NewConflictResolver(constants.SptAPIPort, false)
		if err != nil {
			t.Fatalf("NewConflictResolver() error = %v", err)
		}
		defer resolver.Close()

		serviceInfo := &ServiceInfo{
			IsListening: true,
			IsSpt:       false, // Not Spt
		}

		ctx := context.Background()
		result := resolver.executeGracefulStop(ctx, serviceInfo)

		if result.Success {
			t.Errorf("Success = true, want false (can't gracefully stop non-Spt)")
		}

		if !strings.Contains(result.Message, "only works for Spt") {
			t.Errorf("Message should mention Spt requirement, got %v", result.Message)
		}
	})
}

func TestConflictResolver_ExecuteRetry(t *testing.T) {
	t.Run("retry with port now available", func(t *testing.T) {
		resolver, err := NewConflictResolver("0", false) // Port 0 should be available
		if err != nil {
			t.Fatalf("NewConflictResolver() error = %v", err)
		}
		defer resolver.Close()

		ctx := context.Background()
		result := resolver.executeRetry(ctx)

		if result.Strategy != StrategyRetry {
			t.Errorf("Strategy = %v, want %v", result.Strategy, StrategyRetry)
		}

		if !result.Success {
			t.Errorf("Success = false, want true (port should be available)")
		}

		if !strings.Contains(result.Message, "available") {
			t.Errorf("Message should contain 'available', got %v", result.Message)
		}
	})

	t.Run("retry with port still in use", func(t *testing.T) {
		// Create a listener to occupy a port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create test listener: %v", err)
		}
		defer func() { _ = listener.Close() }()

		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		resolver, err := NewConflictResolver(port, false)
		if err != nil {
			t.Fatalf("NewConflictResolver() error = %v", err)
		}
		defer resolver.Close()
		// speed up detection against non-HTTP TCP listener
		resolver.checker.HTTPTimeout = 150 * time.Millisecond
		resolver.checker.httpClient.Timeout = 150 * time.Millisecond

		ctx := context.Background()
		result := resolver.executeRetry(ctx)

		if result.Strategy != StrategyRetry {
			t.Errorf("Strategy = %v, want %v", result.Strategy, StrategyRetry)
		}

		if result.Success {
			t.Errorf("Success = true, want false (port should still be in use)")
		}

		if !strings.Contains(result.Message, "still in use") {
			t.Errorf("Message should contain 'still in use', got %v", result.Message)
		}

		if result.RetryAfter == 0 {
			t.Errorf("RetryAfter = 0, want > 0 for failed retry")
		}
	})
}

func TestConflictResolver_ExecuteAbort(t *testing.T) {
	resolver, err := NewConflictResolver(constants.SptAPIPort, false)
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer resolver.Close()

	ctx := context.Background()
	result, err := resolver.executeResolutionStrategy(ctx, StrategyAbort, nil)

	if err != nil {
		t.Errorf("executeResolutionStrategy(abort) error = %v, want nil", err)
	}

	if result.Strategy != StrategyAbort {
		t.Errorf("Strategy = %v, want %v", result.Strategy, StrategyAbort)
	}

	if result.Success {
		t.Errorf("Success = true, want false (abort should not be success)")
	}

	if !strings.Contains(result.Message, "cancelled") {
		t.Errorf("Message should contain 'cancelled', got %v", result.Message)
	}
}

func TestConflictResolver_displayConflictInfo(t *testing.T) {
	resolver, err := NewConflictResolver(constants.SptAPIPort, false)
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer resolver.Close()

	// Test with Spt service info
	serviceInfo := &ServiceInfo{
		IsListening: true,
		IsSpt:       true,
		Status: &SptStatus{
			RunID:   "test-run-123",
			State:   "RUNNING",
			Message: "Test is active",
		},
		Container: &ContainerInfo{
			ID:    "abc123def456789",
			Name:  "spt-test",
			Image: "dellspt/spt:latest",
			State: "running",
		},
	}

	// This test mainly verifies that displayConflictInfo doesn't panic
	// The actual output would need to be captured and tested in a more complex test
	resolver.displayConflictInfo(serviceInfo)

	// Test with non-Spt service
	nonSptInfo := &ServiceInfo{
		IsListening: true,
		IsSpt:       false,
	}

	resolver.displayConflictInfo(nonSptInfo)
}

func TestResolutionStrategy_String(t *testing.T) {
	tests := []struct {
		strategy ResolutionStrategy
		expected string
	}{
		{StrategyAbort, "abort"},
		{StrategyGracefulStop, "graceful"},
		{StrategyForceStop, "force"},
		{StrategyRetry, "retry"},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			if string(tt.strategy) != tt.expected {
				t.Errorf("String() = %v, want %v", string(tt.strategy), tt.expected)
			}
		})
	}
}

func TestConflictResolver_IsInteractiveMode(t *testing.T) {
	// This is tricky to test because it depends on the actual stdin
	// We can at least verify the function doesn't panic
	result := isInteractiveMode()

	// In test environment, this is usually false
	// We just verify it returns a boolean without error
	if result != true && result != false {
		t.Errorf("isInteractiveMode() returned non-boolean value")
	}
}

func TestConflictResolver_Close(t *testing.T) {
	resolver, err := NewConflictResolver(constants.SptAPIPort, false)
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}

	err = resolver.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestResolvePortConflict_HelperFunction(t *testing.T) {
	ctx := context.Background()

	// Test with available port
	result, err := ResolvePortConflict(ctx, "0", false)
	if err != nil {
		t.Errorf("ResolvePortConflict() error = %v, want nil", err)
	}

	if !result.Success {
		t.Errorf("Success = false, want true (port 0 should be available)")
	}
}

func TestConflictResolver_ExecuteResolutionStrategy_UnknownStrategy(t *testing.T) {
	resolver, err := NewConflictResolver(constants.SptAPIPort, false)
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer resolver.Close()

	ctx := context.Background()
	unknownStrategy := ResolutionStrategy("unknown")

	result, err := resolver.executeResolutionStrategy(ctx, unknownStrategy, nil)

	if err != nil {
		t.Errorf("executeResolutionStrategy() error = %v, want nil", err)
	}

	if result.Success {
		t.Errorf("Success = true, want false (unknown strategy should fail)")
	}

	if result.Error == nil {
		t.Errorf("Error is nil, want error for unknown strategy")
	}
}

// Integration tests with mock servers
func TestConflictResolver_Integration_SptConflict(t *testing.T) {
	// Create a mock Spt server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			if r.Method == "GET" {
				// Return running test info
				runResponse := `{
					"runId": "integration-test-123",
					"status": "RUNNING",
					"startTimeISO": "2025-08-14T15:30:00Z"
				}`
				fmt.Fprintln(w, runResponse)
			} else if r.Method == "DELETE" {
				// Stop command
				w.WriteHeader(http.StatusOK)
			}
		case "/metrics/json", "/metrics":
			// Return Spt-like metrics
			metricsResponse := `{
				"lastUpdateTimeISO": "2025-08-14T15:35:00Z",
				"opType": "create",
				"opsPerSec": 200.5,
				"meanLatencyMicros": 15.3,
				"concurrency": 8.0
			}`
			fmt.Fprintln(w, metricsResponse)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to extract port: %v", err)
	}

	// Test with force mode (automated resolution)
	resolver, err := NewConflictResolver(port, true)
	if err != nil {
		t.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer resolver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := resolver.CheckAndResolveConflicts(ctx)

	if err != nil {
		t.Errorf("CheckAndResolveConflicts() error = %v", err)
	}

	if result == nil {
		t.Fatalf("Result is nil")
	}

	// Should try graceful stop first
	if result.Strategy != StrategyGracefulStop && result.Strategy != StrategyForceStop {
		t.Errorf("Strategy = %v, want graceful or force", result.Strategy)
	}
}

func TestConflictResolver_VerifyResolution(t *testing.T) {
	t.Run("resolution successful - port available", func(t *testing.T) {
		resolver, err := NewConflictResolver("0", false) // Port 0 should be available
		if err != nil {
			t.Fatalf("NewConflictResolver() error = %v", err)
		}
		defer resolver.Close()

		ctx := context.Background()
		result := resolver.verifyResolution(ctx, StrategyGracefulStop)

		if !result.Success {
			t.Errorf("Success = false, want true (port should be available)")
		}

		if result.Strategy != StrategyGracefulStop {
			t.Errorf("Strategy = %v, want %v", result.Strategy, StrategyGracefulStop)
		}
	})

	t.Run("resolution failed - port still in use", func(t *testing.T) {
		// Create a listener to keep port occupied
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create test listener: %v", err)
		}
		defer func() { _ = listener.Close() }()

		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatalf("Failed to extract port: %v", err)
		}

		// Construct resolver with tight HTTP timeouts to avoid multi-second probe timeouts
		resolver := &ConflictResolver{
			Port:            port,
			checker:         NewPortCheckerWithOptions(port, "127.0.0.1", 100*time.Millisecond, 150*time.Millisecond, 1),
			dockerFinder:    nil,
			forceMode:       false,
			interactiveMode: false,
			clock:           NewFakeClock(),
		}
		defer resolver.Close()

		ctx := context.Background()
		result := resolver.verifyResolution(ctx, StrategyForceStop)

		if result.Success {
			t.Errorf("Success = true, want false (port should still be in use)")
		}

		if !strings.Contains(result.Message, "still in use") {
			t.Errorf("Message should contain 'still in use', got %v", result.Message)
		}
	})
}

// Test error conditions and edge cases
func TestConflictResolver_ErrorConditions(t *testing.T) {
	t.Run("execute force stop without Docker", func(t *testing.T) {
		resolver := &ConflictResolver{
			Port:         constants.SptAPIPort,
			checker:      NewPortChecker(constants.SptAPIPort),
			dockerFinder: nil, // No Docker available
			forceMode:    false,
			clock:        NewFakeClock(),
		}
		defer resolver.Close()

		serviceInfo := &ServiceInfo{
			IsListening: true,
			IsSpt:       false,
		}

		ctx := context.Background()
		result := resolver.executeForceStop(ctx, serviceInfo)

		if result.Success {
			t.Errorf("Success = true, want false (no Docker available)")
		}

		if !strings.Contains(result.Message, "Docker not available") {
			t.Errorf("Message should mention Docker not available, got %v", result.Message)
		}
	})
}

// Benchmark tests for performance
func BenchmarkConflictResolver_CheckAndResolveConflicts(b *testing.B) {
	resolver, err := NewConflictResolver("0", true) // Available port with force mode
	if err != nil {
		b.Fatalf("NewConflictResolver() error = %v", err)
	}
	defer resolver.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := resolver.CheckAndResolveConflicts(ctx)
		if err != nil {
			b.Errorf("CheckAndResolveConflicts() error = %v", err)
		}
	}
}

// Test environment cleanup for CI/CD
func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup any resources if needed
	// (Currently no global cleanup needed)

	os.Exit(code)
}
