package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// TestSetAndGetLogger tests the SetLogger and GetLogger functions
func TestSetAndGetLogger(t *testing.T) {
	// Test default logger when not set
	defaultLogger := GetLogger()
	if defaultLogger == nil {
		t.Error("GetLogger() returned nil when logger not set")
	}

	// Create a test logger with a buffer to capture output
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))

	// Set the logger
	SetLogger(testLogger)

	// Get the logger and verify it's the same
	retrievedLogger := GetLogger()
	if retrievedLogger != testLogger {
		t.Error("GetLogger() did not return the logger set by SetLogger()")
	}

	// Test that we can use the logger
	retrievedLogger.Info("test message")
	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Logger output does not contain expected message: %s", output)
	}
}

// TestWithComponent tests the WithComponent function
func TestWithComponent(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))
	SetLogger(testLogger)

	// Create a logger with component
	componentLogger := WithComponent("test-component")
	componentLogger.Info("component test")

	// Parse the JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Check that component field exists
	if component, ok := logEntry["component"]; !ok || component != "test-component" {
		t.Errorf("Expected component field to be 'test-component', got %v", component)
	}
}

// TestLogContainerEvent tests the LogContainerEvent function
func TestLogContainerEvent(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))
	SetLogger(testLogger)

	// Log a container event
	LogContainerEvent("started", "abc123", "image", "test-image", "port", 8080)

	// Parse the JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Verify fields
	tests := []struct {
		field    string
		expected interface{}
	}{
		{"component", "docker"},
		{"container_id", "abc123"},
		{"event", "started"},
		{"image", "test-image"},
		{"port", float64(8080)}, // JSON unmarshals numbers as float64
		{"msg", "container event"},
	}

	for _, tt := range tests {
		if value, ok := logEntry[tt.field]; !ok || value != tt.expected {
			t.Errorf("Expected %s to be %v, got %v", tt.field, tt.expected, value)
		}
	}
}

// TestLogMetricsParsing tests the LogMetricsParsing function
func TestLogMetricsParsing(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Enable debug level
	}))
	SetLogger(testLogger)

	// Log a metrics parsing event
	LogMetricsParsing("parsed stats", "ops_per_sec", 1234.56, "latency", 42)

	// Parse the JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Verify fields
	if component, ok := logEntry["component"]; !ok || component != "metrics" {
		t.Errorf("Expected component to be 'metrics', got %v", component)
	}
	if level, ok := logEntry["level"]; !ok || level != "DEBUG" {
		t.Errorf("Expected level to be 'DEBUG', got %v", level)
	}
	if msg, ok := logEntry["msg"]; !ok || msg != "parsed stats" {
		t.Errorf("Expected msg to be 'parsed stats', got %v", msg)
	}
}

// TestLogTUIEvent tests the LogTUIEvent function
func TestLogTUIEvent(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Enable debug level
	}))
	SetLogger(testLogger)

	// Log a TUI event
	LogTUIEvent("viewport_switch", "viewport", 1, "from", 0)

	// Parse the JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Verify fields
	if component, ok := logEntry["component"]; !ok || component != "tui" {
		t.Errorf("Expected component to be 'tui', got %v", component)
	}
	if event, ok := logEntry["event"]; !ok || event != "viewport_switch" {
		t.Errorf("Expected event to be 'viewport_switch', got %v", event)
	}
	if msg, ok := logEntry["msg"]; !ok || msg != "TUI event" {
		t.Errorf("Expected msg to be 'TUI event', got %v", msg)
	}
}

// TestLogError tests the LogError function
func TestLogError(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))
	SetLogger(testLogger)

	// Create a test error
	testErr := errors.New("test error")

	// Log an error
	LogError("test-component", "operation failed", testErr, "user", "john", "retry", 3)

	// Parse the JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse log output: %v", err)
	}

	// Verify fields
	tests := []struct {
		field    string
		expected interface{}
	}{
		{"component", "test-component"},
		{"error", "test error"},
		{"user", "john"},
		{"retry", float64(3)},
		{"msg", "operation failed"},
		{"level", "ERROR"},
	}

	for _, tt := range tests {
		if value, ok := logEntry[tt.field]; !ok || value != tt.expected {
			t.Errorf("Expected %s to be %v, got %v", tt.field, tt.expected, value)
		}
	}
}

// TestLoggerNotInitialized tests behavior when logger is not initialized
func TestLoggerNotInitialized(t *testing.T) {
	// Reset the logger to nil
	SetLogger(nil)

	// GetLogger should return a default logger, not nil
	logger := GetLogger()
	if logger == nil {
		t.Fatal("GetLogger() returned nil when logger not initialized")
	}

	// Test that all logging functions work without panic
	t.Run("LogContainerEvent", func(t *testing.T) {
		// Should not panic
		LogContainerEvent("test", "123")
	})

	t.Run("LogMetricsParsing", func(t *testing.T) {
		// Should not panic
		LogMetricsParsing("test")
	})

	t.Run("LogTUIEvent", func(t *testing.T) {
		// Should not panic
		LogTUIEvent("test")
	})

	t.Run("LogError", func(t *testing.T) {
		// Should not panic
		LogError("test", "message", errors.New("error"))
	})
}

// TestConcurrentAccess tests that the logging functions are safe for concurrent use
func TestConcurrentAccess(t *testing.T) {
	var buf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&buf, nil))
	SetLogger(testLogger)

	// Run multiple goroutines that log concurrently
	done := make(chan bool, 4)

	go func() {
		for i := 0; i < 100; i++ {
			LogContainerEvent("event", "id")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			LogMetricsParsing("metric")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			LogTUIEvent("event")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			LogError("component", "error", errors.New("test"))
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 4; i++ {
		<-done
	}

	// If we get here without panic, concurrent access is safe
	// Check that we got some output
	output := buf.String()
	if len(output) == 0 {
		t.Error("Expected some log output from concurrent access")
	}
}
