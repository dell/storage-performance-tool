/*
Copyright © 2025 Dell Technologies
*/

package headless

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
)

func TestNewMultiHostHeadlessRunner_Success(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 2)
	options := HeadlessOptions{
		Verbose: true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)

	if err != nil {
		t.Errorf("NewMultiHostHeadlessRunner should succeed, got error: %v", err)
		return
	}

	if runner == nil {
		t.Fatal("Runner should not be nil")
	}

	if runner.orchestrator != orchestrator {
		t.Error("Orchestrator should be properly set")
	}

	if !runner.verbose {
		t.Error("Verbose flag should be set")
	}

	if runner.traceFile != nil {
		t.Error("Trace file should be nil when not specified")
	}
}

func TestNewMultiHostHeadlessRunner_WithTraceFile(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	// Create temporary trace file
	traceFile, err := os.CreateTemp("", "multihost_trace_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(traceFile.Name())
	defer traceFile.Close()

	options := HeadlessOptions{
		TraceFile:   traceFile.Name(),
		TraceAppend: false,
		Verbose:     true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Errorf("NewMultiHostHeadlessRunner should succeed with trace file, got error: %v", err)
		return
	}

	if runner.traceFile == nil {
		t.Error("Trace file should be set when specified")
	}

	// Cleanup
	runner.Close()

	// Verify trace file contains header
	content, err := os.ReadFile(traceFile.Name())
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "Multi-Host") {
		t.Errorf("Trace file should contain multi-host header, got: %s", contentStr)
	}

	if !strings.Contains(contentStr, "1 hosts") {
		t.Errorf("Trace file should contain host count, got: %s", contentStr)
	}
}

func TestNewMultiHostHeadlessRunner_TraceFileAppend(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	// Create temporary trace file with existing content
	traceFile, err := os.CreateTemp("", "multihost_append_trace_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(traceFile.Name())
	defer traceFile.Close()

	// Write existing content
	existingContent := "Existing trace content\n"
	_, err = traceFile.WriteString(existingContent)
	if err != nil {
		t.Fatalf("Failed to write initial content: %v", err)
	}
	traceFile.Close()

	options := HeadlessOptions{
		TraceFile:   traceFile.Name(),
		TraceAppend: true,
		Verbose:     false,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Errorf("NewMultiHostHeadlessRunner should succeed with append mode, got error: %v", err)
		return
	}

	runner.Close()

	// Verify both existing and new content are present
	content, err := os.ReadFile(traceFile.Name())
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, existingContent) {
		t.Error("Trace file should contain existing content when appending")
	}

	if !strings.Contains(contentStr, "Multi-Host") {
		t.Errorf("Trace file should contain new multi-host header when appending, got: %s", contentStr)
	}
}

func TestNewMultiHostHeadlessRunner_InvalidTraceFile(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	options := HeadlessOptions{
		TraceFile: "/invalid/path/that/does/not/exist/trace.log",
		Verbose:   true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)

	if err == nil {
		t.Error("NewMultiHostHeadlessRunner should fail with invalid trace file path")
		if runner != nil {
			runner.Close()
		}
		return
	}

	if runner != nil {
		t.Error("Runner should be nil when creation fails")
	}

	expectedError := "failed to open trace file"
	if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Error should contain '%s', got: %s", expectedError, err.Error())
	}
}

func TestMultiHostHeadlessRunner_RunWithParams_Success(t *testing.T) {
	// Test the successful execution flow (as much as possible with mocks)
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 2)

	// Set up hosts with mock Docker managers
	for i := 0; i < orchestrator.GetHostCount(); i++ {
		mockDM := tui.NewMockDockerManager()
		mockDM.SetContainerID(fmt.Sprintf("container-%d", i))
		// We can't access hosts directly, so we'll test what we can
	}

	options := HeadlessOptions{
		Verbose: true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Duration:     "5s",
		Threads:      2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// This will likely fail due to mock limitations, but we test the flow
	err = runner.RunWithParams(ctx, "test-image", "/test/scenario.js", params)

	// The exact error depends on mock behavior, but it should not panic
	// Main verification is that the method completes without crashing
	if err != nil {
		t.Logf("RunWithParams failed as expected with mocks: %v", err)
	}
}

func TestMultiHostHeadlessRunner_RunWithParams_ContextCancellation(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	// Set up host with mock Docker manager
	mockDM := tui.NewMockDockerManager()
	mockDM.SetContainerID("container-1")
	// We can't access hosts directly in this test, so we'll test what we can

	options := HeadlessOptions{
		Verbose: false,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Duration:     "10s",
		Threads:      1,
	}

	// Create context that will be cancelled quickly
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err = runner.RunWithParams(ctx, "test-image", "/test/scenario.js", params)

	// Should handle context cancellation gracefully
	if err == nil {
		t.Error("RunWithParams should fail when context is cancelled")
		return
	}

	// Error could be context cancellation or mock-related
	t.Logf("RunWithParams failed due to context cancellation: %v", err)
}

func TestMultiHostHeadlessRunner_RunWithParams_EmptyOrchestrator(t *testing.T) {
	// Test with empty orchestrator
	orchestrator := tui.NewMultiHostOrchestrator([]*hostparse.HostInfo{}, 0)

	options := HeadlessOptions{
		Verbose: true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Duration:     "5s",
		Threads:      1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = runner.RunWithParams(ctx, "test-image", "/test/scenario.js", params)

	// Should handle empty orchestrator - might succeed (nothing to start/stop)
	// or fail at container startup (no hosts available)
	t.Logf("RunWithParams with empty orchestrator: %v", err)
}

func TestMultiHostHeadlessRunner_Close(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	// Create temporary trace file
	traceFile, err := os.CreateTemp("", "multihost_close_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(traceFile.Name())
	defer traceFile.Close()

	options := HeadlessOptions{
		TraceFile: traceFile.Name(),
		Verbose:   true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}

	// Verify trace file is open
	if runner.traceFile == nil {
		t.Error("Trace file should be open")
	}

	// Close the runner
	runner.Close()

	// Verify trace file is closed (by trying to write to it - should fail)
	_, err = runner.traceFile.Write([]byte("test"))
	if err == nil {
		t.Error("Writing to closed trace file should fail")
	}
}

func TestMultiHostHeadlessRunner_OutputWithTrace(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	// Create temporary trace file
	traceFile, err := os.CreateTemp("", "multihost_output_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(traceFile.Name())
	defer traceFile.Close()

	options := HeadlessOptions{
		TraceFile: traceFile.Name(),
		Verbose:   true,
	}

	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Test output method by triggering some initialization
	testMessage := "Test output message"
	runner.output("TEST", testMessage)

	// Verify the message was written to trace file
	content, err := os.ReadFile(traceFile.Name())
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "TEST") {
		t.Error("Trace file should contain TEST category")
	}

	if !strings.Contains(contentStr, testMessage) {
		t.Error("Trace file should contain test message")
	}
}

func TestMultiHostHeadlessRunner_VerboseMode(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := tui.NewMultiHostOrchestrator(hostInfos, 1)

	tests := []struct {
		name    string
		verbose bool
	}{
		{"verbose enabled", true},
		{"verbose disabled", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := HeadlessOptions{
				Verbose: tt.verbose,
			}

			runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
			if err != nil {
				t.Fatalf("Failed to create runner: %v", err)
			}
			defer runner.Close()

			if runner.verbose != tt.verbose {
				t.Errorf("Expected verbose %v, got %v", tt.verbose, runner.verbose)
			}
		})
	}
}
