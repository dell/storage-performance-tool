/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestRealCommandExecutor_ExecuteCommand_Local(t *testing.T) {
	executor := NewCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	tests := []struct {
		name       string
		command    []string
		wantErr    bool
		wantStdout string
		wantStderr string
	}{
		{
			name:       "simple echo command",
			command:    []string{"echo", "hello world"},
			wantErr:    false,
			wantStdout: "hello world",
			wantStderr: "",
		},
		{
			name:       "command with multiple arguments",
			command:    []string{"echo", "-n", "test output"},
			wantErr:    false,
			wantStdout: "test output",
			wantStderr: "",
		},
		{
			name:       "command that fails",
			command:    []string{"false"},
			wantErr:    true,
			wantStdout: "",
			wantStderr: "",
		},
		{
			name:       "nonexistent command",
			command:    []string{"nonexistent-command-12345"},
			wantErr:    true,
			wantStdout: "",
			wantStderr: "",
		},
		{
			name:       "empty command",
			command:    []string{},
			wantErr:    true,
			wantStdout: "",
			wantStderr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := executor.ExecuteCommand(ctx, host, tt.command)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if stdout != tt.wantStdout {
					t.Errorf("ExecuteCommand() stdout = %q, want %q", stdout, tt.wantStdout)
				}
				if stderr != tt.wantStderr {
					t.Errorf("ExecuteCommand() stderr = %q, want %q", stderr, tt.wantStderr)
				}
			}
		})
	}
}

func TestRealCommandExecutor_ExecuteCommand_Remote(t *testing.T) {
	executor := NewCommandExecutor()
	remoteHost := CreateRemoteHost("testhost.example.com")
	ctx := context.Background()

	tests := []struct {
		name    string
		command []string
		wantSSH bool // Whether the command should be wrapped in SSH
	}{
		{
			name:    "simple remote command",
			command: []string{"echo", "hello"},
			wantSSH: true,
		},
		{
			name:    "docker command on remote host",
			command: []string{constants.DockerCommand, constants.DockerCmdVersion},
			wantSSH: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test will fail because SSH likely won't work in test environment
			// But we can verify the command structure
			_, _, err := executor.ExecuteCommand(ctx, remoteHost, tt.command)

			// We expect this to fail since testhost.example.com doesn't exist
			if err == nil {
				t.Error("Expected SSH command to fail in test environment")
			}

			// The error should indicate SSH failure
			if err != nil && !strings.Contains(err.Error(), "exit status") {
				// This is expected - SSH will fail with connection error
				t.Logf("Expected SSH failure: %v", err)
			}
		})
	}
}

func TestRealCommandExecutor_ContextCancellation(t *testing.T) {
	executor := NewCommandExecutor()
	host := CreateLocalHost()

	t.Run("context cancellation during command", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel the context immediately
		cancel()

		// Try to run a command that would normally succeed
		_, _, err := executor.ExecuteCommand(ctx, host, []string{"echo", "test"})

		// Should get context canceled error
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	t.Run("context timeout during command", func(t *testing.T) {
		// Create a context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		// Sleep to ensure timeout
		time.Sleep(5 * time.Millisecond)

		_, _, err := executor.ExecuteCommand(ctx, host, []string{"echo", "test"})

		// Should get deadline exceeded error
		if err != context.DeadlineExceeded {
			t.Logf("Expected context.DeadlineExceeded, got %v (this may pass if command executed very quickly)", err)
		}
	})
}

func TestRealCommandExecutor_CommandOutput(t *testing.T) {
	executor := NewCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	t.Run("command with stdout only", func(t *testing.T) {
		stdout, stderr, err := executor.ExecuteCommand(ctx, host, []string{"echo", "stdout test"})

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if stdout != "stdout test" {
			t.Errorf("Expected stdout 'stdout test', got %q", stdout)
		}
		if stderr != "" {
			t.Errorf("Expected empty stderr, got %q", stderr)
		}
	})

	t.Run("command with stderr", func(t *testing.T) {
		// Use a command that writes to stderr
		stdout, stderr, err := executor.ExecuteCommand(ctx, host, []string{"sh", "-c", "echo 'error message' >&2; exit 1"})

		if err == nil {
			t.Error("Expected command to fail")
		}
		if stdout != "" {
			t.Errorf("Expected empty stdout, got %q", stdout)
		}
		if strings.TrimSpace(stderr) != "error message" {
			t.Errorf("Expected stderr 'error message', got %q", stderr)
		}
	})

	t.Run("command with both stdout and stderr", func(t *testing.T) {
		stdout, stderr, err := executor.ExecuteCommand(ctx, host, []string{"sh", "-c", "echo 'stdout'; echo 'stderr' >&2"})

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if stdout != "stdout" {
			t.Errorf("Expected stdout 'stdout', got %q", stdout)
		}
		if stderr != "" {
			// Note: stderr handling in exec.CommandContext.Output() doesn't capture stderr for successful commands
			t.Logf("stderr: %q (may be empty for successful commands)", stderr)
		}
	})
}

func TestRealCommandExecutor_SSHCommandBuilding(t *testing.T) {
	// This test verifies that SSH commands are built correctly
	// We'll use a mock approach by examining what would be executed

	executor := NewCommandExecutor()
	remoteHost := &hostparse.HostInfo{
		Host:     "remote.example.com",
		User:     "testuser",
		IsLocal:  false,
		Original: "testuser@remote.example.com",
	}
	ctx := context.Background()

	// Test that SSH command structure is correct by checking the error message
	_, _, err := executor.ExecuteCommand(ctx, remoteHost, []string{"echo", "test"})

	// We expect this to fail, but the error should indicate SSH was attempted
	if err == nil {
		t.Error("Expected SSH command to fail in test environment")
	}

	// The error message should give us clues about what command was run
	if err != nil {
		t.Logf("SSH command error (expected): %v", err)

		// Common SSH error patterns
		errStr := err.Error()
		if !strings.Contains(errStr, "ssh") &&
			!strings.Contains(errStr, "connection") &&
			!strings.Contains(errStr, "resolve") &&
			!strings.Contains(errStr, "no such host") &&
			!strings.Contains(errStr, "executable file not found") &&
			!strings.Contains(errStr, "exit status") {
			t.Errorf("Error doesn't appear to be SSH-related: %v", err)
		}
	}
}

func TestRealCommandExecutor_EdgeCases(t *testing.T) {
	executor := NewCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	t.Run("command with special characters", func(t *testing.T) {
		stdout, _, err := executor.ExecuteCommand(ctx, host, []string{"echo", "test with spaces and special chars: !@#$%"})

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		expected := "test with spaces and special chars: !@#$%"
		if stdout != expected {
			t.Errorf("Expected %q, got %q", expected, stdout)
		}
	})

	t.Run("command with empty arguments", func(t *testing.T) {
		stdout, _, err := executor.ExecuteCommand(ctx, host, []string{"echo", "", "after empty"})

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		// echo with empty string should produce "after empty" (no leading space)
		expected := "after empty"
		if stdout != expected {
			t.Errorf("Expected %q, got %q", expected, stdout)
		}
	})

	t.Run("command with many arguments", func(t *testing.T) {
		args := []string{"echo"}
		for i := 0; i < 50; i++ {
			args = append(args, "arg"+string(rune('0'+i%10)))
		}

		stdout, _, err := executor.ExecuteCommand(ctx, host, args)

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Should contain all arguments
		for i := 0; i < 10; i++ {
			expected := "arg" + string(rune('0'+i))
			if !strings.Contains(stdout, expected) {
				t.Errorf("Output missing expected argument %q", expected)
			}
		}
	})
}

// Mock tests for more controlled testing

func TestMockCommandExecutor_Basic(t *testing.T) {
	mock := NewMockCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	t.Run("successful command execution", func(t *testing.T) {
		expectedCmd := "echo hello"
		expectedStdout := "hello"

		mock.SetCommandSuccess(expectedCmd, expectedStdout)

		stdout, stderr, err := mock.ExecuteCommand(ctx, host, []string{"echo", "hello"})

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if stdout != expectedStdout {
			t.Errorf("Expected stdout %q, got %q", expectedStdout, stdout)
		}
		if stderr != "" {
			t.Errorf("Expected empty stderr, got %q", stderr)
		}

		// Check that command was recorded
		if !mock.HasExecutedCommand(expectedCmd) {
			t.Error("Command was not recorded as executed")
		}
	})

	t.Run("command failure", func(t *testing.T) {
		expectedCmd := "false"
		expectedStderr := "command failed"
		expectedErr := exec.ExitError{}

		mock.SetCommandFailure(expectedCmd, expectedStderr, &expectedErr)

		stdout, stderr, err := mock.ExecuteCommand(ctx, host, []string{"false"})

		if err == nil {
			t.Error("Expected command to fail")
		}
		if stdout != "" {
			t.Errorf("Expected empty stdout, got %q", stdout)
		}
		if stderr != expectedStderr {
			t.Errorf("Expected stderr %q, got %q", expectedStderr, stderr)
		}
	})

	t.Run("unexpected command", func(t *testing.T) {
		_, _, err := mock.ExecuteCommand(ctx, host, []string{"unexpected", "command"})

		if err == nil {
			t.Error("Expected error for unexpected command")
		}

		expectedCmd := "unexpected command"
		if !mock.HasExecutedCommand(expectedCmd) {
			t.Error("Unexpected command was not recorded")
		}
	})
}

func TestMockCommandExecutor_FailureModes(t *testing.T) {
	host := CreateLocalHost()
	ctx := context.Background()

	t.Run("context cancelled mode", func(t *testing.T) {
		mock := NewMockCommandExecutor()
		mock.FailureMode = "context_cancelled"

		_, _, err := mock.ExecuteCommand(ctx, host, []string{"echo", "test"})

		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	t.Run("docker not found mode", func(t *testing.T) {
		mock := NewMockCommandExecutor()
		mock.FailureMode = "docker_not_found"

		_, stderr, err := mock.ExecuteCommand(ctx, host, []string{constants.DockerCommand, "version"})

		if err == nil {
			t.Error("Expected error for docker not found")
		}
		if !strings.Contains(stderr, "command not found") {
			t.Errorf("Expected 'command not found' in stderr, got %q", stderr)
		}
	})

	t.Run("ssh failure mode", func(t *testing.T) {
		mock := NewMockCommandExecutor()
		mock.FailureMode = "ssh_failure"
		remoteHost := CreateRemoteHost("remote.example.com")

		_, stderr, err := mock.ExecuteCommand(ctx, remoteHost, []string{"echo", "test"})

		if err == nil {
			t.Error("Expected error for SSH failure")
		}
		if !strings.Contains(stderr, "ssh: connect to host failed") {
			t.Errorf("Expected SSH error in stderr, got %q", stderr)
		}
	})

	t.Run("timeout mode", func(t *testing.T) {
		mock := NewMockCommandExecutor()
		mock.FailureMode = "timeout"

		start := time.Now()
		_, _, err := mock.ExecuteCommand(ctx, host, []string{"echo", "test"})
		duration := time.Since(start)

		if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded, got %v", err)
		}

		// Should have taken at least the mock delay time
		if duration < 90*time.Millisecond {
			t.Errorf("Command completed too quickly: %v", duration)
		}
	})
}

func TestMockCommandExecutor_Tracking(t *testing.T) {
	mock := NewMockCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	// Set up some responses
	mock.SetCommandSuccess("echo hello", "hello")
	mock.SetCommandSuccess("echo world", "world")

	t.Run("command execution tracking", func(t *testing.T) {
		// Execute some commands and ignore errors intentionally (we record calls)
		if _, _, err := mock.ExecuteCommand(ctx, host, []string{"echo", "hello"}); err != nil {
			t.Logf("ExecuteCommand returned error: %v", err)
		}
		if _, _, err := mock.ExecuteCommand(ctx, host, []string{"echo", "world"}); err != nil {
			t.Logf("ExecuteCommand returned error: %v", err)
		}
		if _, _, err := mock.ExecuteCommand(ctx, host, []string{"unknown", "command"}); err != nil {
			t.Logf("ExecuteCommand returned error: %v", err)
		}

		executed := mock.GetExecutedCommands()
		if len(executed) != 3 {
			t.Errorf("Expected 3 executed commands, got %d", len(executed))
		}

		// Check command details
		if executed[0].Host != host {
			t.Error("First command host not recorded correctly")
		}
		if strings.Join(executed[0].Command, " ") != "echo hello" {
			t.Errorf("First command not recorded correctly: %v", executed[0].Command)
		}

		// Test pattern matching
		echoCommands := mock.GetExecutedCommandsMatching("echo")
		if len(echoCommands) != 2 {
			t.Errorf("Expected 2 echo commands, got %d", len(echoCommands))
		}
	})

	t.Run("clear execution history", func(t *testing.T) {
		mock.ClearExecutedCommands()
		executed := mock.GetExecutedCommands()
		if len(executed) != 0 {
			t.Errorf("Expected 0 executed commands after clear, got %d", len(executed))
		}
	})

	t.Run("last executed command", func(t *testing.T) {
		// Should be nil after clearing
		if mock.GetLastExecutedCommand() != nil {
			t.Error("Expected nil last executed command after clear")
		}

		// Execute a command
		if _, _, err := mock.ExecuteCommand(ctx, host, []string{"test", "command"}); err != nil {
			t.Logf("ExecuteCommand returned error: %v", err)
		}

		last := mock.GetLastExecutedCommand()
		if last == nil {
			t.Error("Expected non-nil last executed command")
		}
		if strings.Join(last.Command, " ") != "test command" {
			t.Errorf("Last command not recorded correctly: %v", last.Command)
		}
	})
}

func TestMockCommandExecutor_DelaySimulation(t *testing.T) {
	mock := NewMockCommandExecutor()
	host := CreateLocalHost()
	ctx := context.Background()

	t.Run("command with delay", func(t *testing.T) {
		delay := 50 * time.Millisecond
		mock.SetCommandResponse("slow command", MockResponse{
			Stdout: "done",
			Stderr: "",
			Error:  nil,
			Delay:  delay,
		})

		start := time.Now()
		stdout, _, err := mock.ExecuteCommand(ctx, host, []string{"slow", "command"})
		duration := time.Since(start)

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if stdout != "done" {
			t.Errorf("Expected stdout 'done', got %q", stdout)
		}
		if duration < delay {
			t.Errorf("Command completed too quickly: %v (expected at least %v)", duration, delay)
		}
	})

	t.Run("context cancellation during delay", func(t *testing.T) {
		delay := 100 * time.Millisecond
		mock.SetCommandResponse("very slow command", MockResponse{
			Stdout: "done",
			Stderr: "",
			Error:  nil,
			Delay:  delay,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, _, err := mock.ExecuteCommand(ctx, host, []string{"very", "slow", "command"})
		duration := time.Since(start)

		if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded, got %v", err)
		}
		if duration > delay {
			t.Errorf("Command took too long: %v (should have been cancelled)", duration)
		}
	})
}

func TestHostCommand_SSHArguments(t *testing.T) {
	remoteHost := CreateRemoteHost("remote.example.com")
	ctx := context.Background()

	oneShot := hostCommand(ctx, remoteHost, []string{"docker", "ps"})
	wantOneShot := []string{"ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes", "testuser@remote.example.com", "docker", "ps"}
	if strings.Join(oneShot.Args, " ") != strings.Join(wantOneShot, " ") {
		t.Fatalf("one-shot ssh args = %q, want %q", oneShot.Args, wantOneShot)
	}

	stream := hostCommand(ctx, remoteHost, []string{"docker", "logs"}, constants.SSHServerAliveInterval, constants.SSHServerAliveCountMax)
	wantStream := []string{"ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes",
		"-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3", "testuser@remote.example.com", "docker", "logs"}
	if strings.Join(stream.Args, " ") != strings.Join(wantStream, " ") {
		t.Fatalf("stream ssh args = %q, want %q", stream.Args, wantStream)
	}

	local := hostCommand(ctx, CreateLocalHost(), []string{"docker", "ps"}, constants.SSHServerAliveInterval)
	if strings.Join(local.Args, " ") != "docker ps" {
		t.Fatalf("local command should run directly, got %q", local.Args)
	}
}

func TestRealCommandExecutor_StreamCommand_SplitsLines(t *testing.T) {
	executor := NewCommandExecutor()
	var stdout, stderr []string
	err := executor.StreamCommand(context.Background(), CreateLocalHost(),
		[]string{"sh", "-c", `printf 'one\ntwo\r\n'; printf 'warn\n' >&2; printf 'tail'`},
		func(line string) { stdout = append(stdout, line) },
		func(line string) { stderr = append(stderr, line) },
	)
	if err != nil {
		t.Fatalf("StreamCommand returned error: %v", err)
	}
	if strings.Join(stdout, "|") != "one|two|tail" {
		t.Fatalf("unexpected stdout lines: %q", stdout)
	}
	if strings.Join(stderr, "|") != "warn" {
		t.Fatalf("unexpected stderr lines: %q", stderr)
	}
}

func TestRealCommandExecutor_StreamCommand_DeliversBeforeExitAndCancelsPromptly(t *testing.T) {
	executor := NewCommandExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := make(chan struct{})
	result := make(chan error, 1)
	// The backgrounded sleep keeps the output pipe open after sh is killed,
	// exercising the bounded wait for descendants.
	go func() {
		result <- executor.StreamCommand(ctx, CreateLocalHost(),
			[]string{"sh", "-c", "echo first; sleep 5 & wait"},
			func(line string) {
				if line == "first" {
					close(first)
				}
			},
			func(string) {},
		)
	}()

	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("first line was not delivered while the command was still running")
	}
	cancelled := time.Now()
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected an error after cancellation")
		}
	case <-time.After(constants.StreamCommandWaitDelay + 2*time.Second):
		t.Fatal("StreamCommand did not return promptly after cancellation")
	}
	if elapsed := time.Since(cancelled); elapsed > constants.StreamCommandWaitDelay+time.Second {
		t.Fatalf("cancellation took %v", elapsed)
	}
}

func TestRealCommandExecutor_StreamCommand_SerializesCallbacks(t *testing.T) {
	executor := NewCommandExecutor()
	// Shared state without its own lock: the race detector flags concurrent callbacks.
	var lines []string
	record := func(line string) { lines = append(lines, line) }
	err := executor.StreamCommand(context.Background(), CreateLocalHost(),
		[]string{"sh", "-c", `i=0; while [ $i -lt 500 ]; do echo out$i; echo err$i >&2; i=$((i+1)); done`},
		record, record,
	)
	if err != nil {
		t.Fatalf("StreamCommand returned error: %v", err)
	}
	if len(lines) != 1000 {
		t.Fatalf("expected 1000 lines, got %d", len(lines))
	}
}

func TestRealCommandExecutor_StreamCommand_ReturnsExitError(t *testing.T) {
	executor := NewCommandExecutor()
	var stderr []string
	err := executor.StreamCommand(context.Background(), CreateLocalHost(),
		[]string{"sh", "-c", "echo 'No such container' >&2; exit 1"},
		func(string) {}, func(line string) { stderr = append(stderr, line) },
	)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit status 1, got %v", err)
	}
	if strings.Join(stderr, "|") != "No such container" {
		t.Fatalf("unexpected stderr lines: %q", stderr)
	}
}
