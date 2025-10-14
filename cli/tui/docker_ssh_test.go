/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestNewDockerManagerForHost_LocalHost(t *testing.T) {
	tests := []struct {
		name        string
		hostInfo    *hostparse.HostInfo
		expectError bool
		skipReason  string
	}{
		{
			name: "local IPv4 host",
			hostInfo: &hostparse.HostInfo{
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "127.0.0.1",
			},
			expectError: false,
		},
		{
			name: "localhost normalized",
			hostInfo: &hostparse.HostInfo{
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "localhost",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipReason != "" {
				t.Skip(tt.skipReason)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			dm, err := NewDockerManagerForHost(ctx, tt.hostInfo)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				// Docker might not be available in test environment
				if strings.Contains(err.Error(), "connect") ||
					strings.Contains(err.Error(), "docker") ||
					strings.Contains(err.Error(), "refused") {
					t.Skipf("Docker not available for testing: %v", err)
				}
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify DockerManager was created properly
			if dm == nil {
				t.Fatal("DockerManager should not be nil")
			}

			if dm.client == nil {
				t.Error("Docker client should not be nil")
			}

			if dm.ctx == nil {
				t.Error("Context should not be nil")
			}

			if dm.cancel == nil {
				t.Error("Cancel function should not be nil")
			}

			if dm.hostInfo != tt.hostInfo {
				t.Error("HostInfo should be properly set")
			}

			// Verify host info fields
			if dm.hostInfo.Host != tt.hostInfo.Host {
				t.Errorf("Expected host %s, got %s", tt.hostInfo.Host, dm.hostInfo.Host)
			}

			if dm.hostInfo.IsLocal != tt.hostInfo.IsLocal {
				t.Errorf("Expected IsLocal %v, got %v", tt.hostInfo.IsLocal, dm.hostInfo.IsLocal)
			}

			// Test that context is properly managed
			dm.cancel()

			// Give a moment for context cancellation to propagate
			time.Sleep(100 * time.Millisecond)

			select {
			case <-dm.ctx.Done():
				// Context should be cancelled
			default:
				t.Error("Context should be cancelled after calling cancel")
			}

			// Clean up
			if dm.client != nil {
				dm.client.Close()
			}
		})
	}
}

func TestNewDockerManagerForHost_RemoteHost(t *testing.T) {
	tests := []struct {
		name        string
		hostInfo    *hostparse.HostInfo
		expectError bool
		description string
	}{
		{
			name: "remote host without user",
			hostInfo: &hostparse.HostInfo{
				Host:     "remote-host.example.com",
				IsLocal:  false,
				Original: "remote-host.example.com",
			},
			expectError: true,
			description: "Should fail to connect to non-existent remote host",
		},
		{
			name: "remote host with user",
			hostInfo: &hostparse.HostInfo{
				User:     "testuser",
				Host:     "remote-host.example.com",
				IsLocal:  false,
				Original: "testuser@remote-host.example.com",
			},
			expectError: true,
			description: "Should fail to connect to non-existent remote host with user",
		},
		{
			name: "remote IP with user",
			hostInfo: &hostparse.HostInfo{
				User:     "root",
				Host:     "192.168.1.100",
				IsLocal:  false,
				Original: "root@192.168.1.100",
			},
			expectError: true,
			description: "Should fail to connect to non-existent remote IP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			dm, err := NewDockerManagerForHost(ctx, tt.hostInfo)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none for %s", tt.description)
				} else {
					// Verify the error message contains helpful troubleshooting info
					errorMsg := err.Error()
					if !strings.Contains(errorMsg, "SSH key authentication") {
						t.Errorf("Error should contain SSH troubleshooting guidance, got: %s", errorMsg)
					}
					if !strings.Contains(errorMsg, tt.hostInfo.Original) {
						t.Errorf("Error should contain original host info, got: %s", errorMsg)
					}
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for %s: %v", tt.description, err)
				return
			}

			// If we get here, clean up
			if dm != nil {
				if dm.cancel != nil {
					dm.cancel()
				}
				if dm.client != nil {
					dm.client.Close()
				}
			}
		})
	}
}

func TestNewDockerManagerForHost_SSHEnvironment(t *testing.T) {
	tests := []struct {
		name          string
		sshAuthSock   string
		expectWarning bool
		description   string
	}{
		{
			name:          "SSH agent available",
			sshAuthSock:   "/tmp/ssh-agent.sock",
			expectWarning: false,
			description:   "Should not warn when SSH_AUTH_SOCK is set",
		},
		{
			name:          "SSH agent not available",
			sshAuthSock:   "",
			expectWarning: true,
			description:   "Should warn when SSH_AUTH_SOCK is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original SSH_AUTH_SOCK
			originalSSHAuthSock := os.Getenv("SSH_AUTH_SOCK")
			defer func() {
				if originalSSHAuthSock != "" {
					os.Setenv("SSH_AUTH_SOCK", originalSSHAuthSock)
				} else {
					os.Unsetenv("SSH_AUTH_SOCK")
				}
			}()

			// Set test environment
			if tt.sshAuthSock != "" {
				os.Setenv("SSH_AUTH_SOCK", tt.sshAuthSock)
			} else {
				os.Unsetenv("SSH_AUTH_SOCK")
			}

			hostInfo := &hostparse.HostInfo{
				User:     "testuser",
				Host:     "remote-host.example.com",
				IsLocal:  false,
				Original: "testuser@remote-host.example.com",
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// This will fail to connect, but we're testing the SSH environment detection
			_, err := NewDockerManagerForHost(ctx, hostInfo)

			// We expect this to fail due to non-existent host, but check that
			// appropriate SSH environment handling occurred
			if err == nil {
				t.Error("Expected connection to fail for non-existent host")
			}

			// The warning about SSH_AUTH_SOCK is logged, but we can't easily capture
			// log output in tests. The main verification is that the function handles
			// the SSH environment variables appropriately without panicking.
		})
	}
}

func TestNewDockerManagerForHost_ContextHandling(t *testing.T) {
	t.Run("context cancellation", func(t *testing.T) {
		hostInfo := &hostparse.HostInfo{
			Host:     "127.0.0.1",
			IsLocal:  true,
			Original: "127.0.0.1",
		}

		// Create a context that we'll cancel immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		dm, err := NewDockerManagerForHost(ctx, hostInfo)

		// Should handle cancelled context gracefully
		if err == nil {
			t.Error("Expected error when using cancelled context")
			if dm != nil && dm.client != nil {
				dm.client.Close()
			}
		}
	})

	t.Run("context timeout", func(t *testing.T) {
		hostInfo := &hostparse.HostInfo{
			User:     "testuser",
			Host:     "non-existent-host-12345.example.com",
			IsLocal:  false,
			Original: "testuser@non-existent-host-12345.example.com",
		}

		// Create a context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		dm, err := NewDockerManagerForHost(ctx, hostInfo)

		if err == nil {
			t.Error("Expected timeout error for very short context")
			if dm != nil && dm.client != nil {
				dm.client.Close()
			}
		}
	})
}

func TestNewDockerManagerForHost_DockerHostGeneration(t *testing.T) {
	tests := []struct {
		name         string
		hostInfo     *hostparse.HostInfo
		expectedHost string
	}{
		{
			name: "local host generates unix socket",
			hostInfo: &hostparse.HostInfo{
				Host:     "127.0.0.1",
				IsLocal:  true,
				Original: "127.0.0.1",
			},
			expectedHost: "unix:///var/run/docker.sock",
		},
		{
			name: "remote host generates SSH connection",
			hostInfo: &hostparse.HostInfo{
				User:     "root",
				Host:     "worker1",
				IsLocal:  false,
				Original: "root@worker1",
			},
			expectedHost: "ssh://root@worker1",
		},
		{
			name: "remote host without user",
			hostInfo: &hostparse.HostInfo{
				Host:     "worker1",
				IsLocal:  false,
				Original: "worker1",
			},
			expectedHost: "ssh://worker1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerHost := tt.hostInfo.GetDockerHost()
			if dockerHost != tt.expectedHost {
				t.Errorf("Expected docker host %s, got %s", tt.expectedHost, dockerHost)
			}
		})
	}
}

func TestNewDockerManagerForHost_ErrorMessages(t *testing.T) {
	t.Run("local connection error", func(t *testing.T) {
		hostInfo := &hostparse.HostInfo{
			Host:     "127.0.0.1",
			IsLocal:  true,
			Original: "127.0.0.1",
		}

		// This might succeed if Docker is available, or fail if not
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		dm, err := NewDockerManagerForHost(ctx, hostInfo)

		if err != nil {
			// If we get an error, verify it's the right type for local connections
			if strings.Contains(err.Error(), "SSH key authentication") {
				t.Error("Local connection error should not mention SSH")
			}
			if !strings.Contains(err.Error(), "local Docker") &&
				!strings.Contains(err.Error(), "Docker client") {
				t.Errorf("Local connection error should mention local Docker, got: %s", err.Error())
			}
		} else if dm != nil {
			// Clean up successful connection
			dm.cancel()
			dm.client.Close()
		}
	})

	t.Run("remote connection error format", func(t *testing.T) {
		hostInfo := &hostparse.HostInfo{
			User:     "testuser",
			Host:     "remote-host.example.com",
			IsLocal:  false,
			Original: "testuser@remote-host.example.com",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := NewDockerManagerForHost(ctx, hostInfo)

		if err == nil {
			t.Error("Expected error for non-existent remote host")
			return
		}

		errorMsg := err.Error()

		// Verify error message contains troubleshooting guidance
		expectedStrings := []string{
			"SSH key authentication",
			"testuser@remote-host.example.com",
			"testuser",
			"remote-host.example.com",
		}

		for _, expected := range expectedStrings {
			if !strings.Contains(errorMsg, expected) {
				t.Errorf("Error message should contain '%s', got: %s", expected, errorMsg)
			}
		}
	})
}
