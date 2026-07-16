package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestBuildSptCommand(t *testing.T) {
	tests := []struct {
		name          string
		workloadType  string
		args          []string
		expectedArgs  []string
		shouldError   bool
		errorContains string
	}{
		{
			name:         "Basic write operation",
			workloadType: "write",
			args: []string{
				"--endpoint", "http://minio:9000",
				"--access-key", "minioadmin",
				"--secret-key", "minioadmin",
				"--bucket", "test-bucket",
				"--duration", "5m",
			},
			expectedArgs: []string{
				"--storage-driver-type=s3",
				"--storage-net-node-addrs=minio",
				"--storage-net-node-port=9000",
				"--storage-auth-uid=minioadmin",
				"--storage-auth-secret=minioadmin",
				"--item-output-path=/test-bucket",
				"--load-op-type=create",
				"--storage-driver-limit-concurrency=1",
				"--load-step-limit-time=5m",
			},
			shouldError: false,
		},
		{
			name:         "Write operation with all flags and IEC object size",
			workloadType: "write",
			args: []string{
				"--endpoint", "https://s3.amazonaws.com",
				"--access-key", "AKIAIOSFODNN7EXAMPLE",
				"--secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"--bucket", "my-bucket",
				"--threads", "16",
				"--object-size", "10KiB",
				"--object-count", "1000",
			},
			expectedArgs: []string{
				"--storage-driver-type=s3",
				"--storage-net-node-addrs=s3.amazonaws.com",
				"--storage-net-node-port=443",
				"--storage-net-ssl-enabled=true",
				"--storage-auth-uid=AKIAIOSFODNN7EXAMPLE",
				"--storage-auth-secret=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"--item-output-path=/my-bucket",
				"--load-op-type=create",
				"--storage-driver-limit-concurrency=16",
				"--item-data-size=10KiB",
				"--load-op-limit-count=1000",
			},
			shouldError: false,
		},
		{
			name:         "Write operation with threads",
			workloadType: "write",
			args: []string{
				"--endpoint", "http://storage:80",
				"--access-key", "test",
				"--secret-key", "test",
				"--bucket", "test",
				"--threads", "32",
				"--duration", "10m",
			},
			expectedArgs: []string{
				"--storage-driver-type=s3",
				"--storage-net-node-addrs=storage",
				"--storage-net-node-port=80",
				"--storage-auth-uid=test",
				"--storage-auth-secret=test",
				"--item-output-path=/test",
				"--load-op-type=create",
				"--storage-driver-limit-concurrency=32",
				"--load-step-limit-time=10m",
			},
			shouldError: false,
		},
		{
			name:         "HTTPS endpoint parsing",
			workloadType: "write",
			args: []string{
				"--endpoint", "https://minio.example.com:9443",
				"--access-key", "test",
				"--secret-key", "test",
				"--bucket", "secure-bucket",
				"--duration", "1m",
			},
			expectedArgs: []string{
				"--storage-driver-type=s3",
				"--storage-net-node-addrs=minio.example.com",
				"--storage-net-node-port=9443",
				"--storage-net-ssl-enabled=true",
				"--storage-auth-uid=test",
				"--storage-auth-secret=test",
				"--item-output-path=/secure-bucket",
				"--load-op-type=create",
				"--storage-driver-limit-concurrency=1",
				"--load-step-limit-time=1m",
			},
			shouldError: false,
		},
		{
			name:         "Custom port parsing",
			workloadType: "write",
			args: []string{
				"--endpoint", "http://storage.local:8080",
				"--access-key", "test",
				"--secret-key", "test",
				"--bucket", "test",
				"--duration", "30s",
			},
			expectedArgs: []string{
				"--storage-driver-type=s3",
				"--storage-net-node-addrs=storage.local",
				"--storage-net-node-port=8080",
				"--storage-auth-uid=test",
				"--storage-auth-secret=test",
				"--item-output-path=/test",
				"--load-op-type=create",
				"--storage-driver-limit-concurrency=1",
				"--load-step-limit-time=30s",
			},
			shouldError: false,
		},
		{
			name:         "Non-write operation returns error",
			workloadType: "read",
			args: []string{
				"--endpoint", "http://test:9000",
				"--access-key", "test",
				"--secret-key", "test",
				"--bucket", "test",
				"--duration", "1m",
			},
			shouldError:   true,
			errorContains: "not yet implemented",
		},
		{
			name:         "Mock mode with minimal config",
			workloadType: "mock",
			args: []string{
				"--duration", "30s",
			},
			expectedArgs: []string{
				"--storage-driver-type=dummy-mock",
				"--load-step-limit-time=30s",
			},
			shouldError: false,
		},
		{
			name:         "Mock mode with threads",
			workloadType: "mock",
			args: []string{
				"--threads", "8",
				"--object-count", "1000",
			},
			expectedArgs: []string{
				"--storage-driver-type=dummy-mock",
				"--storage-driver-limit-concurrency=8",
				"--load-op-limit-count=1000",
			},
			shouldError: false,
		},
		{
			name:         "Mock mode with all options",
			workloadType: "mock",
			args: []string{
				"--threads", "4",
				"--object-size", "512KB",
				"--duration", "2m",
			},
			expectedArgs: []string{
				"--storage-driver-type=dummy-mock",
				"--storage-driver-limit-concurrency=4",
				"--load-step-limit-time=2m",
				"--item-data-size=512KB",
			},
			shouldError: false,
		},
		{
			name:         "Mock mode ignores S3 flags",
			workloadType: "mock",
			args: []string{
				"--endpoint", "http://ignored:9000",
				"--access-key", "ignored",
				"--secret-key", "ignored",
				"--bucket", "ignored",
				"--threads", "2",
				"--duration", "1m",
			},
			expectedArgs: []string{
				"--storage-driver-type=dummy-mock",
				"--storage-driver-limit-concurrency=2",
				"--load-step-limit-time=1m",
			},
			shouldError: false,
		},
		{
			name:         "Invalid endpoint URL",
			workloadType: "write",
			args: []string{
				"--endpoint", "not-a-valid-url",
				"--access-key", "test",
				"--secret-key", "test",
				"--bucket", "test",
				"--duration", "1m",
			},
			shouldError:   true,
			errorContains: "endpoint URL must include a hostname",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a command with the test flags
			cmd := &cobra.Command{}
			cmd.Flags().String("endpoint", "", "")
			cmd.Flags().String("access-key", "", "")
			cmd.Flags().String("secret-key", "", "")
			cmd.Flags().String("bucket", "", "")
			cmd.Flags().Int("threads", 1, "")
			cmd.Flags().String("object-size", "", "")
			cmd.Flags().Int("object-count", 0, "")
			cmd.Flags().String("duration", "", "")

			// Parse the test arguments
			if err := cmd.ParseFlags(tt.args); err != nil {
				t.Fatalf("Failed to parse flags: %v", err)
			}

			// Call buildSptCommand
			result, err := buildSptCommand(tt.workloadType, cmd)

			// Check error expectations
			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error for test '%s', got none", tt.name)
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got '%s'", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for test '%s', got: %v", tt.name, err)
				}

				// Compare result with expected args
				if !SlicesEqual(result, tt.expectedArgs) {
					t.Errorf("Command args mismatch for test '%s'\nExpected: %v\nGot: %v",
						tt.name, tt.expectedArgs, result)
				}
			}
		})
	}
}
