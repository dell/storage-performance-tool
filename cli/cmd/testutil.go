/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package cmd provides testing helpers for the command-line interface.
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/spf13/cobra"
)

// TestCommandHelper provides utilities for testing Cobra commands
type TestCommandHelper struct {
	t *testing.T
}

// NewTestHelper creates a new test command helper
func NewTestHelper(t *testing.T) *TestCommandHelper {
	t.Helper()
	return &TestCommandHelper{t: t}
}

// ExecuteCommand executes a command with the given arguments and returns the command, output, and error
func (h *TestCommandHelper) ExecuteCommand(root *cobra.Command, args ...string) (*cobra.Command, string, error) {
	b := new(bytes.Buffer)
	root.SetOut(b)
	root.SetErr(b)
	root.SetArgs(args)

	c, err := root.ExecuteC()
	return c, b.String(), err
}

// CreateTestCommand creates a basic command with common flags for testing
func (h *TestCommandHelper) CreateTestCommand() *cobra.Command {
	cmd := &cobra.Command{}

	// Add all the standard flags
	cmd.Flags().String("endpoint", "", "")
	cmd.Flags().StringSlice("endpoints", []string{}, "")
	cmd.Flags().String("access-key", "", "")
	cmd.Flags().String("secret-key", "", "")
	cmd.Flags().String("bucket", "", "")
	cmd.Flags().Int("auth-version", 4, "")
	cmd.Flags().Int("threads", 1, "")
	cmd.Flags().String("object-size", "", "")
	cmd.Flags().Int("object-count", 0, "")
	cmd.Flags().String("duration", "", "")

	return cmd
}

// GetTestRootCmd creates a new instance of rootCmd for testing
func GetTestRootCmd() *cobra.Command {
	// Create a new rootCmd instance
	newRootCmd := &cobra.Command{
		Use:   "spt",
		Short: "A CLI wrapper for the Spt benchmarking tool for S3-compatible storage.",
		Long: `spt is a command-line interface (CLI) wrapper designed to simplify the use of the Spt benchmarking tool for S3-compatible storage.

It provides a user-friendly interface to execute various benchmark tests (e.g., write, read, mixed, delete) against S3 endpoints, manage Spt Docker images, and inspect test results.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// This Run function is intentionally left empty for testing purposes
			return nil
		},
	}

	// Re-add the runCmd to the new rootCmd instance
	newRunCmd := &cobra.Command{
		Use:   "run <type>",
		Short: "Executes a benchmark test with a specified workload type.",
		Long: `The 'run' command executes a benchmark test. The <type> argument specifies the workload.

Available workload types:
  write: Perform a write-only test, creating new objects.
  read: Perform a read-only test on pre-existing objects.
  mixed: Perform a test with a specified mix of read and write operations.
  delete: Perform a test to measure object deletion performance.`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			workloadType := args[0]

			// For mock mode, S3 flags are optional
			if workloadType != WorkloadTypeMock {
				// Check required S3 flags
				endpoint, _ := cmd.Flags().GetString("endpoint")
				accessKey, _ := cmd.Flags().GetString("access-key")
				secretKey, _ := cmd.Flags().GetString("secret-key")
				bucket, _ := cmd.Flags().GetString("bucket")

				if endpoint == "" {
					return fmt.Errorf(ErrMissingEndpoint, workloadType)
				}
				if accessKey == "" {
					return fmt.Errorf(ErrMissingAccessKey, workloadType)
				}
				if secretKey == "" {
					return fmt.Errorf(ErrMissingSecretKey, workloadType)
				}
				if bucket == "" {
					return fmt.Errorf(ErrMissingBucket, workloadType)
				}
			}

			// Check duration vs object-count mutual exclusivity
			objectCount, _ := cmd.Flags().GetInt("object-count")
			duration, _ := cmd.Flags().GetString("duration")

			if (objectCount == 0 && duration == "") || (objectCount != 0 && duration != "") {
				return errors.New(ErrDurationOrCount)
			}
			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			// This Run function is intentionally left empty for testing purposes
			return nil
		},
	}

	// Add flags to the new runCmd instance (no longer marking as required since we handle validation in PreRunE)
	newRunCmd.Flags().StringSliceP("endpoints", "e", []string{}, "One or more S3 endpoint URLs (comma-separated or repeatable)")
	newRunCmd.Flags().String("endpoint", "", "Deprecated alias for --endpoints")
	_ = newRunCmd.Flags().MarkHidden("endpoint")
	newRunCmd.Flags().StringP("access-key", "a", "", "The S3 access key credential")
	newRunCmd.Flags().StringP("secret-key", "s", "", "The S3 secret key credential")
	newRunCmd.Flags().StringP("bucket", "b", "", "The target bucket to use for the test")
	newRunCmd.Flags().Int("auth-version", 4, "S3 authentication signature version (2 or 4; default 4)")

	newRunCmd.Flags().IntP("threads", "t", 1, "Number of parallel client threads to run (e.g., 16)")
	newRunCmd.Flags().StringP("object-size", "o", "", "The size of each object using human-readable units (e.g., 1MB, 256KB, 4GB)")
	newRunCmd.Flags().IntP("object-count", "n", 0, "Defines the workload by a fixed number of objects to process")
	newRunCmd.Flags().StringP("duration", "d", "", "Defines the workload by a fixed time duration (e.g., 5m, 1h)")

	newRunCmd.Flags().Bool("cleanup", false, "A boolean flag to automatically delete all created objects after the test completes")
	newRunCmd.Flags().Bool("create-prefix", false, "A boolean flag to ensure the target prefix (directory) is created if it doesn't exist")
	newRunCmd.Flags().StringP("output-dir", "O", "", "Specifies a local directory on the host machine to save the detailed Spt report files (e.g., ./results/test-01)")
	newRunCmd.Flags().Bool(flagAttachExistingWorkers, false, "Attach to prestarted worker nodes; spt still launches the entry node")

	newRootCmd.AddCommand(newRunCmd)

	// Add verify command for testing
	newVerifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Test verify command",
		Long:  "Test version of verify command for unit testing",
		RunE: func(_ *cobra.Command, _ []string) error {
			// This Run function is intentionally left empty for testing purposes
			// Individual tests will override this as needed
			return nil
		},
	}

	// Add all verify flags with correct defaults
	newVerifyCmd.Flags().String("test-hosts", "", "Comma-separated list of hosts to verify (defaults to localhost)")
	newVerifyCmd.Flags().Int("min-hosts", 0, "Minimum hosts that must pass (0 = all)")
	newVerifyCmd.Flags().String("network-mode", "bridge", "Docker network mode (bridge/host)")
	newVerifyCmd.Flags().Int("rmi-port-start", 40000, "Starting port for RMI range")
	newVerifyCmd.Flags().Int("rmi-port-count", 10, "Number of RMI ports to verify")
	newVerifyCmd.Flags().Bool("force-cleanup", false, "Automatically clean up conflicting containers without prompting")

	newRootCmd.AddCommand(newVerifyCmd)

	statusCommand := newStatusCommand()
	newRootCmd.AddCommand(statusCommand)

	return newRootCmd
}

// SlicesEqual compares two string slices for equality
func SlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
