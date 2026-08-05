package cmd

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestRootCommand(t *testing.T) {
	isTesting = true
	defer func() { isTesting = false }()

	// Test the help output for the root command
	b := new(bytes.Buffer)
	rootCmd.SetOut(b)
	rootCmd.SetErr(b)
	rootCmd.SetArgs([]string{"--help"})

	err := rootCmd.Execute()
	if err != nil {
		t.Fatalf("rootCmd.Execute() with --help failed: %v", err)
	}

	if rootCmd.Short != "A CLI wrapper for the Spt benchmarking tool for S3-compatible storage." {
		t.Errorf("Expected rootCmd.Short to be 'A CLI wrapper for the Spt benchmarking tool for S3-compatible storage.', but got: %s", rootCmd.Short)
	}
}

func TestLoggingFlags(t *testing.T) {
	isTesting = true
	defer func() { isTesting = false }()

	tests := []struct {
		name           string
		args           []string
		expectedDebug  bool
		expectedLevel  string
		expectedFile   string
		expectedAppend bool
		expectError    bool
	}{
		{
			name:           "Default values",
			args:           []string{"--help"},
			expectedDebug:  false,
			expectedLevel:  "info",
			expectedFile:   "spt.log",
			expectedAppend: false,
			expectError:    false,
		},
		{
			name:           "Debug flag sets log level",
			args:           []string{"--debug", "--help"},
			expectedDebug:  true,
			expectedLevel:  "debug",
			expectedFile:   "spt.log",
			expectedAppend: false,
			expectError:    false,
		},
		{
			name:           "Custom log level",
			args:           []string{"--log-level", "warn", "--help"},
			expectedDebug:  false,
			expectedLevel:  "warn",
			expectedFile:   "spt.log",
			expectedAppend: false,
			expectError:    false,
		},
		{
			name:           "Custom log file",
			args:           []string{"--log-file", "/tmp/test.log", "--help"},
			expectedDebug:  false,
			expectedLevel:  "info",
			expectedFile:   "/tmp/test.log",
			expectedAppend: false,
			expectError:    false,
		},
		{
			name:           "Log append flag",
			args:           []string{"--log-append", "--help"},
			expectedDebug:  false,
			expectedLevel:  "info",
			expectedFile:   "spt.log",
			expectedAppend: true,
			expectError:    false,
		},
		{
			name:           "All flags combined",
			args:           []string{"--debug", "--log-level", "error", "--log-file", "custom.log", "--log-append", "--help"},
			expectedDebug:  true,
			expectedLevel:  "debug", // debug overrides log-level
			expectedFile:   "custom.log",
			expectedAppend: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags to defaults
			debug = false
			logLevel = "info"
			logFile = "spt.log"
			logAppend = false

			// Execute command
			b := new(bytes.Buffer)
			rootCmd.SetOut(b)
			rootCmd.SetErr(b)
			rootCmd.SetArgs(tt.args)

			err := rootCmd.Execute()
			if (err != nil) != tt.expectError {
				t.Fatalf("rootCmd.Execute() error = %v, expectError %v", err, tt.expectError)
			}

			// Check flag values
			if debug != tt.expectedDebug {
				t.Errorf("debug flag = %v, expected %v", debug, tt.expectedDebug)
			}

			// For debug flag test, the actual logLevel will be set during initialization
			if tt.expectedDebug && !strings.Contains(tt.args[0], "--log-level") {
				// Skip checking logLevel here as it's set in initializeLogger
			} else if logLevel != tt.expectedLevel {
				t.Errorf("logLevel = %v, expected %v", logLevel, tt.expectedLevel)
			}

			if logFile != tt.expectedFile {
				t.Errorf("logFile = %v, expected %v", logFile, tt.expectedFile)
			}

			if logAppend != tt.expectedAppend {
				t.Errorf("logAppend = %v, expected %v", logAppend, tt.expectedAppend)
			}
		})
	}
}

func TestInitializeLogger(t *testing.T) {
	tests := []struct {
		name        string
		setupDebug  bool
		setupLevel  string
		setupFile   string
		setupAppend bool
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid info level",
			setupDebug:  false,
			setupLevel:  "info",
			setupFile:   "/tmp/test_spt.log",
			setupAppend: false,
			expectError: false,
		},
		{
			name:        "Valid debug level",
			setupDebug:  false,
			setupLevel:  "debug",
			setupFile:   "/tmp/test_spt.log",
			setupAppend: false,
			expectError: false,
		},
		{
			name:        "Debug flag overrides level",
			setupDebug:  true,
			setupLevel:  "error",
			setupFile:   "/tmp/test_spt.log",
			setupAppend: false,
			expectError: false,
		},
		{
			name:        "Invalid log level",
			setupDebug:  false,
			setupLevel:  "invalid",
			setupFile:   "/tmp/test_spt.log",
			setupAppend: false,
			expectError: true,
			errorMsg:    "invalid log level",
		},
		{
			name:        "Append to existing file",
			setupDebug:  false,
			setupLevel:  "info",
			setupFile:   "/tmp/test_spt_append.log",
			setupAppend: true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test conditions
			debug = tt.setupDebug
			logLevel = tt.setupLevel
			logFile = tt.setupFile
			logAppend = tt.setupAppend

			// Clean up test file
			defer func() {
				if err := os.Remove(tt.setupFile); err != nil && !os.IsNotExist(err) {
					t.Logf("Failed to remove test file %s: %v", tt.setupFile, err)
				}
			}()

			// Initialize logger
			err := initializeLogger()

			if (err != nil) != tt.expectError {
				t.Fatalf("initializeLogger() error = %v, expectError %v", err, tt.expectError)
			}

			if err != nil && tt.expectError {
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing '%s', got: %v", tt.errorMsg, err)
				}
			}

			if err == nil {
				// Check that logger was created
				if Logger == nil {
					t.Error("Logger is nil after successful initialization")
				}

				// Check that log file was created
				if _, err := os.Stat(tt.setupFile); os.IsNotExist(err) {
					t.Errorf("Log file was not created: %s", tt.setupFile)
				}

				// If debug flag was set, verify logLevel was changed
				if tt.setupDebug {
					if logLevel != "debug" {
						t.Errorf("Debug flag did not set log level to debug")
					}
				}
			}
		})
	}
}

func TestExecuteCommandCodeHonorsParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	command := &cobra.Command{
		Use: "wait",
		RunE: func(cmd *cobra.Command, _ []string) error {
			<-cmd.Context().Done()
			return cmd.Context().Err()
		},
	}
	command.SetContext(parent)
	done := make(chan int, 1)
	go func() {
		done <- executeCommandCode(command)
	}()

	cancel()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("executeCommandCode = %d, want 1 after cancellation", code)
		}
	case <-time.After(time.Second):
		t.Fatal("executeCommandCode did not return after context cancellation")
	}
}
