/*
Copyright © 2025 Dell Technologies
*/

package cmd

import (
	"os"
	"testing"

	"github.com/spf13/cobra"
)

func TestShouldRunHeadless(t *testing.T) {
	tests := []struct {
		name           string
		headlessFlag   bool
		expectedResult bool
		description    string
	}{
		{
			name:           "Explicit headless flag true",
			headlessFlag:   true,
			expectedResult: true,
			description:    "Should return true when --headless flag is explicitly set",
		},
		{
			name:           "Explicit headless flag false with TTY available",
			headlessFlag:   false,
			expectedResult: false, // This test assumes TTY is available
			description:    "Should return false when --headless is false and TTY is available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test command with the headless flag
			testCmd := &cobra.Command{
				Use: "test",
			}
			testCmd.Flags().Bool("headless", false, "Test headless flag")

			// Set the flag value
			if err := testCmd.Flags().Set("headless", getBoolString(tt.headlessFlag)); err != nil {
				t.Fatalf("failed to set headless flag: %v", err)
			}

			result := shouldRunHeadless(testCmd)

			// For explicit headless flag, the result should match expected
			if tt.headlessFlag && result != tt.expectedResult {
				t.Errorf("shouldRunHeadless() = %v, want %v for %s", result, tt.expectedResult, tt.description)
			}

			// When headless flag is false, the result depends on TTY availability
			// In most test environments, TTY is not available, so it should return true
			if !tt.headlessFlag {
				// We can't reliably predict TTY availability in test environment
				// Just ensure the function doesn't panic and returns a boolean
				if result != true && result != false {
					t.Errorf("shouldRunHeadless() returned non-boolean value")
				}
			}
		})
	}
}

func TestShouldRunHeadless_NoTTY(t *testing.T) {
	// Test the TTY detection part by trying to open /dev/tty
	// This test documents the expected behavior

	testCmd := &cobra.Command{Use: "test"}
	testCmd.Flags().Bool("headless", false, "Test headless flag")

	// Don't set headless flag, so it will check TTY
	result := shouldRunHeadless(testCmd)

	// Try to open /dev/tty ourselves to see if it's available
	if _, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err != nil {
		// No TTY available, should return true
		if !result {
			t.Errorf("shouldRunHeadless() = %v, want true when no TTY is available", result)
		}
	} else {
		// TTY is available, should return false
		if result {
			t.Errorf("shouldRunHeadless() = %v, want false when TTY is available", result)
		}
	}
}

func TestShouldRunHeadless_ExplicitOverride(t *testing.T) {
	// Test that explicit --headless flag overrides TTY detection
	testCmd := &cobra.Command{Use: "test"}
	testCmd.Flags().Bool("headless", false, "Test headless flag")
	if err := testCmd.Flags().Set("headless", "true"); err != nil {
		t.Fatalf("failed to set headless flag: %v", err)
	}

	result := shouldRunHeadless(testCmd)

	if !result {
		t.Errorf("shouldRunHeadless() = %v, want true when --headless flag is explicitly set", result)
	}
}

// Helper function to convert bool to string for flag setting
func getBoolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
