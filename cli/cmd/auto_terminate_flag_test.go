package cmd

import "testing"

// TestRunCmd_AutoTerminateFlag verifies that the run command registers
// the --auto-terminate-seconds flag with a default value of 0 and that
// it can be set to a positive integer.
func TestRunCmd_AutoTerminateFlag(t *testing.T) {
	// The global runCmd is initialized in this package's init().
	f := runCmd.Flags().Lookup("auto-terminate-seconds")
	if f == nil {
		t.Fatal("runCmd missing --auto-terminate-seconds flag")
	}

	if f.DefValue != "0" {
		t.Fatalf("default value = %q, want %q", f.DefValue, "0")
	}

	// Ensure we can set a non-zero value and read it back
	if err := runCmd.Flags().Set("auto-terminate-seconds", "5"); err != nil {
		t.Fatalf("failed setting flag: %v", err)
	}
	v, err := runCmd.Flags().GetInt("auto-terminate-seconds")
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if v != 5 {
		t.Fatalf("parsed value = %d, want %d", v, 5)
	}
}
