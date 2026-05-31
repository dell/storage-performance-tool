package cmd

import "testing"

func TestBuildHeadlessOptionsCarriesExpectedStepIDs(t *testing.T) {
	expectedStepIDs := []string{"mt-001-seed", "mt-002-read"}
	traceOpts := traceOptions{
		Path:   "/tmp/spt.trace.log",
		Append: true,
	}

	got := buildHeadlessOptions(traceOpts, true, "19090", 42, true, expectedStepIDs)

	if got.TraceFile != traceOpts.Path {
		t.Fatalf("TraceFile = %q, want %q", got.TraceFile, traceOpts.Path)
	}
	if !got.TraceAppend {
		t.Fatal("TraceAppend = false, want true")
	}
	if !got.Verbose {
		t.Fatal("Verbose = false, want true")
	}
	if got.APIPort != "19090" {
		t.Fatalf("APIPort = %q, want 19090", got.APIPort)
	}
	if got.AutoTerminateSeconds != 42 {
		t.Fatalf("AutoTerminateSeconds = %d, want 42", got.AutoTerminateSeconds)
	}
	if !got.DelegateNormalShutdown {
		t.Fatal("DelegateNormalShutdown = false, want true")
	}
	if len(got.ExpectedStepIDs) != len(expectedStepIDs) {
		t.Fatalf("ExpectedStepIDs length = %d, want %d", len(got.ExpectedStepIDs), len(expectedStepIDs))
	}
	for i, want := range expectedStepIDs {
		if got.ExpectedStepIDs[i] != want {
			t.Fatalf("ExpectedStepIDs[%d] = %q, want %q", i, got.ExpectedStepIDs[i], want)
		}
	}

	expectedStepIDs[1] = "mutated"
	if got.ExpectedStepIDs[1] != "mt-002-read" {
		t.Fatalf("ExpectedStepIDs was aliased to caller slice, got %q", got.ExpectedStepIDs[1])
	}
}
