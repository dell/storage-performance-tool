package cmd

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestResultsCommand_Run(t *testing.T) {
	// capture stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	// invoke the command's Run func directly
	if resultsCmd.Run == nil {
		t.Skip("resultsCmd has no Run (maybe refactored)")
	}
	resultsCmd.Run(nil, nil)

	_ = w.Close()
	out, _ := io.ReadAll(r)
	if !strings.Contains(string(out), "results called") {
		t.Fatalf("expected 'results called' in output, got: %q", string(out))
	}
}
