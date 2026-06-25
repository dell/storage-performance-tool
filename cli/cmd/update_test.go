package cmd

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestExecuteCommandCodeMapsExitCodeError(t *testing.T) {
	cmd := &cobra.Command{
		Use: "test",
		RunE: func(*cobra.Command, []string) error {
			return &ExitCodeError{Code: 10}
		},
	}
	if got := executeCommandCode(cmd); got != 10 {
		t.Fatalf("executeCommandCode = %d, want 10", got)
	}
}

func TestUpdatePreRunDoesNotCreateDefaultLogFile(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	defer os.Chdir(oldWD)

	cmd := newUpdateCommand()
	if err := cmd.PersistentPreRunE(cmd, nil); err != nil {
		t.Fatalf("update pre-run returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "spt.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default spt.log was created or stat failed: %v", err)
	}
}

func TestUpdateRejectsCheckWithOutput(t *testing.T) {
	cmd := newUpdateCommand()
	cmd.SetArgs([]string{"--check", "--output", filepath.Join(t.TempDir(), "spt")})
	var errBuf bytes.Buffer
	cmd.SetErr(&errBuf)
	err := cmd.Execute()
	if err == nil {
		t.Fatal("update accepted --check with --output")
	}
	if !strings.Contains(errBuf.String(), "--check cannot be combined with --output") {
		t.Fatalf("stderr = %q", errBuf.String())
	}
}

func TestUpdateRejectsCheckWithYes(t *testing.T) {
	cmd := newUpdateCommand()
	cmd.SetArgs([]string{"--check", "--yes"})
	var errBuf bytes.Buffer
	cmd.SetErr(&errBuf)
	err := cmd.Execute()
	if err == nil {
		t.Fatal("update accepted --check with --yes")
	}
	if !strings.Contains(errBuf.String(), "--check cannot be combined with --yes") {
		t.Fatalf("stderr = %q", errBuf.String())
	}
}
