package cmd

import (
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

func getTestRootCmd() *cobra.Command {
	return GetTestRootCmd()
}

func executeCommand(t *testing.T, root *cobra.Command, args ...string) (*cobra.Command, string, error) {
	helper := NewTestHelper(t)
	return helper.ExecuteCommand(root, args...)
}

// isolateLoggingGlobals saves the package logging globals (debug, logLevel, logFile,
// logAppend), restores them when the test ends, and points logFile into the test's
// temporary directory. Tests that write these globals or execute the shared rootCmd
// call it so that no later test, in any order, inherits their values or a log path
// in a removed directory, and so that no log file is left in the package directory.
func isolateLoggingGlobals(t *testing.T) {
	t.Helper()
	oldDebug, oldLevel, oldFile, oldAppend := debug, logLevel, logFile, logAppend
	t.Cleanup(func() {
		debug, logLevel, logFile, logAppend = oldDebug, oldLevel, oldFile, oldAppend
	})
	logFile = filepath.Join(t.TempDir(), "spt.log")
}

// isCobraGeneratedCommand reports whether c is one of the commands Cobra adds to a
// root on its first Execute: the default help and completion commands. They inherit
// the root's PersistentPreRunE in production, so they define none of their own.
func isCobraGeneratedCommand(c *cobra.Command) bool {
	return c.Name() == "help" || c.Name() == "completion"
}
