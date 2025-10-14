package cmd

import (
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
