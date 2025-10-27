package cmd

import (
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/buildinfo"
	"github.com/spf13/cobra"
)

// versionCmd prints detailed build information. It complements the built-in
// `spt --version` flag so users can script against a stable format.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print build metadata",
	Run: func(cmd *cobra.Command, _ []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "spt version: %s\ncommit: %s\nbuilt: %s\n", buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate)
	},
}

func init() {
	versionCmd.PersistentPreRunE = rootCmd.PersistentPreRunE
	rootCmd.AddCommand(versionCmd)
}
